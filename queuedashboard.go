// Package queuedashboard monitors togo queue jobs (Laravel Horizon / Sidekiq
// Web for togo): it records job lifecycle events, aggregates throughput /
// latency / failure stats, and exposes a REST + Go API to inspect, retry and
// delete jobs.
//
// The togo Queue interface is Handle/Dispatch only, so the dashboard observes
// jobs by wrapping their handlers:
//
//	qd, _ := queuedashboard.FromKernel(k)
//	k.Queue.Handle("emails", qd.Wrap("emails", "send", sendEmail))
//
// Wrapped handlers record running → done/failed with duration; RecordDispatch
// records an enqueue. Retry re-dispatches a recorded job over the kernel Queue.
package queuedashboard

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/togo-framework/togo"
)

// Job status values.
const (
	StatusQueued  = "queued"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
	StatusRetried = "retried"
)

// Job is a recorded queue job and its current state.
type Job struct {
	ID       string     `json:"id"`
	Queue    string     `json:"queue"`
	Type     string     `json:"type"`
	Status   string     `json:"status"`
	Attempts int        `json:"attempts"`
	Payload  any        `json:"payload,omitempty"`
	Queued   time.Time  `json:"queued"`
	Started  *time.Time `json:"started,omitempty"`
	Finished *time.Time `json:"finished,omitempty"`
	Duration int64      `json:"duration_ms,omitempty"`
	Error    string     `json:"error,omitempty"`
}

// Filter narrows a Jobs query.
type Filter struct {
	Queue  string
	Type   string
	Status string
	Since  time.Time
	Limit  int
}

// Service is the dashboard runtime stored on the kernel (k.Get("queue-dashboard")).
type Service struct {
	k       *togo.Kernel
	mu      sync.Mutex
	jobs    map[string]*Job
	order   []string // insertion order of job IDs (bounded)
	maxJobs int
	seq     int
}

func init() {
	togo.RegisterProviderFunc("queue-dashboard", togo.PriorityService, func(k *togo.Kernel) error {
		s := &Service{k: k, jobs: map[string]*Job{}, maxJobs: 2000}
		k.Set("queue-dashboard", s)
		if k.Router != nil {
			s.mountRoutes(k.Router)
		}
		return nil
	})
}

// FromKernel returns the dashboard Service registered on the kernel.
func FromKernel(k *togo.Kernel) (*Service, bool) {
	v, ok := k.Get("queue-dashboard")
	if !ok {
		return nil, false
	}
	s, ok := v.(*Service)
	return s, ok
}

func (s *Service) nextID() string {
	s.seq++
	return fmt.Sprintf("job_%d_%d", time.Now().UnixNano(), s.seq)
}

// put stores/updates a job (caller holds s.mu) with bounded retention.
func (s *Service) put(j *Job) {
	if _, exists := s.jobs[j.ID]; !exists {
		s.order = append(s.order, j.ID)
		for len(s.order) > s.maxJobs {
			old := s.order[0]
			s.order = s.order[1:]
			delete(s.jobs, old)
		}
	}
	s.jobs[j.ID] = j
}

// RecordDispatch records an enqueued job and returns its id.
func (s *Service) RecordDispatch(queue, jobType string, payload any) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := &Job{ID: s.nextID(), Queue: queue, Type: jobType, Status: StatusQueued, Payload: payload, Queued: time.Now()}
	s.put(j)
	return j.ID
}

// Wrap returns a QueueHandler that records running → done/failed with duration.
// Use it when registering a handler: k.Queue.Handle(name, qd.Wrap(name, type, h)).
func (s *Service) Wrap(queue, jobType string, h togo.QueueHandler) togo.QueueHandler {
	return func(ctx context.Context, payload any) error {
		start := time.Now()
		s.mu.Lock()
		j := &Job{ID: s.nextID(), Queue: queue, Type: jobType, Status: StatusRunning, Payload: payload, Queued: start, Started: &start, Attempts: 1}
		s.put(j)
		id := j.ID
		s.mu.Unlock()

		err := h(ctx, payload)
		fin := time.Now()

		s.mu.Lock()
		if jj := s.jobs[id]; jj != nil {
			jj.Finished = &fin
			jj.Duration = fin.Sub(start).Milliseconds()
			if err != nil {
				jj.Status = StatusFailed
				jj.Error = err.Error()
			} else {
				jj.Status = StatusDone
			}
		}
		s.mu.Unlock()
		return err
	}
}

// Jobs returns recorded jobs matching the filter, newest first.
func (s *Service) Jobs(f Filter) []Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Job, 0, len(s.order))
	for i := len(s.order) - 1; i >= 0; i-- {
		j := s.jobs[s.order[i]]
		if j == nil {
			continue
		}
		if f.Queue != "" && j.Queue != f.Queue {
			continue
		}
		if f.Type != "" && j.Type != f.Type {
			continue
		}
		if f.Status != "" && j.Status != f.Status {
			continue
		}
		if !f.Since.IsZero() && j.Queued.Before(f.Since) {
			continue
		}
		out = append(out, *j)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out
}

// Get returns a single recorded job.
func (s *Service) Get(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

// Delete removes a recorded job.
func (s *Service) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return false
	}
	delete(s.jobs, id)
	for i, x := range s.order {
		if x == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return true
}

// Retry re-dispatches a recorded job over the kernel Queue (best with failed jobs).
func (s *Service) Retry(ctx context.Context, id string) error {
	s.mu.Lock()
	j := s.jobs[id]
	if j == nil {
		s.mu.Unlock()
		return fmt.Errorf("queue-dashboard: no job %q", id)
	}
	queue, payload := j.Queue, j.Payload
	j.Status = StatusRetried
	j.Attempts++
	s.mu.Unlock()

	if s.k == nil || s.k.Queue == nil {
		return fmt.Errorf("queue-dashboard: no queue configured to retry on")
	}
	return s.k.Queue.Dispatch(ctx, queue, payload)
}

// QueueStat is aggregate health for a queue.
type QueueStat struct {
	Queue       string  `json:"queue"`
	Total       int     `json:"total"`
	Done        int     `json:"done"`
	Failed      int     `json:"failed"`
	Running     int     `json:"running"`
	Queued      int     `json:"queued"`
	FailureRate float64 `json:"failure_rate"`
	AvgMs       int64   `json:"avg_ms"`
}

// Stats aggregates per-queue health from the recorded jobs.
func (s *Service) Stats() []QueueStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	agg := map[string]*QueueStat{}
	durSum := map[string]int64{}
	durN := map[string]int64{}
	for _, j := range s.jobs {
		st := agg[j.Queue]
		if st == nil {
			st = &QueueStat{Queue: j.Queue}
			agg[j.Queue] = st
		}
		st.Total++
		switch j.Status {
		case StatusDone:
			st.Done++
		case StatusFailed:
			st.Failed++
		case StatusRunning:
			st.Running++
		case StatusQueued:
			st.Queued++
		}
		if j.Duration > 0 {
			durSum[j.Queue] += j.Duration
			durN[j.Queue]++
		}
	}
	out := make([]QueueStat, 0, len(agg))
	for q, st := range agg {
		if st.Total > 0 {
			st.FailureRate = float64(st.Failed) / float64(st.Total)
		}
		if durN[q] > 0 {
			st.AvgMs = durSum[q] / durN[q]
		}
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queue < out[j].Queue })
	return out
}

func (s *Service) mountRoutes(r chi.Router) {
	r.Route("/api/queue-dashboard", func(r chi.Router) {
		r.Get("/jobs", func(w http.ResponseWriter, req *http.Request) {
			f := Filter{
				Queue:  req.URL.Query().Get("queue"),
				Type:   req.URL.Query().Get("type"),
				Status: req.URL.Query().Get("status"),
				Limit:  atoiDefault(req.URL.Query().Get("limit"), 100),
			}
			writeJSON(w, 200, s.Jobs(f))
		})
		r.Get("/stats", func(w http.ResponseWriter, req *http.Request) {
			writeJSON(w, 200, s.Stats())
		})
		r.Get("/dashboard", func(w http.ResponseWriter, req *http.Request) {
			writeJSON(w, 200, map[string]any{"stats": s.Stats(), "recent": s.Jobs(Filter{Limit: 25})})
		})
		r.Post("/jobs/{id}/retry", func(w http.ResponseWriter, req *http.Request) {
			if err := s.Retry(req.Context(), chi.URLParam(req, "id")); err != nil {
				writeJSON(w, 400, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, 200, map[string]string{"status": "retried"})
		})
		r.Delete("/jobs/{id}", func(w http.ResponseWriter, req *http.Request) {
			if !s.Delete(chi.URLParam(req, "id")) {
				writeJSON(w, 404, map[string]string{"error": "not found"})
				return
			}
			writeJSON(w, 200, map[string]string{"status": "deleted"})
		})
	})
}
