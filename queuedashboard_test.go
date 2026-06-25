package queuedashboard

import (
	"context"
	"errors"
	"testing"
)

func newTestService() *Service {
	return &Service{jobs: map[string]*Job{}, maxJobs: 100}
}

func TestWrapRecordsDoneAndFailed(t *testing.T) {
	s := newTestService()
	ok := s.Wrap("emails", "send", func(ctx context.Context, p any) error { return nil })
	bad := s.Wrap("emails", "send", func(ctx context.Context, p any) error { return errors.New("smtp down") })

	_ = ok(context.Background(), map[string]any{"to": "a@b.c"})
	_ = bad(context.Background(), nil)

	jobs := s.Jobs(Filter{})
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	// newest first → the failed job
	if jobs[0].Status != StatusFailed || jobs[0].Error == "" || jobs[0].Finished == nil {
		t.Errorf("failed job = %+v", jobs[0])
	}
	if jobs[1].Status != StatusDone {
		t.Errorf("done job = %+v", jobs[1])
	}
}

func TestRecordDispatchAndFilter(t *testing.T) {
	s := newTestService()
	s.RecordDispatch("reports", "nightly", map[string]any{"day": 1})
	s.RecordDispatch("emails", "send", nil)

	if got := s.Jobs(Filter{Queue: "reports"}); len(got) != 1 || got[0].Type != "nightly" {
		t.Fatalf("queue filter = %+v", got)
	}
	if got := s.Jobs(Filter{Status: StatusQueued}); len(got) != 2 {
		t.Fatalf("status filter = %d", len(got))
	}
	if got := s.Jobs(Filter{Limit: 1}); len(got) != 1 {
		t.Fatalf("limit = %d", len(got))
	}
}

func TestStatsAggregation(t *testing.T) {
	s := newTestService()
	done := s.Wrap("q", "t", func(ctx context.Context, p any) error { return nil })
	fail := s.Wrap("q", "t", func(ctx context.Context, p any) error { return errors.New("x") })
	done(context.Background(), nil)
	done(context.Background(), nil)
	fail(context.Background(), nil)

	stats := s.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(stats))
	}
	st := stats[0]
	if st.Total != 3 || st.Done != 2 || st.Failed != 1 {
		t.Errorf("counts = %+v", st)
	}
	if st.FailureRate < 0.33 || st.FailureRate > 0.34 {
		t.Errorf("failure rate = %v", st.FailureRate)
	}
}

func TestDeleteAndRetryGuard(t *testing.T) {
	s := newTestService()
	id := s.RecordDispatch("q", "t", nil)
	if _, ok := s.Get(id); !ok {
		t.Fatal("job should exist")
	}
	if !s.Delete(id) {
		t.Fatal("delete should succeed")
	}
	if _, ok := s.Get(id); ok {
		t.Fatal("job should be gone")
	}
	// Retry with no kernel/queue returns an error rather than panicking.
	id2 := s.RecordDispatch("q", "t", nil)
	if err := s.Retry(context.Background(), id2); err == nil {
		t.Fatal("retry without a queue should error")
	}
}

func TestBoundedRetention(t *testing.T) {
	s := &Service{jobs: map[string]*Job{}, maxJobs: 3}
	for i := 0; i < 5; i++ {
		s.RecordDispatch("q", "t", i)
	}
	if got := len(s.Jobs(Filter{})); got != 3 {
		t.Fatalf("retention bound not enforced: %d", got)
	}
}
