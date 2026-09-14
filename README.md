<div align="center">
  <picture><source media="(prefers-color-scheme: dark)" srcset=".github/assets/togo-mark-dark.svg" /><img src=".github/assets/togo-mark.svg" alt="ToGO" height="64" /></picture>
  <h1>togo-framework/queue-dashboard</h1>
  <p>
    <a href="https://to-go.dev/marketplace"><img src="https://img.shields.io/badge/marketplace-to--go.dev-1F8A99" alt="marketplace" /></a>
    <a href="https://pkg.go.dev/github.com/togo-framework/queue-dashboard"><img src="https://pkg.go.dev/badge/github.com/togo-framework/queue-dashboard.svg" alt="pkg.go.dev" /></a>
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT" />
  </p>
  <p><strong>Queue & jobs monitoring for <a href="https://to-go.dev">togo</a> — Horizon / Sidekiq-Web for your queue.</strong></p>
</div>

## Install

```bash
togo install togo-framework/queue-dashboard
```

Observe the togo queue: track every job's lifecycle (queued → running → done/failed/retried), aggregate **throughput, latency and failure stats** per queue, and **retry** or **delete** failed jobs — over a REST + Go API.

## How it works

The togo `Queue` interface is `Handle`/`Dispatch` only, so the dashboard observes jobs by **wrapping their handlers**. Wrapped handlers record start/finish, duration and errors automatically.

## Usage

```go
import (
	queuedashboard "github.com/togo-framework/queue-dashboard"
	"github.com/togo-framework/togo"
)

func register(k *togo.Kernel) {
	qd, _ := queuedashboard.FromKernel(k)

	// Wrap handlers so jobs are tracked.
	k.Queue.Handle("emails", qd.Wrap("emails", "send", sendEmail))

	// (optional) record an enqueue for queued-depth visibility:
	id := qd.RecordDispatch("emails", "send", payload)
	_ = id
}
```

Inspect from Go:

```go
qd.Jobs(queuedashboard.Filter{Queue: "emails", Status: queuedashboard.StatusFailed, Limit: 50})
qd.Stats()                       // []QueueStat{total, done, failed, failure_rate, avg_ms, ...}
qd.Retry(ctx, jobID)             // re-dispatch a failed job
qd.Delete(jobID)
```

## REST API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/queue-dashboard/jobs?queue=&type=&status=&limit=` | list recorded jobs |
| GET | `/api/queue-dashboard/stats` | per-queue health (throughput, failure rate, avg ms) |
| GET | `/api/queue-dashboard/dashboard` | stats + recent jobs (one call) |
| POST | `/api/queue-dashboard/jobs/{id}/retry` | re-dispatch a job |
| DELETE | `/api/queue-dashboard/jobs/{id}` | remove a recorded job |

## Configuration

No required env. Records are kept in a bounded in-memory ring (newest 2000) with a pluggable store interface for a DB-backed implementation. Pairs with `togo install togo-framework/queue` + a worker.

---

<div align="center">
  <h3>Premium sponsors</h3>
  <p>
    <a href="https://id8media.com"><strong>ID8 Media</strong></a> &nbsp;·&nbsp;
    <a href="https://one-studio.co"><strong>One Studio</strong></a>
  </p>
  <p><sub>Support togo — <a href="https://github.com/sponsors/fadymondy">become a sponsor</a>.</sub></p>
</div>
