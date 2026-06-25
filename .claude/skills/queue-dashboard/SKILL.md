---
name: queue-dashboard
description: Monitor togo queue jobs — wrap handlers for lifecycle tracking, view throughput/failure stats, and retry or delete failed jobs via the queue-dashboard plugin.
---

# togo queue-dashboard

Use this skill to add queue observability to a togo app with `togo-framework/queue-dashboard`.

## Track jobs
Wrap handlers so jobs are recorded:
```go
qd, _ := queuedashboard.FromKernel(k)
k.Queue.Handle("emails", qd.Wrap("emails", "send", sendEmail))
```

## Inspect & act
- `qd.Jobs(Filter{Queue, Type, Status, Limit})` — recent jobs (newest first)
- `qd.Stats()` — per-queue total/done/failed/running/queued, failure_rate, avg_ms
- `qd.Retry(ctx, id)` — re-dispatch a failed job · `qd.Delete(id)`

## REST
`GET /api/queue-dashboard/{jobs,stats,dashboard}`, `POST /jobs/{id}/retry`, `DELETE /jobs/{id}`.

## Notes
- The togo Queue is Handle/Dispatch only, so tracking is via the `Wrap` handler decorator — wrap every handler you want visible.
- In-memory bounded store by default; a DB store can be plugged in.
- Retry needs `queue` installed + a worker running.
