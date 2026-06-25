# queue-dashboard — usage

## Track jobs
Wrap each queue handler so the dashboard records its lifecycle:

```go
qd, _ := queuedashboard.FromKernel(k)
k.Queue.Handle("emails", qd.Wrap("emails", "send", sendEmail))
```

A wrapped handler records `running` on entry and `done`/`failed` on exit, with
duration and any error. Use `qd.RecordDispatch(queue, type, payload)` to also
record enqueues (queued depth).

## Inspect
```go
qd.Jobs(queuedashboard.Filter{Queue: "emails", Status: queuedashboard.StatusFailed, Limit: 50})
qd.Stats()           // per-queue: total/done/failed/running/queued, failure_rate, avg_ms
qd.Get(id)
```

## Act
```go
qd.Retry(ctx, id)    // re-dispatch over the kernel Queue
qd.Delete(id)
```

## REST
- `GET  /api/queue-dashboard/jobs?queue=&type=&status=&limit=`
- `GET  /api/queue-dashboard/stats`
- `GET  /api/queue-dashboard/dashboard` (stats + recent)
- `POST /api/queue-dashboard/jobs/{id}/retry`
- `DELETE /api/queue-dashboard/jobs/{id}`

## Notes
- Records are kept in a bounded in-memory ring (newest 2000); swap in a DB-backed store via the store interface.
- Retry requires a configured queue (`togo install togo-framework/queue` + a worker).
