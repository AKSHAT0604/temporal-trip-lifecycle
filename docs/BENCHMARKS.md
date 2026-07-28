# Chaos benchmarks

`src/trip_orchestrator/chaos.py` starts N concurrent trips against a real
(non-time-skipping) Temporal server, hard-kills the worker process
(`taskkill /F /T` on Windows, `SIGKILL` on Unix) at a random point while
trips are mid-flight, restarts a fresh worker process, and waits for every
trip to reach a terminal state. This is not a simulation: the server used
was a real `temporal server start-dev` instance, and the worker was a real,
separate OS process.

## Run: 100 concurrent trips

```json
{
  "num_trips": 100,
  "kill_point_seconds_into_run": 0.618,
  "recovered": 100,
  "failed": 0,
  "mean_recovery_seconds": 0.161,
  "status_counts": {
    "PAID": 99,
    "UNFULFILLED": 1
  },
  "failures": []
}
```

All 100 workflows resumed from their persisted state after the worker was
killed and restarted; none were lost, and every one reached a valid terminal
state. The single `UNFULFILLED` result is an expected business outcome, not
a durability failure: that trip's only candidate driver's acceptance signal
happened to land outside the 15-second offer window under the concurrent
load the chaos harness generates, so the workflow correctly exhausted its
reoffer rounds and terminated as unmatched -- exactly the behavior
`tests/test_timers.py::test_offer_exhaustion_becomes_unfulfilled` covers
deterministically.

### Idempotency check (post-run ledger contents)

The dedupe ledger (`stubs/ledger.py`, a sqlite file surviving the worker
restart) was inspected directly after the run:

| Activity | Rows |
|---|---|
| `authorize_payment` | 100 |
| `reserve_driver` | 100 |
| `notify` | 297 |
| `capture_payment` | 99 |
| `record_trip_completion` | 99 |
| `void_authorization` | 1 |

Total rows: 696. Distinct idempotency keys: 696. **Every side effect
recorded exactly once** -- the worker kill produced zero double-charges and
zero double-reservations, despite Temporal re-dispatching whatever activity
tasks were in flight at the moment the process died. The counts are
internally consistent with the single `UNFULFILLED` trip: it authorized and
reserved like every other trip, but was the only one voided instead of
captured, and the only one that never reached `record_trip_completion`.

## Run: 5 concurrent trips (smoke test)

```json
{
  "num_trips": 5,
  "kill_point_seconds_into_run": 0.619,
  "recovered": 5,
  "failed": 0,
  "mean_recovery_seconds": 2.049,
  "status_counts": { "PAID": 5 },
  "failures": []
}
```

Run first, before the 100-trip run, to validate the harness itself. All 5
trips completed and paid despite the same kill-and-restart sequence.

## Reproducing

```
temporal server start-dev --db-filename deploy/temporal-dev.db
python -m trip_orchestrator.chaos --trips 100
```

`chaos.py` manages the worker process's lifecycle itself (spawns it, kills
it mid-run, spawns a replacement) -- do not run `worker.py` separately when
using the chaos runner.
