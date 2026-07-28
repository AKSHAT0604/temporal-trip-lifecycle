# Architecture

## Overview

`TripWorkflow` is the durable state machine for a single ride. It runs on a
Temporal worker, is orchestrated entirely through `await`-style Python
control flow, and its execution history -- every activity call, every signal,
every timer -- is persisted by the Temporal server, not held in the worker's
memory. That's what lets a worker process be killed mid-trip and resumed by
a different worker with no lost or duplicated state (see `docs/BENCHMARKS.md`).

## State progression

```
REQUESTED -> MATCHING -> MATCHED -> DRIVER_ARRIVED -> IN_PROGRESS -> COMPLETED -> PAID
                 |            |            |               |
                 v            v            v               v
            UNFULFILLED   CANCELLED    CANCELLED       PAYMENT_FAILED
                          (no fee)     (with fee)      (retry, then manual)
```

`MATCHING` includes an internal reoffer loop (up to 5 rounds, each racing a
15-second offer timer against the `DriverAccepted` signal). `MATCHED` can
also loop back to `MATCHING` if the driver cancels -- see
`docs/FAILURE_MODES.md` for every branch.

## Components

```
src/trip_orchestrator/
  workflow/
    trip_workflow.py   TripWorkflow: signals, query, timers, the saga itself
    states.py          TripStatus, TripRequest, TripState -- the wire types
    saga.py            CompensationStack: push on success, unwind on failure
  activities/
    payment.py          authorize / void / capture / refund
    driver.py            reserve / release
    notify.py             rider / driver push notifications
    records.py             record_trip_completion
    contracts.py          input/output dataclasses shared by workflow + activities
    errors.py               translates stub exceptions into retryable/non-retryable ApplicationErrors
  stubs/                 fake downstream services (payment gateway, dispatch, notifier, trip store)
    ledger.py             sqlite-backed idempotency dedupe store, shared by every stub
  worker.py             registers TripWorkflow + activities, connects to Temporal
  api/main.py           FastAPI: start a trip, send it signals, query its state
  chaos.py              starts N trips, kills the worker mid-flight, restarts it, reports results
```

## Why activities are classes, not functions

Each activity module defines a class (`PaymentActivities`, `DriverActivities`,
...) whose `__init__` takes the fake downstream service it wraps
(`PaymentGateway`, `DispatchService`, ...). `worker.py` constructs one
instance of each and registers its bound methods with the `Worker`. This is
the Temporal Python SDK's supported pattern for dependency injection: tests
construct their own stub instances (see `tests/conftest.py`) and hand them to
the same activity classes, so a test's fake payment gateway is exercised by
exactly the same activity code the real worker runs -- no parallel
test-only code path to keep in sync.

## Why the stubs share one sqlite-backed ledger

Every fake downstream (`stubs/*.py`) records its successful side effects in
`stubs/ledger.py`, a small sqlite table keyed by idempotency key. This
matters for one specific reason: sqlite writes go to disk, so the ledger
survives a killed worker process. If the stubs only tracked state in memory,
restarting a worker after `chaos.py` kills it would silently reset the
idempotency bookkeeping and the chaos run would prove nothing. The ledger is
what makes "no duplicated side effects across a real process kill" a
checkable claim instead of an assumption.

## Observability: query without a database read

`GetTripState` (`workflow/trip_workflow.py`) returns the workflow's current
status, driver, elapsed time, and failure reason straight out of the
workflow's own in-memory fields -- there is no database, no separate trips
table, nothing to query but the workflow itself. This is a genuinely
surprising capability if you haven't used an engine that supports it:
`api/main.py`'s `GET /trips/{id}` is a five-line handler that calls
`handle.query(TripWorkflow.get_trip_state)` and nothing else.
