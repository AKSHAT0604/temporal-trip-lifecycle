# Trip Lifecycle Orchestration

A ride's lifecycle -- request, match, arrival, in-trip, completion, payment --
modeled as a single durable [Temporal](https://temporal.io) workflow instead
of a status column plus a cron job. Every side-effecting step (payment
authorization, driver reservation) has a documented, tested rollback; the
workflow survives its worker process being killed mid-trip with no lost or
duplicated state; and its current state is readable at any time through a
Temporal query, with no database involved.

## Why Temporal, not Cadence

Temporal was created by Cadence's original authors and shares its
architecture and programming model, but has a materially better-maintained
SDK ecosystem and documentation. See [docs/DECISIONS.md](docs/DECISIONS.md#2-why-temporal-rather-than-cadence-given-cadence-is-ubers-original)
for the full reasoning -- knowing *why* Cadence exists and choosing its
better-supported successor is the point, not just picking the popular option.

## What this demonstrates

| | |
|---|---|
| **Durable execution** | Workflow state is persisted by the Temporal server, not held in worker memory. Killing the worker process mid-trip loses nothing. |
| **Saga-style compensation** | 9 distinct failure paths, each unwinding a LIFO compensation stack rather than hand-coded per-path rollback logic. See [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md). |
| **Idempotent activities** | Every activity carries an idempotency key; a fake downstream's dedupe ledger proves duplicate execution produces no double side effect. |
| **Per-activity retry policies** | A failed notification retries aggressively; a failed payment capture retries conservatively then surfaces `PAYMENT_FAILED` for manual reconciliation; non-retryable failures (card declined) stop immediately instead of walking a backoff schedule. |
| **Time-skipping tests** | A 15-second offer timeout or a 10-minute arrival timeout is verified in milliseconds of wall-clock time, using Temporal's test environment. |
| **Verified crash recovery** | `chaos.py` hard-kills a real worker process mid-run against a real Temporal server. Results: [docs/BENCHMARKS.md](docs/BENCHMARKS.md). |

## State machine

```
REQUESTED -> MATCHING -> MATCHED -> DRIVER_ARRIVED -> IN_PROGRESS -> COMPLETED -> PAID
                 |            |            |               |
                 v            v            v               v
            UNFULFILLED   CANCELLED    CANCELLED       PAYMENT_FAILED
                          (no fee)     (with fee)      (retry, then manual)
```

Full architecture writeup: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Project layout

```
src/trip_orchestrator/
  workflow/       TripWorkflow: signals, query, timers, the saga
  activities/     payment, driver, notification, and record-keeping activities
  stubs/          fake downstream services with injectable failure + a shared idempotency ledger
  worker.py       registers the workflow and activities, connects to Temporal
  api/main.py     FastAPI surface: start a trip, send signals, query state
  chaos.py        crash-recovery benchmark runner
docs/             architecture, decisions, failure modes, chaos benchmarks
tests/            time-skipping tests for every phase
deploy/           docker-compose (Temporal server + UI + Postgres)
```

## Running it

Requires Python 3.11+ and either the [Temporal CLI](https://docs.temporal.io/cli)
or Docker.

```bash
python -m venv .venv
.venv/Scripts/activate            # .venv/bin/activate on macOS/Linux
pip install -e ".[dev]"
```

Start a local Temporal server (either works):

```bash
temporal server start-dev                      # single binary, includes the Web UI at localhost:8233
# or
docker compose -f deploy/docker-compose.yml up # UI at localhost:8080
```

Run the worker and the API in separate terminals:

```bash
python -m trip_orchestrator.worker
uvicorn trip_orchestrator.api.main:app --reload
```

Drive a trip end to end:

```bash
curl -X POST localhost:8000/trips -H "content-type: application/json" -d '{
  "trip_id": "trip-1", "rider_id": "rider-1",
  "pickup": "123 Main St", "dropoff": "456 Oak Ave",
  "fare_estimate_cents": 2500
}'

curl -X POST localhost:8000/trips/trip-1/signals/driver-accepted -d '{"driver_id": "driver-1"}'
curl -X POST localhost:8000/trips/trip-1/signals/driver-arrived
curl -X POST localhost:8000/trips/trip-1/signals/trip-started
curl -X POST localhost:8000/trips/trip-1/signals/trip-completed -d '{"distance_km": 5.2, "duration_minutes": 12}'

curl localhost:8000/trips/trip-1   # GetTripState query -- no database involved
```

Watch every activity and signal land, in order, in the Temporal Web UI.

## Tests

```bash
pytest
```

Runs entirely against Temporal's time-skipping test environment -- no server
needed. 18 tests cover the happy path, every timer and reoffer path, all 9
documented failure modes, and idempotency/retry-policy behavior, in about
three seconds of wall-clock time.

## Chaos benchmark

```bash
temporal server start-dev --db-filename deploy/temporal-dev.db
python -m trip_orchestrator.chaos --trips 100
```

`chaos.py` starts N concurrent trips, hard-kills the worker process at a
random point mid-flight, restarts it, and confirms every trip reaches a
valid terminal state with no lost or duplicated side effects. Recorded
results (100/100 recovered, zero duplicate side effects, verified against
the fake payment gateway's dedupe ledger): [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) -- components and design rationale
- [docs/DECISIONS.md](docs/DECISIONS.md) -- durable execution vs. cron, saga vs. 2PC, signals vs. queries, and more
- [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md) -- every failure path, its detection, its compensation, its terminal state
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) -- chaos test results against a real Temporal server
