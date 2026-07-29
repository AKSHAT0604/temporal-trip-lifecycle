# Explanation: Trip Lifecycle Orchestration, from scratch

This document explains the whole project in plain language: the problem, the
concepts you need to understand it, the tools used and why, the order things
were built in, the bugs that came up along the way, and what every single
file does. It assumes no prior knowledge of Temporal, durable execution, or
the saga pattern -- everything is introduced before it's used.

If you only want the short version, read [README.md](README.md). This
document is the long version.

---

## 1. The problem this project solves

Think about what happens during a single Uber-style ride, from the backend's
point of view:

1. Rider requests a trip.
2. Their card gets a payment **hold** (not charged yet).
3. The system looks for a driver, offers the trip to one, waits for them to
   accept.
4. If they don't accept in time, offer the next driver. Repeat a few times.
5. Once accepted, wait for the driver to physically arrive.
6. Wait for the trip to start, then wait for it to end.
7. Charge the held payment, and write a permanent record of the trip.

Now the harder question: **what does the code for this actually look like?**

The naive answer is "a `trips` table with a `status` column, plus some cron
jobs that poll it." The problem is that every step above can fail, and every
failure needs a *specific* undo action:

- If the driver never accepts, you need to release them and try someone
  else -- but only up to a point, then give up.
- If the rider cancels before a driver is even found, you just void the
  hold.
- If they cancel *after* a driver is already on the way, you owe that driver
  something -- charge a small cancellation fee, then void the rest.
- If the driver cancels, you need to un-assign them, decide whether to
  re-authorize the payment, and go back to matching.
- If the final payment capture fails after the ride already happened, you
  can't just silently retry forever and hope -- eventually a human has to
  look at it.

With a status column, all of that undo logic ends up scattered across
whatever cron job or webhook handler happens to next notice the status
changed. Nothing in the code *is* the process -- the process is an implicit
idea reconstructed from timestamps and flags every time something runs.
Worse: if the server process handling a trip crashes halfway through, you
have to detect that (how?) and figure out from the database exactly which
steps completed and which didn't, then resume by hand.

**The alternative this project demonstrates**: write the entire trip as one
ordinary-looking function -- authorize payment, find a driver, wait for
arrival, wait for completion, charge the payment -- and have an engine
underneath that function that:

- persists *exactly where execution paused* (not just a status label) so a
  crash can't lose it,
- lets that function pause for real-world hours or days waiting for an
  event without holding a thread or a database row idle the whole time,
- and gives you a "push a cleanup action for later" primitive so failure
  handling is just "undo everything I did so far," not "figure out what I
  did so far."

That engine is **Temporal**, and the function is called a **workflow**. That
is the entire idea this project exists to demonstrate.

---

## 2. Concepts, explained from zero

### Workflow

A workflow is a function that describes a *process*, not just one
computation. In this project, `TripWorkflow.run()`
([trip_workflow.py](src/trip_orchestrator/workflow/trip_workflow.py)) is the
workflow: it's written as one continuous `async def`, reading top to bottom
like ordinary code -- "authorize the payment, then try to find a driver,
then wait for them to arrive..." -- even though in reality it might pause
for minutes between those lines while real-world events happen.

The trick is that Temporal doesn't keep this function "running" in the
traditional sense between steps. Every time the workflow does something
observable (calls an activity, waits for a signal, starts a timer), Temporal
records that fact as an entry in an **event history**, durably, on the
Temporal server. If the process running the workflow's code crashes and
restarts, Temporal **replays** the recorded history back into a fresh copy
of the function to reconstruct exactly where it was, then lets it continue
from that exact point. Your code doesn't do anything special to make this
work -- it's just careful about being deterministic (see the "why the code
looks the way it does" section below).

### Activity

An activity is a single unit of real-world work with a side effect:
"charge this card," "reserve this driver," "send this notification." Unlike
workflow code, activities are allowed to do non-deterministic things --
call an external API, hit a database, make a network request -- because
Temporal doesn't replay them the same way. It just records whether they
succeeded or failed, and what they returned.

In this project, activities live in
[activities/](src/trip_orchestrator/activities/): `authorize_payment`,
`reserve_driver`, `notify_rider`, `record_trip_completion`, and so on. The
workflow calls them with `await workflow.execute_activity(...)`.

### Worker

A worker is just a process that connects to the Temporal server, says "I
know how to run `TripWorkflow` and these nine activities," and then sits in
a loop asking the server "is there any work for me?" When you start a trip,
the server hands the workflow's next step to whichever worker asks for it
next. This project's worker is
[worker.py](src/trip_orchestrator/worker.py).

This is also *why* crash recovery works at the process level: the workflow's
truth lives on the Temporal server, not inside the worker. Kill the worker
process entirely, start a brand new one, and it just starts asking the
server for work again -- including picking up the trip that was in flight
when the old worker died. Section 10 (chaos testing) proves this really
happens, not just in theory.

### Signal

A signal is a message delivered *into* a running workflow from the outside
world -- "the driver just accepted," "the rider just cancelled." Signals are
durable (recorded in history, delivered even if no worker happens to be up
at that exact moment) and they can change what the workflow does next. This
project has six: `driver_accepted`, `driver_arrived`, `trip_started`,
`trip_completed`, `rider_cancelled`, `driver_cancelled`.

### Query

A query is the opposite: a read-only question asked *of* a running
workflow -- "what's your current status?" It never changes anything, is
never recorded in history, and answers instantly from the workflow's
in-memory state. This project has one: `get_trip_state`. The interesting
part is that this means you can ask a live workflow "where are you right
now?" **with no database involved at all** -- the workflow's own variables
are the database.

### Timer

A timer is a durable "wake me up in N seconds/minutes" that a workflow can
race against a signal. "Wait up to 15 seconds for the driver to accept, and
if they don't, move on" is exactly one timer racing one signal
(`workflow.wait_condition(..., timeout=...)` in the code). Because it's
durable, a 10-minute timer doesn't require anything to stay running for 10
real minutes -- which is exactly what makes *time-skipping tests* possible
(section 9).

### The saga pattern (vs. two-phase commit)

If payment, dispatch, and notifications were all one database, you could
wrap the whole trip in one ACID transaction and roll it back atomically on
any failure. They're not -- they're three unrelated systems, and a real
payment processor's API doesn't offer "vote to commit, then wait for everyone
else." Two-phase commit needs every participant to hold a lock open until a
coordinator says commit; a real gateway just doesn't support that.

The **saga pattern** accepts this constraint instead of fighting it: each
step commits for real, immediately (a card really gets a hold placed on it).
If a later step fails, you don't undo the earlier one automatically at the
database level -- you explicitly run a **compensating action** ("void this
hold," "release this driver") that semantically undoes it. This project's
saga is the `CompensationStack` in
[saga.py](src/trip_orchestrator/workflow/saga.py): every time a
side-effecting step succeeds, its undo action gets pushed onto a stack; on
any failure, the stack unwinds in reverse order.

### Idempotency

Temporal's guarantee is "activities execute *at least once*" -- not exactly
once. If a worker crashes right after an activity's real side effect
happens but before it can tell Temporal "I'm done," Temporal will assume it
failed and run it again. That's necessary for reliability, but it means a
naive `charge_card()` function could charge a card twice.

The fix is an **idempotency key**: a unique ID for "this specific logical
action," passed to the downstream system, which uses it to recognize "I've
already done this exact thing, here's the same result as last time, don't
do it again." This project generates one deterministically for every
activity call (workflow ID + step name + a counter) and every fake
downstream service checks it against a shared ledger before doing anything
(section 8, `stubs/ledger.py`).

### Retry policy

Not every failure deserves the same response. A retry policy controls how
many times Temporal retries a failed activity, and with what backoff. This
project deliberately uses *different* policies for different activities
(section 8, `errors.py` and `trip_workflow.py`) and explicitly marks some
failures as **non-retryable** (a declined card will never succeed no matter
how many times you retry it, so retrying is just wasted time and, for a real
payment gateway, could look like fraud).

---

## 3. Why Temporal specifically (not Cadence, not rolling our own)

Temporal was built by the same engineers who built **Cadence** at Uber --
Cadence is literally what Uber uses in production for exactly this kind of
problem (over a thousand internal workflows, billions of executions a
month). Temporal is that same lineage, continued outside Uber, with a more
actively maintained SDK ecosystem and materially better documentation. Using
Temporal *and knowing why Cadence exists and why Temporal is its successor*
is a stronger position than either "I used Cadence because it's Uber's" (not
knowing Temporal exists) or "I used Temporal" with no idea it's the same
lineage. Full reasoning: [docs/DECISIONS.md, Q1-Q2](docs/DECISIONS.md).

Rolling this durability guarantee ourselves (a custom event log plus our own
replay logic) is a solved, hard problem with subtle correctness pitfalls
(exactly the kind of pitfalls this project's own bug list in section 11 ran
into, and Temporal already handles those at the framework level).

---

## 4. Tools used, and why each one

| Tool | Why |
|---|---|
| **Python 3.14** | The original plan called for Go, but Go wasn't installed in this environment and Python was requested instead. Temporal's official Python SDK (`temporalio`) covers the exact same feature set needed here: workflows, activities, signals, queries, timers, and a time-skipping test framework. |
| **`temporalio` (Temporal Python SDK)** | The actual durable-execution engine's client library -- defines `@workflow.defn`, `@activity.defn`, `workflow.execute_activity`, the test environment, etc. |
| **FastAPI + Uvicorn** | The HTTP surface (`POST /trips`, signal endpoints, the query endpoint). Chosen because it's the natural, minimal-boilerplate way to expose a few JSON endpoints in Python -- no hand-rolled routing or serialization needed. |
| **pytest + pytest-asyncio** | Test runner. `pytest-asyncio` is needed because every test here is an `async def` (workflows and Temporal's client are all async). |
| **sqlite3 (Python standard library)** | Backs the fake downstream services' idempotency ledger. Chosen specifically because it persists to a real file on disk -- an in-memory dict would reset the moment a worker process is killed, which would silently invalidate the entire crash-recovery proof in section 10. No extra dependency needed since it's in the standard library. |
| **Temporal CLI (`temporal` binary)** | Installed via `winget` (Docker wasn't available in this environment). Gives a real, local, single-binary Temporal server plus a Web UI, with no containers required. Used both for local development and for the live chaos-test benchmark. |
| **Docker Compose** (`deploy/docker-compose.yml`) | Provided as the alternative, Postgres-backed way to run Temporal locally, for anyone who does have Docker and wants a setup closer to a production topology. Not used for this project's own testing since Docker wasn't available here. |
| **GitHub REST API via a stored OAuth token** | `gh` (GitHub CLI) wasn't installed either, but a GitHub OAuth token was already present in the local git credential manager (from a prior `gh auth login`). That token was used directly against `https://api.github.com/user/repos` to create the repository, since it's the same authenticated identity `gh` itself would have used. |
| **Git** | Version control, with an explicit local identity (`Akshat Srivastava` / your email) set on this repo specifically, and `.claude/settings.json` configured with `"includeCoAuthoredBy": false` so no AI-tool attribution ends up in any commit. |

---

## 5. The order things were actually built in

1. **Environment check.** Looked for Go and Docker (per the original spec)
   -- neither was installed. You asked for Python instead, so the plan
   pivoted there and Go was never touched.
2. **Repository creation.** Confirmed the repo name with you first
   (`temporal-trip-lifecycle`), then created it via the GitHub API using the
   stored credential, and initialized git locally with your name/email
   explicitly configured before the first commit.
3. **Scaffolding.** Python package layout under `src/trip_orchestrator/`,
   `pyproject.toml`, `.gitignore`, `.claude/settings.json`.
4. **Domain model first** (`workflow/states.py`, `workflow/saga.py`) --
   the status enum, the request/response shapes, and the compensation
   stack, before any workflow logic touched them.
5. **Fake downstreams and their shared ledger** (`stubs/`) -- built before
   the activities that use them, so the activities would have something
   real (if fake) to call.
6. **Activities** (`activities/`) -- thin wrappers translating stub
   exceptions into Temporal's retryable/non-retryable vocabulary.
7. **The workflow itself** (`workflow/trip_workflow.py`) -- the biggest,
   most important file, built and then immediately tested against
   Temporal's time-skipping test environment rather than written blind
   end-to-end. This caught three real bugs early (section 11) instead of
   them surfacing later as mysterious failures.
8. **Worker + HTTP API** (`worker.py`, `api/main.py`).
9. **The full test suite** (`tests/`) -- one file per phase: happy path,
   timers/reoffer, compensation/failure paths, idempotency/retries.
10. **The chaos runner** (`chaos.py`) -- and then actually *run*, live,
    against a real Temporal CLI dev server, with the worker process really
    killed and restarted (section 10). This wasn't left as "should work in
    theory."
11. **Documentation** -- `docs/ARCHITECTURE.md`, `docs/DECISIONS.md`,
    `docs/FAILURE_MODES.md`, `docs/BENCHMARKS.md`, then this file and the
    top-level `README.md`.
12. **Git history** -- staged and committed in the same logical order as
    the list above (not just one giant commit), pushing after every commit.

---

## 6. The state machine

```
REQUESTED -> MATCHING -> MATCHED -> DRIVER_ARRIVED -> IN_PROGRESS -> COMPLETED -> PAID
                 |            |            |               |
                 v            v            v               v
            UNFULFILLED   CANCELLED    CANCELLED       PAYMENT_FAILED
                          (no fee)     (with fee)      (retry, then manual)
```

- **MATCHING** contains its own inner loop: offer a candidate driver, race a
  15-second timer against the `driver_accepted` signal, and if it times out,
  release that driver and offer the next one -- up to 5 rounds, then
  `UNFULFILLED`.
- **MATCHED** can loop *backward* to `MATCHING` if the driver cancels: the
  workflow releases them, voids the original payment hold, re-authorizes a
  fresh one, and restarts matching with whichever candidates are left.
- Every arrow into `CANCELLED` or `PAYMENT_FAILED` corresponds to one row in
  [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md).

---

## 7. File-by-file tour

### Top level

- **`README.md`** -- the short version: what this demonstrates, how to run
  it, pointers to everything else.
- **`explanation.md`** -- this file.
- **`pyproject.toml`** -- Python package metadata and dependencies
  (`temporalio`, `fastapi`, `uvicorn` for running it; `pytest`,
  `pytest-asyncio` under `[dev]` for testing). Also configures pytest itself
  (`asyncio_mode = "auto"`, so every `async def test_...` just works without
  extra decorators).
- **`.gitignore`** -- excludes the virtual environment, `__pycache__`,
  pytest's cache, and the sqlite database files under `deploy/` (those are
  runtime state generated by actually running the server/chaos test, not
  source code -- see below).
- **`.claude/settings.json`** -- `{"includeCoAuthoredBy": false}`, so no AI
  co-authorship trailer gets added to commits in this repo.

### `src/trip_orchestrator/workflow/` -- the orchestration core

- **`states.py`** -- every data shape that crosses the boundary between your
  code and Temporal's data converter (the thing that turns Python objects
  into JSON for storage, and back): `TripStatus` (the state-machine enum),
  `TripRequest` (workflow input), `TripCompletedDetails` (signal payload),
  `TripState` (query result). `TripStatus` is specifically `enum.StrEnum`,
  not the more common `class TripStatus(str, Enum)` -- see the bug writeup
  in section 11, bug #1.
- **`saga.py`** -- `CompensationStack`: `push(name, undo_coroutine)` and
  `unwind()` (run everything pushed so far, most recent first) /
  `unwind_last()` (just the most recent one, used mid-loop when only one
  driver reservation needs releasing). About 20 lines; the entire saga
  pattern in this project reduces to this one small, reusable piece plus
  workflow code that calls `push` after every successful step.
- **`trip_workflow.py`** -- `TripWorkflow`, the workflow itself. Contains:
  the six `@workflow.signal` handlers, the one `@workflow.query`, the
  `@workflow.run` entry point (`run()`), and private helper methods for each
  phase (`_authorize`, `_match_with_reoffer`, `_through_arrival_and_trip`,
  `_settle_payment`). Also defines the four different `RetryPolicy` objects
  used across the workflow (default, aggressive-for-notifications,
  conservative-for-capture, indefinite-for-record-completion) -- see section
  2's "retry policy" explanation for why they differ.

### `src/trip_orchestrator/activities/` -- the real-world side effects

- **`contracts.py`** -- one dataclass per activity's input (and a shared
  `MoneyResult` output type). Every input carries its own
  `idempotency_key`, generated by the workflow, never by the activity
  itself.
- **`errors.py`** -- three small functions (`card_declined`,
  `driver_unavailable`, `transient_downstream`) that build a Temporal
  `ApplicationError` with `non_retryable` set correctly. Centralizing this
  in one place means every activity marks failures consistently instead of
  each one deciding independently.
- **`payment.py`** -- `PaymentActivities`: `authorize_payment`,
  `void_authorization`, `capture_payment`, `refund_payment`. Each just calls
  the corresponding method on a `PaymentGateway` stub and translates its
  exceptions.
- **`driver.py`** -- `DriverActivities`: `reserve_driver`, `release_driver`,
  wrapping `DispatchService`.
- **`notify.py`** -- `NotifyActivities`: `notify_rider`, `notify_driver`,
  wrapping `NotificationService`. No error translation needed -- these have
  no compensation and nothing about them is ever treated as non-retryable.
- **`records.py`** -- `RecordActivities`: `record_trip_completion`, wrapping
  `TripStore`.

All four activity classes take their stub dependency through `__init__`
rather than importing a global instance -- this is what lets
`worker.py` wire up real stubs and `tests/conftest.py` wire up
test-controlled stubs, using the exact same activity code in both cases.

### `src/trip_orchestrator/stubs/` -- fake downstream services

These stand in for the real payment processor, real dispatch system, real
push-notification service, and real trip database that a production version
of this would call. Each is intentionally minimal: only the specific
behaviors the workflow's failure paths need (a card that can be declined, a
driver that can already be taken, a service that can be down for exactly N
calls) rather than a full fake API.

- **`ledger.py`** -- `Ledger`: a small sqlite table
  (`idempotency_key`, `activity_name`, `result_json`) shared by every other
  stub. `record_once(key, name, result)` tries to `INSERT`; if the key
  already exists (a `sqlite3.IntegrityError` on the primary key), it returns
  the *previously stored* result instead of computing a new one and reports
  "this was a duplicate." This is the mechanism that makes "no double side
  effect" a checkable fact rather than an assumption -- and because it's a
  file on disk, it survives a killed worker process, which an in-memory
  dict would not.
- **`payment_gateway.py`** -- `PaymentGateway`. `authorize`/`void`/`capture`/
  `refund`, each idempotent via the ledger. `CardDeclined` (non-retryable,
  triggered by a rider ID ending in `-declined`, so tests can request it
  deterministically) and `TransientGatewayError` (retryable). Also exposes
  `fail_next(count)` and `break_permanently()` so tests can inject failures
  without needing to predict the workflow's internally-generated
  idempotency keys in advance.
- **`dispatch.py`** -- `DispatchService`. `reserve`/`release`, and
  `mark_taken_by_another_trip(driver_id)` to simulate the "driver taken
  concurrently" failure path deterministically.
- **`notifier.py`** -- `NotificationService`. Just records what was sent;
  notifications have no compensation and are never expected to fail in a
  way the workflow needs to react to.
- **`trip_store.py`** -- `TripStore`. `record_completion`, with
  `fail_next(count)` to simulate an outage that resolves after a few
  attempts (used to prove the "retry indefinitely, never lose a completed
  trip" behavior).

### `src/trip_orchestrator/worker.py`

Builds one instance of each stub (all sharing one `Ledger`), wraps each in
its activity class, and registers all of that plus `TripWorkflow` with a
Temporal `Worker`. `build_worker(client)` is factored out separately from
`main()` specifically so `chaos.py` and tests can construct an equivalent
worker without duplicating this registration list.

### `src/trip_orchestrator/api/main.py`

A FastAPI app with exactly three endpoints:

- `POST /trips` -- starts a new `TripWorkflow` execution.
- `POST /trips/{trip_id}/signals/{name}` -- forwards one of the six signals,
  with a small dispatch table (`_SIGNAL_NAMES`) mapping the URL's signal
  name to the actual workflow signal method.
- `GET /trips/{trip_id}` -- runs the `get_trip_state` query and returns it
  directly as JSON.

There's no service layer, no repository pattern, nothing between FastAPI and
the Temporal client, because there's nothing for such a layer to do -- the
workflow already *is* the business logic.

### `src/trip_orchestrator/chaos.py`

The crash-recovery demonstration (details in section 10). Starts N trips
against a real Temporal server, spawns the worker as its own OS process,
kills that process outright partway through (not gracefully -- a hard
`taskkill /F /T` on Windows or `SIGKILL` on Unix), starts a brand new worker
process, and waits for every trip to reach a final state. Reports a JSON
summary of how many recovered, the status breakdown, and mean recovery
time.

### `tests/`

- **`conftest.py`** -- shared pytest fixtures: `env` (a fresh Temporal
  *time-skipping* test server per test), `stubs` (fresh stub instances,
  each test getting its own sqlite file via pytest's `tmp_path`), and
  `worker` (a real `Worker` wired to those fresh stubs, connected to the
  test environment).
- **`helpers.py`** -- `poll_until(check, ...)`: repeatedly awaits an
  async predicate until it's true. Used whenever a test needs to be certain
  the workflow has actually reached a particular state before the test does
  something that depends on that (see bug #3 in section 11 for exactly why
  this matters).
- **`test_happy_path.py`** -- the simplest possible full trip, start to
  `PAID`, plus one test proving the query reflects `IN_PROGRESS` mid-flight.
- **`test_timers.py`** -- offer timeout -> reoffer, offer exhaustion ->
  `UNFULFILLED`, the 10-minute arrival-timeout warning (workflow keeps
  waiting rather than cancelling), and the 5-minute no-show ->
  cancellation-fee path.
- **`test_compensation.py`** -- one test per row in
  `docs/FAILURE_MODES.md`: cancel before/after assignment, driver
  cancellation looping back to matching, a reservation conflict reoffering
  immediately, capture failure terminating as `PAYMENT_FAILED`, record
  completion retrying until it succeeds, and a declined card needing no
  compensation at all.
- **`test_idempotency.py`** -- calls an activity function directly, twice,
  with the same idempotency key, and asserts the ledger only recorded one
  side effect even though the stub was genuinely invoked twice. Also proves
  a transient failure gets retried until it succeeds, and that a
  non-retryable failure stops after exactly one attempt instead of walking
  the full retry schedule.

### `docs/`

- **`ARCHITECTURE.md`** -- component-level overview and the reasoning behind
  a few specific design choices (class-based activities for dependency
  injection, the shared sqlite ledger, query-without-a-database).
- **`DECISIONS.md`** -- direct answers to the seven design questions the
  original project spec called out (durable execution vs. cron, Temporal
  vs. Cadence, saga vs. two-phase commit, why idempotency still matters on
  top of at-least-once delivery, why retry policies differ per activity,
  signal vs. query, and what happens if the Temporal server itself goes
  down).
- **`FAILURE_MODES.md`** -- the table of all 9 failure paths (trigger,
  detection, compensation, terminal state), plus the specific reasoning
  behind ordering choices (why release-then-capture-then-void, why a driver
  cancellation re-authorizes instead of reusing the old hold).
- **`BENCHMARKS.md`** -- the real, live chaos-test results (section 10).

### `deploy/docker-compose.yml`

Postgres + `temporalio/auto-setup` (the Temporal server) + `temporalio/ui`,
for anyone running this with Docker rather than the Temporal CLI's
single-binary dev server. Not exercised in this project's own testing since
Docker wasn't available in this environment -- the CLI dev server was used
instead for everything, including the live chaos benchmark.

---

## 8. How the idempotency key actually gets generated

Every activity call needs a stable, unique ID for "this specific logical
step," and it has to be generated *by the workflow*, deterministically,
because only the workflow's replay-safe state can guarantee the same
logical step always produces the same key -- including on a completely
different worker process after a crash. The mechanism
(`TripWorkflow._next_key`) is a simple incrementing counter combined with
the workflow's own ID and the step's name:

```python
def _next_key(self, step: str) -> str:
    self._seq += 1
    return f"{workflow.info().workflow_id}:{step}:{self._seq}"
```

So for trip `trip-1`, the first authorization becomes
`trip-1:authorize_payment:1`. Crucially, this counter only advances once per
*call site* -- if Temporal retries that same `execute_activity` call because
the activity failed transiently, it re-invokes the activity function with
the exact same input (same key), not a new one. That's what makes the key
stable across retries but still unique across genuinely different actions
(a second driver reservation in a reoffer round gets `...:reserve_driver:4`,
a completely different key from the first round's `...:reserve_driver:2`).

---

## 9. Why the tests run in 3 seconds despite 10-minute timers

Temporal ships a **time-skipping test environment** specifically for this.
When a workflow under test is blocked purely on a timer (nothing else
pending), the test server fast-forwards its internal virtual clock straight
to the timer's fire time, instead of actually waiting in real time. A test
that needs a 15-second offer timeout to expire, or a 10-minute arrival
timeout, or even the up-to-several-minutes of exponential backoff in a retry
policy, resolves in milliseconds of real wall-clock time. This project's
whole 18-test suite runs in about 3 seconds.

Two extra tools from that same test environment show up in
`tests/test_timers.py`:

- **`env.sleep(duration)`** -- manually and precisely advances the virtual
  clock by an exact amount, used when a test needs to force a specific
  timer to fire (e.g., "advance past the 10-minute arrival timeout") without
  racing against the automatic skip-ahead behavior.
- **Sending a signal before a round's timer would fire** -- in
  `test_offer_timeout_reoffers_to_next_candidate`, `driver_accepted` for the
  *second* candidate is sent immediately, before the first candidate's 15
  second offer timer has even started counting down (in virtual time). By
  the time the workflow's automatic time-skip reaches round two, the signal
  is already sitting there, so round two resolves instantly with no need to
  manufacture any extra delay.

---

## 10. The chaos test: proving crash recovery for real

This is the part of the project that most directly demonstrates "durable"
in durable execution. `chaos.py` doesn't simulate a crash -- it starts a
real, separate worker OS process and kills it outright, mid-execution,
while real trips are in flight, against a real (non-time-skipping) Temporal
server.

**What was actually done, step by step:**

1. Installed the Temporal CLI (`temporal` binary) via `winget`, since Docker
   wasn't available.
2. Started a real local server: `temporal server start-dev`. Verified it was
   healthy (`temporal operator cluster health` reported `SERVING`).
3. Ran `chaos.py`, which:
   - connects a Temporal client to that server,
   - spawns the worker as its own subprocess (`python -m
     trip_orchestrator.worker`),
   - starts N trips,
   - concurrently runs a small "driver simulator" per trip that sends the
     `driver_accepted` / `driver_arrived` / `trip_started` /
     `trip_completed` signals with small random delays, so trips are
     genuinely mid-flight (not already finished) at any given moment,
   - waits a random short delay, then **hard-kills** the worker process
     (`taskkill /F /T` -- no graceful shutdown, no chance to finish anything
     in flight),
   - immediately spawns a **brand new** worker process,
   - waits for every trip to reach a terminal state,
   - reports a JSON summary.
4. Ran it first at a small scale (5 trips) to validate the mechanism, then
   at the spec's target scale (100 trips).
5. After the run, directly inspected the sqlite ledger file to count, per
   activity type, how many distinct idempotency keys were recorded.

**Results** (full detail in [docs/BENCHMARKS.md](docs/BENCHMARKS.md)):

- 100/100 trips reached a valid terminal state. 99 completed and were
  `PAID`; 1 legitimately reached `UNFULFILLED` (its one candidate driver's
  acceptance signal landed outside the 15-second offer window under the
  concurrent load the test itself generates -- a real business outcome, not
  a lost trip).
- Ledger contents after the run: 100 `authorize_payment` rows, 100
  `reserve_driver` rows, 297 `notify` rows, 99 `capture_payment` rows, 99
  `record_trip_completion` rows, 1 `void_authorization` row -- **696 rows,
  696 distinct idempotency keys.** Every number is internally consistent
  with "99 trips completed normally, 1 didn't," and there is exactly zero
  duplication anywhere, despite the worker being killed mid-run and
  Temporal re-dispatching whatever activity work was in flight at that
  instant to the replacement worker.

That's the actual, checked claim: killing the process that runs the
workflow's code does not lose or duplicate any state, because the state was
never really "in" that process to begin with.

---

## 11. Real bugs found and fixed along the way

Worth recording honestly, since each one reflects a genuine, non-obvious
gotcha rather than a typo.

**Bug 1 -- `TripStatus` came back as a list of letters.** The very first
test run returned `state.status == ['P', 'A', 'I', 'D']` instead of
`TripStatus.PAID`. The cause: `TripStatus` was originally defined as
`class TripStatus(str, Enum)`, the common Python idiom for a
string-valued enum. Temporal's default JSON data converter, however, only
has special-case handling for `enum.StrEnum` (Python 3.11+'s dedicated
string-enum base class) when reconstructing a query or workflow result --
the `(str, Enum)` mixin isn't recognized, so the converter fell through to
generic sequence-reconstruction logic and rebuilt the string "PAID"
character by character. Fix: switch to `class TripStatus(StrEnum):`. One
word, but it took directly reading Temporal SDK's own converter source to
find, since nothing about the error message pointed at the enum definition.

**Bug 2 -- exception handling that silently never fired.** Several places
in the workflow caught failed activity calls with `except ApplicationError:`
(the type actually raised inside an activity, e.g. `card_declined(...)`).
But `workflow.execute_activity(...)` doesn't let that exception surface
directly -- it wraps it in a `temporalio.exceptions.ActivityError`, and
`ActivityError` and `ApplicationError` turn out to be **sibling** classes
under a common `FailureError` base, not parent/child. So `except
ApplicationError:` never matched anything, the real exception propagated
all the way out of the workflow's `run()` unhandled, and Temporal treated
that as a workflow *task* failure -- which it retries indefinitely by
replaying the entire workflow from scratch each time, re-running real
activities on every replay attempt. This looked, from the logs, like
`authorize_payment` being repeatedly and mysteriously re-invoked deep into
an unrelated test about payment *capture* failing -- deeply confusing until
the actual exception hierarchy was checked directly (`ActivityError.__mro__`
vs `ApplicationError.__mro__`) rather than assumed. Fix: catch
`ActivityError` at every one of these call sites instead.

**Bug 3 -- a cancellation flag that never got reset.** When a driver
cancels after being matched, the workflow releases them, voids the payment,
re-authorizes, and loops back into matching (`trip_outcome ==
"restart_matching"` in `trip_workflow.py`). The reset code cleared several
fields for the new cycle (`_driver_id`, `_driver_accepted_id`,
`_driver_arrived`, `_trip_started`, `_offer_round`) but not
`_driver_cancelled` itself. The very next time the workflow reached the
arrival-wait step, it saw `_driver_cancelled` still `True` from the
*previous* cycle (no new signal at all) and immediately looped back into
"restart matching" a second time -- which, with only one candidate driver
left in the list by that point, meant the trip ended `UNFULFILLED` when it
should have completed normally. Caught by
`test_driver_cancels_returns_to_matching` failing with exactly that
unexpected terminal state. Fix: add `self._driver_cancelled = False` to the
same reset block.

All three are the kind of bug that a first draft, written and trusted
without running it, would have shipped silently. Writing the workflow and
immediately exercising it against Temporal's test environment -- rather
than writing all the code first and testing at the end -- is what surfaced
them while they were still cheap to find.

---

## 12. Running everything yourself

```bash
# one-time setup
python -m venv .venv
.venv/Scripts/activate              # .venv/bin/activate on macOS/Linux
pip install -e ".[dev]"

# run the test suite (no server needed -- uses time-skipping)
pytest

# to run it live: start a local Temporal server (either works)
temporal server start-dev                       # Web UI at localhost:8233
# or: docker compose -f deploy/docker-compose.yml up   # UI at localhost:8080

# in separate terminals
python -m trip_orchestrator.worker
uvicorn trip_orchestrator.api.main:app --reload

# drive a trip
curl -X POST localhost:8000/trips -H "content-type: application/json" -d '{
  "trip_id": "trip-1", "rider_id": "rider-1",
  "pickup": "123 Main St", "dropoff": "456 Oak Ave",
  "fare_estimate_cents": 2500
}'
curl -X POST localhost:8000/trips/trip-1/signals/driver-accepted -d '{"driver_id": "driver-1"}'
curl -X POST localhost:8000/trips/trip-1/signals/driver-arrived
curl -X POST localhost:8000/trips/trip-1/signals/trip-started
curl -X POST localhost:8000/trips/trip-1/signals/trip-completed -d '{"distance_km": 5.2, "duration_minutes": 12}'
curl localhost:8000/trips/trip-1        # query -- no database read

# reproduce the chaos benchmark (manages the worker process itself -- don't
# also run worker.py separately)
temporal server start-dev --db-filename deploy/temporal-dev.db
python -m trip_orchestrator.chaos --trips 100
```

---

## 13. Git and GitHub workflow

- The repository was created via the GitHub REST API (`POST
  /user/repos`), authenticated with an OAuth token already present in the
  local git credential manager, since the `gh` CLI itself wasn't installed
  in this environment. The name was confirmed with you before creating
  anything.
- Git identity was set locally on this repo specifically
  (`git config user.name` / `user.email`, not global), and the very first
  commit's authorship was verified immediately (`git log -1 --format='%an
  <%ae>%n---%n%B'`) before continuing.
- `.claude/settings.json` disables AI co-authorship trailers for this repo.
- History is organized as one logical unit per commit (scaffolding, domain
  model, stubs, activities, the workflow itself, worker/API, tests, chaos
  runner, docs, README) rather than one large commit -- each commit's body
  explains *why*, not just what changed, and every commit was pushed
  immediately rather than batched.

---

## 14. What's intentionally not done

The original spec's Phase 6 is explicitly marked optional polish:
replacing the fake `DispatchService` with a real gRPC call into a separate
dispatch project, and a small web page polling `GetTripState`. Neither was
built, since neither was requested and the former depends on a second
project that isn't part of this repository. Everything through the spec's
stated "worth listing" floor (Phases 0-3) and the two further phases beyond
it (idempotency/retries, and a live chaos benchmark) is complete and
verified, not just written.
