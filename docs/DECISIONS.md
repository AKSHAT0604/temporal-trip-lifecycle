# Decisions

## 1. Why durable execution instead of a status column plus a cron job?

A status column models a trip as a snapshot; the actual behavior -- "if the
authorization succeeds but the driver reservation fails, void the hold" --
ends up scattered across whatever cron job or callback happens to poll that
status next. Every one of those handlers has to independently reconstruct
"where was this trip, and what have I already done about it," using
timestamps and secondary flags as a proxy for state that was never actually
recorded as a program's control flow.

A durable workflow *is* that control flow. `TripWorkflow.run` reads like an
ordinary linear async function -- authorize, reserve, wait, capture -- and
the engine persists the fact that execution is paused at a particular
`await` point, not just a status label. Crash recovery is a consequence of
that persistence, not a feature that had to be separately built.

## 2. Why Temporal rather than Cadence, given Cadence is Uber's original?

Temporal was created by Cadence's original authors and shares its
architecture and programming model, but has a more actively maintained SDK
ecosystem and materially better documentation. Choosing the better-supported
fork of the same lineage -- and being able to say why the lineage exists --
is a stronger signal than defaulting to Cadence out of brand recognition
without understanding that Temporal is the same idea carried forward by the
same people.

## 3. Why the saga pattern rather than a distributed two-phase commit?

Two-phase commit needs every participant (payment processor, dispatch
service) to speak the same transaction protocol and hold locks until a
coordinator says commit or abort. A real payment gateway will not do that;
it exposes authorize/capture/void as independent, already-committed
operations. A saga accepts that constraint: each step commits immediately
and durably, and failure is handled by running compensating actions
(`void`, `release`) rather than by preventing the step from committing in
the first place. It trades atomicity for availability -- exactly the
trade a system built on third-party APIs has to make anyway.

## 4. Why must activities be idempotent when the workflow engine already guarantees at-least-once execution?

"At-least-once" is precisely the problem: the engine will re-run an activity
if it can't confirm the previous attempt's result was recorded (worker
crashed after the real side effect happened but before reporting success).
That guarantee describes *when Temporal retries*, not what happens
downstream when it does. Without an idempotency key that the payment
gateway or dispatch service deduplicates on, "at least once" becomes "the
card may be charged more than once." The engine's guarantee and the
activity's idempotency are two different halves of the same requirement;
neither one alone is enough. `docs/FAILURE_MODES.md` and
`tests/test_idempotency.py` demonstrate the second half directly: the same
activity invoked twice against the same key produces one side effect.

## 5. Why do different activities warrant different retry policies?

Retrying is a bet that trades latency against the cost of being wrong, and
that cost is not uniform. A failed push notification is free to retry
aggressively -- worst case, a slightly late message. A failed payment
capture is user-visible and potentially harmful if hammered, so it gets a
long backoff and a low attempt ceiling before the workflow gives up and
surfaces `PAYMENT_FAILED` for a human. `RecordTripCompletion` sits at the
other extreme: the ride already happened, so losing that record is not an
acceptable outcome at any retry cost, and it retries indefinitely. One
global policy would have to pick a single point on that spectrum for
every activity, which is wrong for most of them.

## 6. What is the actual difference between a signal and a query, and when does each apply?

A signal is a durable, asynchronous *write* into a running workflow: it is
recorded in the workflow's history, can trigger state changes and further
activities, and is delivered exactly once even if the workflow was
unreachable when it was sent. A query is a synchronous, read-only snapshot
of the workflow's current in-memory state -- it never appears in history,
never triggers activities, and if the workflow doesn't exist or has closed,
it simply fails; it has no persistence of its own. `DriverAccepted` has
to be a signal because it changes what the workflow does next. `GetTripState`
has to be a query because a `net/http` handler polling trip status every
second must not each be able to accidentally clobber invisible workflow state
or wake it up as new history.

## 7. What happens if the Temporal server itself goes down mid-workflow?

Nothing is lost. Workflow state is persisted in the server's own datastore
(SQLite for `temporal server start-dev`, Postgres/Cassandra/MySQL in
production) as an append-only event history, not held in the worker's
memory. When the server comes back, workers reconnect, resume polling their
task queues, and any workflow that was waiting on a timer or signal picks up
exactly where its history left off -- the same mechanism `chaos.py`
exercises by killing the *worker* instead of the server, since a worker
crash and a server crash are recovered from through the same persisted-history
mechanism. See `docs/BENCHMARKS.md` for a worker-crash run recorded against a
real (non-time-skipping) Temporal server.
