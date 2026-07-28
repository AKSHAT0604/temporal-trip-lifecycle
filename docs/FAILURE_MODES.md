# Failure modes

Every side-effecting activity pushes its compensating action onto a saga
stack (`workflow/saga.py`) the instant it succeeds. Every row below is a
distinct branch in `TripWorkflow.run` (`workflow/trip_workflow.py`), and each
has a dedicated test in `tests/test_compensation.py` or `tests/test_timers.py`
asserting both the terminal state and that the ledger recorded exactly the
compensations listed here -- no skipped rollback, no extra side effect.

| # | Trigger | Detection | Compensation | Terminal state |
|---|---|---|---|---|
| 1 | Card declined at initial authorization | `AuthorizePayment` raises a non-retryable `CardDeclined` | None -- nothing succeeded yet | `CANCELLED` |
| 2 | Rider cancels before a driver is assigned | `RiderCancelled` signal observed while still in `MATCHING` | Void the authorization hold | `CANCELLED` (no fee) |
| 3 | Rider cancels after a driver is assigned | `RiderCancelled` signal observed in `MATCHED` / `DRIVER_ARRIVED` | Release the driver, partially capture the cancellation fee, void the remainder of the hold | `CANCELLED` (with fee) |
| 4 | Rider no-show | 5 minutes pass after `DriverArrived` with no `TripStarted` signal | Same as #3 -- treated as a late cancellation | `CANCELLED` (with fee) |
| 5 | Driver cancels after being matched | `DriverCancelled` signal observed after acceptance | Release the driver, void the authorization, re-authorize a fresh hold, restart matching | Loops back to `MATCHING`, or `CANCELLED` if re-authorization is declined |
| 6 | `ReserveDriver` fails -- driver already committed elsewhere | Activity raises a non-retryable `DriverUnavailable` | None -- the reservation never succeeded | Reoffer to the next candidate; `UNFULFILLED` if candidates are exhausted |
| 7 | Offer round times out (15s, no `DriverAccepted`) | `workflow.wait_condition` timeout | Release the driver that was offered | Reoffer to the next candidate; `UNFULFILLED` after 5 rounds |
| 8 | `CapturePayment` fails after trip completion | `CapturePaymentActivity` exhausts its conservative retry policy | None -- the ride already happened, so unwinding driver/notification steps after the fact would be wrong. The failure is recorded, not swallowed | `PAYMENT_FAILED` (flagged for manual reconciliation) |
| 9 | `RecordTripCompletion` fails | Activity raises a retryable `TripStoreUnavailable` | Retried indefinitely (`maximum_attempts=0`) -- a completed trip must never be lost | Eventually `PAID` (or `PAYMENT_FAILED` if #8 also occurred) |

## Design notes

**Why release-then-capture-then-void, in that order, for #3/#4.** The
compensation stack is LIFO: `release_driver` was pushed after
`void_authorization`, so it naturally unwinds first. The cancellation fee is
captured against the *same* hold before the remainder is voided, because
voiding first would release the whole hold and leave nothing to capture
against.

**Why #5 re-authorizes instead of reusing the original hold.** A hold that
outlives a failed match cycle risks being stale by the time a new driver is
found; re-authorizing gets a fresh hold with a fresh timeout, at the cost of
one extra authorization call. The compensation stack is unwound in full
(release + void) before re-authorizing, so there is never a moment with two
live holds for the same trip.

**Why #6 and #7 are not the same code path even though both "reoffer".** #6
is an immediate, non-retryable activity failure -- the driver is gone, no
amount of waiting fixes that. #7 is a timeout with no failure at all -- the
driver might still accept a moment later, Temporal just isn't waiting for it.
Conflating them would mean either retrying a reservation that can't succeed,
or failing instantly on an offer that simply hasn't been answered yet.

**Why #8 doesn't unwind anything.** Unwinding after a completed ride would
mean trying to release a driver who has already finished driving and
notifying people about an undone trip that actually happened. The money
question is real and unresolved, so it terminates in `PAYMENT_FAILED` -- an
explicit state for a human to act on -- rather than being silently retried
forever or hidden behind a rollback that doesn't reflect reality.

**`ChargeCancellationFee` is not a separate activity.** It is `CapturePayment`
called with a partial amount. Both operations are "capture some of this hold"
against the same payment gateway; giving them separate names would just be
two call sites for identical logic. The failure-mode table lists it as its
own row because it is its own *scenario*, not because it is its own
*activity*.
