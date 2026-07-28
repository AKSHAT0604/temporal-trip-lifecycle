"""TripWorkflow: the durable state machine for one ride.

State progression (see docs/ARCHITECTURE.md for the full diagram):

    REQUESTED -> MATCHING -> MATCHED -> DRIVER_ARRIVED -> IN_PROGRESS -> COMPLETED -> PAID
                     |            |            |               |
                     v            v            v               v
                UNFULFILLED   CANCELLED    CANCELLED       PAYMENT_FAILED

Every side-effecting activity pushes its compensating action onto a
`CompensationStack` (workflow/saga.py) the moment it succeeds. Failure paths
never hand-code "what do I undo here" -- they just unwind the stack.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from ..activities.contracts import (
        AuthorizePaymentInput,
        CapturePaymentInput,
        NotifyInput,
        RecordTripCompletionInput,
        ReleaseDriverInput,
        ReserveDriverInput,
        VoidAuthorizationInput,
    )
    from ..activities.driver import DriverActivities
    from ..activities.notify import NotifyActivities
    from ..activities.payment import PaymentActivities
    from ..activities.records import RecordActivities
    from .saga import CompensationStack
    from .states import TripCompletedDetails, TripRequest, TripState, TripStatus

MAX_OFFER_ROUNDS = 5
OFFER_TIMEOUT = timedelta(seconds=15)
ARRIVAL_TIMEOUT = timedelta(minutes=10)
NO_SHOW_TIMEOUT = timedelta(minutes=5)

# Per-activity retry policies: uniform retries would hide a real distinction.
# A failed push notification is cheap to retry hard; a failed payment capture
# is user-visible and must not be hammered.
DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)
NOTIFY_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=200),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=2),
    maximum_attempts=10,
)
CAPTURE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=4,
)
INDEFINITE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    maximum_attempts=0,
)
# Reserving a specific driver either succeeds or fails because someone else
# already has them; retrying the same reservation can't change that, so the
# activity itself gets exactly one attempt and the workflow reoffers instead.
RESERVE_DRIVER_RETRY = RetryPolicy(maximum_attempts=1)


@workflow.defn
class TripWorkflow:
    def __init__(self) -> None:
        self._status = TripStatus.REQUESTED
        self._rider_id = ""
        self._driver_id: str | None = None
        self._offer_round = 0
        self._driver_accepted_id: str | None = None
        self._driver_arrived = False
        self._trip_started = False
        self._trip_completed_details: TripCompletedDetails | None = None
        self._rider_cancelled = False
        self._driver_cancelled = False
        self._failure_reason: str | None = None
        self._seq = 0
        self._started_at = None

    def _next_key(self, step: str) -> str:
        self._seq += 1
        return f"{workflow.info().workflow_id}:{step}:{self._seq}"

    # --- Signals -----------------------------------------------------

    @workflow.signal
    def driver_accepted(self, driver_id: str) -> None:
        self._driver_accepted_id = driver_id

    @workflow.signal
    def driver_arrived(self) -> None:
        self._driver_arrived = True

    @workflow.signal
    def trip_started(self) -> None:
        self._trip_started = True

    @workflow.signal
    def trip_completed(self, details: TripCompletedDetails) -> None:
        self._trip_completed_details = details

    @workflow.signal
    def rider_cancelled(self) -> None:
        self._rider_cancelled = True

    @workflow.signal
    def driver_cancelled(self) -> None:
        self._driver_cancelled = True

    # --- Query ---------------------------------------------------------

    @workflow.query
    def get_trip_state(self) -> TripState:
        elapsed = (workflow.now() - self._started_at).total_seconds() if self._started_at else 0.0
        return TripState(
            status=self._status,
            driver_id=self._driver_id,
            rider_id=self._rider_id,
            elapsed_seconds=elapsed,
            offer_round=self._offer_round,
            failure_reason=self._failure_reason,
        )

    def _final_state(self) -> TripState:
        return self.get_trip_state()

    # --- Main run --------------------------------------------------------

    @workflow.run
    async def run(self, request: TripRequest) -> TripState:
        self._rider_id = request.rider_id
        self._started_at = workflow.now()
        candidates = list(request.candidate_driver_ids)
        compensation = CompensationStack()

        try:
            hold = await self._authorize(request.rider_id, request.fare_estimate_cents, compensation)
        except ActivityError:
            self._status = TripStatus.CANCELLED
            self._failure_reason = "payment authorization declined"
            return self._final_state()

        self._status = TripStatus.MATCHING

        while True:
            outcome = await self._match_with_reoffer(request, candidates, compensation)

            if outcome == "unfulfilled":
                self._status = TripStatus.UNFULFILLED
                await compensation.unwind()
                return self._final_state()

            if outcome == "rider_cancelled":
                self._status = TripStatus.CANCELLED
                await compensation.unwind()
                return self._final_state()

            # outcome == "driver_secured"
            self._status = TripStatus.MATCHED
            trip_outcome = await self._through_arrival_and_trip(request, compensation)

            if trip_outcome == "restart_matching":
                await compensation.unwind()
                self._driver_id = None
                self._driver_accepted_id = None
                self._driver_arrived = False
                self._trip_started = False
                self._driver_cancelled = False
                self._offer_round = 0
                try:
                    hold = await self._authorize(request.rider_id, request.fare_estimate_cents, compensation)
                except ActivityError:
                    self._status = TripStatus.CANCELLED
                    self._failure_reason = "payment re-authorization declined after driver cancellation"
                    return self._final_state()
                self._status = TripStatus.MATCHING
                continue

            if trip_outcome == "cancelled_after_match":
                self._status = TripStatus.CANCELLED
                await compensation.unwind_last()  # release the driver
                await self._charge_cancellation_fee(hold, request.cancellation_fee_cents)
                await compensation.unwind()  # void whatever remains of the hold
                return self._final_state()

            # trip_outcome == "completed"
            self._status = TripStatus.COMPLETED
            await self._settle_payment(hold, compensation)
            return self._final_state()

    # --- Payment -----------------------------------------------------------

    async def _authorize(self, rider_id: str, amount_cents: int, compensation: CompensationStack):
        key = self._next_key("authorize_payment")
        hold = await workflow.execute_activity(
            PaymentActivities.authorize_payment,
            AuthorizePaymentInput(idempotency_key=key, rider_id=rider_id, amount_cents=amount_cents),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DEFAULT_RETRY,
        )
        compensation.push("void_authorization", self._make_void(hold.reference_id))
        return hold

    def _make_void(self, hold_id: str):
        async def _void() -> None:
            key = self._next_key("void_authorization")
            await workflow.execute_activity(
                PaymentActivities.void_authorization,
                VoidAuthorizationInput(idempotency_key=key, hold_id=hold_id),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=DEFAULT_RETRY,
            )

        return _void

    async def _charge_cancellation_fee(self, hold, fee_cents: int) -> None:
        key = self._next_key("charge_cancellation_fee")
        await workflow.execute_activity(
            PaymentActivities.capture_payment,
            CapturePaymentInput(idempotency_key=key, hold_id=hold.reference_id, amount_cents=fee_cents),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DEFAULT_RETRY,
        )

    async def _settle_payment(self, hold, compensation: CompensationStack) -> None:
        details = self._trip_completed_details
        capture_key = self._next_key("capture_payment")
        try:
            await workflow.execute_activity(
                PaymentActivities.capture_payment,
                CapturePaymentInput(idempotency_key=capture_key, hold_id=hold.reference_id, amount_cents=hold.amount_cents),
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=CAPTURE_RETRY,
            )
            self._status = TripStatus.PAID
        except ActivityError:
            # The ride already happened -- do not unwind driver/notification
            # steps retroactively. The money question is real and must
            # terminate in a defined state for manual reconciliation, not be
            # swallowed.
            self._status = TripStatus.PAYMENT_FAILED
            self._failure_reason = "capture failed after exhausting retry policy; needs manual reconciliation"

        record_key = self._next_key("record_trip_completion")
        await workflow.execute_activity(
            RecordActivities.record_trip_completion,
            RecordTripCompletionInput(
                idempotency_key=record_key,
                trip_id=workflow.info().workflow_id,
                distance_km=details.distance_km,
                duration_minutes=details.duration_minutes,
            ),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=INDEFINITE_RETRY,
        )

    # --- Matching ------------------------------------------------------

    async def _match_with_reoffer(self, request: TripRequest, candidates: list[str], compensation: CompensationStack) -> str:
        for round_no in range(1, MAX_OFFER_ROUNDS + 1):
            self._offer_round = round_no
            if self._rider_cancelled:
                self._failure_reason = "rider cancelled before driver assignment"
                return "rider_cancelled"
            if not candidates:
                break
            driver_id = candidates.pop(0)

            try:
                key = self._next_key("reserve_driver")
                await workflow.execute_activity(
                    DriverActivities.reserve_driver,
                    ReserveDriverInput(idempotency_key=key, trip_id=workflow.info().workflow_id, driver_id=driver_id),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=RESERVE_DRIVER_RETRY,
                )
            except ActivityError:
                continue  # driver taken concurrently -- reoffer to the next candidate

            compensation.push("release_driver", self._make_release(driver_id))
            self._driver_id = driver_id

            await self._notify_rider(request.rider_id, f"Driver {driver_id} has been offered your trip.")
            await self._notify_driver(driver_id, f"New trip request {workflow.info().workflow_id}.")

            try:
                await workflow.wait_condition(
                    lambda: self._driver_accepted_id == driver_id or self._rider_cancelled,
                    timeout=OFFER_TIMEOUT,
                )
            except asyncio.TimeoutError:
                pass

            if self._rider_cancelled:
                self._failure_reason = "rider cancelled before driver assignment"
                return "rider_cancelled"
            if self._driver_accepted_id == driver_id:
                return "driver_secured"

            # Offer timed out: release this driver and try the next candidate.
            await compensation.unwind_last()
            self._driver_id = None

        return "unfulfilled"

    def _make_release(self, driver_id: str):
        async def _release() -> None:
            key = self._next_key("release_driver")
            await workflow.execute_activity(
                DriverActivities.release_driver,
                ReleaseDriverInput(idempotency_key=key, driver_id=driver_id),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=DEFAULT_RETRY,
            )

        return _release

    # --- Arrival through completion --------------------------------------

    async def _through_arrival_and_trip(self, request: TripRequest, compensation: CompensationStack) -> str:
        try:
            await workflow.wait_condition(
                lambda: self._driver_arrived or self._rider_cancelled or self._driver_cancelled,
                timeout=ARRIVAL_TIMEOUT,
            )
        except asyncio.TimeoutError:
            await self._notify_rider(request.rider_id, "Your driver is running late. You may cancel free of charge.")
            await workflow.wait_condition(
                lambda: self._driver_arrived or self._rider_cancelled or self._driver_cancelled
            )

        if self._driver_cancelled:
            return "restart_matching"
        if self._rider_cancelled:
            self._failure_reason = "rider cancelled after driver assignment"
            return "cancelled_after_match"

        self._status = TripStatus.DRIVER_ARRIVED
        await self._notify_rider(request.rider_id, "Your driver has arrived.")

        no_show = False
        try:
            await workflow.wait_condition(
                lambda: self._trip_started or self._rider_cancelled or self._driver_cancelled,
                timeout=NO_SHOW_TIMEOUT,
            )
        except asyncio.TimeoutError:
            no_show = True

        if self._driver_cancelled:
            return "restart_matching"
        if self._rider_cancelled:
            self._failure_reason = "rider cancelled after driver assignment"
            return "cancelled_after_match"
        if no_show:
            self._failure_reason = "rider no-show after driver arrival"
            return "cancelled_after_match"

        self._status = TripStatus.IN_PROGRESS
        await workflow.wait_condition(lambda: self._trip_completed_details is not None)
        return "completed"

    # --- Notifications ---------------------------------------------------

    async def _notify_rider(self, rider_id: str, message: str) -> None:
        key = self._next_key("notify_rider")
        await workflow.execute_activity(
            NotifyActivities.notify_rider,
            NotifyInput(idempotency_key=key, recipient_id=rider_id, message=message),
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=NOTIFY_RETRY,
        )

    async def _notify_driver(self, driver_id: str, message: str) -> None:
        key = self._next_key("notify_driver")
        await workflow.execute_activity(
            NotifyActivities.notify_driver,
            NotifyInput(idempotency_key=key, recipient_id=driver_id, message=message),
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=NOTIFY_RETRY,
        )
