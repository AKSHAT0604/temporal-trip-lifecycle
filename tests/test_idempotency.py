"""Idempotency and retry-policy behaviour.

Two different guarantees are covered here and they're easy to conflate:

1. Idempotency: if the *same* activity invocation (same idempotency key) runs
   twice -- because Temporal redelivers it, or because something upstream
   retried at a layer Temporal doesn't see -- the downstream side effect
   happens exactly once. Tested by calling the activity function directly
   twice, bypassing Temporal's retry machinery entirely.

2. Retry policy: distinguishing a transient failure (worth retrying, and the
   workflow should observe eventual success) from a terminal one (not worth
   retrying at all, stop immediately). Tested through the full workflow so
   the actual `non_retryable` wiring on ApplicationError is exercised.
"""

from trip_orchestrator.activities.contracts import AuthorizePaymentInput
from trip_orchestrator.activities.errors import card_declined, transient_downstream
from trip_orchestrator.activities.payment import PaymentActivities
from trip_orchestrator.stubs.ledger import Ledger
from trip_orchestrator.stubs.payment_gateway import PaymentGateway
from trip_orchestrator.workflow.states import TripCompletedDetails, TripRequest, TripStatus
from trip_orchestrator.workflow.trip_workflow import TripWorkflow

TASK_QUEUE = "test-trip-orchestrator"


def test_card_declined_error_is_marked_non_retryable():
    assert card_declined("declined").non_retryable is True


def test_transient_downstream_error_is_marked_retryable():
    assert transient_downstream("blip").non_retryable is False


async def test_duplicate_activity_execution_has_no_second_side_effect(tmp_path):
    ledger = Ledger(tmp_path / "ledger.db")
    gateway = PaymentGateway(ledger)
    activities = PaymentActivities(gateway)
    duplicated_input = AuthorizePaymentInput(
        idempotency_key="trip-x:authorize_payment:1", rider_id="rider-1", amount_cents=1500
    )

    first = await activities.authorize_payment(duplicated_input)
    second = await activities.authorize_payment(duplicated_input)

    assert first == second
    # The gateway really was invoked twice (this is what "at-least-once
    # execution" means)...
    assert gateway.calls[duplicated_input.idempotency_key] == 2
    # ...but the dedupe ledger only ever recorded one hold.
    assert ledger.count_by_activity("authorize_payment") == 1


async def test_transient_failure_is_retried_until_it_succeeds(env, worker, stubs):
    stubs.payment_gateway.fail_next(2)
    request = TripRequest(
        rider_id="rider-7", pickup="A", dropoff="B", fare_estimate_cents=1000,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-retry-1", task_queue=TASK_QUEUE)
    await handle.signal(TripWorkflow.driver_accepted, "driver-1")
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    await handle.signal(TripWorkflow.trip_completed, TripCompletedDetails(distance_km=1.0, duration_minutes=3.0))

    state = await handle.result()

    assert state.status == TripStatus.PAID
    # 2 injected failures plus the successful 3rd attempt, all under the
    # same idempotency key -- Temporal retries the same activity invocation.
    assert stubs.payment_gateway.calls["trip-retry-1:authorize_payment:1"] == 3
    assert stubs.ledger.count_by_activity("authorize_payment") == 1


async def test_non_retryable_failure_stops_after_a_single_attempt(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-declined", pickup="A", dropoff="B", fare_estimate_cents=1000,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-declined-2", task_queue=TASK_QUEUE)

    state = await handle.result()

    assert state.status == TripStatus.CANCELLED
    # DEFAULT_RETRY allows up to 5 attempts; a card decline must not walk
    # that whole schedule -- it's marked non_retryable and stops at 1.
    assert stubs.payment_gateway.calls["trip-declined-2:authorize_payment:1"] == 1
