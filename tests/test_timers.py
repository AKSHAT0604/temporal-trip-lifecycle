"""Timer, reoffer, and exhaustion paths.

All of these rely on the Temporal test environment's time-skipping: a 15
second offer timeout or a 10 minute arrival timeout resolves in milliseconds
of wall-clock time because the test server fast-forwards its virtual clock
whenever nothing else is pending.
"""

from datetime import timedelta

from trip_orchestrator.workflow.states import TripCompletedDetails, TripRequest, TripStatus
from trip_orchestrator.workflow.trip_workflow import TripWorkflow

TASK_QUEUE = "test-trip-orchestrator"


async def test_offer_timeout_reoffers_to_next_candidate(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-1",
        pickup="A",
        dropoff="B",
        fare_estimate_cents=2000,
        candidate_driver_ids=["driver-1", "driver-2"],
    )
    handle = await env.client.start_workflow(
        TripWorkflow.run, request, id="trip-reoffer-1", task_queue=TASK_QUEUE
    )

    # Never accept driver-1. Accept driver-2 up front -- round 1 still has to
    # time out for real (driver-2's acceptance doesn't apply to round 1), but
    # by the time round 2 offers driver-2, the signal is already sitting there.
    await handle.signal(TripWorkflow.driver_accepted, "driver-2")
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    await handle.signal(
        TripWorkflow.trip_completed,
        TripCompletedDetails(distance_km=4.0, duration_minutes=10.0),
    )

    state = await handle.result()

    assert state.status == TripStatus.PAID
    assert state.driver_id == "driver-2"
    assert state.offer_round == 2
    assert stubs.ledger.count_by_activity("reserve_driver") == 2
    assert stubs.ledger.count_by_activity("release_driver") == 1


async def test_offer_exhaustion_becomes_unfulfilled(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-2",
        pickup="A",
        dropoff="B",
        fare_estimate_cents=1000,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(
        TripWorkflow.run, request, id="trip-unfulfilled-1", task_queue=TASK_QUEUE
    )

    state = await handle.result()

    assert state.status == TripStatus.UNFULFILLED
    assert stubs.ledger.count_by_activity("authorize_payment") == 1
    assert stubs.ledger.count_by_activity("void_authorization") == 1
    assert stubs.ledger.count_by_activity("capture_payment") == 0


async def test_arrival_timeout_warns_rider_then_continues(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-3",
        pickup="A",
        dropoff="B",
        fare_estimate_cents=1200,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(
        TripWorkflow.run, request, id="trip-late-arrival-1", task_queue=TASK_QUEUE
    )
    await handle.signal(TripWorkflow.driver_accepted, "driver-1")

    # Force the virtual clock past the 10-minute arrival timeout before the
    # driver ever arrives, so the workflow must take the "warn, then keep
    # waiting" branch instead of the happy path.
    await env.sleep(timedelta(minutes=11))

    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    await handle.signal(
        TripWorkflow.trip_completed,
        TripCompletedDetails(distance_km=6.0, duration_minutes=15.0),
    )

    state = await handle.result()

    assert state.status == TripStatus.PAID
    # Two rider notifications: the offer and the "running late" warning.
    assert stubs.ledger.count_by_activity("notify") >= 3


async def test_no_show_charges_cancellation_fee(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-4",
        pickup="A",
        dropoff="B",
        fare_estimate_cents=3000,
        cancellation_fee_cents=750,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(
        TripWorkflow.run, request, id="trip-no-show-1", task_queue=TASK_QUEUE
    )
    await handle.signal(TripWorkflow.driver_accepted, "driver-1")
    await handle.signal(TripWorkflow.driver_arrived)

    # Never send trip_started -- let the 5 minute no-show timeout fire.
    state = await handle.result()

    assert state.status == TripStatus.CANCELLED
    assert state.failure_reason is not None and "no-show" in state.failure_reason
    assert stubs.ledger.count_by_activity("release_driver") == 1
    assert stubs.ledger.count_by_activity("capture_payment") == 1
    assert stubs.ledger.count_by_activity("void_authorization") == 1
