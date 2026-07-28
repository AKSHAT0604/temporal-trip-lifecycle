"""One test per documented failure path in docs/FAILURE_MODES.md.

Each asserts both the terminal state *and* that the ledger shows exactly the
compensations the failure mode calls for -- no skipped rollback, no extra
side effect.
"""

from trip_orchestrator.workflow.states import TripCompletedDetails, TripRequest, TripStatus
from trip_orchestrator.workflow.trip_workflow import TripWorkflow

from helpers import poll_until

TASK_QUEUE = "test-trip-orchestrator"


async def _status_is(handle, status: TripStatus) -> bool:
    state = await handle.query(TripWorkflow.get_trip_state)
    return state.status == status


async def test_rider_cancels_before_assignment_voids_only(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-1", pickup="A", dropoff="B", fare_estimate_cents=2000,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-cancel-before-1", task_queue=TASK_QUEUE)
    await poll_until(lambda: _status_is(handle, TripStatus.MATCHING))

    await handle.signal(TripWorkflow.rider_cancelled)
    state = await handle.result()

    assert state.status == TripStatus.CANCELLED
    assert state.failure_reason == "rider cancelled before driver assignment"
    assert stubs.ledger.count_by_activity("authorize_payment") == 1
    assert stubs.ledger.count_by_activity("void_authorization") == 1
    assert stubs.ledger.count_by_activity("capture_payment") == 0


async def test_rider_cancels_after_assignment_charges_fee(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-2", pickup="A", dropoff="B", fare_estimate_cents=2000,
        cancellation_fee_cents=500, candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-cancel-after-1", task_queue=TASK_QUEUE)
    await handle.signal(TripWorkflow.driver_accepted, "driver-1")
    await poll_until(lambda: _status_is(handle, TripStatus.MATCHED))

    await handle.signal(TripWorkflow.rider_cancelled)
    state = await handle.result()

    assert state.status == TripStatus.CANCELLED
    assert state.failure_reason == "rider cancelled after driver assignment"
    assert stubs.ledger.count_by_activity("release_driver") == 1
    assert stubs.ledger.count_by_activity("capture_payment") == 1
    assert stubs.ledger.count_by_activity("void_authorization") == 1


async def test_driver_cancels_returns_to_matching(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-3", pickup="A", dropoff="B", fare_estimate_cents=1800,
        candidate_driver_ids=["driver-1", "driver-2"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-driver-cancel-1", task_queue=TASK_QUEUE)
    await handle.signal(TripWorkflow.driver_accepted, "driver-1")
    await poll_until(lambda: _status_is(handle, TripStatus.MATCHED))

    await handle.signal(TripWorkflow.driver_cancelled)
    await poll_until(lambda: _status_is(handle, TripStatus.MATCHING))

    await handle.signal(TripWorkflow.driver_accepted, "driver-2")
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    await handle.signal(TripWorkflow.trip_completed, TripCompletedDetails(distance_km=2.0, duration_minutes=6.0))

    state = await handle.result()

    assert state.status == TripStatus.PAID
    assert state.driver_id == "driver-2"
    assert stubs.ledger.count_by_activity("authorize_payment") == 2
    assert stubs.ledger.count_by_activity("void_authorization") == 1
    assert stubs.ledger.count_by_activity("release_driver") == 1
    assert stubs.ledger.count_by_activity("capture_payment") == 1


async def test_reserve_driver_conflict_reoffers_immediately(env, worker, stubs):
    stubs.dispatch.mark_taken_by_another_trip("driver-1")
    request = TripRequest(
        rider_id="rider-4", pickup="A", dropoff="B", fare_estimate_cents=1600,
        candidate_driver_ids=["driver-1", "driver-2"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-conflict-1", task_queue=TASK_QUEUE)
    await handle.signal(TripWorkflow.driver_accepted, "driver-2")
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    await handle.signal(TripWorkflow.trip_completed, TripCompletedDetails(distance_km=1.0, duration_minutes=4.0))

    state = await handle.result()

    assert state.status == TripStatus.PAID
    assert state.driver_id == "driver-2"
    assert state.offer_round == 2
    # driver-1's reservation attempt raised before ever writing to the
    # ledger, so nothing was reserved and nothing needs releasing.
    assert stubs.ledger.count_by_activity("reserve_driver") == 1
    assert stubs.ledger.count_by_activity("release_driver") == 0


async def test_capture_failure_terminates_as_payment_failed(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-5", pickup="A", dropoff="B", fare_estimate_cents=2500,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-capture-fail-1", task_queue=TASK_QUEUE)
    await handle.signal(TripWorkflow.driver_accepted, "driver-1")
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    # Breaking the gateway only after IN_PROGRESS is confirmed guarantees the
    # earlier, legitimate authorize_payment call already succeeded -- signals
    # alone don't guarantee the worker has processed prior steps yet.
    await poll_until(lambda: _status_is(handle, TripStatus.IN_PROGRESS))

    stubs.payment_gateway.break_permanently()
    await handle.signal(TripWorkflow.trip_completed, TripCompletedDetails(distance_km=9.0, duration_minutes=20.0))

    state = await handle.result()

    assert state.status == TripStatus.PAYMENT_FAILED
    assert state.failure_reason is not None and "manual reconciliation" in state.failure_reason
    assert stubs.ledger.count_by_activity("capture_payment") == 0
    # The completed trip must never be lost even though payment failed.
    assert stubs.ledger.count_by_activity("record_trip_completion") == 1


async def test_record_completion_retries_until_it_succeeds(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-6", pickup="A", dropoff="B", fare_estimate_cents=2200,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-record-retry-1", task_queue=TASK_QUEUE)
    await handle.signal(TripWorkflow.driver_accepted, "driver-1")
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    await poll_until(lambda: _status_is(handle, TripStatus.IN_PROGRESS))

    stubs.trip_store.fail_next(5)
    await handle.signal(TripWorkflow.trip_completed, TripCompletedDetails(distance_km=3.3, duration_minutes=9.0))

    state = await handle.result()

    assert state.status == TripStatus.PAID
    assert stubs.ledger.count_by_activity("record_trip_completion") == 1
    # 5 injected failures plus the eventual success, all against the same
    # idempotency key since Temporal retries the same activity invocation.
    assert sum(stubs.trip_store.calls.values()) == 6
    assert len(stubs.trip_store.calls) == 1


async def test_card_declined_at_authorization_needs_no_compensation(env, worker, stubs):
    request = TripRequest(
        rider_id="rider-declined", pickup="A", dropoff="B", fare_estimate_cents=2000,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(TripWorkflow.run, request, id="trip-declined-1", task_queue=TASK_QUEUE)

    state = await handle.result()

    assert state.status == TripStatus.CANCELLED
    assert state.failure_reason == "payment authorization declined"
    assert stubs.ledger.count_by_activity("authorize_payment") == 0
    assert stubs.ledger.count_by_activity("void_authorization") == 0
    assert stubs.ledger.count_by_activity("reserve_driver") == 0
