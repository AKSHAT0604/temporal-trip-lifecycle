import asyncio

from temporalio.client import WorkflowHandle

from trip_orchestrator.workflow.states import TripCompletedDetails, TripRequest, TripStatus
from trip_orchestrator.workflow.trip_workflow import TripWorkflow

TASK_QUEUE = "test-trip-orchestrator"


async def _drive_to_completion(handle: WorkflowHandle, driver_id: str) -> None:
    await handle.signal(TripWorkflow.driver_accepted, driver_id)
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)
    await handle.signal(
        TripWorkflow.trip_completed,
        TripCompletedDetails(distance_km=5.2, duration_minutes=12.0),
    )


async def test_full_trip_completes_and_pays(env, worker):
    request = TripRequest(
        rider_id="rider-1",
        pickup="A",
        dropoff="B",
        fare_estimate_cents=2000,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(
        TripWorkflow.run, request, id="trip-happy-1", task_queue=TASK_QUEUE
    )

    await asyncio.sleep(0.1)
    await _drive_to_completion(handle, "driver-1")

    state = await handle.result()

    assert state.status == TripStatus.PAID
    assert state.driver_id == "driver-1"
    assert state.failure_reason is None


async def test_query_reflects_in_progress_state(env, worker):
    request = TripRequest(
        rider_id="rider-2",
        pickup="A",
        dropoff="B",
        fare_estimate_cents=1500,
        candidate_driver_ids=["driver-1"],
    )
    handle = await env.client.start_workflow(
        TripWorkflow.run, request, id="trip-query-1", task_queue=TASK_QUEUE
    )

    await handle.signal(TripWorkflow.driver_accepted, "driver-1")
    await handle.signal(TripWorkflow.driver_arrived)
    await handle.signal(TripWorkflow.trip_started)

    async def in_progress() -> bool:
        state = await handle.query(TripWorkflow.get_trip_state)
        return state.status == TripStatus.IN_PROGRESS

    for _ in range(50):
        if await in_progress():
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("workflow never reached IN_PROGRESS")

    await handle.signal(
        TripWorkflow.trip_completed,
        TripCompletedDetails(distance_km=3.0, duration_minutes=8.0),
    )
    state = await handle.result()
    assert state.status == TripStatus.PAID
