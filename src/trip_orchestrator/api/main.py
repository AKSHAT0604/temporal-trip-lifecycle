"""HTTP surface: start a trip, send it signals, query its state.

Deliberately thin -- FastAPI's routing and pydantic validation are the whole
API layer. There is no service layer between this and the Temporal client
because there is nothing for one to do; the workflow is the business logic.
"""

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from temporalio.client import Client

from ..worker import TASK_QUEUE
from ..workflow.states import TripCompletedDetails, TripRequest, TripState
from ..workflow.trip_workflow import TripWorkflow

app = FastAPI(title="Trip Orchestrator API")
_client: Client | None = None


async def get_client() -> Client:
    global _client
    if _client is None:
        _client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    return _client


class StartTripBody(BaseModel):
    trip_id: str
    rider_id: str
    pickup: str
    dropoff: str
    fare_estimate_cents: int
    candidate_driver_ids: list[str] | None = None
    cancellation_fee_cents: int = 500


class SignalBody(BaseModel):
    driver_id: str | None = None
    distance_km: float | None = None
    duration_minutes: float | None = None


_SIGNAL_NAMES = {
    "driver-accepted",
    "driver-arrived",
    "trip-started",
    "trip-completed",
    "rider-cancelled",
    "driver-cancelled",
}


@app.post("/trips")
async def start_trip(body: StartTripBody) -> dict[str, str]:
    client = await get_client()
    request = TripRequest(
        rider_id=body.rider_id,
        pickup=body.pickup,
        dropoff=body.dropoff,
        fare_estimate_cents=body.fare_estimate_cents,
        candidate_driver_ids=body.candidate_driver_ids or [f"driver-{n}" for n in range(1, 6)],
        cancellation_fee_cents=body.cancellation_fee_cents,
    )
    await client.start_workflow(TripWorkflow.run, request, id=body.trip_id, task_queue=TASK_QUEUE)
    return {"trip_id": body.trip_id, "status": "started"}


@app.post("/trips/{trip_id}/signals/{name}")
async def send_signal(trip_id: str, name: str, body: SignalBody) -> dict[str, str]:
    if name not in _SIGNAL_NAMES:
        raise HTTPException(status_code=404, detail=f"unknown signal '{name}'")

    client = await get_client()
    handle = client.get_workflow_handle_for(TripWorkflow.run, trip_id)

    if name == "driver-accepted":
        if not body.driver_id:
            raise HTTPException(status_code=400, detail="driver_id is required")
        await handle.signal(TripWorkflow.driver_accepted, body.driver_id)
    elif name == "driver-arrived":
        await handle.signal(TripWorkflow.driver_arrived)
    elif name == "trip-started":
        await handle.signal(TripWorkflow.trip_started)
    elif name == "trip-completed":
        if body.distance_km is None or body.duration_minutes is None:
            raise HTTPException(status_code=400, detail="distance_km and duration_minutes are required")
        details = TripCompletedDetails(distance_km=body.distance_km, duration_minutes=body.duration_minutes)
        await handle.signal(TripWorkflow.trip_completed, details)
    elif name == "rider-cancelled":
        await handle.signal(TripWorkflow.rider_cancelled)
    elif name == "driver-cancelled":
        await handle.signal(TripWorkflow.driver_cancelled)

    return {"trip_id": trip_id, "signal": name, "status": "sent"}


@app.get("/trips/{trip_id}")
async def get_trip(trip_id: str) -> TripState:
    client = await get_client()
    handle = client.get_workflow_handle_for(TripWorkflow.run, trip_id)
    return await handle.query(TripWorkflow.get_trip_state)
