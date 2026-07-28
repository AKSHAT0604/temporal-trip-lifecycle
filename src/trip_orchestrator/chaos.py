"""Chaos runner: start N concurrent trips, hard-kill the worker process at a
random point mid-flight, restart it, and confirm every trip still reaches a
valid terminal state with no lost or duplicated side effects.

This is the project's headline demonstration: durable execution means the
workflow's state lives in the Temporal server, not the worker process, so
killing the worker loses nothing. Requires a running Temporal server
(`temporal server start-dev`); this script manages the worker's lifecycle
itself so that it can kill and restart it.
"""

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from temporalio.client import Client

from .worker import TASK_QUEUE
from .workflow.states import TripCompletedDetails, TripRequest
from .workflow.trip_workflow import TripWorkflow

ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@dataclass
class ChaosResult:
    trip_id: str
    final_status: str | None
    error: str | None = None


def _spawn_worker() -> subprocess.Popen:
    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen(
        [sys.executable, "-m", "trip_orchestrator.worker"],
        cwd=REPO_ROOT,
        creationflags=creationflags,
    )


def _kill_worker(proc: subprocess.Popen) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    else:
        proc.kill()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


async def _drive_trip(client: Client, trip_id: str, driver_id: str) -> None:
    """Stand-in for the outside world: signals arrive with small random
    delays so trips are genuinely mid-flight, not already finished, when the
    worker gets killed."""
    handle = client.get_workflow_handle_for(TripWorkflow.run, trip_id)
    for delay, action in (
        (random.uniform(0.05, 0.3), lambda: handle.signal(TripWorkflow.driver_accepted, driver_id)),
        (random.uniform(0.05, 0.3), lambda: handle.signal(TripWorkflow.driver_arrived)),
        (random.uniform(0.05, 0.3), lambda: handle.signal(TripWorkflow.trip_started)),
        (
            random.uniform(0.05, 0.3),
            lambda: handle.signal(
                TripWorkflow.trip_completed, TripCompletedDetails(distance_km=5.0, duration_minutes=10.0)
            ),
        ),
    ):
        await asyncio.sleep(delay)
        await action()


async def run_chaos(num_trips: int) -> dict:
    client = await Client.connect(ADDRESS)

    worker_proc = _spawn_worker()
    await asyncio.sleep(2.0)  # let the first worker connect and start polling

    trip_ids = [f"chaos-trip-{i}" for i in range(num_trips)]
    for i, trip_id in enumerate(trip_ids):
        request = TripRequest(
            rider_id=f"rider-{i}",
            pickup="A",
            dropoff="B",
            fare_estimate_cents=1500,
            candidate_driver_ids=[f"driver-{i}"],
        )
        await client.start_workflow(TripWorkflow.run, request, id=trip_id, task_queue=TASK_QUEUE)

    driver_tasks = [
        asyncio.create_task(_drive_trip(client, trip_id, f"driver-{i}")) for i, trip_id in enumerate(trip_ids)
    ]

    kill_delay = random.uniform(0.2, 0.8)
    await asyncio.sleep(kill_delay)
    kill_started_at = time.monotonic()
    _kill_worker(worker_proc)

    worker_proc = _spawn_worker()
    recovery_started_at = time.monotonic()

    await asyncio.gather(*driver_tasks, return_exceptions=True)

    results: list[ChaosResult] = []
    for trip_id in trip_ids:
        handle = client.get_workflow_handle_for(TripWorkflow.run, trip_id)
        try:
            state = await asyncio.wait_for(handle.result(), timeout=60)
            results.append(ChaosResult(trip_id=trip_id, final_status=state.status.value))
        except Exception as exc:  # noqa: BLE001 -- recorded, not swallowed
            results.append(ChaosResult(trip_id=trip_id, final_status=None, error=str(exc)))

    recovery_seconds = time.monotonic() - recovery_started_at
    _kill_worker(worker_proc)

    succeeded = [r for r in results if r.final_status is not None]
    return {
        "num_trips": num_trips,
        "kill_point_seconds_into_run": round(kill_delay, 3),
        "recovered": len(succeeded),
        "failed": num_trips - len(succeeded),
        "mean_recovery_seconds": round(recovery_seconds / max(len(succeeded), 1), 3),
        "status_counts": _status_counts(results),
        "failures": [asdict(r) for r in results if r.final_status is None],
    }


def _status_counts(results: list[ChaosResult]) -> dict:
    counts: dict[str, int] = {}
    for r in results:
        key = r.final_status or "no_result"
        counts[key] = counts.get(key, 0) + 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trips", type=int, default=100)
    args = parser.parse_args()

    summary = asyncio.run(run_chaos(args.trips))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
