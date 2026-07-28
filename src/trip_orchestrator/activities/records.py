from temporalio import activity

from ..stubs.trip_store import TripStore, TripStoreUnavailable
from . import errors
from .contracts import RecordTripCompletionInput


class RecordActivities:
    def __init__(self, store: TripStore):
        self._store = store

    @activity.defn
    async def record_trip_completion(self, input: RecordTripCompletionInput) -> str:
        try:
            result = self._store.record_completion(
                input.idempotency_key, input.trip_id, input.distance_km, input.duration_minutes
            )
        except TripStoreUnavailable as exc:
            raise errors.transient_downstream(str(exc)) from exc
        return result["trip_id"]
