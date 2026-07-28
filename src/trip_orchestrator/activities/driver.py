from temporalio import activity

from ..stubs.dispatch import DispatchService, DriverUnavailable
from . import errors
from .contracts import ReleaseDriverInput, ReserveDriverInput


class DriverActivities:
    def __init__(self, dispatch: DispatchService):
        self._dispatch = dispatch

    @activity.defn
    async def reserve_driver(self, input: ReserveDriverInput) -> str:
        try:
            result = self._dispatch.reserve(input.idempotency_key, input.trip_id, input.driver_id)
        except DriverUnavailable as exc:
            raise errors.driver_unavailable(str(exc)) from exc
        return result["driver_id"]

    @activity.defn
    async def release_driver(self, input: ReleaseDriverInput) -> str:
        result = self._dispatch.release(input.idempotency_key, input.driver_id)
        return result["driver_id"]
