"""Fake driver dispatch service.

Stands in for a real-time supply system. The one behaviour this project
actually needs from it is a driver that can be "already committed elsewhere",
which is what forces the workflow's reoffer path.
"""

from .ledger import Ledger


class DriverUnavailable(Exception):
    """The candidate driver was reserved by another trip first."""


class DispatchService:
    def __init__(self, ledger: Ledger | None = None):
        self._ledger = ledger or Ledger()
        self.calls: dict[str, int] = {}
        self._taken_by_another_trip: set[str] = set()

    def _touch(self, key: str) -> int:
        self.calls[key] = self.calls.get(key, 0) + 1
        return self.calls[key]

    def mark_taken_by_another_trip(self, driver_id: str) -> None:
        self._taken_by_another_trip.add(driver_id)

    def reserve(self, idempotency_key: str, trip_id: str, driver_id: str) -> dict:
        self._touch(idempotency_key)
        if driver_id in self._taken_by_another_trip:
            raise DriverUnavailable(f"driver {driver_id} already committed to another trip")
        result = {"trip_id": trip_id, "driver_id": driver_id}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "reserve_driver", result)
        return stored

    def release(self, idempotency_key: str, driver_id: str) -> dict:
        self._touch(idempotency_key)
        result = {"driver_id": driver_id}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "release_driver", result)
        return stored
