"""Fake system-of-record for finished trips.

A completed ride must never be lost, so `record_completion` supports being
made unavailable for the next N calls, to prove the workflow retries this
step indefinitely rather than giving up.
"""

from .ledger import Ledger


class TripStoreUnavailable(Exception):
    pass


class TripStore:
    def __init__(self, ledger: Ledger | None = None):
        self._ledger = ledger or Ledger()
        self.calls: dict[str, int] = {}
        self._fail_next_calls = 0

    def _touch(self, key: str) -> int:
        self.calls[key] = self.calls.get(key, 0) + 1
        return self.calls[key]

    def fail_next(self, count: int) -> None:
        self._fail_next_calls = count

    def record_completion(self, idempotency_key: str, trip_id: str, distance_km: float, duration_minutes: float) -> dict:
        self._touch(idempotency_key)
        if self._fail_next_calls > 0:
            self._fail_next_calls -= 1
            raise TripStoreUnavailable("trip store unreachable")
        result = {"trip_id": trip_id, "distance_km": distance_km, "duration_minutes": duration_minutes}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "record_trip_completion", result)
        return stored
