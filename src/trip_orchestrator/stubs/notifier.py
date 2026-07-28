"""Fake push notification service. Fire-and-forget, no compensation needed."""

from .ledger import Ledger


class NotificationService:
    def __init__(self, ledger: Ledger | None = None):
        self._ledger = ledger or Ledger()
        self.calls: dict[str, int] = {}

    def _touch(self, key: str) -> int:
        self.calls[key] = self.calls.get(key, 0) + 1
        return self.calls[key]

    def send(self, idempotency_key: str, recipient_id: str, message: str) -> dict:
        self._touch(idempotency_key)
        result = {"recipient_id": recipient_id, "message": message}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "notify", result)
        return stored
