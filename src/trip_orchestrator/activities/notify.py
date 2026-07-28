from temporalio import activity

from ..stubs.notifier import NotificationService
from .contracts import NotifyInput


class NotifyActivities:
    def __init__(self, notifier: NotificationService):
        self._notifier = notifier

    @activity.defn
    async def notify_rider(self, input: NotifyInput) -> None:
        self._notifier.send(input.idempotency_key, input.recipient_id, input.message)

    @activity.defn
    async def notify_driver(self, input: NotifyInput) -> None:
        self._notifier.send(input.idempotency_key, input.recipient_id, input.message)
