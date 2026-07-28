"""Worker process: connects to Temporal and executes TripWorkflow plus its activities.

Stubs are constructed once here and injected into the activity classes, so
tests can substitute their own stub instances without touching this file.
"""

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from .activities.driver import DriverActivities
from .activities.notify import NotifyActivities
from .activities.payment import PaymentActivities
from .activities.records import RecordActivities
from .stubs.dispatch import DispatchService
from .stubs.ledger import Ledger
from .stubs.notifier import NotificationService
from .stubs.payment_gateway import PaymentGateway
from .stubs.trip_store import TripStore
from .workflow.trip_workflow import TripWorkflow

TASK_QUEUE = "trip-orchestrator"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_worker(client: Client) -> Worker:
    ledger = Ledger()
    payment = PaymentActivities(PaymentGateway(ledger))
    driver = DriverActivities(DispatchService(ledger))
    notify = NotifyActivities(NotificationService(ledger))
    records = RecordActivities(TripStore(ledger))

    return Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[TripWorkflow],
        activities=[
            payment.authorize_payment,
            payment.void_authorization,
            payment.capture_payment,
            payment.refund_payment,
            driver.reserve_driver,
            driver.release_driver,
            notify.notify_rider,
            notify.notify_driver,
            records.record_trip_completion,
        ],
    )


async def main() -> None:
    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(address)
    worker = build_worker(client)
    logger.info("worker started; connected to %s; task queue %s", address, TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
