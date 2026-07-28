from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from trip_orchestrator.activities.driver import DriverActivities
from trip_orchestrator.activities.notify import NotifyActivities
from trip_orchestrator.activities.payment import PaymentActivities
from trip_orchestrator.activities.records import RecordActivities
from trip_orchestrator.stubs.dispatch import DispatchService
from trip_orchestrator.stubs.ledger import Ledger
from trip_orchestrator.stubs.notifier import NotificationService
from trip_orchestrator.stubs.payment_gateway import PaymentGateway
from trip_orchestrator.stubs.trip_store import TripStore
from trip_orchestrator.workflow.trip_workflow import TripWorkflow

TASK_QUEUE = "test-trip-orchestrator"


class Stubs:
    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger
        self.payment_gateway = PaymentGateway(ledger)
        self.dispatch = DispatchService(ledger)
        self.notifier = NotificationService(ledger)
        self.trip_store = TripStore(ledger)


@pytest_asyncio.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        yield environment


@pytest_asyncio.fixture
async def stubs(tmp_path) -> Stubs:
    return Stubs(Ledger(tmp_path / "ledger.db"))


@pytest_asyncio.fixture
async def worker(env: WorkflowEnvironment, stubs: Stubs) -> AsyncIterator[Worker]:
    payment = PaymentActivities(stubs.payment_gateway)
    driver = DriverActivities(stubs.dispatch)
    notify = NotifyActivities(stubs.notifier)
    records = RecordActivities(stubs.trip_store)

    async with Worker(
        env.client,
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
    ) as w:
        yield w
