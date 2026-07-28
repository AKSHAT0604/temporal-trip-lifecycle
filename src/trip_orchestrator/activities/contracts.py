"""Input/output shapes for every activity.

Every input carries its own `idempotency_key`. The workflow computes that key
deterministically (workflow ID + step name + a monotonic sequence number) and
passes it in explicitly, rather than letting the activity invent one, because
only the workflow's replay-safe state can guarantee the same logical step
always gets the same key -- including across a worker crash and restart.
"""

from dataclasses import dataclass


@dataclass
class AuthorizePaymentInput:
    idempotency_key: str
    rider_id: str
    amount_cents: int


@dataclass
class VoidAuthorizationInput:
    idempotency_key: str
    hold_id: str


@dataclass
class CapturePaymentInput:
    idempotency_key: str
    hold_id: str
    amount_cents: int


@dataclass
class RefundPaymentInput:
    idempotency_key: str
    hold_id: str
    amount_cents: int


@dataclass
class ReserveDriverInput:
    idempotency_key: str
    trip_id: str
    driver_id: str


@dataclass
class ReleaseDriverInput:
    idempotency_key: str
    driver_id: str


@dataclass
class NotifyInput:
    idempotency_key: str
    recipient_id: str
    message: str


@dataclass
class RecordTripCompletionInput:
    idempotency_key: str
    trip_id: str
    distance_km: float
    duration_minutes: float


@dataclass
class MoneyResult:
    reference_id: str
    amount_cents: int | None = None
