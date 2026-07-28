"""Trip status enum and the data shapes that cross the workflow boundary.

Kept as plain dataclasses (not pydantic models) because Temporal's data
converter only needs types it can round-trip through JSON, and pydantic
would be one dependency this project does not need.

TripStatus is a StrEnum specifically -- not the `class X(str, Enum)` mixin --
because Temporal's default data converter only special-cases `enum.StrEnum`
when reconstructing a query/workflow result; the manual mixin round-trips
through JSON as a bare string and gets rebuilt character-by-character.
"""

from dataclasses import dataclass, field
from enum import StrEnum


class TripStatus(StrEnum):
    REQUESTED = "REQUESTED"
    MATCHING = "MATCHING"
    MATCHED = "MATCHED"
    DRIVER_ARRIVED = "DRIVER_ARRIVED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    PAID = "PAID"
    UNFULFILLED = "UNFULFILLED"
    CANCELLED = "CANCELLED"
    PAYMENT_FAILED = "PAYMENT_FAILED"


@dataclass
class TripRequest:
    rider_id: str
    pickup: str
    dropoff: str
    fare_estimate_cents: int
    candidate_driver_ids: list[str] = field(
        default_factory=lambda: [f"driver-{n}" for n in range(1, 6)]
    )
    cancellation_fee_cents: int = 500


@dataclass
class TripCompletedDetails:
    distance_km: float
    duration_minutes: float


@dataclass
class TripState:
    """Returned by the GetTripState query. No database read involved."""

    status: TripStatus
    driver_id: str | None
    rider_id: str
    elapsed_seconds: float
    offer_round: int
    failure_reason: str | None = field(default=None)
