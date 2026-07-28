"""Fake payment processor.

Standing in for something like Stripe: authorize a hold, capture part or all
of it, void it, refund it. Real gateways de-duplicate on a client-supplied
idempotency key; this stub does the same via the shared ledger.

Failure injection is global rather than per-key: tests don't know the
idempotency keys the workflow will generate ahead of time (they're derived
from the workflow ID and an internal step counter), so "the next N calls to
this gateway fail" is what's actually controllable from outside.
"""

from .ledger import Ledger


class CardDeclined(Exception):
    """The issuer rejected the charge outright. Not worth retrying."""


class TransientGatewayError(Exception):
    """A blip on the processor's side. Worth retrying."""


class PaymentGateway:
    def __init__(self, ledger: Ledger | None = None):
        self._ledger = ledger or Ledger()
        self.calls: dict[str, int] = {}
        self._fail_next_calls = 0
        self._broken = False

    def _touch(self, key: str) -> int:
        self.calls[key] = self.calls.get(key, 0) + 1
        return self.calls[key]

    def fail_next(self, count: int) -> None:
        """The next `count` calls to any method raise TransientGatewayError."""
        self._fail_next_calls = count

    def break_permanently(self) -> None:
        """Every call from now on fails; used to prove a retry policy exhausts."""
        self._broken = True

    def _maybe_fail(self) -> None:
        if self._broken:
            raise TransientGatewayError("gateway permanently unreachable")
        if self._fail_next_calls > 0:
            self._fail_next_calls -= 1
            raise TransientGatewayError("transient gateway error")

    def authorize(self, idempotency_key: str, rider_id: str, amount_cents: int) -> dict:
        self._touch(idempotency_key)
        if rider_id.endswith("-declined"):
            raise CardDeclined(f"card declined for rider {rider_id}")
        self._maybe_fail()
        result = {"hold_id": idempotency_key, "rider_id": rider_id, "amount_cents": amount_cents}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "authorize_payment", result)
        return stored

    def void(self, idempotency_key: str, hold_id: str) -> dict:
        self._touch(idempotency_key)
        self._maybe_fail()
        result = {"voided_hold_id": hold_id}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "void_authorization", result)
        return stored

    def capture(self, idempotency_key: str, hold_id: str, amount_cents: int) -> dict:
        self._touch(idempotency_key)
        self._maybe_fail()
        result = {"captured_hold_id": hold_id, "amount_cents": amount_cents}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "capture_payment", result)
        return stored

    def refund(self, idempotency_key: str, hold_id: str, amount_cents: int) -> dict:
        self._touch(idempotency_key)
        self._maybe_fail()
        result = {"refunded_hold_id": hold_id, "amount_cents": amount_cents}
        stored, _duplicate = self._ledger.record_once(idempotency_key, "refund_payment", result)
        return stored
