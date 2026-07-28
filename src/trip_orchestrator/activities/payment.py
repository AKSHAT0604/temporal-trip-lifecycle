from temporalio import activity

from ..stubs.payment_gateway import CardDeclined, PaymentGateway, TransientGatewayError
from . import errors
from .contracts import (
    AuthorizePaymentInput,
    CapturePaymentInput,
    MoneyResult,
    RefundPaymentInput,
    VoidAuthorizationInput,
)


class PaymentActivities:
    def __init__(self, gateway: PaymentGateway):
        self._gateway = gateway

    @activity.defn
    async def authorize_payment(self, input: AuthorizePaymentInput) -> MoneyResult:
        try:
            result = self._gateway.authorize(input.idempotency_key, input.rider_id, input.amount_cents)
        except CardDeclined as exc:
            raise errors.card_declined(str(exc)) from exc
        except TransientGatewayError as exc:
            raise errors.transient_downstream(str(exc)) from exc
        return MoneyResult(reference_id=result["hold_id"], amount_cents=result["amount_cents"])

    @activity.defn
    async def void_authorization(self, input: VoidAuthorizationInput) -> MoneyResult:
        try:
            result = self._gateway.void(input.idempotency_key, input.hold_id)
        except TransientGatewayError as exc:
            raise errors.transient_downstream(str(exc)) from exc
        return MoneyResult(reference_id=result["voided_hold_id"])

    @activity.defn
    async def capture_payment(self, input: CapturePaymentInput) -> MoneyResult:
        try:
            result = self._gateway.capture(input.idempotency_key, input.hold_id, input.amount_cents)
        except TransientGatewayError as exc:
            raise errors.transient_downstream(str(exc)) from exc
        return MoneyResult(reference_id=result["captured_hold_id"], amount_cents=result["amount_cents"])

    @activity.defn
    async def refund_payment(self, input: RefundPaymentInput) -> MoneyResult:
        try:
            result = self._gateway.refund(input.idempotency_key, input.hold_id, input.amount_cents)
        except TransientGatewayError as exc:
            raise errors.transient_downstream(str(exc)) from exc
        return MoneyResult(reference_id=result["refunded_hold_id"], amount_cents=result["amount_cents"])
