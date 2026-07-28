"""Translates stub exceptions into Temporal's retryable/non-retryable vocabulary.

`ApplicationError(..., non_retryable=True)` tells the Temporal server to stop
retrying this activity immediately instead of walking the retry policy's
backoff schedule -- the distinction most candidates get wrong when asked
about retries in a system design interview (see docs/DECISIONS.md, Q5).
"""

from temporalio.exceptions import ApplicationError


def card_declined(message: str) -> ApplicationError:
    return ApplicationError(message, type="CardDeclined", non_retryable=True)


def driver_unavailable(message: str) -> ApplicationError:
    """Retrying the same reservation is pointless -- the workflow reoffers
    to a different driver instead of Temporal retrying this activity call."""
    return ApplicationError(message, type="DriverUnavailable", non_retryable=True)


def transient_downstream(message: str) -> ApplicationError:
    return ApplicationError(message, type="TransientDownstreamError", non_retryable=False)
