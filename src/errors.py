"""Failure taxonomy for the order processor.

The distinction drives the whole retry/DLQ decision:

  TransientError - the operation could plausibly succeed if tried again
                   (network blip, database deadlock, downstream 503).
                   -> retry with exponential backoff.

  PermanentError - the message itself is wrong; no number of retries will
                   help (undecodable bytes, negative price, missing product).
                   -> go straight to the Dead Letter Queue.
"""


class ProcessingError(Exception):
    """Base class for anything raised while processing an order."""


class TransientError(ProcessingError):
    """A retryable failure."""


class PermanentError(ProcessingError):
    """A non-retryable failure - send the record to the DLQ immediately."""
