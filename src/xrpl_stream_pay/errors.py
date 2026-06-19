"""Exception hierarchy for xrpl-stream-pay.

Everything raised by the library is a subclass of :class:`StreamPayError`, so a
caller can catch the whole family with one ``except``.
"""

from __future__ import annotations


class StreamPayError(Exception):
    """Base class for all xrpl-stream-pay errors."""


class ChannelError(StreamPayError):
    """A payment channel could not be opened, found, funded, or closed."""


class PaymentError(StreamPayError):
    """A claim was missing, malformed, stale, unsigned, or under-funded.

    Raised by the provider-side gate when a client's claim does not justify
    releasing more of the stream.
    """


class StallTimeout(PaymentError):
    """The client stopped paying: no fresh claim arrived before the deadline.

    The provider uses this to cut a non-paying stream mid-flight.  It carries
    the amount still owed so the caller can log/settle what was earned.
    """

    def __init__(self, outstanding_drops: int, deadline_seconds: float) -> None:
        self.outstanding_drops = outstanding_drops
        self.deadline_seconds = deadline_seconds
        super().__init__(
            f"no claim covering {outstanding_drops} drops arrived within "
            f"{deadline_seconds:g}s — cutting stream"
        )


class ProtocolError(StreamPayError):
    """A wire message was malformed or arrived out of the expected order."""


class BudgetExceeded(StreamPayError):
    """The provider asked the client to authorize more than its session budget.

    Raised client-side so an agent never signs a claim larger than it agreed to.
    """
