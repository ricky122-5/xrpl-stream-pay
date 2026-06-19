"""The provider-side payment gate — withhold tokens until you've been paid.

The gate is the policy brain of the server, kept deliberately free of any
transport (no FastAPI, no websockets) so it can be unit-tested with plain
function calls.  :mod:`xrpl_stream_pay.server` wires it to a websocket.

Model: **post-pay with a bounded trust window.**  A provider can't bill for
tokens it hasn't generated yet, so it streams a little ahead and lets the client
catch up with claims.  The gate tracks two running numbers:

* ``owed_drops``   — cost of everything sent so far (tokens_sent x price)
* ``paid_drops``   — the amount in the highest valid claim received so far

``outstanding = owed - paid`` is the provider's unpaid exposure.  As long as the
client keeps signing claims, outstanding stays near zero and tokens flow freely.
If outstanding climbs past ``max_outstanding_drops`` the gate applies
backpressure (the sender awaits a claim); if no qualifying claim arrives within
``stall_timeout`` the gate raises :class:`StallTimeout` and the stream is cut.

This is the honest streaming-payments trust model: the provider's worst-case
loss to a non-paying client is one ``max_outstanding`` window, never the whole
response.
"""

from __future__ import annotations

import asyncio

from .claims import Claim, verify_claim
from .errors import PaymentError, StallTimeout


class StreamGate:
    """Tracks payment for a single streaming session and gates token release."""

    def __init__(
        self,
        *,
        channel_id: str,
        public_key: str,
        drops_per_token: int,
        capacity_drops: int,
        max_outstanding_drops: int,
        stall_timeout: float = 10.0,
    ) -> None:
        if drops_per_token < 0:
            raise ValueError("drops_per_token must be >= 0")
        if max_outstanding_drops < 0:
            raise ValueError("max_outstanding_drops must be >= 0")
        self.channel_id = channel_id
        self.public_key = public_key
        self.drops_per_token = drops_per_token
        self.capacity_drops = capacity_drops
        self.max_outstanding_drops = max_outstanding_drops
        self.stall_timeout = stall_timeout

        self.tokens_sent = 0
        self.paid_drops = 0
        self.latest_claim: Claim | None = None
        self.claims_accepted = 0
        # A terminal fault raised asynchronously (bad claim / disconnect) by the
        # claim-reader task; the sender re-raises it at its next await point.
        self.fault: Exception | None = None
        # Set whenever a claim is accepted or a fault is raised; the sender
        # awaits it under backpressure.
        self._progress = asyncio.Event()

    # ----- running totals ---------------------------------------------------

    @property
    def owed_drops(self) -> int:
        return self.tokens_sent * self.drops_per_token

    @property
    def outstanding_drops(self) -> int:
        return self.owed_drops - self.paid_drops

    def record_sent(self, n_tokens: int) -> None:
        """Account for ``n_tokens`` just handed to the client."""
        if n_tokens < 0:
            raise ValueError("n_tokens must be >= 0")
        self.tokens_sent += n_tokens

    # ----- accepting claims -------------------------------------------------

    def submit_claim(self, claim: Claim) -> bool:
        """Validate a claim. Returns ``True`` if it advanced payment.

        Raises :class:`PaymentError` only for claims that are *invalid or
        unsafe* — wrong channel/key, a bad signature, or an amount beyond the
        channel's capacity — because those signal a misbehaving peer and should
        cut the stream.  A merely *stale* claim (amount not greater than what's
        already paid, e.g. a duplicate or a harmless replay) is ignored and
        returns ``False``; cutting the stream over one would let anyone DoS a
        session by echoing an old claim.
        """
        if claim.channel_id != self.channel_id:
            raise PaymentError("claim is for a different channel")
        if claim.public_key != self.public_key:
            raise PaymentError("claim signed by an unexpected key")
        if not verify_claim(claim):
            raise PaymentError("claim signature is invalid")
        if claim.amount_drops > self.capacity_drops:
            raise PaymentError(
                f"claim exceeds channel capacity "
                f"({claim.amount_drops} > {self.capacity_drops})"
            )
        if claim.amount_drops <= self.paid_drops:
            return False  # stale / duplicate — harmless, ignore
        self.paid_drops = claim.amount_drops
        self.latest_claim = claim
        self.claims_accepted += 1
        self._progress.set()
        return True

    def fail(self, exc: Exception) -> None:
        """Record a terminal fault and wake the sender so it can cut the stream.

        Called by the transport layer when a claim is invalid or the client
        disconnects mid-stream.
        """
        if self.fault is None:
            self.fault = exc
        self._progress.set()

    # ----- backpressure -----------------------------------------------------

    async def await_within_budget(self) -> None:
        """Block while the client is more than one window behind.

        Returns as soon as ``outstanding <= max_outstanding_drops``.  Raises
        :class:`StallTimeout` if that doesn't happen within ``stall_timeout``.
        """
        await self._await_until(lambda: self.outstanding_drops <= self.max_outstanding_drops)

    async def drain(self) -> None:
        """Block until the client has paid for everything sent (outstanding 0).

        Called after the last token so the provider collects the final claim
        before declaring the session done.
        """
        await self._await_until(lambda: self.outstanding_drops <= 0)

    async def _await_until(self, condition) -> None:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + self.stall_timeout
        while True:
            if self.fault is not None:
                raise self.fault
            if condition():
                return
            self._progress.clear()
            # re-check after clearing: a claim/fault may have landed in the gap.
            if self.fault is not None:
                raise self.fault
            if condition():
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise StallTimeout(self.outstanding_drops, self.stall_timeout)
            try:
                await asyncio.wait_for(self._progress.wait(), remaining)
            except asyncio.TimeoutError:
                raise StallTimeout(self.outstanding_drops, self.stall_timeout) from None
