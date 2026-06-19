"""Agent side: stream a paid response, signing a claim each time the meter fires.

:class:`StreamClient` connects to a provider's ``/pay-stream`` websocket, agrees
on terms, then yields text as it arrives.  Between yields it runs the
:class:`~xrpl_stream_pay.meter.Meter`; whenever the meter fires it signs a fresh
cumulative claim for what it has received and pushes it back to the provider.

The amount it signs is the provider's reported ``owed_drops`` for the batch — but
only after checking that figure matches the agreed price and stays under the
session budget, so a misbehaving provider can never trick the agent into
authorizing more than it agreed to.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

import websockets

from . import protocol
from .channel import ChannelInfo
from .claims import Claim, authorize_claim
from .errors import BudgetExceeded, ProtocolError, StreamPayError
from .meter import Meter, MeterConfig


@dataclass
class SessionResult:
    """What happened over a session — returned by :meth:`StreamClient.run`."""

    text: str
    tokens_received: int
    final_claim: Claim | None
    paid_drops: int
    claims_sent: int
    cut: bool = False
    cut_reason: str | None = None


class StreamClient:
    """One paid streaming session against one channel."""

    def __init__(
        self,
        *,
        channel: ChannelInfo,
        private_key: str,
        meter_config: MeterConfig,
        max_budget_drops: int | None = None,
    ) -> None:
        self.channel = channel
        self.private_key = private_key
        self.meter_config = meter_config
        # Never authorize more than the channel holds, or an explicit budget.
        self.max_budget_drops = (
            min(max_budget_drops, channel.capacity_drops)
            if max_budget_drops is not None
            else channel.capacity_drops
        )
        # Filled in over a session; persists across stream() calls when the
        # same client (i.e. the same channel) is reused for many sessions.
        self.final_claim: Claim | None = None
        self.claims_sent = 0
        self.tokens_received = 0  # tokens in the most recent session
        self.total_tokens = 0  # tokens over the channel's whole life
        self._last_authorized = 0  # cumulative drops authorized so far
        self._baseline = 0  # cumulative already authorized when a session starts

    @classmethod
    def from_wallet(
        cls,
        wallet,
        channel: ChannelInfo,
        meter_config: MeterConfig,
        max_budget_drops: int | None = None,
    ) -> "StreamClient":
        """Convenience constructor using a wallet's private key for signing."""
        return cls(
            channel=channel,
            private_key=wallet.private_key,
            meter_config=meter_config,
            max_budget_drops=max_budget_drops,
        )

    def _sign(self, amount_drops: int) -> Claim:
        if amount_drops > self.max_budget_drops:
            raise BudgetExceeded(
                f"provider wants {amount_drops} drops authorized but budget is "
                f"{self.max_budget_drops}"
            )
        claim = authorize_claim(
            self.channel.channel_id,
            amount_drops,
            self.private_key,
            self.channel.public_key,
        )
        self.final_claim = claim
        self.claims_sent += 1
        return claim

    async def stream(
        self, url: str, prompt: str, **params: object
    ) -> AsyncIterator[str]:
        """Connect and yield text batches as they arrive, paying as you go.

        Drive it like any async iterator::

            async for text in client.stream(url, "Explain X"):
                print(text, end="")

        After the loop finishes, :attr:`final_claim` holds the claim the provider
        will settle and :attr:`StreamClient.result` summarizes the session.
        """
        meter = Meter(self.meter_config)
        async with websockets.connect(url) as ws:
            await ws.send(
                json.dumps(
                    protocol.hello(
                        self.channel.channel_id,
                        self.channel.public_key,
                        prompt,
                        **params,
                    )
                )
            )
            ready = json.loads(await ws.recv())
            self._check_ready(ready)
            # Continue from wherever this (possibly reused) channel left off.
            self._baseline = int(ready.get("already_authorized_drops", 0))
            self._last_authorized = max(self._last_authorized, self._baseline)

            async for text in self._consume(ws, meter):
                yield text

    async def _consume(self, ws, meter: Meter) -> AsyncIterator[str]:
        self._cut = False
        self._cut_reason: str | None = None
        prev_tokens = 0
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            kind = msg.get("type")

            if kind == protocol.CHUNK:
                self._guard_price(msg)
                # tokens_sent is cumulative for the session; meter wants the delta.
                delta = max(0, msg["tokens_sent"] - prev_tokens)
                prev_tokens = msg["tokens_sent"]
                meter.record(delta)
                self.tokens_received = msg["tokens_sent"]
                self.total_tokens += delta
                if meter.fired():
                    await self._send_claim(ws, msg["owed_drops"])
                yield msg["text"]

            elif kind == protocol.END_OF_STREAM:
                # Always settle the remainder in full, regardless of the meter.
                await self._send_claim(ws, msg["owed_drops"])

            elif kind == protocol.DONE:
                self.tokens_received = msg.get("tokens_sent", self.tokens_received)
                return

            elif kind == protocol.CUT:
                self._cut = True
                self._cut_reason = msg.get("reason")
                return

            elif kind == protocol.ERROR:
                raise StreamPayError(f"provider error: {msg.get('message')}")

            else:
                raise ProtocolError(f"unexpected message type: {kind!r}")

    async def _send_claim(self, ws, owed_drops: int) -> None:
        # Skip redundant claims (e.g. a final claim equal to the last metered
        # one); a duplicate buys nothing and just adds wire noise.
        if owed_drops <= self._last_authorized:
            return
        claim = self._sign(owed_drops)
        self._last_authorized = owed_drops
        await ws.send(json.dumps(protocol.claim_msg(claim)))

    def _check_ready(self, ready: dict) -> None:
        if ready.get("type") != protocol.READY:
            raise ProtocolError(f"expected 'ready', got {ready.get('type')!r}")
        price = ready.get("drops_per_token")
        if price is not None and price > self.meter_config.drops_per_token:
            raise BudgetExceeded(
                f"provider charges {price} drops/token, agent agreed to at most "
                f"{self.meter_config.drops_per_token}"
            )

    def _guard_price(self, chunk: dict) -> None:
        # owed is cumulative over the channel; subtract the session baseline.
        expected = self._baseline + chunk["tokens_sent"] * self.meter_config.drops_per_token
        if chunk["owed_drops"] > expected:
            raise BudgetExceeded(
                f"provider billed {chunk['owed_drops']} drops for "
                f"{chunk['tokens_sent']} tokens above a {self._baseline}-drop baseline; "
                f"agreed price implies at most {expected}"
            )

    @property
    def result(self) -> SessionResult:
        return SessionResult(
            text="",
            tokens_received=self.tokens_received,
            final_claim=self.final_claim,
            paid_drops=self.final_claim.amount_drops if self.final_claim else 0,
            claims_sent=self.claims_sent,
            cut=getattr(self, "_cut", False),
            cut_reason=getattr(self, "_cut_reason", None),
        )

    async def run(self, url: str, prompt: str, **params: object) -> SessionResult:
        """Convenience: consume the whole stream and return a summary + full text."""
        parts: list[str] = []
        async for text in self.stream(url, prompt, **params):
            parts.append(text)
        result = self.result
        return SessionResult(
            text="".join(parts),
            tokens_received=result.tokens_received,
            final_claim=result.final_claim,
            paid_drops=result.paid_drops,
            claims_sent=result.claims_sent,
            cut=result.cut,
            cut_reason=result.cut_reason,
        )
