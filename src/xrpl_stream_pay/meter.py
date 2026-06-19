"""The claim ticker — decides *when* a fresh claim is due.

The meter is deliberately tiny and side-effect free so it can be unit-tested
without a clock or a network: you feed it tokens and a timestamp, it tells you
whether it's time to cut a new claim.  It does **not** sign anything; the client
owns the keypair and signs when :meth:`Meter.fired` says so.

A claim becomes due when *either* threshold trips:

* ``every_n_tokens`` new tokens have streamed since the last claim, or
* ``every_ms`` milliseconds have elapsed since the last claim.

Two knobs instead of one because a streaming model can be bursty (100 tokens in
a blink) or slow (one token every few seconds); ``every_n_tokens`` keeps the
provider's unpaid exposure bounded on fast streams, ``every_ms`` keeps claims
flowing on slow ones so a stall is detected promptly.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class MeterConfig:
    """Pricing + cadence for a session."""

    drops_per_token: int
    """Price of one token, in drops. The unit economics of the whole stream."""

    every_n_tokens: int = 32
    """Cut a claim at least this often by token count."""

    every_ms: int = 500
    """...or this often by wall-clock time, whichever trips first."""

    def __post_init__(self) -> None:
        if self.drops_per_token < 0:
            raise ValueError("drops_per_token must be >= 0")
        if self.every_n_tokens < 1:
            raise ValueError("every_n_tokens must be >= 1")
        if self.every_ms < 1:
            raise ValueError("every_ms must be >= 1")

    def cost(self, tokens: int) -> int:
        """Cumulative cost in drops for ``tokens`` total tokens."""
        return tokens * self.drops_per_token


class Meter:
    """Stateful counter that fires when a claim is due.

    Typical use, one call per streamed token batch::

        meter = Meter(config)
        for batch in stream:
            meter.record(len(batch))
            if meter.fired():
                claim = authorize_claim(chan, meter.cost(), priv, pub)
                send(claim)
    """

    def __init__(self, config: MeterConfig, *, now: float | None = None) -> None:
        self.config = config
        self.total_tokens = 0
        self._tokens_at_last_claim = 0
        self._time_at_last_claim = now if now is not None else time.monotonic()

    # ----- accounting -------------------------------------------------------

    def record(self, n_tokens: int = 1) -> None:
        """Account for ``n_tokens`` newly streamed tokens."""
        if n_tokens < 0:
            raise ValueError("n_tokens must be >= 0")
        self.total_tokens += n_tokens

    def cost(self) -> int:
        """Cumulative drops owed for everything streamed so far."""
        return self.config.cost(self.total_tokens)

    @property
    def tokens_since_claim(self) -> int:
        return self.total_tokens - self._tokens_at_last_claim

    # ----- the tick ---------------------------------------------------------

    def due(self, *, now: float | None = None) -> bool:
        """Is a fresh claim due *right now*? Pure query — does not reset."""
        if self.tokens_since_claim == 0:
            return False  # nothing new to bill for
        if self.tokens_since_claim >= self.config.every_n_tokens:
            return True
        now = now if now is not None else time.monotonic()
        elapsed_ms = (now - self._time_at_last_claim) * 1000.0
        return elapsed_ms >= self.config.every_ms

    def fired(self, *, now: float | None = None) -> bool:
        """Like :meth:`due`, but if it returns ``True`` it also arms the next
        interval (records that a claim is being cut now). Call this exactly once
        per decision point.
        """
        now = now if now is not None else time.monotonic()
        if self.due(now=now):
            self._tokens_at_last_claim = self.total_tokens
            self._time_at_last_claim = now
            return True
        return False
