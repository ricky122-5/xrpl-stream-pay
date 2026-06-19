"""Provider-side claim store — what makes a channel reusable across sessions.

Claims are cumulative over a channel's *whole life*, not one session.  So when a
second session opens on the same channel, the provider must remember the highest
claim it already holds and require the new session to keep signing *above* it —
otherwise the gap between the last claim and the channel's on-ledger balance
could be spent for free.

This module holds the smallest interface that captures that, plus an in-memory
implementation good enough for a single-process demo.  A production provider
would back the same interface with Redis/Postgres so claims survive a restart;
on-ledger balance protects already-*settled* value regardless.
"""

from __future__ import annotations

from typing import Protocol

from .claims import Claim


class ClaimStore(Protocol):
    """Remembers the highest claim held for each channel."""

    def get(self, channel_id: str) -> Claim | None:
        """Highest claim seen for ``channel_id``, or ``None`` if never seen."""
        ...

    def put(self, claim: Claim) -> None:
        """Record ``claim`` if it's higher than what's stored for its channel."""
        ...


class MemoryClaimStore:
    """In-process :class:`ClaimStore`. Loses state on restart (demo-grade)."""

    def __init__(self) -> None:
        self._claims: dict[str, Claim] = {}

    def get(self, channel_id: str) -> Claim | None:
        return self._claims.get(channel_id)

    def put(self, claim: Claim) -> None:
        current = self._claims.get(claim.channel_id)
        if current is None or claim.amount_drops > current.amount_drops:
            self._claims[claim.channel_id] = claim

    def baseline_drops(self, channel_id: str) -> int:
        """Cumulative drops already authorized on this channel (0 if unseen)."""
        claim = self._claims.get(channel_id)
        return claim.amount_drops if claim else 0
