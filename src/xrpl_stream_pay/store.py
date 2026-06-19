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

import json
import os
import tempfile
import threading
from pathlib import Path
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


class FileClaimStore:
    """JSON-file-backed :class:`ClaimStore` that survives a process restart.

    Good enough to make a channel genuinely long-lived: kill the provider, bring
    it back, and it still remembers the highest claim per channel, so a reused
    channel keeps climbing instead of re-spending the gap.  Writes are atomic
    (temp file + ``os.replace``) and guarded by a lock; a production deployment
    would swap this for Redis/Postgres behind the same interface.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._claims: dict[str, Claim] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return
        for channel_id, claim in raw.items():
            try:
                self._claims[channel_id] = Claim.from_wire(claim)
            except (KeyError, ValueError, TypeError):
                continue

    def _flush(self) -> None:
        data = {cid: claim.to_wire() for cid, claim in self._claims.items()}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def get(self, channel_id: str) -> Claim | None:
        with self._lock:
            return self._claims.get(channel_id)

    def put(self, claim: Claim) -> None:
        with self._lock:
            current = self._claims.get(claim.channel_id)
            if current is not None and claim.amount_drops <= current.amount_drops:
                return
            self._claims[claim.channel_id] = claim
            self._flush()

    def baseline_drops(self, channel_id: str) -> int:
        with self._lock:
            claim = self._claims.get(channel_id)
            return claim.amount_drops if claim else 0
