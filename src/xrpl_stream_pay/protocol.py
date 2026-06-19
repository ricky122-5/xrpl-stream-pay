"""The little JSON protocol spoken over the websocket.

One session looks like::

    client ── hello ──────────────▶ server     (which channel, which key, prompt)
    client ◀───────── ready ─────── server     (price, capacity, limits)
    client ◀───────── chunk ─────── server  ┐  text + running totals
    client ── claim ──────────────▶ server  ├─ repeat, off-ledger, per batch
    client ◀───────── chunk ─────── server  ┘
              ...                                (server applies backpressure /
                                                 cuts via `cut` if claims lapse)
    client ◀──── end_of_stream ──── server     (generation finished — pay up)
    client ── claim (final) ──────▶ server
    client ◀───────── done ──────── server     (settled-ready; final claim held)

Messages are plain JSON dicts with a ``"type"`` discriminator.  We keep them as
dicts (not pydantic models) so the hot path has zero per-message object
overhead; the constants and builders here are the single source of truth for the
shape of each one.
"""

from __future__ import annotations

from typing import Any

from .claims import Claim

# ----- client -> server -----------------------------------------------------
HELLO = "hello"
CLAIM = "claim"

# ----- server -> client -----------------------------------------------------
READY = "ready"
CHUNK = "chunk"
END_OF_STREAM = "end_of_stream"
DONE = "done"
CUT = "cut"
ERROR = "error"


def hello(channel_id: str, public_key: str, prompt: str, **params: Any) -> dict[str, Any]:
    """Client opens a session against a channel it has already funded."""
    return {
        "type": HELLO,
        "channel_id": channel_id,
        "public_key": public_key,
        "prompt": prompt,
        "params": params,
    }


def ready(
    *,
    drops_per_token: int,
    capacity_drops: int,
    max_outstanding_drops: int,
    stall_timeout: float,
    provider: str,
    network: str,
) -> dict[str, Any]:
    """Server accepts and states its terms."""
    return {
        "type": READY,
        "drops_per_token": drops_per_token,
        "capacity_drops": capacity_drops,
        "max_outstanding_drops": max_outstanding_drops,
        "stall_timeout": stall_timeout,
        "provider": provider,
        "network": network,
    }


def chunk(*, text: str, tokens_sent: int, owed_drops: int) -> dict[str, Any]:
    """A batch of generated text plus the running totals the client signs against."""
    return {
        "type": CHUNK,
        "text": text,
        "tokens_sent": tokens_sent,
        "owed_drops": owed_drops,
    }


def claim_msg(claim: Claim) -> dict[str, Any]:
    """Client's cumulative authorization for everything received so far."""
    return {"type": CLAIM, "claim": claim.to_wire()}


def end_of_stream(*, tokens_sent: int, owed_drops: int) -> dict[str, Any]:
    """Generation is finished; the client owes a final claim for ``owed_drops``."""
    return {"type": END_OF_STREAM, "tokens_sent": tokens_sent, "owed_drops": owed_drops}


def done(*, tokens_sent: int, paid_drops: int, claims_accepted: int) -> dict[str, Any]:
    """Session complete; provider holds the final claim, ready to settle."""
    return {
        "type": DONE,
        "tokens_sent": tokens_sent,
        "paid_drops": paid_drops,
        "claims_accepted": claims_accepted,
    }


def cut(*, reason: str, owed_drops: int, paid_drops: int) -> dict[str, Any]:
    """Stream terminated early (stall or bad claim). Provider keeps what was paid."""
    return {
        "type": CUT,
        "reason": reason,
        "owed_drops": owed_drops,
        "paid_drops": paid_drops,
    }


def error(message: str) -> dict[str, Any]:
    return {"type": ERROR, "message": message}
