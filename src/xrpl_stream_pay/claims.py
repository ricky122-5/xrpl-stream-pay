"""Off-ledger payment-channel claims — the teal part of the picture.

A *claim* is a signature that says "the holder of this channel's key authorizes
the destination to pull up to ``amount_drops`` from channel ``channel_id``."
Claims are **cumulative**: each new claim names the total authorized so far, not
the increment.  The destination only ever needs to redeem the single highest
claim, so we can throw away every intermediate one.

Why we sign locally instead of calling rippled's ``channel_authorize``:

* ``channel_authorize`` is an *admin-only* RPC (it needs the account secret on
  the server), so it isn't available on public testnet nodes.
* Signing locally means the per-token billing loop touches **no network at
  all** — it runs at raw ed25519/secp256k1 signing speed, which is the entire
  point of the design.  ``channel_verify`` exists as a public RPC, but verifying
  locally is the same cryptography without the round trip.

The signed message is exactly what rippled signs (so a claim produced here can
be redeemed by a ``PaymentChannelClaim`` transaction, and one produced by
rippled validates here): the 4-byte prefix ``CLM\\0``, the 32-byte channel id,
then the amount as a big-endian unsigned 64-bit integer.
"""

from __future__ import annotations

from dataclasses import dataclass

from xrpl.core import keypairs

# rippled's hash prefix for a payment-channel claim: ASCII "CLM" + a null byte.
CLAIM_PREFIX = b"CLM\x00"

# A channel id is a 256-bit value -> 32 bytes -> 64 hex characters.
CHANNEL_ID_HEX_LEN = 64


@dataclass(frozen=True)
class Claim:
    """A signed, cumulative authorization to pull from a channel.

    Immutable on purpose: a claim is a fact ("this much was authorized"), and
    sessions keep a running *latest* claim rather than mutating one in place.
    """

    channel_id: str
    """Hex id (64 chars) of the on-ledger PayChannel object."""

    amount_drops: int
    """Cumulative drops authorized — monotonically increasing across a session."""

    signature: str
    """Hex signature over the claim message, valid under ``public_key``."""

    public_key: str
    """Hex public key registered on the channel (the key that verifies claims)."""

    def to_wire(self) -> dict[str, object]:
        """Minimal JSON form sent across the wire each token batch.

        ``channel_id`` and ``public_key`` are fixed for a session, so the hot
        path only really needs ``amount_drops`` + ``signature``; we include all
        four so a single message is self-verifying.
        """
        return {
            "channel_id": self.channel_id,
            "amount_drops": self.amount_drops,
            "signature": self.signature,
            "public_key": self.public_key,
        }

    @classmethod
    def from_wire(cls, data: dict[str, object]) -> "Claim":
        return cls(
            channel_id=str(data["channel_id"]),
            amount_drops=int(data["amount_drops"]),  # type: ignore[arg-type]
            signature=str(data["signature"]),
            public_key=str(data["public_key"]),
        )


def claim_message(channel_id: str, amount_drops: int) -> bytes:
    """Build the exact byte string rippled signs for a channel claim."""
    if len(channel_id) != CHANNEL_ID_HEX_LEN:
        raise ValueError(
            f"channel_id must be {CHANNEL_ID_HEX_LEN} hex chars, got {len(channel_id)}"
        )
    if amount_drops < 0 or amount_drops > 0xFFFF_FFFF_FFFF_FFFF:
        raise ValueError("amount_drops out of range for a uint64")
    return CLAIM_PREFIX + bytes.fromhex(channel_id) + amount_drops.to_bytes(8, "big")


def authorize_claim(
    channel_id: str,
    amount_drops: int,
    private_key: str,
    public_key: str,
) -> Claim:
    """Sign a cumulative claim. The off-ledger equivalent of ``channel_authorize``.

    ``private_key``/``public_key`` are the channel's signing keypair — typically
    ``wallet.private_key`` / ``wallet.public_key`` of the account that opened the
    channel.
    """
    message = claim_message(channel_id, amount_drops)
    signature = keypairs.sign(message, private_key)
    return Claim(
        channel_id=channel_id,
        amount_drops=amount_drops,
        signature=signature,
        public_key=public_key,
    )


def verify_claim(claim: Claim) -> bool:
    """Check a claim's signature locally. The equivalent of ``channel_verify``.

    Returns ``True`` only if ``signature`` is a valid signature over
    ``(channel_id, amount_drops)`` under ``public_key``.  Does *not* check that
    the amount is high enough or within channel capacity — that policy lives in
    the gate (see :mod:`xrpl_stream_pay.gate`).
    """
    try:
        message = claim_message(claim.channel_id, claim.amount_drops)
        return keypairs.is_valid_message(
            message, bytes.fromhex(claim.signature), claim.public_key
        )
    except (ValueError, KeyError):
        # Malformed hex / length / amount -> simply not a valid claim.
        return False
