"""Open / fund / look up / close a payment channel — the gray bookends.

These are the only functions that submit transactions to the ledger.  Opening
and (final) closing are the two on-ledger transactions that bracket a whole
session; everything between them is off-ledger claims.

They are synchronous (``xrpl-py``'s ``JsonRpcClient`` + ``submit_and_wait``)
because they're one-shot and reliability matters more than concurrency here.
Async callers (the FastAPI server) run them with ``asyncio.to_thread``.

Note on "close": XRPL has no ``PaymentChannelClose`` transaction.  A channel is
closed through ``PaymentChannelClaim`` with the ``tfClose`` flag — the
*destination* closing this way returns the unclaimed remainder to the source
immediately; the *source* closing this way only *requests* closure and the
channel actually expires after its ``settle_delay``.  Final settlement (redeem
the last claim, then close) lives in :mod:`xrpl_stream_pay.settle`.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from pathlib import Path

from xrpl.clients import JsonRpcClient
from xrpl.models.requests import LedgerEntry
from xrpl.models.transactions import (
    PaymentChannelClaim,
    PaymentChannelCreate,
    PaymentChannelFund,
)
from xrpl.models.transactions.payment_channel_claim import PaymentChannelClaimFlag
from xrpl.transaction import submit_and_wait
from xrpl.wallet import Wallet

from .errors import ChannelError
from .network import NETWORKS, TESTNET, Network


@dataclass(frozen=True)
class ChannelInfo:
    """Everything a session needs to know about an open channel."""

    channel_id: str
    source: str
    destination: str
    public_key: str
    """The key registered on the channel — claims must verify under this."""
    capacity_drops: int
    """Total drops locked in the channel; the ceiling for any claim."""
    settle_delay: int
    open_tx_hash: str
    network: Network = TESTNET

    @property
    def explorer_url(self) -> str:
        return self.network.channel_url(self.channel_id)

    def to_dict(self) -> dict[str, object]:
        """JSON-serializable form (network stored by name)."""
        return {
            "channel_id": self.channel_id,
            "source": self.source,
            "destination": self.destination,
            "public_key": self.public_key,
            "capacity_drops": self.capacity_drops,
            "settle_delay": self.settle_delay,
            "open_tx_hash": self.open_tx_hash,
            "network": self.network.name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ChannelInfo":
        return cls(
            channel_id=data["channel_id"],
            source=data["source"],
            destination=data["destination"],
            public_key=data["public_key"],
            capacity_drops=int(data["capacity_drops"]),
            settle_delay=int(data["settle_delay"]),
            open_tx_hash=data["open_tx_hash"],
            network=NETWORKS.get(data.get("network", "testnet"), TESTNET),
        )


@dataclass(frozen=True)
class ChannelState:
    """Live on-ledger state of a channel (from a ``ledger_entry`` lookup)."""

    channel_id: str
    source: str
    destination: str
    public_key: str
    amount_drops: int
    """Total locked (== :attr:`ChannelInfo.capacity_drops`)."""
    balance_drops: int
    """Total already paid out to the destination via redeemed claims."""

    @property
    def claimable_drops(self) -> int:
        """Drops still pullable by the destination (capacity minus claimed)."""
        return self.amount_drops - self.balance_drops


def _new_client(network: Network) -> JsonRpcClient:
    return JsonRpcClient(network.json_rpc)


def _channel_id_from_meta(response) -> str:
    """Pull the created PayChannel's id out of transaction metadata."""
    meta = response.result.get("meta") or response.result.get("metaData") or {}
    for node in meta.get("AffectedNodes", []):
        created = node.get("CreatedNode")
        if created and created.get("LedgerEntryType") == "PayChannel":
            return created["LedgerIndex"]
    raise ChannelError("PaymentChannelCreate succeeded but no PayChannel node in metadata")


def _require_success(response, what: str) -> None:
    code = response.result.get("meta", {}).get("TransactionResult") or response.result.get(
        "engine_result"
    )
    if not response.is_successful() or (code is not None and code != "tesSUCCESS"):
        raise ChannelError(f"{what} failed: {code or response.result}")


def open_channel(
    sender: Wallet,
    destination: str,
    capacity_drops: int,
    *,
    settle_delay: int = 60,
    public_key: str | None = None,
    network: Network = TESTNET,
    client: JsonRpcClient | None = None,
) -> ChannelInfo:
    """Open and fund a channel (``PaymentChannelCreate``). On-ledger transaction #1.

    ``capacity_drops`` XRP is locked up front; it bounds the total the stream can
    cost.  ``public_key`` defaults to the sender's key, which is what claims will
    be signed with.  ``settle_delay`` is how long (seconds) the source must wait
    to reclaim funds if it closes unilaterally.
    """
    client = client or _new_client(network)
    pub = public_key or sender.public_key
    tx = PaymentChannelCreate(
        account=sender.address,
        amount=str(capacity_drops),
        destination=destination,
        settle_delay=settle_delay,
        public_key=pub,
    )
    response = submit_and_wait(tx, client, sender)
    _require_success(response, "PaymentChannelCreate")
    channel_id = _channel_id_from_meta(response)
    return ChannelInfo(
        channel_id=channel_id,
        source=sender.address,
        destination=destination,
        public_key=pub,
        capacity_drops=capacity_drops,
        settle_delay=settle_delay,
        open_tx_hash=response.result["hash"],
        network=network,
    )


def fund_channel(
    sender: Wallet,
    channel_id: str,
    additional_drops: int,
    *,
    network: Network = TESTNET,
    client: JsonRpcClient | None = None,
) -> str:
    """Top up an open channel (``PaymentChannelFund``). Returns the tx hash.

    Lets a long session extend its budget mid-flight without opening a new
    channel.  Only the source can fund.
    """
    client = client or _new_client(network)
    tx = PaymentChannelFund(
        account=sender.address,
        channel=channel_id,
        amount=str(additional_drops),
    )
    response = submit_and_wait(tx, client, sender)
    _require_success(response, "PaymentChannelFund")
    return response.result["hash"]


def lookup_channel(
    channel_id: str,
    *,
    network: Network = TESTNET,
    client: JsonRpcClient | None = None,
) -> ChannelState:
    """Read a channel's current on-ledger state (``ledger_entry``).

    The provider uses this to confirm a client's channel really pays *it*, with
    the expected key, and how much capacity is left — before streaming a single
    token.
    """
    client = client or _new_client(network)
    response = client.request(LedgerEntry(payment_channel=channel_id))
    if not response.is_successful():
        raise ChannelError(f"channel {channel_id} not found: {response.result}")
    node = response.result["node"]
    return ChannelState(
        channel_id=channel_id,
        source=node["Account"],
        destination=node["Destination"],
        public_key=node["PublicKey"],
        amount_drops=int(node["Amount"]),
        balance_drops=int(node.get("Balance", "0")),
    )


def close_channel(
    wallet: Wallet,
    channel_id: str,
    *,
    network: Network = TESTNET,
    client: JsonRpcClient | None = None,
) -> str:
    """Close a channel via ``PaymentChannelClaim`` + ``tfClose``. Returns tx hash.

    If ``wallet`` is the destination, the channel closes now and any remainder
    returns to the source.  If ``wallet`` is the source, this only *requests*
    closure; the channel expires after ``settle_delay``.  For a normal end of
    session, prefer :func:`xrpl_stream_pay.settle.settle`, which redeems the
    final claim and closes in one transaction.
    """
    client = client or _new_client(network)
    tx = PaymentChannelClaim(
        account=wallet.address,
        channel=channel_id,
        flags=PaymentChannelClaimFlag.TF_CLOSE,
    )
    response = submit_and_wait(tx, client, wallet)
    _require_success(response, "PaymentChannelClaim(close)")
    return response.result["hash"]


def ensure_capacity(
    sender: Wallet,
    channel: ChannelInfo,
    min_capacity_drops: int,
    *,
    buffer_drops: int = 0,
    network: Network = TESTNET,
    client: JsonRpcClient | None = None,
) -> ChannelInfo:
    """Top up a long-lived channel if it's about to run dry. Returns updated info.

    A channel that lives across many sessions eventually approaches its capacity
    (claims are cumulative).  Call this between sessions with the cumulative
    amount you expect to authorize next; if the channel can't cover it, this funds
    the shortfall (plus ``buffer_drops``) via ``PaymentChannelFund`` and returns a
    new :class:`ChannelInfo` with the larger capacity.  A no-op when there's room.
    """
    if channel.capacity_drops >= min_capacity_drops:
        return channel
    shortfall = min_capacity_drops + buffer_drops - channel.capacity_drops
    fund_channel(sender, channel.channel_id, shortfall, network=network, client=client)
    return replace(channel, capacity_drops=channel.capacity_drops + shortfall)


def save_channel(path: str | os.PathLike[str], channel: ChannelInfo) -> None:
    """Persist a channel handle so a restarted agent can reuse the same channel."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(channel.to_dict(), indent=2))


def load_channel(path: str | os.PathLike[str]) -> ChannelInfo | None:
    """Load a persisted channel handle, or ``None`` if absent/unreadable."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        return ChannelInfo.from_dict(json.loads(p.read_text()))
    except (json.JSONDecodeError, OSError, KeyError, ValueError):
        return None
