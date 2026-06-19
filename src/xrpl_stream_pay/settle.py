"""Redeem the final claim — the second and last on-ledger transaction.

After a whole session of off-ledger claims, the provider holds exactly one that
matters: the highest one.  Settling submits a single ``PaymentChannelClaim``
that credits the provider that amount and (optionally) closes the channel,
returning the unspent remainder to the client.  N tokens billed, one redeem.
"""

from __future__ import annotations

from dataclasses import dataclass

from xrpl.clients import JsonRpcClient
from xrpl.models.transactions import PaymentChannelClaim
from xrpl.models.transactions.payment_channel_claim import PaymentChannelClaimFlag
from xrpl.transaction import submit_and_wait
from xrpl.wallet import Wallet

from .claims import Claim, verify_claim
from .errors import PaymentError
from .network import TESTNET, Network, format_drops


@dataclass(frozen=True)
class Receipt:
    """The outcome of settling a session — print it, log it, return it."""

    channel_id: str
    claimed_drops: int
    settle_tx_hash: str
    closed: bool
    network: Network = TESTNET

    @property
    def explorer_url(self) -> str:
        return self.network.tx_url(self.settle_tx_hash)

    def __str__(self) -> str:
        state = "closed" if self.closed else "left open"
        return (
            f"Settled {format_drops(self.claimed_drops)} on channel "
            f"{self.channel_id[:8]}… ({state})\n  {self.explorer_url}"
        )


def settle(
    destination: Wallet,
    claim: Claim,
    *,
    close: bool = True,
    network: Network = TESTNET,
    client: JsonRpcClient | None = None,
) -> Receipt:
    """Redeem ``claim`` on-ledger as the channel's destination.

    Validates the signature locally first (no point paying a fee to submit a
    claim rippled will reject), then submits one ``PaymentChannelClaim`` with
    ``Balance`` == ``Amount`` == the claim amount.  With ``close=True`` it also
    sets ``tfClose`` so the channel settles and any remainder returns to the
    client in the same transaction.
    """
    if not verify_claim(claim):
        raise PaymentError("refusing to settle: claim signature is invalid")

    client = client or JsonRpcClient(network.json_rpc)
    amount = str(claim.amount_drops)
    tx = PaymentChannelClaim(
        account=destination.address,
        channel=claim.channel_id,
        balance=amount,
        amount=amount,
        signature=claim.signature,
        public_key=claim.public_key,
        flags=PaymentChannelClaimFlag.TF_CLOSE if close else 0,
    )
    response = submit_and_wait(tx, client, destination)
    code = response.result.get("meta", {}).get("TransactionResult")
    if not response.is_successful() or code != "tesSUCCESS":
        raise PaymentError(f"settlement failed: {code or response.result}")

    return Receipt(
        channel_id=claim.channel_id,
        claimed_drops=claim.amount_drops,
        settle_tx_hash=response.result["hash"],
        closed=close,
        network=network,
    )
