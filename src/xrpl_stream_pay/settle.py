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

from .channel import close_channel
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
        if self.claimed_drops == 0 and self.closed:
            return (
                f"Closed channel {self.channel_id[:8]}… (remainder returned to source)"
                f"\n  {self.explorer_url}"
            )
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


class PeriodicSettler:
    """Redeem a reused channel occasionally instead of once per session.

    This is where the economics pay off: keep one long-lived channel and call
    :meth:`maybe_settle` after each session.  It only submits an on-ledger
    ``PaymentChannelClaim`` once the unsettled amount crosses ``every_drops`` (or
    when forced), so K sessions cost far fewer than K transactions.  Each redeem
    leaves the channel open (``close=False``) so the next session continues on
    top; call :meth:`final_settle` at the very end to redeem the remainder and
    close.
    """

    def __init__(
        self,
        destination: Wallet,
        *,
        every_drops: int | None = None,
        network: Network = TESTNET,
        client: JsonRpcClient | None = None,
    ) -> None:
        self.destination = destination
        self.every_drops = every_drops
        self.network = network
        self.client = client or JsonRpcClient(network.json_rpc)
        self.settled_drops = 0
        self.receipts: list[Receipt] = []

    @property
    def settlements(self) -> int:
        return len(self.receipts)

    def maybe_settle(self, claim: Claim | None, *, force: bool = False) -> Receipt | None:
        """Settle if enough has accrued (or ``force``). Returns a receipt or None."""
        if claim is None:
            return None
        pending = claim.amount_drops - self.settled_drops
        if pending <= 0:
            return None
        if not force and self.every_drops is not None and pending < self.every_drops:
            return None
        receipt = settle(
            self.destination, claim, close=False, network=self.network, client=self.client
        )
        self.settled_drops = claim.amount_drops
        self.receipts.append(receipt)
        return receipt

    def final_settle(self, claim: Claim | None) -> Receipt | None:
        """Close the channel, redeeming any remaining unsettled amount first.

        Always closes (unless there were no claims at all), so the channel's
        unspent remainder returns to the source — leaving a long-lived channel
        open would lock those funds until its settle delay.
        """
        if claim is None:
            return None
        if claim.amount_drops > self.settled_drops:
            # Remainder to redeem: one tx that both claims it and closes.
            receipt = settle(
                self.destination, claim, close=True, network=self.network, client=self.client
            )
            self.settled_drops = claim.amount_drops
        else:
            # Everything already redeemed; just close to free the remainder.
            tx_hash = close_channel(
                self.destination, claim.channel_id, network=self.network, client=self.client
            )
            receipt = Receipt(
                channel_id=claim.channel_id,
                claimed_drops=0,
                settle_tx_hash=tx_hash,
                closed=True,
                network=self.network,
            )
        self.receipts.append(receipt)
        return receipt
