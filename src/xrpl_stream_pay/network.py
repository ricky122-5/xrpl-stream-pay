"""Network presets and small unit helpers.

XRP's smallest unit is the *drop*: 1 XRP = 1,000,000 drops.  All accounting in
this library is done in integer drops so there is never a float rounding error
in a signed claim.
"""

from __future__ import annotations

from dataclasses import dataclass

DROPS_PER_XRP = 1_000_000


@dataclass(frozen=True)
class Network:
    """A rippled cluster plus the matching block explorer."""

    name: str
    json_rpc: str
    faucet_host: str | None
    explorer: str

    def tx_url(self, tx_hash: str) -> str:
        """Explorer link for a transaction hash."""
        return f"{self.explorer}/transactions/{tx_hash}"

    def channel_url(self, channel_id: str) -> str:
        """Explorer link for a payment-channel object."""
        return f"{self.explorer}/objects/{channel_id}"

    def account_url(self, address: str) -> str:
        """Explorer link for an account."""
        return f"{self.explorer}/accounts/{address}"


TESTNET = Network(
    name="testnet",
    json_rpc="https://s.altnet.rippletest.net:51234",
    faucet_host=None,  # xrpl-py picks the right faucet for the altnet client
    explorer="https://testnet.xrpl.org",
)

DEVNET = Network(
    name="devnet",
    json_rpc="https://s.devnet.rippletest.net:51234",
    faucet_host=None,
    explorer="https://devnet.xrpl.org",
)

NETWORKS = {n.name: n for n in (TESTNET, DEVNET)}


def xrp_to_drops(xrp: float | int) -> int:
    """Convert XRP to integer drops, rounding to the nearest drop."""
    return int(round(float(xrp) * DROPS_PER_XRP))


def drops_to_xrp(drops: int) -> float:
    """Convert integer drops to XRP (for display only — never for accounting)."""
    return drops / DROPS_PER_XRP


def format_drops(drops: int) -> str:
    """Human-friendly ``1234567 -> '1.234567 XRP'`` rendering."""
    return f"{drops_to_xrp(drops):.6f} XRP"
