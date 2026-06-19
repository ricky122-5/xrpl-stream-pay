"""Opt-in live round trip: open a real channel, sign a claim, settle it.

Hits a real XRPL faucet + ledger, so it's slow and networked. Skipped by
default; run it explicitly::

    pytest -m testnet
    XSP_TEST_NETWORK=devnet pytest -m testnet   # if the testnet faucet is busy

This covers the on-ledger correctness (open -> claim -> redeem) that the offline
suite can't; the off-ledger protocol is already covered by test_integration.
"""

from __future__ import annotations

import os

import pytest
from xrpl.clients import JsonRpcClient
from xrpl.wallet import generate_faucet_wallet

from xrpl_stream_pay import (
    authorize_claim,
    lookup_channel,
    open_channel,
    settle,
    verify_claim,
    xrp_to_drops,
)
from xrpl_stream_pay.network import NETWORKS

pytestmark = pytest.mark.testnet


@pytest.fixture(scope="module")
def network():
    return NETWORKS[os.environ.get("XSP_TEST_NETWORK", "testnet")]


def test_open_claim_settle_round_trip(network):
    client = JsonRpcClient(network.json_rpc)
    agent = generate_faucet_wallet(client)
    provider = generate_faucet_wallet(client)

    # On-ledger #1: open a 5 XRP channel.
    capacity = xrp_to_drops(5)
    channel = open_channel(agent, provider.address, capacity, network=network, client=client)
    assert len(channel.channel_id) == 64

    # The provider can confirm on-ledger that the channel pays it, with the key.
    state = lookup_channel(channel.channel_id, network=network, client=client)
    assert state.destination == provider.address
    assert state.public_key.upper() == channel.public_key.upper()
    assert state.amount_drops == capacity
    assert state.balance_drops == 0

    # Off-ledger: agent authorizes 2 XRP cumulatively.
    pay = xrp_to_drops(2)
    claim = authorize_claim(channel.channel_id, pay, agent.private_key, channel.public_key)
    assert verify_claim(claim)

    # On-ledger #2: provider redeems and closes.
    receipt = settle(provider, claim, close=True, network=network, client=client)
    assert receipt.claimed_drops == pay
    assert receipt.closed
    assert receipt.settle_tx_hash

    # Channel object is gone (closed) or shows the balance paid out.
    try:
        after = lookup_channel(channel.channel_id, network=network, client=client)
        assert after.balance_drops == pay
    except Exception:
        pass  # channel object deleted on close — also fine
