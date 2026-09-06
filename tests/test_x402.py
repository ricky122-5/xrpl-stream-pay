"""x402 HTTP profile: 402 challenge, pay-and-retry, cumulative claims."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from xrpl.wallet import Wallet

from xrpl_stream_pay import ChannelPaywall, MemoryClaimStore, X402Client
from xrpl_stream_pay.channel import ChannelState
from xrpl_stream_pay.claims import Claim, authorize_claim
from xrpl_stream_pay.errors import BudgetExceeded
from xrpl_stream_pay.x402 import (
    CHANNEL_HEADER,
    PAYMENT_HEADER,
    PAYMENT_RESPONSE_HEADER,
    SCHEME,
    add_paid_route,
    encode_payment,
)

PRICE = 5000


@pytest.fixture
def paywall() -> ChannelPaywall:
    return ChannelPaywall(
        provider_address="rPROVIDERxxxxxxxxxxxxxxxxxxxxxxxxxx",
        claim_store=MemoryClaimStore(),
        verify_on_chain=False,
    )


@pytest.fixture
def app(paywall: ChannelPaywall) -> FastAPI:
    application = FastAPI()

    def quote(request: Request) -> dict:
        return {"symbol": request.query_params.get("symbol", "XRP"), "price": 0.52}

    add_paid_route(application, paywall, "/quote", PRICE, quote)
    return application


def test_unpaid_request_gets_402_with_terms(app):
    http = TestClient(app)
    resp = http.get("/quote")
    assert resp.status_code == 402
    accepts = resp.json()["accepts"][0]
    assert accepts["scheme"] == SCHEME
    assert accepts["maxAmountRequired"] == str(PRICE)
    assert accepts["payTo"].startswith("rPROVIDER")


def test_pay_and_retry_succeeds(app, paywall, fake_channel, wallet):
    client = X402Client.from_wallet(wallet, fake_channel, http=TestClient(app))
    resp = client.get("/quote?symbol=XRP")
    assert resp.status_code == 200
    assert resp.json()["symbol"] == "XRP"
    assert PAYMENT_RESPONSE_HEADER in resp.headers
    assert client.authorized_drops == PRICE
    assert client.requests_paid == 1
    assert paywall.baseline(fake_channel.channel_id) == PRICE


def test_repeated_requests_accumulate(app, paywall, fake_channel, wallet):
    client = X402Client.from_wallet(wallet, fake_channel, http=TestClient(app))
    for i in range(1, 4):
        resp = client.get("/quote")
        assert resp.status_code == 200
        assert client.authorized_drops == PRICE * i
    # Three paid requests, one cumulative claim worth 3x the price.
    assert paywall.baseline(fake_channel.channel_id) == PRICE * 3
    assert client.latest_claim.amount_drops == PRICE * 3


def test_forged_claim_is_rejected(app, fake_channel, wallet):
    attacker = type(wallet).create()
    http = TestClient(app)
    # Signed by the attacker but presented under the channel's real key.
    forged = authorize_claim(
        fake_channel.channel_id, PRICE, attacker.private_key, fake_channel.public_key
    )
    resp = http.get(
        "/quote",
        headers={CHANNEL_HEADER: fake_channel.channel_id, PAYMENT_HEADER: encode_payment(forged)},
    )
    assert resp.status_code == 402  # signature check fails -> back to challenge


def test_underpayment_is_rejected(app, fake_channel, wallet):
    http = TestClient(app)
    # A valid claim, but for less than the price.
    cheap = authorize_claim(fake_channel.channel_id, PRICE - 1, wallet.private_key, fake_channel.public_key)
    resp = http.get(
        "/quote",
        headers={CHANNEL_HEADER: fake_channel.channel_id, PAYMENT_HEADER: encode_payment(cheap)},
    )
    assert resp.status_code == 402


def test_budget_cap_blocks_request(app, fake_channel, wallet):
    client = X402Client.from_wallet(wallet, fake_channel, http=TestClient(app), max_budget_drops=PRICE)
    assert client.get("/quote").status_code == 200  # first request fits the budget
    with pytest.raises(BudgetExceeded):
        client.get("/quote")  # second would need 2x the price


def test_attacker_key_on_known_channel_is_rejected(monkeypatch, wallet):
    """Regression: with on-ledger verification, a claim signed by a different key
    on a publicly-known channel id must be rejected on EVERY request — not just
    served because the channel was validated once for the honest payer."""
    victim = wallet
    attacker = Wallet.create()
    provider_addr = "rPROVIDERxxxxxxxxxxxxxxxxxxxxxxxxxx"
    channel_id = "AB" * 32

    # The channel's real, on-ledger key is the victim's.
    def fake_lookup(cid, network=None, client=None):
        return ChannelState(
            channel_id=cid,
            source="rSOURCExxxxxxxxxxxxxxxxxxxxxxxxxxx",
            destination=provider_addr,
            public_key=victim.public_key,
            amount_drops=10**9,
            balance_drops=0,
        )

    monkeypatch.setattr("xrpl_stream_pay.x402.lookup_channel", fake_lookup)
    paywall = ChannelPaywall(
        provider_address=provider_addr, claim_store=MemoryClaimStore(), verify_on_chain=True
    )
    application = FastAPI()
    add_paid_route(application, paywall, "/quote", PRICE, lambda r: {"ok": True})
    http = TestClient(application)

    def pay(w, amount):
        claim = authorize_claim(channel_id, amount, w.private_key, w.public_key)
        return http.get(
            "/quote",
            headers={CHANNEL_HEADER: channel_id, PAYMENT_HEADER: encode_payment(claim)},
        )

    # 1. Honest payer is served, and validates the channel on-ledger.
    assert pay(victim, PRICE).status_code == 200
    assert paywall.baseline(channel_id) == PRICE

    # 2. Attacker signs with their OWN key on the same channel -> must be 402.
    assert pay(attacker, PRICE * 5).status_code == 402
    # Store not poisoned: baseline unchanged, still the victim's amount.
    assert paywall.baseline(channel_id) == PRICE
    assert paywall.claim_store.get(channel_id).public_key == victim.public_key

    # 3. Honest payer's next request still works (not locked out).
    assert pay(victim, PRICE * 2).status_code == 200
    assert paywall.baseline(channel_id) == PRICE * 2


def test_resync_to_server_baseline(app, paywall, fake_channel, wallet):
    # Server already saw a higher claim (e.g. from a prior client instance).
    paywall.claim_store.put(
        authorize_claim(fake_channel.channel_id, PRICE * 5, wallet.private_key, fake_channel.public_key)
    )
    # A fresh client (authorized_drops=0) must catch up to the server's baseline.
    client = X402Client.from_wallet(wallet, fake_channel, http=TestClient(app))
    resp = client.get("/quote")
    assert resp.status_code == 200
    assert client.authorized_drops == PRICE * 6  # baseline 5x + this request
