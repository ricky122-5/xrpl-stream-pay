"""Gate: backpressure, stall timeout, and claim validation."""

from __future__ import annotations

import asyncio

import pytest
from xrpl.wallet import Wallet

from xrpl_stream_pay.claims import Claim, authorize_claim
from xrpl_stream_pay.errors import PaymentError, StallTimeout
from xrpl_stream_pay.gate import StreamGate

CHAN = "5DB01B7FFED6B67E6B0414DED11E051D2EE2B7619CE0EAA6286D67A3A4D5BDB3"


def make_gate(wallet: Wallet, **kw) -> StreamGate:
    defaults = dict(
        channel_id=CHAN,
        public_key=wallet.public_key,
        drops_per_token=10,
        capacity_drops=1_000_000,
        max_outstanding_drops=50,
        stall_timeout=0.3,
    )
    defaults.update(kw)
    return StreamGate(**defaults)


def test_running_totals():
    g = make_gate(Wallet.create())
    g.record_sent(3)
    assert g.owed_drops == 30
    assert g.outstanding_drops == 30


def test_within_budget_returns_immediately():
    async def go():
        g = make_gate(Wallet.create())
        g.record_sent(5)  # owed 50 == max_outstanding -> ok
        await asyncio.wait_for(g.await_within_budget(), 0.5)
    asyncio.run(go())


def test_backpressure_releases_on_claim():
    w = Wallet.create()

    async def go():
        g = make_gate(w)
        g.record_sent(10)  # owed 100, outstanding 100 > 50

        async def pay():
            await asyncio.sleep(0.02)
            g.submit_claim(authorize_claim(CHAN, 100, w.private_key, w.public_key))

        await asyncio.gather(g.await_within_budget(), pay())
        assert g.outstanding_drops == 0
        assert g.paid_drops == 100

    asyncio.run(go())


def test_stall_timeout_cuts():
    w = Wallet.create()

    async def go():
        g = make_gate(w)
        g.record_sent(10)  # owed 100 > 50, nobody pays
        with pytest.raises(StallTimeout):
            await g.await_within_budget()

    asyncio.run(go())


def test_ignores_stale_claim_without_faulting():
    w = Wallet.create()
    g = make_gate(w)
    assert g.submit_claim(authorize_claim(CHAN, 200, w.private_key, w.public_key)) is True
    # Duplicate and lower claims are ignored (return False), not errors —
    # otherwise a replayed claim could be used to DoS the session.
    assert g.submit_claim(authorize_claim(CHAN, 200, w.private_key, w.public_key)) is False
    assert g.submit_claim(authorize_claim(CHAN, 100, w.private_key, w.public_key)) is False
    assert g.paid_drops == 200
    assert g.claims_accepted == 1


def test_rejects_bad_signature():
    w, attacker = Wallet.create(), Wallet.create()
    g = make_gate(w)
    forged = authorize_claim(CHAN, 100, attacker.private_key, w.public_key)  # wrong signer
    with pytest.raises(PaymentError):
        g.submit_claim(forged)


def test_rejects_over_capacity():
    w = Wallet.create()
    g = make_gate(w, capacity_drops=150)
    with pytest.raises(PaymentError):
        g.submit_claim(authorize_claim(CHAN, 200, w.private_key, w.public_key))


def test_rejects_wrong_channel():
    w = Wallet.create()
    g = make_gate(w)
    other = "A" * 64
    with pytest.raises(PaymentError):
        g.submit_claim(authorize_claim(other, 100, w.private_key, w.public_key))


def test_fault_propagates_to_sender():
    w = Wallet.create()

    async def go():
        g = make_gate(w)
        g.record_sent(10)  # outstanding 100 > 50, will await

        async def fail_later():
            await asyncio.sleep(0.02)
            g.fail(PaymentError("forged claim"))

        with pytest.raises(PaymentError, match="forged"):
            await asyncio.gather(g.await_within_budget(), fail_later())

    asyncio.run(go())


def test_baseline_makes_owed_cumulative():
    w = Wallet.create()
    g = make_gate(w, baseline_drops=500, max_outstanding_drops=1_000)
    assert g.paid_drops == 500  # starts already paid up to the baseline
    assert g.owed_drops == 500  # nothing sent this session yet
    assert g.outstanding_drops == 0
    g.record_sent(10)  # +100 drops this session
    assert g.owed_drops == 600
    assert g.session_paid_drops == 0


def test_claims_must_exceed_baseline():
    w = Wallet.create()
    g = make_gate(w, baseline_drops=500, max_outstanding_drops=1_000)
    # A claim at or below the baseline is stale (already authorized last session).
    assert g.submit_claim(authorize_claim(CHAN, 500, w.private_key, w.public_key)) is False
    # A claim above the baseline advances payment for the new session.
    assert g.submit_claim(authorize_claim(CHAN, 600, w.private_key, w.public_key)) is True
    assert g.paid_drops == 600
    assert g.session_paid_drops == 100


def test_drain_requires_full_payment():
    w = Wallet.create()

    async def go():
        g = make_gate(w, max_outstanding_drops=1000)
        g.record_sent(10)  # owed 100
        # within budget (1000) but not fully paid -> drain must still wait
        g.submit_claim(authorize_claim(CHAN, 60, w.private_key, w.public_key))

        async def pay_rest():
            await asyncio.sleep(0.02)
            g.submit_claim(authorize_claim(CHAN, 100, w.private_key, w.public_key))

        await asyncio.gather(g.drain(), pay_rest())
        assert g.outstanding_drops == 0

    asyncio.run(go())
