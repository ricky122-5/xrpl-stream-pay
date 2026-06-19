"""Claims: local signing/verification must match rippled's claim format exactly."""

from __future__ import annotations

import pytest
from xrpl.wallet import Wallet

from xrpl_stream_pay.claims import (
    CLAIM_PREFIX,
    Claim,
    authorize_claim,
    claim_message,
    verify_claim,
)

CHAN = "5DB01B7FFED6B67E6B0414DED11E051D2EE2B7619CE0EAA6286D67A3A4D5BDB3"


def test_claim_message_layout():
    msg = claim_message(CHAN, 1000)
    assert msg[:4] == CLAIM_PREFIX == b"CLM\x00"
    assert msg[4:36] == bytes.fromhex(CHAN)
    assert msg[36:] == (1000).to_bytes(8, "big")
    assert len(msg) == 4 + 32 + 8


def test_claim_message_rejects_bad_channel():
    with pytest.raises(ValueError):
        claim_message("DEAD", 10)


def test_sign_and_verify_roundtrip():
    w = Wallet.create()
    claim = authorize_claim(CHAN, 5000, w.private_key, w.public_key)
    assert claim.amount_drops == 5000
    assert verify_claim(claim)


def test_tampered_amount_fails():
    w = Wallet.create()
    claim = authorize_claim(CHAN, 5000, w.private_key, w.public_key)
    forged = Claim(claim.channel_id, 6000, claim.signature, claim.public_key)
    assert not verify_claim(forged)


def test_wrong_key_fails():
    signer, attacker = Wallet.create(), Wallet.create()
    claim = authorize_claim(CHAN, 5000, signer.private_key, signer.public_key)
    impersonated = Claim(claim.channel_id, claim.amount_drops, claim.signature, attacker.public_key)
    assert not verify_claim(impersonated)


def test_garbage_signature_is_false_not_exception():
    w = Wallet.create()
    claim = Claim(CHAN, 100, "not-hex-zzz", w.public_key)
    assert verify_claim(claim) is False


def test_wire_roundtrip():
    w = Wallet.create()
    claim = authorize_claim(CHAN, 777, w.private_key, w.public_key)
    assert Claim.from_wire(claim.to_wire()) == claim


def test_cumulative_claims_increase():
    w = Wallet.create()
    c1 = authorize_claim(CHAN, 100, w.private_key, w.public_key)
    c2 = authorize_claim(CHAN, 250, w.private_key, w.public_key)
    assert c2.amount_drops > c1.amount_drops
    assert verify_claim(c1) and verify_claim(c2)
    assert c1.signature != c2.signature
