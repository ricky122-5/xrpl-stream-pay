"""Claim store keeps only the highest claim per channel."""

from __future__ import annotations

from xrpl.wallet import Wallet

from xrpl_stream_pay.claims import authorize_claim
from xrpl_stream_pay.store import MemoryClaimStore

CHAN = "5DB01B7FFED6B67E6B0414DED11E051D2EE2B7619CE0EAA6286D67A3A4D5BDB3"
OTHER = "A" * 64


def test_empty_store():
    s = MemoryClaimStore()
    assert s.get(CHAN) is None
    assert s.baseline_drops(CHAN) == 0


def test_keeps_highest_claim():
    w = Wallet.create()
    s = MemoryClaimStore()
    s.put(authorize_claim(CHAN, 100, w.private_key, w.public_key))
    s.put(authorize_claim(CHAN, 300, w.private_key, w.public_key))
    s.put(authorize_claim(CHAN, 200, w.private_key, w.public_key))  # lower — ignored
    assert s.baseline_drops(CHAN) == 300


def test_channels_are_independent():
    w = Wallet.create()
    s = MemoryClaimStore()
    s.put(authorize_claim(CHAN, 100, w.private_key, w.public_key))
    s.put(authorize_claim(OTHER, 999, w.private_key, w.public_key))
    assert s.baseline_drops(CHAN) == 100
    assert s.baseline_drops(OTHER) == 999
