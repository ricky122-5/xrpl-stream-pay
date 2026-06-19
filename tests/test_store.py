"""Claim store keeps only the highest claim per channel."""

from __future__ import annotations

from xrpl.wallet import Wallet

from xrpl_stream_pay.claims import authorize_claim, verify_claim
from xrpl_stream_pay.store import FileClaimStore, MemoryClaimStore

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


def test_file_store_survives_reload(tmp_path):
    w = Wallet.create()
    path = tmp_path / "claims.json"

    s1 = FileClaimStore(path)
    s1.put(authorize_claim(CHAN, 100, w.private_key, w.public_key))
    s1.put(authorize_claim(CHAN, 300, w.private_key, w.public_key))

    # A brand-new store from the same file (simulating a restart) remembers it.
    s2 = FileClaimStore(path)
    assert s2.baseline_drops(CHAN) == 300
    reloaded = s2.get(CHAN)
    assert reloaded is not None and verify_claim(reloaded)


def test_file_store_keeps_highest_across_reloads(tmp_path):
    w = Wallet.create()
    path = tmp_path / "claims.json"
    FileClaimStore(path).put(authorize_claim(CHAN, 500, w.private_key, w.public_key))
    # Lower claim after "restart" must not overwrite the higher one.
    s = FileClaimStore(path)
    s.put(authorize_claim(CHAN, 200, w.private_key, w.public_key))
    assert FileClaimStore(path).baseline_drops(CHAN) == 500


def test_file_store_missing_file_is_empty(tmp_path):
    assert FileClaimStore(tmp_path / "nope.json").baseline_drops(CHAN) == 0
