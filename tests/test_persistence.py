"""Agent-side channel persistence and the auto-top-up no-op path (offline)."""

from __future__ import annotations

from xrpl_stream_pay import DEVNET, ChannelInfo, ensure_capacity, load_channel, save_channel


def make_info(capacity: int = 5_000_000) -> ChannelInfo:
    return ChannelInfo(
        channel_id="A" * 64,
        source="rAGENTxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        destination="rPROVIDERxxxxxxxxxxxxxxxxxxxxxxxxxx",
        public_key="ED" + "00" * 32,
        capacity_drops=capacity,
        settle_delay=60,
        open_tx_hash="HASH",
        network=DEVNET,
    )


def test_channel_info_dict_roundtrip():
    info = make_info()
    assert ChannelInfo.from_dict(info.to_dict()) == info


def test_save_and_load_channel(tmp_path):
    info = make_info()
    path = tmp_path / "channel.json"
    save_channel(path, info)
    loaded = load_channel(path)
    assert loaded == info
    assert loaded.network is DEVNET  # network reconstructed by name


def test_load_missing_channel_is_none(tmp_path):
    assert load_channel(tmp_path / "absent.json") is None


def test_ensure_capacity_is_noop_when_sufficient():
    info = make_info(capacity=5_000_000)
    # Needs less than capacity -> no funding, no network, same handle returned.
    out = ensure_capacity(None, info, 4_000_000)  # sender unused on the no-op path
    assert out is info
