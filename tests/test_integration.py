"""Full client <-> server loop over a real local websocket (no testnet).

``verify_on_chain=False`` lets us exercise the entire protocol — hello, chunks,
metered claims, backpressure, end-of-stream, settle-ready — using only local
signature checks against a throwaway keypair.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from xrpl_stream_pay import (
    MeterConfig,
    ProviderConfig,
    StreamClient,
    create_app,
    verify_claim,
)

from conftest import serve


def make_generator(n: int, text: str = "tok"):
    async def gen(prompt: str, params: dict) -> AsyncIterator[tuple[str, int]]:
        for _ in range(n):
            yield (text + " ", 1)

    return gen


def make_config(captured: dict, **kw) -> ProviderConfig:
    defaults = dict(
        provider_address="rPROVIDERdestinationADDRESSxxxxxxxx",
        drops_per_token=10,
        max_outstanding_tokens=5,
        stall_timeout=2.0,
        verify_on_chain=False,
        on_session_end=lambda g: captured.__setitem__("gate", g),
    )
    defaults.update(kw)
    return ProviderConfig(**defaults)


async def test_happy_path_pays_for_every_token(fake_channel, wallet):
    captured: dict = {}
    app = create_app(make_generator(20), make_config(captured))
    client = StreamClient.from_wallet(
        wallet, fake_channel, MeterConfig(drops_per_token=10, every_n_tokens=2, every_ms=10_000)
    )
    with serve(app) as url:
        result = await client.run(url, "hi")

    assert not result.cut
    assert result.tokens_received == 20
    assert result.claims_sent >= 1
    assert result.final_claim is not None
    assert result.final_claim.amount_drops == 200  # 20 tokens * 10 drops
    assert verify_claim(result.final_claim)
    # Provider ends up holding a claim worth the full stream.
    assert captured["gate"].paid_drops == 200
    assert captured["gate"].outstanding_drops == 0


async def test_stingy_agent_gets_cut(fake_channel, wallet):
    captured: dict = {}
    app = create_app(
        make_generator(200),
        make_config(captured, max_outstanding_tokens=3, stall_timeout=0.4),
    )
    # Meter so coarse it never sends an interim claim.
    client = StreamClient.from_wallet(
        wallet, fake_channel, MeterConfig(drops_per_token=10, every_n_tokens=10**9, every_ms=10**9)
    )
    with serve(app) as url:
        result = await client.run(url, "hi")

    assert result.cut
    assert "stall" in (result.cut_reason or "").lower() or "claim" in (result.cut_reason or "").lower()
    # Got at most the trust window plus a little, never the whole 200.
    assert result.tokens_received <= 10
    # Provider accepted no claims, so there's nothing to settle.
    assert captured["gate"].paid_drops == 0


async def test_reused_channel_accumulates_across_sessions(fake_channel, wallet):
    # One provider (one claim store), one client (one channel), three sessions.
    captured: dict = {}
    config = make_config(captured)  # default MemoryClaimStore persists across sessions
    app = create_app(make_generator(10), config)
    client = StreamClient.from_wallet(
        wallet, fake_channel, MeterConfig(drops_per_token=10, every_n_tokens=2, every_ms=10_000)
    )
    with serve(app) as url:
        r1 = await client.run(url, "s1")
        r2 = await client.run(url, "s2")
        r3 = await client.run(url, "s3")

    # Claims are cumulative over the channel's life: 10 tokens * 10 drops * 3.
    assert r1.final_claim.amount_drops == 100
    assert r2.final_claim.amount_drops == 200
    assert r3.final_claim.amount_drops == 300
    assert client.total_tokens == 30
    # The provider remembers the highest claim for the channel.
    assert config.claim_store.baseline_drops(fake_channel.channel_id) == 300
    assert captured["gate"].baseline_drops == 200  # last session started at 200


async def test_price_higher_than_agreed_is_rejected(fake_channel, wallet):
    captured: dict = {}
    # Provider charges 50, agent agreed to at most 10.
    app = create_app(make_generator(10), make_config(captured, drops_per_token=50))
    client = StreamClient.from_wallet(
        wallet, fake_channel, MeterConfig(drops_per_token=10, every_n_tokens=2, every_ms=10_000)
    )
    with serve(app) as url:
        with pytest.raises(Exception):  # BudgetExceeded surfaced from ready check
            await client.run(url, "hi")


async def test_budget_cap_blocks_oversized_claim(fake_channel, wallet):
    captured: dict = {}
    app = create_app(make_generator(100), make_config(captured))
    # Agent budgets only 50 drops total (5 tokens worth) even though channel is huge.
    client = StreamClient(
        channel=fake_channel,
        private_key=wallet.private_key,
        meter_config=MeterConfig(drops_per_token=10, every_n_tokens=2, every_ms=10_000),
        max_budget_drops=50,
    )
    with serve(app) as url:
        with pytest.raises(Exception):  # BudgetExceeded when owed passes 50
            await client.run(url, "hi")
