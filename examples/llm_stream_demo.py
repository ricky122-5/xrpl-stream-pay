#!/usr/bin/env python3
"""Same machinery as the mock demo, but the tokens come from a real model.

The only thing that changes versus ``mock_stream_demo.py`` is the provider's
token generator: instead of synthetic words it streams from Anthropic's API, so
you are paying — per token, over a payment channel — for actual model output.

    pip install -e ".[llm]"
    export ANTHROPIC_API_KEY=sk-ant-...
    python examples/llm_stream_demo.py --prompt "Explain payment channels in 3 sentences."

Billing note: this demo bills one unit per streamed text delta. A production
provider would meter on the model's real token usage (e.g. the ``usage`` on the
final stream event); the payment mechanism is identical either way.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import threading
from collections.abc import AsyncIterator
from typing import Any

import common  # local demo helpers

from xrpl_stream_pay import (
    MeterConfig,
    ProviderConfig,
    StreamClient,
    create_app,
    format_drops,
    open_channel,
    settle,
)
from xrpl_stream_pay.network import NETWORKS


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prompt", default="Explain XRPL payment channels in three sentences.")
    p.add_argument("--model", default="claude-haiku-4-5-20251001")
    p.add_argument("--max-tokens", type=int, default=400)
    p.add_argument("--price", type=int, default=2000, help="drops per streamed unit")
    p.add_argument("--trust-window", type=int, default=24)
    p.add_argument("--network", choices=sorted(NETWORKS), default="testnet")
    p.add_argument("--no-settle", action="store_true")
    return p.parse_args()


def make_anthropic_generator(model: str, max_tokens: int):
    """Build a provider generator backed by Anthropic streaming."""
    try:
        from anthropic import AsyncAnthropic
    except ImportError:  # pragma: no cover - optional dep
        print('The llm demo needs the anthropic SDK: pip install -e ".[llm]"', file=sys.stderr)
        raise SystemExit(1)

    aclient = AsyncAnthropic()

    async def generate(prompt: str, params: dict[str, Any]) -> AsyncIterator[tuple[str, int]]:
        async with aclient.messages.stream(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            async for text in stream.text_stream:
                if text:
                    # One billing unit per delta (see module docstring).
                    yield (text, 1)

    return generate


async def run_stream(ws_url, client, ticker, prompt):
    print()
    async for text in client.stream(ws_url, prompt):
        sys.stdout.write(text)
        sys.stdout.flush()
    print()
    return client.result


def main() -> None:
    args = parse_args()
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("Set ANTHROPIC_API_KEY to run the LLM demo.", file=sys.stderr)
        raise SystemExit(1)

    network = NETWORKS[args.network]
    capacity = max(args.max_tokens * args.price * 2, 200_000)

    print("xrpl-stream-pay — live LLM streaming demo")
    print("=" * 64)
    print(f"network={network.name}  model={args.model}  price={args.price} drops/unit")
    print(f"\nFunding {network.name} wallets (first run can take ~10-30s)…")
    rpc, (agent, provider) = common.fund_wallets(network)
    print(f"  agent={agent.address}  provider={provider.address}")

    print("\n[on-ledger #1] opening payment channel…")
    channel = open_channel(agent, provider.address, capacity, network=network, client=rpc)
    print(f"  {channel.explorer_url}")

    captured: dict[str, object] = {}
    done_evt = threading.Event()

    config = ProviderConfig(
        provider_address=provider.address,
        drops_per_token=args.price,
        max_outstanding_tokens=args.trust_window,
        stall_timeout=15.0,  # generous: first model token can take a moment
        verify_on_chain=True,
        network=network,
        on_session_end=lambda g: (captured.__setitem__("gate", g), done_evt.set()),
    )
    app = create_app(make_anthropic_generator(args.model, args.max_tokens), config)
    client = StreamClient.from_wallet(
        agent, channel, MeterConfig(drops_per_token=args.price, every_n_tokens=16, every_ms=500)
    )
    ticker = common.Ticker(channel, args.price)

    print(f"\n[off-ledger] prompt: {args.prompt!r}")
    with common.ServerThread(app) as server:
        result = asyncio.run(run_stream(server.ws_url, client, ticker, args.prompt))
        done_evt.wait(timeout=3.0)

    gate = captured.get("gate")
    print(f"\n{result.tokens_received} units, {result.claims_sent} claims signed off-ledger.")
    final_claim = gate.latest_claim if gate else result.final_claim
    if final_claim is None:
        print("Nothing to settle.")
        return
    print(f"Provider holds a claim for {format_drops(final_claim.amount_drops)}.")
    if args.no_settle:
        return
    print("\n[on-ledger #2] redeeming the final claim…")
    receipt = settle(provider, final_claim, close=True, network=network, client=rpc)
    print(f"  {receipt}")


if __name__ == "__main__":
    main()
