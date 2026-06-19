#!/usr/bin/env python3
"""End-to-end demo with synthetic tokens — no API keys, real testnet channel.

What it does, in one command:

1. funds two testnet wallets (agent + provider) from the faucet;
2. opens a real payment channel  ............................ on-ledger tx #1;
3. streams synthetic tokens through the real FastAPI gate over a websocket,
   the agent signing a fresh claim each time its meter fires  ... off-ledger;
4. redeems the single highest claim  ....................... on-ledger tx #2;
5. prints a receipt with explorer links.

Run it normally to watch a paid stream complete::

    python examples/mock_stream_demo.py

Run it stingy to watch a non-paying agent get cut off mid-stream::

    python examples/mock_stream_demo.py --stingy
"""

from __future__ import annotations

import argparse
import asyncio
import threading

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
    p.add_argument("--tokens", type=int, default=160, help="how many tokens to stream")
    p.add_argument("--price", type=int, default=2000, help="drops per token")
    p.add_argument("--delay", type=float, default=0.03, help="seconds between tokens")
    p.add_argument(
        "--trust-window",
        type=int,
        default=24,
        help="tokens the provider will stream ahead of payment before pausing",
    )
    p.add_argument(
        "--stingy",
        action="store_true",
        help="agent stops paying — provider should cut the stream",
    )
    p.add_argument("--no-settle", action="store_true", help="skip the on-ledger redeem")
    p.add_argument(
        "--network",
        choices=sorted(NETWORKS),
        default="testnet",
        help="which XRPL test network to use (devnet has a separate faucet)",
    )
    return p.parse_args()


async def run_stream(ws_url: str, client: StreamClient, ticker: common.Ticker, prompt: str, tokens: int, delay: float):
    """Consume the stream while updating the live ticker."""
    async for _text in client.stream(ws_url, prompt, max_tokens=tokens, delay=delay):
        ticker.update(
            tokens=client.tokens_received,
            authorized_drops=client.final_claim.amount_drops if client.final_claim else 0,
            claims=client.claims_sent,
        )
    ticker.done()
    return client.result


def main() -> None:
    args = parse_args()
    network = NETWORKS[args.network]
    total_cost = args.tokens * args.price
    capacity = max(int(total_cost * 1.4), 100_000)

    print("xrpl-stream-pay — mock streaming demo")
    print("=" * 64)
    print(f"network={network.name}  tokens={args.tokens}  price={args.price} drops/token")
    print(f"max stream cost ≈ {format_drops(total_cost)}  channel capacity {format_drops(capacity)}")
    print(f"\nFunding {network.name} wallets (first run can take ~10-30s)…")
    rpc, (agent, provider) = common.fund_wallets(network)
    print(f"  agent    {agent.address}")
    print(f"           {network.account_url(agent.address)}")
    print(f"  provider {provider.address}")

    print("\n[on-ledger #1] opening payment channel…")
    channel = open_channel(agent, provider.address, capacity, network=network, client=rpc)
    print(f"  channel {channel.channel_id}")
    print(f"  {channel.explorer_url}")
    print(f"  open tx {network.tx_url(channel.open_tx_hash)}")

    # Provider config. Capture the final gate so we can settle what it actually earned.
    captured: dict[str, object] = {}
    done_evt = threading.Event()

    def on_end(gate):
        captured["gate"] = gate
        done_evt.set()

    config = ProviderConfig(
        provider_address=provider.address,
        drops_per_token=args.price,
        max_outstanding_tokens=args.trust_window,
        stall_timeout=2.0 if args.stingy else 8.0,
        verify_on_chain=True,
        network=network,
        on_session_end=on_end,
    )
    app = create_app(common.mock_generator, config)

    # A stingy agent meters so rarely it never sends an interim claim.
    if args.stingy:
        meter = MeterConfig(drops_per_token=args.price, every_n_tokens=10**9, every_ms=10**9)
    else:
        meter = MeterConfig(drops_per_token=args.price, every_n_tokens=16, every_ms=400)

    client = StreamClient.from_wallet(agent, channel, meter)
    ticker = common.Ticker(channel, args.price)

    print(f"\n[off-ledger] streaming {args.tokens} tokens, paying as we go" + (" — STINGY MODE" if args.stingy else ""))
    print("-" * 64)
    with common.ServerThread(app) as server:
        result = asyncio.run(
            run_stream(server.ws_url, client, ticker, "Explain payment channels.", args.tokens, args.delay)
        )
        done_evt.wait(timeout=3.0)

    print("-" * 64)
    gate = captured.get("gate")
    if result.cut:
        print(f"✂  stream CUT: {result.cut_reason}")
        free_tokens = gate.tokens_sent if gate else result.tokens_received
        print(f"   agent received ~{free_tokens} tokens; provider's exposure was bounded to the trust window")
    else:
        print(f"✓  stream complete: {result.tokens_received} tokens, {result.claims_sent} claims signed")

    final_claim = gate.latest_claim if gate else result.final_claim
    if final_claim is None:
        print("\nNothing to settle — provider accepted no claims.")
        return
    print(f"\nProvider holds a claim for {format_drops(final_claim.amount_drops)} "
          f"(off-ledger, never broadcast until now).")

    if args.no_settle:
        print("Skipping settlement (--no-settle).")
        return

    print("\n[on-ledger #2] redeeming the final claim…")
    receipt = settle(provider, final_claim, close=True, network=network, client=rpc)
    print(f"  {receipt}")
    print("\nTwo on-ledger transactions total. Everything else was signatures.")


if __name__ == "__main__":
    main()
