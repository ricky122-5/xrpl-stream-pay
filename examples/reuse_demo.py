#!/usr/bin/env python3
"""The economic payoff: one channel, many sessions, a handful of settlements.

The single-session demo opens and settles a channel per response — two on-ledger
transactions every time.  That's fine to prove the mechanism, but the real win
is a *long-lived* channel between an agent and a provider it talks to over and
over.  Claims are cumulative over the channel's whole life, so you can:

* open the channel once,
* run many streaming sessions on it (each signing claims that climb higher),
* redeem on-ledger only occasionally (when enough has accrued),
* close once at the end.

So K sessions cost roughly ``1 + settlements + 1`` on-ledger transactions
instead of ``2K``.  This demo runs K sessions on one channel and prints the
tally so you can see the amortization.

    python examples/reuse_demo.py --network devnet --sessions 6
"""

from __future__ import annotations

import argparse
import asyncio
import threading

import common  # local demo helpers

from xrpl_stream_pay import (
    MeterConfig,
    PeriodicSettler,
    ProviderConfig,
    StreamClient,
    create_app,
    format_drops,
    open_channel,
    xrp_to_drops,
)
from xrpl_stream_pay.network import NETWORKS


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sessions", type=int, default=6, help="number of streaming sessions")
    p.add_argument("--tokens", type=int, default=40, help="tokens per session")
    p.add_argument("--price", type=int, default=1000, help="drops per token")
    p.add_argument("--delay", type=float, default=0.01)
    p.add_argument(
        "--settle-every",
        type=int,
        default=2,
        help="redeem on-ledger once this many sessions' worth has accrued",
    )
    p.add_argument("--network", choices=sorted(NETWORKS), default="testnet")
    return p.parse_args()


async def run_session(url: str, client: StreamClient, prompt: str, tokens: int, delay: float):
    async for _ in client.stream(url, prompt, max_tokens=tokens, delay=delay):
        pass
    return client.result


def main() -> None:
    args = parse_args()
    network = NETWORKS[args.network]
    session_cost = args.tokens * args.price
    total_cost = session_cost * args.sessions
    capacity = max(int(total_cost * 1.3), xrp_to_drops(0.3))
    settle_threshold = session_cost * args.settle_every

    print("xrpl-stream-pay — reused-channel demo")
    print("=" * 70)
    print(f"network={network.name}  sessions={args.sessions}  tokens/session={args.tokens}  "
          f"price={args.price} drops/token")
    print(f"per-session cost {format_drops(session_cost)}  channel capacity {format_drops(capacity)}")
    print(f"settling on-ledger every ~{format_drops(settle_threshold)} accrued")

    print(f"\nFunding {network.name} wallets…")
    rpc, (agent, provider) = common.fund_wallets(network)
    print(f"  agent={agent.address}  provider={provider.address}")

    print("\n[on-ledger] opening ONE channel for all sessions…")
    channel = open_channel(agent, provider.address, capacity, network=network, client=rpc)
    print(f"  {channel.explorer_url}")

    captured: dict[str, object] = {}
    config = ProviderConfig(
        provider_address=provider.address,
        drops_per_token=args.price,
        max_outstanding_tokens=16,
        stall_timeout=8.0,
        verify_on_chain=True,
        network=network,
        on_session_end=lambda g: captured.__setitem__("gate", g),
    )
    app = create_app(common.mock_generator, config)
    client = StreamClient.from_wallet(
        agent, channel, MeterConfig(drops_per_token=args.price, every_n_tokens=8, every_ms=300)
    )
    settler = PeriodicSettler(provider, every_drops=settle_threshold, network=network, client=rpc)

    print(f"\nRunning {args.sessions} sessions on the same channel:")
    print("-" * 70)
    with common.ServerThread(app) as server:
        for i in range(1, args.sessions + 1):
            result = asyncio.run(
                run_session(server.ws_url, client, f"session {i}", args.tokens, args.delay)
            )
            cumulative = result.final_claim.amount_drops if result.final_claim else 0
            receipt = settler.maybe_settle(result.final_claim)
            tag = f"⚖ settled → {receipt.settle_tx_hash[:10]}…" if receipt else "(off-ledger only)"
            print(
                f"  session {i:>2}: +{args.tokens} tok │ cumulative authorized "
                f"{format_drops(cumulative):>12} │ {tag}"
            )

    redeems = settler.settlements  # on-ledger redeems that happened mid-run
    print("-" * 70)
    print("\n[on-ledger] final settle + close (returns the remainder to the agent)…")
    final = settler.final_settle(client.final_claim)
    if final:
        print(f"  {final}")

    on_ledger = 1 + redeems + (1 if final else 0)
    naive = 2 * args.sessions
    print("\nTally")
    print(f"  sessions run ............ {args.sessions}")
    print(f"  tokens streamed ......... {client.total_tokens}")
    print(f"  value settled on-ledger . {format_drops(settler.settled_drops)}")
    print(f"  on-ledger transactions .. {on_ledger}  (1 open + {redeems} redeems + 1 close)")
    print(f"  vs open+settle each time  {naive}")
    print(f"  amortization ............ {naive / on_ledger:.1f}x fewer on-ledger txns")


if __name__ == "__main__":
    main()
