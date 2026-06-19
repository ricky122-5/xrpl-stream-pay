#!/usr/bin/env python3
"""A channel that outlives the process. Open once, run, kill, re-run, resume.

The reuse demo keeps a channel alive within one process.  This one keeps it alive
*across* processes: the agent persists its channel handle to disk and the
provider persists its claim store, so you can run this script, exit, and run it
again — and it continues the very same channel, with claims climbing from where
they left off.  When the channel runs low it tops itself up
(``PaymentChannelFund``).  Settlement happens once, at the end, with ``--close``.

    python examples/persistent_demo.py --network devnet            # run 1: open + 3 sessions
    python examples/persistent_demo.py --network devnet            # run 2: resume + 3 more
    python examples/persistent_demo.py --network devnet --close    # settle once and close
    python examples/persistent_demo.py --network devnet --reset    # forget state, start over
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
from pathlib import Path

import common  # local demo helpers

from xrpl_stream_pay import (
    FileClaimStore,
    MeterConfig,
    ProviderConfig,
    StreamClient,
    create_app,
    ensure_capacity,
    format_drops,
    load_channel,
    lookup_channel,
    open_channel,
    save_channel,
    settle,
    xrp_to_drops,
)
from xrpl_stream_pay.network import NETWORKS

STATE_DIR = Path(".persist")
CHANNEL_FILE = STATE_DIR / "channel.json"
CLAIMS_FILE = STATE_DIR / "claims.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sessions", type=int, default=3, help="sessions to run this invocation")
    p.add_argument("--tokens", type=int, default=40)
    p.add_argument("--price", type=int, default=1000)
    p.add_argument("--delay", type=float, default=0.01)
    p.add_argument("--network", choices=sorted(NETWORKS), default="testnet")
    p.add_argument("--close", action="store_true", help="settle once and close the channel")
    p.add_argument("--reset", action="store_true", help="delete persisted state and exit")
    return p.parse_args()


def load_or_open_channel(agent, provider, capacity, network, rpc):
    """Reuse a persisted channel if it's still valid on-ledger, else open one."""
    existing = load_channel(CHANNEL_FILE)
    if existing is not None and existing.source == agent.address and existing.destination == provider.address:
        try:
            lookup_channel(existing.channel_id, network=network, client=rpc)
            print(f"  resuming channel {existing.channel_id[:16]}… (capacity {format_drops(existing.capacity_drops)})")
            return existing
        except Exception:
            print("  persisted channel is gone on-ledger; opening a fresh one")
    print("  [on-ledger] opening a new channel…")
    channel = open_channel(agent, provider.address, capacity, network=network, client=rpc)
    save_channel(CHANNEL_FILE, channel)
    print(f"  {channel.explorer_url}")
    return channel


async def run_session(url: str, client: StreamClient, prompt: str, tokens: int, delay: float):
    async for _ in client.stream(url, prompt, max_tokens=tokens, delay=delay):
        pass
    return client.result


def main() -> None:
    args = parse_args()
    if args.reset:
        shutil.rmtree(STATE_DIR, ignore_errors=True)
        print("Persisted state cleared.")
        return

    network = NETWORKS[args.network]
    session_cost = args.tokens * args.price
    start_capacity = max(session_cost * 3, xrp_to_drops(0.2))

    print("xrpl-stream-pay — persistent (cross-restart) channel demo")
    print("=" * 70)
    print(f"network={network.name}  sessions this run={args.sessions}  per-session cost {format_drops(session_cost)}")

    print(f"\nFunding {network.name} wallets…")
    rpc, (agent, provider) = common.fund_wallets(network)
    print(f"  agent={agent.address}  provider={provider.address}")

    # Provider's claim store persists across restarts — this is the key piece.
    store = FileClaimStore(CLAIMS_FILE)

    print("\nResolving channel…")
    channel = load_or_open_channel(agent, provider, start_capacity, network, rpc)
    authorized = store.baseline_drops(channel.channel_id)
    print(f"  already authorized on this channel: {format_drops(authorized)}")

    if args.close:
        claim = store.get(channel.channel_id)
        if claim is None:
            print("\nNothing authorized yet — nothing to settle.")
            return
        print(f"\n[on-ledger] settling the accumulated claim {format_drops(claim.amount_drops)} and closing…")
        receipt = settle(provider, claim, close=True, network=network, client=rpc)
        print(f"  {receipt}")
        shutil.rmtree(STATE_DIR, ignore_errors=True)
        print("  persisted state cleared; channel is closed.")
        return

    config = ProviderConfig(
        provider_address=provider.address,
        drops_per_token=args.price,
        max_outstanding_tokens=16,
        stall_timeout=8.0,
        verify_on_chain=True,
        network=network,
        claim_store=store,
    )
    app = create_app(common.mock_generator, config)
    client = StreamClient.from_wallet(
        agent, channel, MeterConfig(drops_per_token=args.price, every_n_tokens=8, every_ms=300)
    )

    print(f"\nRunning {args.sessions} sessions (state persists to {STATE_DIR}/):")
    print("-" * 70)
    with common.ServerThread(app) as server:
        for i in range(1, args.sessions + 1):
            # Top up if the next session would exceed capacity, then persist.
            need = authorized + session_cost
            topped = ensure_capacity(
                agent, channel, need, buffer_drops=session_cost * 2, network=network, client=rpc
            )
            if topped is not channel:
                channel = topped
                client.channel = channel
                client.max_budget_drops = channel.capacity_drops
                save_channel(CHANNEL_FILE, channel)
                print(f"  ↑ topped up channel to {format_drops(channel.capacity_drops)}")

            result = asyncio.run(
                run_session(server.ws_url, client, f"session {i}", args.tokens, args.delay)
            )
            authorized = result.final_claim.amount_drops if result.final_claim else authorized
            print(f"  session {i:>2}: +{args.tokens} tok │ cumulative authorized {format_drops(authorized):>12} (persisted)")

    print("-" * 70)
    print(f"\nChannel left OPEN. Provider has persisted a claim for {format_drops(authorized)}.")
    print("Run this script again to add more sessions on the same channel,")
    print("or with --close to settle it once on-ledger and close.")


if __name__ == "__main__":
    main()
