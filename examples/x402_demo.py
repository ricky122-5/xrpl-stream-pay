#!/usr/bin/env python3
"""Pay for HTTP requests with channel claims, x402 style — no per-request tx.

A paywalled endpoint answers an unpaid request with ``402 Payment Required``.
The client signs a channel claim and retries; the server verifies and serves.
Because the claims are cumulative, hitting the endpoint many times costs **zero**
on-ledger transactions during the requests — the provider settles once at the
end.  This is the HTTP-request analogue of the streaming demo, and it speaks the
emerging x402 convention so it can interoperate with that tooling.

    python examples/x402_demo.py --network devnet --requests 8
"""

from __future__ import annotations

import argparse

import common  # local demo helpers
from fastapi import FastAPI, Request

from xrpl_stream_pay import (
    ChannelPaywall,
    MemoryClaimStore,
    X402Client,
    add_paid_route,
    format_drops,
    open_channel,
    settle,
    xrp_to_drops,
)
from xrpl_stream_pay.network import NETWORKS
from xrpl_stream_pay.x402 import PAYMENT_RESPONSE_HEADER


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--requests", type=int, default=8, help="how many paid requests to make")
    p.add_argument("--price", type=int, default=3000, help="drops per request")
    p.add_argument("--network", choices=sorted(NETWORKS), default="testnet")
    p.add_argument("--no-settle", action="store_true")
    return p.parse_args()


def build_app(paywall: ChannelPaywall, price: int) -> FastAPI:
    app = FastAPI()

    def quote(request: Request) -> dict:
        symbol = request.query_params.get("symbol", "XRP")
        return {"symbol": symbol, "price_usd": 0.52, "served": True}

    add_paid_route(app, paywall, "/quote", price, quote)
    return app


def main() -> None:
    args = parse_args()
    network = NETWORKS[args.network]
    capacity = max(args.requests * args.price * 3, xrp_to_drops(0.1))

    print("xrpl-stream-pay — x402 HTTP payment demo")
    print("=" * 66)
    print(f"network={network.name}  requests={args.requests}  price={args.price} drops/request")

    print(f"\nFunding {network.name} wallets…")
    rpc, (agent, provider) = common.fund_wallets(network)
    print(f"  agent={agent.address}  provider={provider.address}")

    print("\n[on-ledger] opening payment channel…")
    channel = open_channel(agent, provider.address, capacity, network=network, client=rpc)
    print(f"  {channel.explorer_url}")

    store = MemoryClaimStore()
    paywall = ChannelPaywall(
        provider_address=provider.address,
        network=network,
        claim_store=store,
        verify_on_chain=True,
    )
    app = build_app(paywall, args.price)

    print(f"\n[off-ledger] making {args.requests} paid HTTP requests (402 → pay → 200):")
    print("-" * 66)
    with common.ServerThread(app) as server:
        base = f"http://{server.host}:{server.port}"
        client = X402Client.from_wallet(agent, channel)
        for i in range(1, args.requests + 1):
            resp = client.get(f"{base}/quote?symbol=XRP")
            paid = resp.headers.get(PAYMENT_RESPONSE_HEADER, "")
            print(
                f"  request {i:>2}: HTTP {resp.status_code} │ "
                f"cumulative authorized {format_drops(client.authorized_drops):>12} │ "
                f"{'paywalled' if paid else 'free'}"
            )
        client.close()

    print("-" * 66)
    claim = store.get(channel.channel_id)
    print(f"\n{args.requests} requests served. Provider holds one claim for "
          f"{format_drops(claim.amount_drops)} (no per-request on-ledger tx).")

    if args.no_settle:
        return
    print("\n[on-ledger] settling once and closing…")
    receipt = settle(provider, claim, close=True, network=network, client=rpc)
    print(f"  {receipt}")
    print(f"\nTotal on-ledger transactions: 2 (open + settle) for {args.requests} paid requests.")


if __name__ == "__main__":
    main()
