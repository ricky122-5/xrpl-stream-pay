"""``xrpl-stream-pay`` command line: run a real paid-streaming provider.

    xrpl-stream-pay serve --price 2000 --network devnet

Faucets a provider wallet (or use ``--seed``), then serves a mock-token provider
that won't stream without payment.  Point any :class:`StreamClient` at the
printed ``ws://`` URL.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator
from typing import Any

from . import __version__
from .network import NETWORKS

_WORDS = (
    "A payment channel lets two parties exchange many signed claims off ledger "
    "and reconcile once, so a thousand tokens cost two transactions instead of "
    "a thousand. This stream is metered and paid per token batch in real time. "
).split()


async def _mock_generator(prompt: str, params: dict[str, Any]) -> AsyncIterator[tuple[str, int]]:
    n = int(params.get("max_tokens", 256))
    delay = float(params.get("delay", 0.03))
    for i in range(n):
        await asyncio.sleep(delay)
        yield (_WORDS[i % len(_WORDS)] + " ", 1)


def _serve(args: argparse.Namespace) -> None:
    import uvicorn
    from xrpl.clients import JsonRpcClient
    from xrpl.wallet import Wallet, generate_faucet_wallet

    from .server import ProviderConfig, create_app

    network = NETWORKS[args.network]
    if args.seed:
        wallet = Wallet.from_seed(args.seed)
    else:
        print(f"Faucet-funding a provider wallet on {network.name}…")
        wallet = generate_faucet_wallet(JsonRpcClient(network.json_rpc))
        print(f"  provider seed (save to reuse): {wallet.seed}")

    config = ProviderConfig(
        provider_address=wallet.address,
        drops_per_token=args.price,
        max_outstanding_tokens=args.max_outstanding_tokens,
        stall_timeout=args.stall_timeout,
        verify_on_chain=not args.no_verify,
        network=network,
    )
    app = create_app(_mock_generator, config)

    print("\nProvider ready:")
    print(f"  address  {wallet.address}")
    print(f"  network  {network.name}  ({network.account_url(wallet.address)})")
    print(f"  price    {args.price} drops/token")
    print(f"  endpoint ws://{args.host}:{args.port}/pay-stream\n")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="xrpl-stream-pay", description=__doc__)
    parser.add_argument("--version", action="version", version=f"xrpl-stream-pay {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run a paid-streaming provider")
    serve.add_argument("--seed", help="provider wallet seed (else faucet a new one)")
    serve.add_argument("--price", type=int, default=2000, help="drops per token")
    serve.add_argument("--network", choices=sorted(NETWORKS), default="testnet")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--max-outstanding-tokens", type=int, default=64)
    serve.add_argument("--stall-timeout", type=float, default=10.0)
    serve.add_argument("--no-verify", action="store_true", help="skip on-ledger channel checks")
    serve.set_defaults(func=_serve)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
