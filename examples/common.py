"""Shared helpers for the demos: faucet wallets, an in-process server, a ticker.

Kept out of the package itself because it's demo glue (running uvicorn in a
thread, drawing a terminal status line) rather than library surface.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import uvicorn
from xrpl.clients import JsonRpcClient
from xrpl.models.requests import AccountInfo
from xrpl.wallet import Wallet, generate_faucet_wallet

from xrpl_stream_pay import ChannelInfo, Network, format_drops

# ---------------------------------------------------------------------------
# Mock token generator
# ---------------------------------------------------------------------------

_PARAGRAPH = (
    "A payment channel locks a pool of XRP between two parties so they can "
    "exchange many signed promises off the ledger and reconcile only once. "
    "Each promise, called a claim, names the cumulative amount authorized so "
    "far, so the destination only ever needs to redeem the single highest one. "
    "For a streaming response this is ideal: the provider hands over tokens, the "
    "agent signs a slightly larger claim, and neither party touches the network "
    "until the very end. The cost of a thousand tokens is two transactions and a "
    "thousand signatures, not a thousand transactions. "
).split()


async def mock_generator(
    prompt: str, params: dict[str, Any]
) -> AsyncIterator[tuple[str, int]]:
    """Yield ``(text, n_tokens)`` synthetic tokens, one word at a time."""
    n = int(params.get("max_tokens", 160))
    delay = float(params.get("delay", 0.03))
    for i in range(n):
        word = _PARAGRAPH[i % len(_PARAGRAPH)]
        await asyncio.sleep(delay)
        yield (word + " ", 1)


# ---------------------------------------------------------------------------
# Testnet wallets
# ---------------------------------------------------------------------------


_CACHE = Path(os.environ.get("XSP_WALLET_CACHE", ".demo_wallets.json"))


def _balance_drops(client: JsonRpcClient, address: str) -> int:
    """Account balance in drops, or 0 if the account isn't funded yet."""
    resp = client.request(AccountInfo(account=address, ledger_index="validated"))
    if not resp.is_successful():
        return 0
    return int(resp.result["account_data"]["Balance"])


def _faucet_with_retry(
    client: JsonRpcClient, wallet: Wallet | None, *, tries: int = 5
) -> Wallet:
    """Fund a (new or existing) wallet, backing off on the faucet's 429s."""
    delay = 4.0
    for attempt in range(1, tries + 1):
        try:
            return generate_faucet_wallet(client, wallet=wallet, debug=False)
        except Exception as exc:  # noqa: BLE001 - faucet is flaky; retry on anything
            if attempt == tries:
                raise
            print(
                f"    faucet busy ({type(exc).__name__}); retrying in {delay:.0f}s "
                f"[{attempt}/{tries - 1}]…",
                flush=True,
            )
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    raise RuntimeError("unreachable")


def fund_wallets(
    network: Network, *, count: int = 2, min_drops: int = 20_000_000, use_cache: bool = True
) -> tuple[JsonRpcClient, list[Wallet]]:
    """Get ``count`` funded testnet wallets, reusing cached ones when possible.

    Caching seeds in a local (gitignored) file means reruns don't hammer the
    faucet — which aggressively rate-limits (HTTP 429).  Cached wallets are
    reused as long as they still hold at least ``min_drops``.
    """
    client = JsonRpcClient(network.json_rpc)
    cached: list[str] = []
    if use_cache and _CACHE.exists():
        try:
            data = json.loads(_CACHE.read_text())
            if data.get("network") == network.name:
                cached = data.get("seeds", [])
        except (json.JSONDecodeError, OSError):
            cached = []

    wallets: list[Wallet] = []
    for i in range(count):
        wallet = Wallet.from_seed(cached[i]) if i < len(cached) else None
        if wallet is not None and _balance_drops(client, wallet.address) >= min_drops:
            print(f"  wallet {i + 1}/{count} reused from cache: {wallet.address}", flush=True)
        else:
            print(f"  funding wallet {i + 1}/{count} from the {network.name} faucet…", flush=True)
            wallet = _faucet_with_retry(client, wallet)
        wallets.append(wallet)

    if use_cache:
        try:
            _CACHE.write_text(
                json.dumps({"network": network.name, "seeds": [w.seed for w in wallets]})
            )
        except OSError:
            pass
    return client, wallets


# ---------------------------------------------------------------------------
# In-process uvicorn server (so a demo is one command, real socket)
# ---------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerThread:
    """Run a FastAPI app with uvicorn in a background thread."""

    def __init__(self, app, host: str = "127.0.0.1", port: int | None = None) -> None:
        self.host = host
        self.port = port or free_port()
        self._config = uvicorn.Config(app, host=host, port=self.port, log_level="warning")
        self._server = uvicorn.Server(self._config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    @property
    def ws_url(self) -> str:
        return f"ws://{self.host}:{self.port}/pay-stream"

    def __enter__(self) -> "ServerThread":
        self._thread.start()
        while not self._server.started:
            time.sleep(0.02)
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Live terminal ticker
# ---------------------------------------------------------------------------


class Ticker:
    """One-line, in-place status display of the running off-ledger tally."""

    def __init__(self, channel: ChannelInfo, drops_per_token: int) -> None:
        self.channel = channel
        self.price = drops_per_token
        self.start = time.monotonic()

    def update(self, *, tokens: int, authorized_drops: int, claims: int) -> None:
        elapsed = max(time.monotonic() - self.start, 1e-6)
        owed = tokens * self.price
        remaining = self.channel.capacity_drops - owed
        rate = claims / elapsed
        line = (
            f"\r  {tokens:>4} tok │ owed {format_drops(owed):>12} │ "
            f"authd {format_drops(authorized_drops):>12} │ "
            f"chan left {format_drops(max(remaining, 0)):>12} │ "
            f"{claims:>3} claims ({rate:4.1f}/s)"
        )
        sys.stdout.write(line)
        sys.stdout.flush()

    def done(self) -> None:
        sys.stdout.write("\n")
        sys.stdout.flush()
