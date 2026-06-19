"""Shared test fixtures."""

from __future__ import annotations

import os
import socket
import threading
import time
from contextlib import contextmanager

import pytest
import uvicorn
from xrpl.wallet import Wallet

from xrpl_stream_pay import ChannelInfo


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def serve(app):
    """Run a FastAPI app under uvicorn in a thread; yield its ws:// URL."""
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("server did not start in time")
            time.sleep(0.02)
        yield f"ws://127.0.0.1:{port}/pay-stream"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.fixture
def live_server():
    """Expose :func:`serve` as a fixture."""
    return serve


@pytest.fixture
def wallet() -> Wallet:
    return Wallet.create()


@pytest.fixture
def fake_channel(wallet: Wallet) -> ChannelInfo:
    """A ChannelInfo not backed by any real ledger object (offline tests)."""
    return ChannelInfo(
        channel_id=os.urandom(32).hex().upper(),
        source=wallet.address,
        destination="rPROVIDERdestinationADDRESSxxxxxxxx",
        public_key=wallet.public_key,
        capacity_drops=1_000_000_000,
        settle_delay=60,
        open_tx_hash="",
    )
