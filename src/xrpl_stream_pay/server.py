"""Provider side: a FastAPI app that won't stream without being paid.

The provider exposes one websocket endpoint, ``/pay-stream``.  A token generator
produces the content; the :class:`~xrpl_stream_pay.gate.StreamGate` decides
whether to keep handing it over.  Two asyncio tasks run per session:

* the **sender** generates tokens, ships each batch, and applies backpressure
  (``await_within_budget``) so it never gets more than one trust-window ahead of
  what's been paid;
* the **reader** consumes incoming claims and feeds them to the gate.

If claims stop, the gate raises :class:`StallTimeout` and the sender cuts the
stream.  If a claim is forged or replayed, the reader marks the gate faulted and
the sender cuts on its next await.

The token generator is injected, so the same server runs the mock demo and a
real LLM with no changes here.  A generator is ``(prompt, params) -> async
iterator of (text, n_tokens)``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from . import protocol
from .channel import lookup_channel
from .claims import Claim
from .errors import PaymentError, StallTimeout
from .gate import StreamGate
from .network import TESTNET, Network

# A generator turns a prompt into a stream of (text, n_tokens) batches.
TokenStream = AsyncIterator[tuple[str, int]]
Generator = Callable[[str, dict[str, Any]], TokenStream]

# Optional hook invoked once a session ends, with its final gate state.
SessionEndHook = Callable[[StreamGate], Awaitable[None] | None]


@dataclass
class ProviderConfig:
    """How the provider prices and protects its stream."""

    provider_address: str
    """The provider's XRPL address — the channel's destination must match this."""

    drops_per_token: int = 10
    max_outstanding_tokens: int = 64
    """Trust window: how far ahead (in tokens) the sender may get before pausing."""
    stall_timeout: float = 10.0
    """Seconds to wait for a qualifying claim before cutting the stream."""
    verify_on_chain: bool = True
    """Look the channel up on-ledger to confirm it pays us, with the right key."""
    network: Network = TESTNET
    on_session_end: SessionEndHook | None = field(default=None)

    @property
    def max_outstanding_drops(self) -> int:
        return self.max_outstanding_tokens * self.drops_per_token


async def _resolve_capacity(
    config: ProviderConfig, channel_id: str, public_key: str
) -> int:
    """Confirm the channel is real, pays us, with the expected key; return capacity.

    Raises :class:`PaymentError` if the channel can't be used.  When
    ``verify_on_chain`` is off (tests, offline demos) we trust the declared key
    and report an effectively unbounded capacity.
    """
    if not config.verify_on_chain:
        return 2**63 - 1
    try:
        state = await asyncio.to_thread(
            lookup_channel, channel_id, network=config.network
        )
    except Exception as exc:  # noqa: BLE001 - surface any lookup failure as payment error
        raise PaymentError(f"could not verify channel on-ledger: {exc}") from exc
    if state.destination != config.provider_address:
        raise PaymentError("channel does not pay this provider")
    if state.public_key.upper() != public_key.upper():
        raise PaymentError("channel public key does not match the one offered")
    if state.claimable_drops <= 0:
        raise PaymentError("channel has no claimable balance left")
    return state.amount_drops


async def _read_claims(ws: WebSocket, gate: StreamGate) -> None:
    """Background task: accept claims, fault the gate on anything wrong."""
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("type") != protocol.CLAIM:
                continue  # ignore anything that isn't a claim on this channel
            try:
                gate.submit_claim(Claim.from_wire(msg["claim"]))
            except PaymentError as exc:
                gate.fail(exc)
                return
    except WebSocketDisconnect:
        gate.fail(PaymentError("client disconnected mid-stream"))
    except (KeyError, TypeError, ValueError) as exc:
        gate.fail(PaymentError(f"malformed claim message: {exc}"))


async def _run_session(
    ws: WebSocket, generate: Generator, config: ProviderConfig
) -> StreamGate | None:
    """Drive one streaming session end-to-end. Returns the final gate, or None."""
    hello = await ws.receive_json()
    if hello.get("type") != protocol.HELLO:
        await ws.send_json(protocol.error("expected a 'hello' message"))
        return None

    channel_id = hello["channel_id"]
    public_key = hello["public_key"]
    prompt = hello.get("prompt", "")
    params = hello.get("params", {})

    try:
        capacity = await _resolve_capacity(config, channel_id, public_key)
    except PaymentError as exc:
        await ws.send_json(protocol.error(str(exc)))
        return None

    gate = StreamGate(
        channel_id=channel_id,
        public_key=public_key,
        drops_per_token=config.drops_per_token,
        capacity_drops=capacity,
        max_outstanding_drops=config.max_outstanding_drops,
        stall_timeout=config.stall_timeout,
    )
    await ws.send_json(
        protocol.ready(
            drops_per_token=config.drops_per_token,
            capacity_drops=capacity,
            max_outstanding_drops=config.max_outstanding_drops,
            stall_timeout=config.stall_timeout,
            provider=config.provider_address,
            network=config.network.name,
        )
    )

    reader = asyncio.create_task(_read_claims(ws, gate))
    try:
        async for text, n_tokens in generate(prompt, params):
            gate.record_sent(n_tokens)
            await ws.send_json(
                protocol.chunk(
                    text=text,
                    tokens_sent=gate.tokens_sent,
                    owed_drops=gate.owed_drops,
                )
            )
            # Backpressure: don't get more than one trust window ahead of payment.
            await gate.await_within_budget()

        # Generation done — ask for and wait on the final claim.
        await ws.send_json(
            protocol.end_of_stream(
                tokens_sent=gate.tokens_sent, owed_drops=gate.owed_drops
            )
        )
        await gate.drain()
        await ws.send_json(
            protocol.done(
                tokens_sent=gate.tokens_sent,
                paid_drops=gate.paid_drops,
                claims_accepted=gate.claims_accepted,
            )
        )
    except StallTimeout as exc:
        with contextlib.suppress(Exception):
            await ws.send_json(
                protocol.cut(
                    reason=str(exc),
                    owed_drops=gate.owed_drops,
                    paid_drops=gate.paid_drops,
                )
            )
    except PaymentError as exc:
        with contextlib.suppress(Exception):
            await ws.send_json(
                protocol.cut(
                    reason=str(exc),
                    owed_drops=gate.owed_drops,
                    paid_drops=gate.paid_drops,
                )
            )
    except Exception as exc:  # noqa: BLE001 - generator/transport failure
        # Something on the provider's side broke (e.g. the model errored). Tell
        # the client and still return the gate so earned claims can be settled.
        with contextlib.suppress(Exception):
            await ws.send_json(protocol.error(f"provider failure: {exc}"))
    finally:
        reader.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await reader
    return gate


def create_app(generate: Generator, config: ProviderConfig) -> FastAPI:
    """Build the FastAPI app exposing the paid streaming endpoint."""
    app = FastAPI(title="xrpl-stream-pay provider")

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "provider": config.provider_address,
            "drops_per_token": config.drops_per_token,
            "network": config.network.name,
        }

    @app.websocket("/pay-stream")
    async def pay_stream(ws: WebSocket) -> None:
        await ws.accept()
        gate: StreamGate | None = None
        try:
            gate = await _run_session(ws, generate, config)
        except WebSocketDisconnect:
            pass
        finally:
            if gate is not None and config.on_session_end is not None:
                result = config.on_session_end(gate)
                if asyncio.iscoroutine(result):
                    await result
            with contextlib.suppress(Exception):
                await ws.close()

    return app
