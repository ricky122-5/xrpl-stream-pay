"""An x402-style HTTP profile: channel claims behind ``402 Payment Required``.

`x402 <https://www.x402.org>`_ revives HTTP 402: a server answers an unpaid
request with ``402`` and a JSON description of what it wants; the client pays and
retries with an ``X-Payment`` header; the server verifies and serves.  The usual
x402 scheme is one stablecoin micro-payment *per request*.

This module is a **profile** of that flow whose payment instrument is an XRPL
payment-channel claim.  Because claims are cumulative, repeated requests don't
each cost an on-ledger transaction — the client just signs a slightly larger
claim every time, and the provider settles once, later.  So you get x402's HTTP
ergonomics *and* the channel's efficiency for chatty agent ↔ provider traffic.

Wire shape (a profile, not byte-for-byte x402 — see ``scheme``):

* request carries ``X-Payment-Channel: <channel_id>`` so the server can quote the
  channel's current baseline;
* an unpaid request gets ``402`` with an ``accepts`` block stating the per-request
  price, the provider address, the network, and the cumulative drops required;
* the client signs a cumulative claim for ``baseline + price`` and retries with
  ``X-Payment: <base64 json claim>``;
* on success the server returns the resource plus an ``X-Payment-Response`` header
  with the new cumulative total.
"""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass
from typing import Any

import httpx
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .channel import ChannelInfo, lookup_channel
from .claims import Claim, authorize_claim, verify_claim
from .errors import BudgetExceeded, PaymentError
from .network import TESTNET, Network
from .store import ClaimStore, MemoryClaimStore

X402_VERSION = 1
SCHEME = "xrpl-payment-channel"

PAYMENT_HEADER = "X-Payment"
PAYMENT_RESPONSE_HEADER = "X-Payment-Response"
CHANNEL_HEADER = "X-Payment-Channel"


# ---------------------------------------------------------------------------
# Encoding helpers
# ---------------------------------------------------------------------------


def encode_payment(claim: Claim) -> str:
    """Base64-encode a claim for the ``X-Payment`` header."""
    return base64.b64encode(json.dumps(claim.to_wire()).encode()).decode()


def decode_payment(header: str) -> Claim:
    """Decode an ``X-Payment`` header back into a claim (raises on garbage)."""
    try:
        data = json.loads(base64.b64decode(header.encode()).decode())
        return Claim.from_wire(data)
    except (ValueError, KeyError, TypeError) as exc:
        raise PaymentError(f"malformed X-Payment header: {exc}") from exc


def encode_payment_response(payload: dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(payload).encode()).decode()


# ---------------------------------------------------------------------------
# Result of charging a request
# ---------------------------------------------------------------------------


@dataclass
class ChargeResult:
    """Outcome of :meth:`ChannelPaywall.charge`."""

    accepted: bool
    status_code: int
    claim: Claim | None = None
    challenge: dict[str, Any] | None = None
    response_header: str | None = None
    reason: str | None = None


# ---------------------------------------------------------------------------
# Server-side paywall
# ---------------------------------------------------------------------------


class ChannelPaywall:
    """Gate HTTP routes on a channel claim, x402 style.

    Reuses the same :class:`~xrpl_stream_pay.store.ClaimStore` as the streaming
    server, so a channel can be paid for streaming *and* per-request behind the
    same cumulative claim.  On-ledger validation of a channel (it pays us, with
    the right key) happens once per channel and is cached.
    """

    def __init__(
        self,
        *,
        provider_address: str,
        network: Network = TESTNET,
        claim_store: ClaimStore | None = None,
        verify_on_chain: bool = True,
    ) -> None:
        self.provider_address = provider_address
        self.network = network
        self.claim_store = claim_store if claim_store is not None else MemoryClaimStore()
        self.verify_on_chain = verify_on_chain
        # channel_id -> (real on-ledger public key (upper), capacity_drops), cached
        # after the first on-ledger lookup. We keep the *real* key so every later
        # charge re-checks the claim's key against it — caching on channel_id
        # alone would let anyone sign with their own key on a known channel id.
        self._channels: dict[str, tuple[str, int]] = {}

    def baseline(self, channel_id: str) -> int:
        """Cumulative drops already authorized on a channel."""
        claim = self.claim_store.get(channel_id)
        return claim.amount_drops if claim else 0

    def requirements(self, *, resource: str, price_drops: int, channel_id: str | None) -> dict[str, Any]:
        """Build the ``402`` body describing what we want for ``resource``."""
        baseline = self.baseline(channel_id) if channel_id else 0
        return {
            "x402Version": X402_VERSION,
            "accepts": [
                {
                    "scheme": SCHEME,
                    "network": self.network.name,
                    "resource": resource,
                    "description": f"{price_drops} drops per request via XRPL payment channel",
                    "asset": "XRP",
                    "payTo": self.provider_address,
                    "maxAmountRequired": str(price_drops),
                    "extra": {
                        "alreadyAuthorizedDrops": baseline,
                        "requiredCumulativeDrops": baseline + price_drops,
                    },
                }
            ],
        }

    async def _capacity(self, channel_id: str, public_key: str) -> int:
        """Confirm ``public_key`` is the channel's real key; return its capacity.

        The key is re-checked on *every* charge, not just the first: we look the
        channel up on-ledger once and cache its real public key, then compare the
        presented key against that cached value on every subsequent request. That
        closes the hole where caching on ``channel_id`` alone would let anyone
        present a claim signed with their own key on a publicly-known channel id.
        """
        if not self.verify_on_chain:
            return 2**63 - 1
        cached = self._channels.get(channel_id)
        if cached is not None:
            real_key, capacity = cached
            if public_key.upper() != real_key:
                raise PaymentError("channel public key does not match the claim")
            return capacity
        try:
            state = await asyncio.to_thread(
                lookup_channel, channel_id, network=self.network
            )
        except Exception as exc:  # noqa: BLE001
            raise PaymentError(f"could not verify channel on-ledger: {exc}") from exc
        if state.destination != self.provider_address:
            raise PaymentError("channel does not pay this provider")
        if state.public_key.upper() != public_key.upper():
            raise PaymentError("channel public key does not match the claim")
        self._channels[channel_id] = (state.public_key.upper(), state.amount_drops)
        return state.amount_drops

    async def charge(
        self, *, resource: str, price_drops: int, headers: dict[str, str]
    ) -> ChargeResult:
        """Charge a request. Returns an accept (with the stored claim) or a 402.

        ``headers`` is the (case-insensitively read) request headers.  On a valid
        payment the claim is recorded in the store and the result carries the
        ``X-Payment-Response`` value to echo back.
        """
        lower = {k.lower(): v for k, v in headers.items()}
        channel_hint = lower.get(CHANNEL_HEADER.lower())
        payment = lower.get(PAYMENT_HEADER.lower())

        def challenge(reason: str | None = None) -> ChargeResult:
            return ChargeResult(
                accepted=False,
                status_code=402,
                challenge=self.requirements(
                    resource=resource, price_drops=price_drops, channel_id=channel_hint
                ),
                reason=reason,
            )

        if not payment:
            return challenge()

        try:
            claim = decode_payment(payment)
            if not verify_claim(claim):
                raise PaymentError("claim signature is invalid")
            if channel_hint and claim.channel_id != channel_hint:
                raise PaymentError("claim channel does not match X-Payment-Channel")
            capacity = await self._capacity(claim.channel_id, claim.public_key)
            required = self.baseline(claim.channel_id) + price_drops
            if claim.amount_drops < required:
                raise PaymentError(
                    f"claim authorizes {claim.amount_drops} drops, need cumulative {required}"
                )
            if claim.amount_drops > capacity:
                raise PaymentError("claim exceeds channel capacity")
        except PaymentError as exc:
            return challenge(reason=str(exc))

        # Accept: record the claim and report the new cumulative total.
        self.claim_store.put(claim)
        header = encode_payment_response(
            {
                "scheme": SCHEME,
                "network": self.network.name,
                "channelId": claim.channel_id,
                "cumulativeDrops": claim.amount_drops,
                "settled": False,  # off-ledger; provider settles the channel later
            }
        )
        return ChargeResult(
            accepted=True, status_code=200, claim=claim, response_header=header
        )


# ---------------------------------------------------------------------------
# Client-side: do the 402 -> pay -> retry dance
# ---------------------------------------------------------------------------


class X402Client:
    """An HTTP client that pays for ``402`` responses with channel claims.

    Tracks the cumulative amount authorized on its channel, so each paid request
    signs a slightly larger claim and no request touches the ledger.  Pass your
    own ``httpx.Client`` (e.g. one wired to an ASGI app) for testing.
    """

    def __init__(
        self,
        *,
        channel: ChannelInfo,
        private_key: str,
        http: httpx.Client | None = None,
        max_budget_drops: int | None = None,
    ) -> None:
        self.channel = channel
        self.private_key = private_key
        self.http = http if http is not None else httpx.Client()
        self.max_budget_drops = (
            min(max_budget_drops, channel.capacity_drops)
            if max_budget_drops is not None
            else channel.capacity_drops
        )
        self.authorized_drops = 0
        self.latest_claim: Claim | None = None
        self.requests_paid = 0

    @classmethod
    def from_wallet(cls, wallet, channel: ChannelInfo, **kw) -> "X402Client":
        return cls(channel=channel, private_key=wallet.private_key, **kw)

    def _channel_headers(self) -> dict[str, str]:
        return {CHANNEL_HEADER: self.channel.channel_id}

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Make a request, paying once if the server answers ``402``."""
        headers = {**kwargs.pop("headers", {}), **self._channel_headers()}
        resp = self.http.request(method, url, headers=headers, **kwargs)
        if resp.status_code != 402:
            return resp

        terms = resp.json()["accepts"][0]
        price = int(terms["maxAmountRequired"])
        server_baseline = int(terms.get("extra", {}).get("alreadyAuthorizedDrops", 0))
        cumulative = max(self.authorized_drops, server_baseline) + price
        if cumulative > self.max_budget_drops:
            raise BudgetExceeded(
                f"request needs {cumulative} cumulative drops authorized, budget is "
                f"{self.max_budget_drops}"
            )

        claim = authorize_claim(
            self.channel.channel_id, cumulative, self.private_key, self.channel.public_key
        )
        self.authorized_drops = cumulative
        self.latest_claim = claim
        self.requests_paid += 1

        paid_headers = {**headers, PAYMENT_HEADER: encode_payment(claim)}
        return self.http.request(method, url, headers=paid_headers, **kwargs)

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def close(self) -> None:
        self.http.close()


# ---------------------------------------------------------------------------
# FastAPI glue: mount a paid GET route
# ---------------------------------------------------------------------------

Handler = Callable[[Request], dict[str, Any] | Awaitable[dict[str, Any]]]


def add_paid_route(
    app: FastAPI,
    paywall: ChannelPaywall,
    path: str,
    price_drops: int,
    handler: Handler,
) -> None:
    """Register a GET ``path`` that costs ``price_drops`` per request.

    ``handler`` receives the request and returns the JSON body to serve once
    payment is verified.
    """

    @app.get(path)
    async def _route(request: Request) -> JSONResponse:  # noqa: ANN202 - closure
        result = await paywall.charge(
            resource=path, price_drops=price_drops, headers=dict(request.headers)
        )
        if not result.accepted:
            return JSONResponse(result.challenge, status_code=402)
        body = handler(request)
        if asyncio.iscoroutine(body):
            body = await body
        response = JSONResponse(body)
        if result.response_header:
            response.headers[PAYMENT_RESPONSE_HEADER] = result.response_header
        return response
