# xrpl-stream-pay

**Per-token streaming payments over a single XRPL payment channel.**

An agent pays a provider *as the tokens stream*, not once per request. Two
on-ledger transactions bookend the whole session — open the channel, redeem the
final claim — and everything in between, the actual per-token billing, happens
**off-ledger at signing speed**.

```
 on-ledger          off-ledger (per token batch, signing speed)         on-ledger
 ┌────────────┐     ┌───────┐  ┌───────┐  ┌───────┐        ┌───────┐    ┌────────────┐
 │ open       │ ──▶ │ claim │─▶│ claim │─▶│ claim │─▶ ...  │ claim │ ─▶ │ settle     │
 │ (fund chan)│     └───────┘  └───────┘  └───────┘        └───────┘    │ (1 redeem) │
 └────────────┘        ▲ verify locally, no network in this loop ▲      └────────────┘
   PaymentChannel                                                         PaymentChannel
     Create                                                                  Claim
```

A 500-token answer that would otherwise be ~500 micro-transactions (or one
coarse up-front charge) becomes **two** ledger transactions plus 500 signatures.
The provider's worst case if the agent stops paying is one trust-window of
tokens, never the whole response.

> Status: working proof of the core idea on **XRPL testnet**. Out of scope for
> v1: bidirectional channels, provider discovery/routing, mainnet, a production
> facilitator. See [docs/architecture.md](docs/architecture.md) for the honest
> tradeoffs (including "why not just Solana/Base").

---

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"          # add ",llm" for the real-model demo
```

Requires Python ≥ 3.10.

## 60-second demo (no API keys)

Opens real testnet channels with faucet wallets, streams *synthetic* tokens
through the real FastAPI gate over a real websocket, and settles on-ledger:

```bash
python examples/mock_stream_demo.py
```

You'll watch the running tally tick up live (tokens, drops authorized, channel
remaining, claims/sec), then see the single settlement transaction with an
explorer link. Add `--stingy` to watch a non-paying agent get cut off mid-stream.

## Real streaming model (optional)

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python examples/llm_stream_demo.py --prompt "Explain payment channels in 3 sentences."
```

Same machinery, but the provider's tokens come from a live streaming model
instead of a generator — so you're paying, per token, for real output.

---

## How it fits together

| Module | Job |
|---|---|
| [`channel.py`](src/xrpl_stream_pay/channel.py) | Open / fund / look up / close the channel (`PaymentChannelCreate`, `PaymentChannelFund`, `PaymentChannelClaim`+`tfClose`). |
| [`claims.py`](src/xrpl_stream_pay/claims.py) | Sign & verify cumulative claims **locally** — the off-ledger equivalent of `channel_authorize` / `channel_verify`. |
| [`meter.py`](src/xrpl_stream_pay/meter.py) | The claim ticker: a new claim is due every *N* tokens or every *M* ms, whichever trips first. |
| [`gate.py`](src/xrpl_stream_pay/gate.py) | Provider policy, transport-free: track owed vs paid, apply backpressure, raise `StallTimeout` when claims lapse. |
| [`server.py`](src/xrpl_stream_pay/server.py) | FastAPI websocket provider that withholds the next chunk until a fresh valid claim arrives. |
| [`client.py`](src/xrpl_stream_pay/client.py) | Agent session: open the channel, stream the response, sign a claim each time the meter fires. |
| [`settle.py`](src/xrpl_stream_pay/settle.py) | Redeem the final claim in one `PaymentChannelClaim`; return a receipt with an explorer link. |

(The spec's `claims.py` "wraps `channel_authorize`/`channel_verify`" — we do the
same cryptography *locally* instead of via RPC, because `channel_authorize` is
admin-only on public nodes and because a network round trip in the per-token loop
would defeat the entire "signing speed" premise. See `claims.py` for the detail.)

## Library usage

```python
import asyncio
from xrpl.clients import JsonRpcClient
from xrpl.wallet import generate_faucet_wallet
from xrpl_stream_pay import (
    open_channel, StreamClient, MeterConfig, settle, TESTNET, xrp_to_drops,
)

client_rpc = JsonRpcClient(TESTNET.json_rpc)
agent  = generate_faucet_wallet(client_rpc)     # pays
provider = generate_faucet_wallet(client_rpc)   # gets paid

# on-ledger #1: lock 10 XRP in a channel that pays the provider
channel = open_channel(agent, provider.address, xrp_to_drops(10))

# ... run a provider (see server.py / examples) and stream against it:
agent_client = StreamClient.from_wallet(
    agent, channel, MeterConfig(drops_per_token=10, every_n_tokens=32, every_ms=500),
)
result = await agent_client.run("ws://127.0.0.1:8000/pay-stream", "Explain X")

# on-ledger #2: provider redeems the single highest claim
receipt = settle(provider, result.final_claim)
print(receipt)        # -> "Settled 0.00xyz XRP ... https://testnet.xrpl.org/transactions/..."
```

## Tests

```bash
pytest                  # fast, offline: claims, meter, gate, client/server over an in-memory socket
pytest -m testnet       # opt-in: real channel open → stream → settle round trip on testnet (slow)
```

## The trust model in one paragraph

You can't bill for tokens you haven't generated, so the provider streams a little
ahead and the agent pays as it goes. The gate caps how far ahead the provider
will get (`max_outstanding_tokens`) and how long it'll wait for the next claim
(`stall_timeout`). Stay paid up and tokens flow continuously; fall a window
behind and the provider pauses; stop paying and it cuts the stream — keeping
every claim you already signed. Neither side can cheat the other by more than one
window. Full reasoning in [docs/architecture.md](docs/architecture.md).

## License

MIT
