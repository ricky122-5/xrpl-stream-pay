# Architecture & honest tradeoffs

This document explains *why* xrpl-stream-pay is shaped the way it is, what it
deliberately doesn't do, and the case for (and against) building it on the XRP
Ledger versus Solana, Base, or a plain per-request rail. It's written to be the
thing you'd hand a skeptical reviewer — including the XRPL Grants committee —
rather than a marketing page.

## The problem

An agent calls a model and gets a streaming response. Billing options today:

1. **Pay once, up front.** Simple, but you either over-charge (quote for the
   worst case) or the provider eats the risk of a long generation. And the agent
   pays before it knows whether the output is any good.
2. **Pay once, after.** The provider streams the whole thing, then bills. Now the
   *provider* eats the risk: an agent can consume a 10k-token answer and vanish.
3. **Pay per request, on-chain.** One transaction per call. Fine at low volume;
   at agent volume (thousands of short calls) you're paying fees and eating
   confirmation latency on every single one.

None of these fit *streaming*, where value is delivered token by token and
either side can walk away mid-stream. What you actually want is to pay
*continuously, as the value arrives*, without a blockchain transaction per token.

That is exactly what a **payment channel** is for.

## The mechanism

```
 on-ledger          off-ledger (per token batch, signing speed)         on-ledger
 ┌────────────┐     ┌───────┐  ┌───────┐  ┌───────┐        ┌───────┐    ┌────────────┐
 │ open       │ ──▶ │ claim │─▶│ claim │─▶│ claim │─▶ ...  │ claim │ ─▶ │ settle     │
 │ (fund chan)│     └───────┘  └───────┘  └───────┘        └───────┘    │ (1 redeem) │
 └────────────┘                                                          └────────────┘
 PaymentChannelCreate                                                   PaymentChannelClaim
```

1. **Open** (`PaymentChannelCreate`): the agent locks XRP into a unidirectional
   channel that pays the provider, registering a public key. *On-ledger tx #1.*
2. **Stream + claim**: the provider streams tokens; the agent signs a
   **cumulative claim** — "you may pull up to *N* drops" — each time its meter
   fires. Claims are just ed25519/secp256k1 signatures over `("CLM\0", channel,
   amount)`. They never touch the network. This is the entire hot loop, and it
   runs at signing speed.
3. **Settle** (`PaymentChannelClaim`): at the end, the provider submits the single
   highest claim it holds, crediting itself that amount and returning the
   remainder to the agent. *On-ledger tx #2.*

A 1,000-token answer is **2 transactions and ~1,000 signatures**, not 1,000
transactions. Claims are cumulative, so intermediate ones can be dropped on the
floor; only the last one is ever redeemed.

### Why we sign claims locally instead of via `channel_authorize`

The spec called for wrapping rippled's `channel_authorize` / `channel_verify`
RPCs. We do the identical cryptography **locally** instead, for two reasons:

- `channel_authorize` is an **admin-only** method (it needs the account secret
  on the server), so it isn't callable on public testnet/mainnet nodes.
- A network round trip inside the per-token loop would defeat the whole premise.
  Local signing keeps the loop dependency-free and bounded only by CPU.

The signed byte layout is exactly rippled's, so a locally produced claim is
redeemable by a real `PaymentChannelClaim`, and `channel_verify` would accept it.
We verified this round trip against testnet (see `tests/test_testnet.py`).

## The trust model

You cannot bill for tokens you haven't generated yet, so *someone* extends a
little credit. We make that explicit and bounded:

- The provider streams ahead by at most **`max_outstanding_tokens`** before it
  pauses for payment (backpressure).
- If a qualifying claim doesn't arrive within **`stall_timeout`**, the provider
  **cuts** the stream and keeps every claim already signed.

So:

- **Provider's worst case**: an agent consumes one trust window of tokens and
  never pays. Loss is bounded to `max_outstanding_tokens`, never the whole
  response. (Demonstrated by `--stingy` in the mock demo.)
- **Agent's worst case**: it signs claims only for tokens it has actually
  received and at a price it pre-agreed (the client verifies `owed == tokens ×
  price` and refuses anything above its budget). A malicious provider can stop
  streaming after banking a claim, but it can never make the agent authorize more
  than the agent received. The agent's real exposure is *capital lockup* (see
  below), not over-payment.

Neither side can cheat the other by more than a single window. Stale or replayed
claims are ignored rather than treated as attacks, so they can't be used to force
a cut.

## What this deliberately does NOT do (v1)

- **Bidirectional channels.** XRPL channels are one-way. Refunds/credits would
  need a second channel or a different primitive.
- **Provider discovery / routing.** One agent, one provider, one channel.
- **Mainnet / a production facilitator.** Testnet/devnet only.
- **A watchtower.** With a non-trivial `settle_delay`, a channel can be closed
  unilaterally; a production system wants something watching for that.

## Why XRPL — and the honest counterarguments

### The case for XRPL

- **Payment channels are a native, first-class ledger primitive.** `PayChannel`
  objects and claim verification are part of the protocol. On Solana or Base you
  would deploy and audit a custom state-channel program/contract — more code,
  more attack surface, more to get wrong. Here the channel *is* the chain.
- **Fees are tiny, fixed, and predictable** (~10 drops), with no gas auctions or
  priority fees to reason about. The two bookend transactions cost a rounding
  error, and there is *zero* fee in the per-token loop.
- **3–5 second deterministic finality** via consensus (no probabilistic
  confirmations, no reorg risk to design around).
- **Claim signing is in the protocol**, not a convention you invent on top.

### The honest counterarguments (why you might not)

- **XRP is volatile.** Pricing tokens directly in drops means the real cost of a
  token drifts with the XRP/USD rate. The credible answer is to denominate in
  **RLUSD** (Ripple's USD stablecoin) or an IOU once channel support for issued
  currencies fits the use case; this v1 prices in drops for simplicity. This is
  the single biggest gap versus a USDC-on-Base design.
- **Base / Solana have the ecosystem and the emerging standard.** The **x402**
  pattern (HTTP `402 Payment Required` + a stablecoin micro-payment per request,
  championed on Base) is gaining real traction for agent payments, with wallets,
  facilitators, and tooling. It's *per-request*, not *per-token*, so it's
  complementary rather than identical — but it's where the mindshare is.
- **Solana is cheap and fast enough that per-request on-chain is viable** for
  many workloads, sidestepping channels entirely. Channels win specifically when
  per-token granularity matters and volume is high; below that bar, "just pay per
  request" is simpler.
- **Capital lockup.** A channel ties up XRP for its lifetime plus the
  `settle_delay` window, and each channel costs an owner reserve. That's fine for
  a busy long-lived channel, wasteful for a one-shot call.

### When channels (this design) actually win

High-frequency, fine-grained, repeated billing between the *same two parties*:
an agent that hammers one provider thousands of times, paying per token, over a
long-lived channel it opens once a day and settles once a day. That's a handful
of on-ledger transactions for a day's worth of traffic. That is the niche, and
it's a real one for autonomous agents.

This is implemented (see `examples/reuse_demo.py`): claims are cumulative over a
channel's whole life, the provider remembers the highest claim per channel
(`ClaimStore`), and `PeriodicSettler` redeems only when enough has accrued. A
6-session run settles 3 times instead of 6 — and the ratio improves with volume.

It even survives a restart (see `examples/persistent_demo.py`): the agent
persists its channel handle (`save_channel`/`load_channel`), the provider
persists its claims (`FileClaimStore`), and a long-lived channel tops itself up
with `PaymentChannelFund` (`ensure_capacity`) when it runs low. Six sessions
across *two separate process lifetimes* cost three on-ledger transactions (open,
one top-up, one settle/close). The remaining gap to a production deployment is
swapping the JSON files for a real datastore (Redis/Postgres) behind the same
interfaces — the seam is already there.

## Where it goes next

1. **Production-grade persistence**: back `ClaimStore` with Redis/Postgres and
   add a watchtower for unilateral channel closes.
2. **RLUSD / IOU denomination** so prices are stable in USD terms.
3. **A facilitator** that brokers (agent ↔ provider) channels and handles
   discovery, so an agent doesn't manage channels by hand.
4. **Watchtower** for unilateral-close protection.
5. **An x402-style HTTP profile** — expose the same gate behind `402 Payment
   Required` semantics so it interops with the agent-payments tooling forming
   around that standard, with XRPL channels as the settlement layer underneath.

The point of v1 is to prove the core loop end-to-end on a real ledger: two
on-ledger transactions, everything else signatures. That part works today.
