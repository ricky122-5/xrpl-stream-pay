"""xrpl-stream-pay — per-token streaming payments over one XRPL payment channel.

Two on-ledger transactions bookend a session (open the channel, redeem the final
claim); everything in between — the per-token billing — happens off-ledger at
signing speed.

Quick tour:

* :mod:`~xrpl_stream_pay.channel`  open / fund / look up / close the channel
* :mod:`~xrpl_stream_pay.claims`   sign & verify cumulative claims (off-ledger)
* :mod:`~xrpl_stream_pay.meter`    the claim ticker (every N tokens or M ms)
* :mod:`~xrpl_stream_pay.gate`     provider policy: withhold tokens until paid
* :mod:`~xrpl_stream_pay.server`   FastAPI websocket provider
* :mod:`~xrpl_stream_pay.client`   agent-side streaming session
* :mod:`~xrpl_stream_pay.settle`   redeem the final claim, return a receipt
"""

from __future__ import annotations

from .channel import (
    ChannelInfo,
    ChannelState,
    close_channel,
    fund_channel,
    lookup_channel,
    open_channel,
)
from .claims import Claim, authorize_claim, claim_message, verify_claim
from .client import SessionResult, StreamClient
from .errors import (
    BudgetExceeded,
    ChannelError,
    PaymentError,
    ProtocolError,
    StallTimeout,
    StreamPayError,
)
from .gate import StreamGate
from .meter import Meter, MeterConfig
from .network import DEVNET, TESTNET, Network, drops_to_xrp, format_drops, xrp_to_drops
from .server import Generator, ProviderConfig, create_app
from .settle import Receipt, settle

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # channel
    "ChannelInfo",
    "ChannelState",
    "open_channel",
    "fund_channel",
    "lookup_channel",
    "close_channel",
    # claims
    "Claim",
    "authorize_claim",
    "verify_claim",
    "claim_message",
    # meter
    "Meter",
    "MeterConfig",
    # gate / server
    "StreamGate",
    "ProviderConfig",
    "Generator",
    "create_app",
    # client
    "StreamClient",
    "SessionResult",
    # settle
    "settle",
    "Receipt",
    # network
    "Network",
    "TESTNET",
    "DEVNET",
    "xrp_to_drops",
    "drops_to_xrp",
    "format_drops",
    # errors
    "StreamPayError",
    "ChannelError",
    "PaymentError",
    "StallTimeout",
    "ProtocolError",
    "BudgetExceeded",
]
