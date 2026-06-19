"""Meter: the claim ticker fires on token count or elapsed time, whichever first."""

from __future__ import annotations

import pytest

from xrpl_stream_pay.meter import Meter, MeterConfig


def test_fires_on_token_threshold():
    m = Meter(MeterConfig(drops_per_token=10, every_n_tokens=5, every_ms=10_000), now=0.0)
    fires = []
    for _ in range(11):
        m.record(1)
        fires.append(m.fired(now=0.0))
    assert sum(fires) == 2  # at token 5 and token 10
    assert m.cost() == 110


def test_fires_on_time_threshold():
    m = Meter(MeterConfig(drops_per_token=1, every_n_tokens=1_000, every_ms=100), now=0.0)
    m.record(1)
    assert not m.fired(now=0.0)  # only 1 token, no time yet
    m.record(1)
    assert m.fired(now=0.2)  # 200ms elapsed >= 100ms


def test_no_fire_without_new_tokens():
    m = Meter(MeterConfig(drops_per_token=1, every_n_tokens=1, every_ms=1), now=0.0)
    assert not m.due(now=1000.0)  # tons of time, but zero tokens streamed


def test_fired_arms_next_interval():
    m = Meter(MeterConfig(drops_per_token=1, every_n_tokens=2, every_ms=10_000), now=0.0)
    m.record(2)
    assert m.fired(now=0.0)
    assert not m.due(now=0.0)  # interval reset; need 2 more tokens
    m.record(2)
    assert m.due(now=0.0)


def test_config_validation():
    with pytest.raises(ValueError):
        MeterConfig(drops_per_token=-1)
    with pytest.raises(ValueError):
        MeterConfig(drops_per_token=1, every_n_tokens=0)
    with pytest.raises(ValueError):
        MeterConfig(drops_per_token=1, every_ms=0)
