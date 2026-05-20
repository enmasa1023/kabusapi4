#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Volatility-regime entry gate for the 1570 monitor.

This module is intentionally independent from monitor_1570_kabusapi0511.py.
It reads only generic attributes from feature/tick objects so the existing
order, position, and exit logic can remain the source of truth.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from statistics import pstdev
from typing import Any, Iterable, Optional

ENTRY_SIGNALS = {"LONG_CANDIDATE", "SHORT_CANDIDATE"}
VALID_MODES = {"off", "warn_only", "filter"}


@dataclass
class GateFeatures:
    return_1m: float = 0.0
    return_3m: float = 0.0
    return_5m: float = 0.0
    realized_vol_1m: float = 0.0
    realized_vol_5m: float = 0.0
    range_5m: float = 0.0
    volume_delta: float = 0.0
    volume_ratio: float = 1.0
    spread_ticks: float = 0.0
    spread_ratio: float = 0.0
    board_imbalance: float = 0.5
    price_vs_vwap: float = 0.0
    regime: str = "NORMAL"
    price: float = 0.0
    vwap: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


@dataclass
class GateDecision:
    action: str
    reason: str
    regime: str
    final_signal: str
    raw_signal: str
    mode: str
    applied: bool
    features: GateFeatures

    def to_record(self) -> dict[str, Any]:
        return {
            "gate_action": self.action,
            "gate_reason": self.reason,
            "regime": self.regime,
            "raw_signal": self.raw_signal,
            "final_signal": self.final_signal,
            "gate_mode": self.mode,
            "gate_applied": self.applied,
            "features": self.features.to_dict(),
        }


class VolatilityRegimeGate:
    def __init__(self, config: Optional[dict[str, Any]] = None) -> None:
        self.config = config or {}
        self.enabled = bool(self.config.get("enabled", False))
        mode = str(self.config.get("mode", "warn_only" if self.enabled else "off"))
        # Treat enabled=false as a true off switch even if a stale mode remains in config.
        if not self.enabled:
            mode = "off"
        self.mode = mode if mode in VALID_MODES else "warn_only"
        self.thresholds = dict(self.config.get("thresholds") or {})
        self.actions = dict(self.config.get("actions") or {})
        self.logging = dict(self.config.get("logging") or {})

    def compute_features(self, ticks: Iterable[Any], feature: Any) -> GateFeatures:
        rows = [t for t in ticks if _float_attr(t, "price") is not None]
        latest = rows[-1] if rows else None
        price = _num(_float_attr(feature, "price"), _num(_float_attr(latest, "price"), 0.0))
        vwap = _num(_float_attr(feature, "vwap"), _num(_float_attr(latest, "vwap"), 0.0))
        spread_ticks = _num(_float_attr(feature, "spread_ticks"), 0.0)
        obi_l1 = _num(_float_attr(feature, "obi_l1"), 0.0)
        board_imbalance = min(max((obi_l1 + 1.0) / 2.0, 0.0), 1.0)
        volume_1m = _num(_float_attr(feature, "volume_1m"), 0.0)
        volume_3m = _num(_float_attr(feature, "volume_3m"), 0.0)
        volume_ratio = volume_1m / max(volume_3m / 3.0, 1.0)
        price_vs_vwap = ((price - vwap) / vwap) if vwap else 0.0
        spread_price = _spread_price(latest)
        spread_ratio = (spread_price / price) if price and spread_price is not None else 0.0

        returns_1m = _period_returns(rows, 60)
        returns_5m = _period_returns(rows, 300)
        price_5m_ago = _price_at_or_before(rows, 300)
        return_5m = ((price / price_5m_ago) - 1.0) if price and price_5m_ago else 0.0
        prices_5m = _period_prices(rows, 300)
        range_5m = ((max(prices_5m) - min(prices_5m)) / price) if price and prices_5m else 0.0
        volume_delta = _volume_delta(rows, 60)

        gf = GateFeatures(
            return_1m=_num(_float_attr(feature, "ret_1m"), 0.0),
            return_3m=_num(_float_attr(feature, "ret_3m"), 0.0),
            return_5m=return_5m,
            realized_vol_1m=pstdev(returns_1m) if len(returns_1m) >= 2 else 0.0,
            realized_vol_5m=pstdev(returns_5m) if len(returns_5m) >= 2 else 0.0,
            range_5m=range_5m,
            volume_delta=volume_delta,
            volume_ratio=volume_ratio,
            spread_ticks=spread_ticks,
            spread_ratio=spread_ratio,
            board_imbalance=board_imbalance,
            price_vs_vwap=price_vs_vwap,
            price=price,
            vwap=vwap,
            raw={
                "obi_l1": obi_l1,
                "obi_l3": _num(_float_attr(feature, "obi_l3"), 0.0),
                "vwap_gap_bps": _num(_float_attr(feature, "vwap_gap_bps"), 0.0),
                "trade_intensity_30s": _num(_float_attr(feature, "trade_intensity_30s"), 0.0),
                "volume_ratio_definition": "volume_1m / max(volume_3m / 3, 1)",
                "board_imbalance_definition": "(obi_l1 + 1) / 2",
            },
        )
        gf.regime = self.detect_regime(gf)
        return gf

    def detect_regime(self, features: GateFeatures) -> str:
        low_vol_5m = _num(self.thresholds.get("low_vol_5m"), 0.0004)
        panic_vol_1m = _num(self.thresholds.get("panic_vol_1m"), 0.0030)
        min_volume_ratio = _num(self.thresholds.get("min_volume_ratio"), 0.8)
        if features.realized_vol_5m < low_vol_5m and features.volume_ratio < min_volume_ratio:
            return "DEAD"
        if features.realized_vol_1m > panic_vol_1m:
            return "PANIC"
        if features.realized_vol_5m >= low_vol_5m and features.volume_ratio >= min_volume_ratio:
            return "ACTIVE"
        return "NORMAL"

    def evaluate(self, raw_signal: str, features: GateFeatures, current_position: Any = None) -> GateDecision:
        if not self.enabled or self.mode == "off":
            return GateDecision("ALLOW", "gate off", features.regime, raw_signal, raw_signal, self.mode, False, features)
        if raw_signal not in ENTRY_SIGNALS or current_position is not None:
            return GateDecision("ALLOW", "not a new entry", features.regime, raw_signal, raw_signal, self.mode, False, features)

        reasons: list[str] = []
        action = "ALLOW"
        t = self.thresholds
        a = self.actions
        if bool(a.get("block_wide_spread", True)) and features.spread_ticks > _num(t.get("max_spread_ticks"), 2.0):
            action = "BLOCK"
            reasons.append(f"spread_ticks {features.spread_ticks:.2f} > {_num(t.get('max_spread_ticks'), 2.0):.2f}")
        if bool(a.get("block_wide_spread", True)) and features.spread_ratio > _num(t.get("max_spread_ratio"), 0.0010):
            action = "BLOCK"
            reasons.append(f"spread_ratio {features.spread_ratio:.6f} > {_num(t.get('max_spread_ratio'), 0.0010):.6f}")
        if bool(a.get("block_dead_market", True)) and features.regime == "DEAD":
            action = "BLOCK"
            reasons.append("dead market")
        if bool(a.get("block_panic_market", True)) and features.regime == "PANIC":
            action = "BLOCK"
            reasons.append("panic market")
        if bool(a.get("block_low_volume", True)) and features.volume_ratio < _num(t.get("min_volume_ratio"), 0.8):
            action = "BLOCK"
            reasons.append(f"low volume_ratio {features.volume_ratio:.2f}")

        warn_reasons, directional_block = self._directional_warnings(raw_signal, features)
        reasons.extend(warn_reasons)
        if directional_block:
            action = "BLOCK"
        elif action == "ALLOW" and warn_reasons:
            action = "WARN_ONLY"
        reason = "; ".join(reasons) if reasons else "ok"
        final_signal = self.apply_for_entry_only(raw_signal, action)
        applied = self.mode == "filter" and action == "BLOCK" and final_signal != raw_signal
        return GateDecision(action, reason, features.regime, final_signal, raw_signal, self.mode, applied, features)

    def apply_for_entry_only(self, raw_signal: str, gate_action: str | GateDecision) -> str:
        action = gate_action.action if isinstance(gate_action, GateDecision) else str(gate_action)
        if self.enabled and self.mode == "filter" and raw_signal in ENTRY_SIGNALS and action == "BLOCK":
            return "NO_ACTION"
        return raw_signal

    def _directional_warnings(self, raw_signal: str, features: GateFeatures) -> tuple[list[str], bool]:
        t = self.thresholds
        a = self.actions
        reasons: list[str] = []
        directional_block = False
        if raw_signal == "LONG_CANDIDATE":
            if bool(a.get("warn_when_vwap_conflicts", True)) and features.price_vs_vwap < _num(t.get("min_price_vs_vwap_long"), 0.0):
                reasons.append("long vwap conflict")
            if bool(a.get("warn_when_board_conflicts", True)) and features.board_imbalance < _num(t.get("board_imbalance_long"), 0.55):
                reasons.append("long board conflict")
            if bool(a.get("block_when_vwap_conflicts", False)) and features.price_vs_vwap < _num(t.get("min_price_vs_vwap_long"), 0.0):
                reasons.append("long vwap block")
                directional_block = True
            if bool(a.get("block_when_board_conflicts", False)) and features.board_imbalance < _num(t.get("board_imbalance_long"), 0.55):
                reasons.append("long board block")
                directional_block = True
        elif raw_signal == "SHORT_CANDIDATE":
            if bool(a.get("warn_when_vwap_conflicts", True)) and features.price_vs_vwap > _num(t.get("max_price_vs_vwap_short"), 0.0):
                reasons.append("short vwap conflict")
            if bool(a.get("warn_when_board_conflicts", True)) and features.board_imbalance > _num(t.get("board_imbalance_short"), 0.45):
                reasons.append("short board conflict")
            if bool(a.get("block_when_vwap_conflicts", False)) and features.price_vs_vwap > _num(t.get("max_price_vs_vwap_short"), 0.0):
                reasons.append("short vwap block")
                directional_block = True
            if bool(a.get("block_when_board_conflicts", False)) and features.board_imbalance > _num(t.get("board_imbalance_short"), 0.45):
                reasons.append("short board block")
                directional_block = True
        return reasons, directional_block


def decision_features_json(features: GateFeatures) -> str:
    return json.dumps(features.to_dict(), ensure_ascii=False, default=str)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


def _float_attr(obj: Any, name: str) -> Optional[float]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _time_attr(obj: Any) -> Any:
    if isinstance(obj, dict):
        return obj.get("ts")
    return getattr(obj, "ts", None)


def _spread_price(tick: Any) -> Optional[float]:
    if tick is None:
        return None
    ask = _float_attr(tick, "sell1_price")
    bid = _float_attr(tick, "buy1_price")
    if ask is None or bid is None:
        return None
    return abs(float(ask) - float(bid))


def _period_rows(rows: list[Any], seconds: int) -> list[Any]:
    if not rows:
        return []
    latest_ts = _time_attr(rows[-1])
    if latest_ts is None:
        return rows
    cutoff = latest_ts - timedelta(seconds=seconds)
    return [r for r in rows if _time_attr(r) is not None and _time_attr(r) >= cutoff]


def _period_prices(rows: list[Any], seconds: int) -> list[float]:
    return [_num(_float_attr(r, "price"), 0.0) for r in _period_rows(rows, seconds) if _float_attr(r, "price") is not None]


def _period_returns(rows: list[Any], seconds: int) -> list[float]:
    prices = _period_prices(rows, seconds)
    out: list[float] = []
    for prev, cur in zip(prices, prices[1:]):
        if prev:
            out.append((cur / prev) - 1.0)
    return out


def _price_at_or_before(rows: list[Any], seconds_ago: int) -> Optional[float]:
    if not rows:
        return None
    latest_ts = _time_attr(rows[-1])
    if latest_ts is None:
        return None
    target = latest_ts - timedelta(seconds=seconds_ago)
    candidate = None
    for r in rows:
        ts = _time_attr(r)
        if ts is not None and ts <= target and _float_attr(r, "price") is not None:
            candidate = _num(_float_attr(r, "price"), 0.0)
        if ts is not None and ts > target:
            break
    return candidate


def _volume_delta(rows: list[Any], seconds: int) -> float:
    if not rows:
        return 0.0
    latest_vol = _float_attr(rows[-1], "volume")
    latest_ts = _time_attr(rows[-1])
    if latest_vol is None or latest_ts is None:
        return 0.0
    target = latest_ts - timedelta(seconds=seconds)
    base_vol = None
    for r in reversed(rows):
        ts = _time_attr(r)
        if ts is not None and ts <= target and _float_attr(r, "volume") is not None:
            base_vol = _float_attr(r, "volume")
            break
    if base_vol is None:
        return 0.0
    return max(_num(latest_vol) - _num(base_vol), 0.0)
