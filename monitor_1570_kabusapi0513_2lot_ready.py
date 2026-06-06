#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1570 monitor v1.5
Purpose: patch-oriented monitor focused on 1m/3m logic.
Changes vs v1.4 concept:
- relax OVEREXTENDED / VWAP gap gate
- adaptive VWAP gate by regime
- slightly longer minimum holding time
- softer hard edge-break immediately after entry
- richer report fields

This file is designed to be practical and robust rather than minimal.
It uses REST polling against kabu station API.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib import error, request


from volatility_regime_gate import GateDecision, VolatilityRegimeGate, decision_features_json

JST = timezone(timedelta(hours=9))
API_BASE_DEFAULT = "http://localhost:18080/kabusapi"
SYMBOL_DEFAULT = "1570"
EXCHANGE_DEFAULT = 1
POLL_INTERVAL_SEC = 1.0
CONSOLE_STATUS_INTERVAL_SEC = 8.0
LIVE_ENTRY_TIMEOUT_SEC = 8
LIVE_EXIT_TIMEOUT_SEC = 10
LIVE_RETRY_MAX = 1
ENTRY_ERROR_BLOCK_SEC = 300
MARGIN_ENTRY_EXCHANGES = (9, 27)
RECOVERY_COOLDOWN_SEC = 10
ENTRY_POSITION_VERIFY_RETRY_MAX = 3
ENTRY_POSITION_VERIFY_RETRY_INTERVAL_SEC = 0.5

# ===== user-editable direct settings =====
API_PASSWORD_HARDCODED = "enmasa1023"  # ここにAPIパスワードを入れる
# =======================================


TRADE_WINDOWS = [
    ("09:03:00", "11:25:00"),
    ("12:35:00", "15:20:00"),
]
STOP_AFTER = "15:30:00"
FORCE_CLOSE_AFTER = "15:20:00"
NEW_ENTRY_CUTOFF_TIME = "14:50:00"
MIDDAY_ORDER_CANCEL_START = "11:29:00"
MIDDAY_ORDER_CANCEL_END = "11:30:30"

# v1.6 exit-tuned parameters
SPREAD_TICKS_MAX = 2.0
VWAP_GATE_MODES: dict[str, tuple[float, float, float]] = {
    "1x": (30.0, 40.0, 18.0),
    "2x": (60.0, 80.0, 36.0),
    "4x": (120.0, 160.0, 72.0),
}
CURRENT_VWAP_MODE = "2x"
VWAP_GAP_BPS_MAX_BASE = VWAP_GATE_MODES[CURRENT_VWAP_MODE][0]
VWAP_GAP_BPS_MAX_TREND = VWAP_GATE_MODES[CURRENT_VWAP_MODE][1]
VWAP_GAP_BPS_MAX_RANGE = VWAP_GATE_MODES[CURRENT_VWAP_MODE][2]

ADAPTIVE_CONTROL_ENABLED = True
LIGHT_BRAKE_ENABLED = True
LIGHT_BRAKE_EVALUATION_START = "09:30:00"
LIGHT_BRAKE_LOOKBACK_MINUTES = 30
LIGHT_BRAKE_STRAT_1M_MIN_TRADES = 5
LIGHT_BRAKE_STRAT_1M_PNL_LIMIT = -25.0
LIGHT_BRAKE_STRAT_1M_FREEZE_MINUTES = 20
LIGHT_BRAKE_4X_MIN_TRADES = 3
LIGHT_BRAKE_4X_PNL_LIMIT = -20.0
LIGHT_BRAKE_4X_BLOCK_MINUTES = 30
ENTRY_COOLDOWN_SEC = 45
REENTRY_AFTER_STOP_SEC = 90
MIN_HOLD_SEC_1M = 300
MIN_HOLD_SEC_3M = 300
MAX_HOLD_SEC_1M = 300
MAX_HOLD_SEC_3M = 300
STOP_TICKS_1M = 10
STOP_TICKS_3M = 10
HARD_STOP_TICKS = 15
TAKE_TICKS_1M = 10
TAKE_TICKS_3M = 10
PROB_UPPER_1M = 0.58
PROB_UPPER_3M = 0.54
PROB_EXIT_EDGE = 0.49
BOOK_NEUTRAL_OBI_L1_MIN = 0.02
BOOK_NEUTRAL_OBI_L3_MIN = 0.01

# Scalping entries are intentionally separated from the 1m/3m trend model.
# They target short lived reversals, squeeze moves, pullbacks, and breakouts.
SCALPING_ENABLED = True
SCALP_MAX_SPREAD_TICKS = 2.0
SCALP_MAX_VWAP_GAP_BPS = 140.0
SCALP_REBOUND_RET30_MIN = 0.00015
SCALP_REBOUND_OBI_L1_MIN = 0.55
SCALP_SQUEEZE_VWAP_GAP_BPS_MIN = 80.0
SCALP_SQUEEZE_RET30_MIN = 0.00030
SCALP_SQUEEZE_OBI_L1_MIN = 0.65
SCALP_MIN_TRADE_INTENSITY_30S = 50.0
SCALP_PULLBACK_VWAP_GAP_BPS_MAX = 80.0
SCALP_BREAKOUT_RET30_MIN = 0.00050
SCALP_SHORT_OBI_L1_MAX = -0.45
SCALP_SHORT_RET30_MAX = -0.00020
SCALP_EXIT_PARAMS: dict[str, tuple[int, int, int, int]] = {
    "SCALP_REBOUND_LONG": (10, 10, 8, 60),
    "SCALP_SQUEEZE_LONG": (10, 10, 5, 45),
    "SCALP_VWAP_PULLBACK_LONG": (10, 10, 15, 75),
    "SCALP_BREAKOUT_LONG": (10, 10, 10, 60),
    "SCALP_STRICT_SHORT": (10, 10, 8, 60),
}

RSI9_PERIOD = 9
RSI9_LONG_ENTRY = 20.0
RSI9_LONG_TP = 50.0
RSI9_LONG_SL = 0.0
RSI9_SHORT_ENTRY = 70.0
RSI9_SHORT_TP = 40.0
RSI9_SHORT_SL = 0.0
RSI9_LONG_ADD_ENTRY = 10.0
RSI20_LONG_WATCH_MINUTES = 10
RSI17_DROP_MA75_GAP_THRESHOLD = 0.008
RSI17_DROP_ENTRY_RULES = {
    "long_b_drop_ma75_up",
    "long_b_drop_ma75_up_all_ma_below_allowed",
    "short_b_drop_ma75_down",
    "short_b_drop_from_rsi70_ma75_up",
}


def now_jst() -> datetime:
    return datetime.now(JST)


def load_config(path: Optional[str]) -> dict[str, Any]:
    cfg: dict[str, Any] = {}
    if path:
        if not os.path.exists(path):
            raise SystemExit(f"config file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    return cfg


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None)
    p.add_argument("--api-password", dest="api_password", default=None)
    p.add_argument("--live-mode", action="store_true")
    p.add_argument("--paper-mode", action="store_true")
    p.add_argument("--order-password", default=None)
    p.add_argument("--order-qty", type=int, default=None)
    p.add_argument("--account-type", type=int, default=None)
    p.add_argument("--margin-trade-type", type=int, default=None)
    p.add_argument("--entry-cash-margin", type=int, default=None)
    p.add_argument("--exit-cash-margin", type=int, default=None)
    p.add_argument("--entry-deliv-type", type=int, default=None)
    p.add_argument("--exit-deliv-type", type=int, default=None)
    p.add_argument("--live-entry-timeout-sec", type=int, default=None)
    p.add_argument("--live-exit-timeout-sec", type=int, default=None)
    p.add_argument("--live-retry-max", type=int, default=None)
    p.add_argument("--disable-adaptive-control", action="store_true")
    p.add_argument("--initial-vwap-mode", choices=["1x", "2x", "4x"], default=None)
    p.add_argument("--outdir", default=None)
    p.add_argument("--runtime-minutes", type=float, default=None)
    p.add_argument("--base-url", default=None)
    return p.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def jst_date_str(dt: Optional[datetime] = None) -> str:
    return (dt or now_jst()).strftime("%Y-%m-%d")


def jst_date_compact(dt: Optional[datetime] = None) -> str:
    return (dt or now_jst()).strftime("%Y%m%d")


def time_in_windows(t: str, windows: list[tuple[str, str]]) -> bool:
    return any(s <= t <= e for s, e in windows)


def new_entry_cutoff_reached(config: dict[str, Any], ts_or_tstr: datetime | str) -> bool:
    cutoff = str(config.get("new_entry_cutoff_time", NEW_ENTRY_CUTOFF_TIME))
    tstr = ts_or_tstr.strftime("%H:%M:%S") if isinstance(ts_or_tstr, datetime) else str(ts_or_tstr)
    return tstr >= cutoff


def log_new_entry_cutoff_block(
    storage: Optional[Storage],
    config: dict[str, Any],
    ts: datetime,
    signal: str,
    reason_3: str,
    side: str,
) -> None:
    if storage is not None:
        storage.log_structured(
            "WARN",
            "NEW_ENTRY_BLOCKED_BY_CUTOFF_TIME",
            {
                "ts": ts.isoformat(),
                "cutoff_time": str(config.get("new_entry_cutoff_time", NEW_ENTRY_CUTOFF_TIME)),
                "signal": signal,
                "reason_3": reason_3,
                "side": side,
            },
        )


def tick_size_for_1570(price: float) -> float:
    _ = price
    return 10.0


def price_to_ticks(delta_price: float, ref_price: float) -> float:
    ts = tick_size_for_1570(ref_price)
    return delta_price / ts if ts else 0.0


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def set_vwap_mode(mode: str) -> None:
    global CURRENT_VWAP_MODE, VWAP_GAP_BPS_MAX_BASE, VWAP_GAP_BPS_MAX_TREND, VWAP_GAP_BPS_MAX_RANGE
    if mode not in VWAP_GATE_MODES:
        mode = "2x"
    CURRENT_VWAP_MODE = mode
    VWAP_GAP_BPS_MAX_BASE, VWAP_GAP_BPS_MAX_TREND, VWAP_GAP_BPS_MAX_RANGE = VWAP_GATE_MODES[mode]


def vwap_threshold_for_regime(regime: str) -> float:
    if regime in {"trend_up", "trend_down"}:
        return VWAP_GAP_BPS_MAX_TREND
    if regime == "range":
        return VWAP_GAP_BPS_MAX_RANGE
    return VWAP_GAP_BPS_MAX_BASE


def normalize_exchange_for_margin(config: dict[str, Any]) -> None:
    if not config.get("live_mode"):
        return
    if int(config.get("entry_cash_margin", 2)) != 2:
        return
    current = int(config.get("order_exchange", config.get("exchange", EXCHANGE_DEFAULT)))
    if current in MARGIN_ENTRY_EXCHANGES:
        return
    preferred = config.get("margin_entry_exchange")
    if preferred is not None:
        preferred_int = int(preferred)
        if preferred_int in MARGIN_ENTRY_EXCHANGES:
            config["order_exchange"] = preferred_int
            print(
                f"[WARN] exchange={current} may reject margin new orders (Code:100368). "
                f"Using margin_entry_exchange={preferred_int}."
            )
            return
    config["order_exchange"] = MARGIN_ENTRY_EXCHANGES[0]
    print(
        f"[WARN] exchange={current} may reject margin new orders (Code:100368). "
        f"Auto-switched order_exchange to {config['order_exchange']} (supported: {MARGIN_ENTRY_EXCHANGES})."
    )


def order_exchange(config: dict[str, Any]) -> int:
    return int(config.get("order_exchange", config.get("exchange", EXCHANGE_DEFAULT)))


def exit_exchange(config: dict[str, Any]) -> int:
    # Credit repayments are safest on the listed market. New margin entries may use
    # SOR/TSE+, but TSE-held margin positions cannot be repaid via SOR/TSE+.
    return int(config.get("exit_order_exchange", config.get("exchange", EXCHANGE_DEFAULT)))


def position_exchanges(config: dict[str, Any]) -> list[int]:
    exchanges: list[int] = []
    for ex in (order_exchange(config), exit_exchange(config), int(config.get("exchange", EXCHANGE_DEFAULT))):
        if ex not in exchanges:
            exchanges.append(ex)
    return exchanges


def apply_runtime_threshold_overrides(cfg: dict[str, Any]) -> None:
    global PROB_UPPER_1M, PROB_UPPER_3M, BOOK_NEUTRAL_OBI_L1_MIN, BOOK_NEUTRAL_OBI_L3_MIN, SCALPING_ENABLED
    if "prob_upper_1m" in cfg:
        PROB_UPPER_1M = float(cfg.get("prob_upper_1m", PROB_UPPER_1M))
    if "prob_upper_3m" in cfg:
        PROB_UPPER_3M = float(cfg.get("prob_upper_3m", PROB_UPPER_3M))
    if "book_neutral_obi_l1_min" in cfg:
        BOOK_NEUTRAL_OBI_L1_MIN = float(cfg.get("book_neutral_obi_l1_min", BOOK_NEUTRAL_OBI_L1_MIN))
    if "book_neutral_obi_l3_min" in cfg:
        BOOK_NEUTRAL_OBI_L3_MIN = float(cfg.get("book_neutral_obi_l3_min", BOOK_NEUTRAL_OBI_L3_MIN))
    scalping_cfg = cfg.get("scalping", {})
    if isinstance(scalping_cfg, dict) and "enabled" in scalping_cfg:
        SCALPING_ENABLED = bool(scalping_cfg.get("enabled", SCALPING_ENABLED))


class ApiHttpError(RuntimeError):
    def __init__(self, status_code: int, reason: str, body: str) -> None:
        self.status_code = status_code
        self.reason = reason
        self.body = body
        self.raw_json = _try_json(body)
        self.api_code = str(self.raw_json.get("Code", "")) if isinstance(self.raw_json, dict) else ""
        self.api_message = str(self.raw_json.get("Message", "")) if isinstance(self.raw_json, dict) else ""
        super().__init__(f"HTTP {status_code} {reason}: {body}")


def _try_json(raw: str) -> Any:
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return raw


def api_error_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, ApiHttpError):
        return {
            "http_status": exc.status_code,
            "http_reason": exc.reason,
            "api_code": exc.api_code,
            "api_message": exc.api_message,
            "raw_response_json": exc.raw_json,
            "raw_error": str(exc),
        }
    return {"raw_error": str(exc)}


def api_error_code(exc: Exception) -> str:
    return str(api_error_payload(exc).get("api_code") or "")


def _http_json(
    method: str,
    url: str,
    token: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> Any:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-API-KEY"] = token
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise ApiHttpError(e.code, e.reason, body) from e


class KabuApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token: Optional[str] = None

    def get_token(self, api_password: str) -> str:
        res = _http_json("POST", f"{self.base_url}/token", payload={"APIPassword": api_password})
        tok = res.get("Token")
        if not tok:
            raise RuntimeError(f"token missing: {res}")
        self.token = tok
        return tok

    def register_symbol(self, symbol: str, exchange: int) -> Any:
        payload = {"Symbols": [{"Symbol": symbol, "Exchange": exchange}]}
        return _http_json("PUT", f"{self.base_url}/register", token=self.token, payload=payload)

    def get_board(self, symbol: str, exchange: int) -> dict[str, Any]:
        sym = f"{symbol}@{exchange}"
        return _http_json("GET", f"{self.base_url}/board/{sym}", token=self.token)

    def send_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _http_json("POST", f"{self.base_url}/sendorder", token=self.token, payload=payload)

    def cancel_order(self, order_id: str, order_password: str = "") -> dict[str, Any]:
        _ = order_password
        # kabu STATION API cancelorder requires `OrderId` (lowercase d) only.
        # `OrderID`/`Password` can make cancellation fail, which blocks protective exits.
        payload = {"OrderId": order_id}
        return _http_json("PUT", f"{self.base_url}/cancelorder", token=self.token, payload=payload)

    def get_positions(self, symbol: str, exchange: Optional[int] = None) -> list[dict[str, Any]]:
        _ = exchange
        # /positions does not define an exchange query parameter. It returns the
        # exchange on each position row, so filter/match from the response instead.
        url = f"{self.base_url}/positions?product=2&symbol={symbol}"
        res = _http_json("GET", url, token=self.token)
        return res if isinstance(res, list) else []

    def get_orders(self, order_id: str = "", product: int = 2) -> list[dict[str, Any]]:
        params = [f"product={product}"]
        if order_id:
            params.append(f"id={order_id}")
        url = f"{self.base_url}/orders?{'&'.join(params)}"
        res = _http_json("GET", url, token=self.token)
        return res if isinstance(res, list) else []


@dataclass
class TickSnapshot:
    ts: datetime
    price: Optional[float]
    volume: Optional[float]
    vwap: Optional[float]
    sell1_price: Optional[float]
    sell1_qty: Optional[float]
    buy1_price: Optional[float]
    buy1_qty: Optional[float]
    sell2_price: Optional[float] = None
    sell2_qty: Optional[float] = None
    buy2_price: Optional[float] = None
    buy2_qty: Optional[float] = None
    sell3_price: Optional[float] = None
    sell3_qty: Optional[float] = None
    buy3_price: Optional[float] = None
    buy3_qty: Optional[float] = None
    raw_json: Optional[str] = None


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    ma5: Optional[float] = None
    ma13: Optional[float] = None
    ma25: Optional[float] = None
    ma75: Optional[float] = None
    atr14: Optional[float] = None


@dataclass
class FeatureSnapshot:
    ts: datetime
    price: float
    vwap: float
    spread_ticks: float
    obi_l1: float
    obi_l3: float
    vwap_gap_bps: float
    ret_30s: float
    ret_1m: float
    ret_3m: float
    close_pos_in_bar_1m: float
    close_pos_in_bar_3m: float
    ma_trend_score_3m: float
    pullback_quality: float
    reacceleration_score: float
    overextension_penalty: float
    volume_1m: float
    volume_3m: float
    trade_intensity_30s: float
    regime: str


@dataclass
class PredictionSnapshot:
    ts: datetime
    regime: str
    p_up_1m: float
    p_down_1m: float
    p_up_3m: float
    p_down_3m: float
    signal: str
    rsi9_value: float
    reason_1: str
    reason_2: str
    reason_3: str


@dataclass
class PositionState:
    side: str
    strategy: str
    entry_ts: datetime
    entry_price: float
    entry_p_up_1m: float
    entry_p_up_3m: float
    stop_ticks: int
    take_ticks: int
    min_hold_sec: int
    max_hold_sec: int
    entry_vwap_gap_bps: float = 0.0
    entry_regime: str = ""
    entry_vwap_mode: str = "2x"
    margin_trade_type: int = 3
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None
    take_profit_order_id: Optional[str] = None
    take_profit_order_ids: list[str] = field(default_factory=list)
    order_qty: int = 2
    filled_qty: int = 0
    remaining_qty: int = 0
    take_profit_trigger_ts: Optional[datetime] = None
    entry_fill_price: Optional[float] = None
    exit_fill_price: Optional[float] = None
    rsi_special_entry: bool = False
    rsi_special_tp_stage: int = 0
    rsi_special_tp_order_ts: Optional[datetime] = None
    rsi10_add_done: bool = False
    managed_execution_ids: list[str] = field(default_factory=list)
    managed_close_positions: list[dict[str, Any]] = field(default_factory=list)
    trailing_active: bool = False
    trailing_trigger_ticks: int = 10
    trailing_floor_ticks: int = 5
    trailing_width_ticks: int = 20
    trailing_high: Optional[float] = None
    trailing_low: Optional[float] = None
    trailing_stop_price: Optional[float] = None
    ma5_breach_count: int = 0
    trailing_started_at: Optional[datetime] = None
    trailing_ma5_bar_bucket: Optional[datetime] = None
    trailing_ma5_bar_open: Optional[float] = None
    trailing_ma5_reference: Optional[float] = None
    trailing_ma5_open_relation: str = ""
    ma5_exit_skip_count: int = 0
    last_ma5_exit_deferred_ts: Optional[datetime] = None
    last_hold_score: Optional[float] = None
    hard_stop_ticks: int = HARD_STOP_TICKS


@dataclass
class ClosedTradeSummary:
    exit_ts: datetime
    side: str
    strategy: str
    pnl_ticks: float
    exit_reason: str
    entry_vwap_mode: str


@dataclass
class LiveOrderResult:
    ok: bool
    message: str
    order_id: str = ""
    api_code: str = ""
    api_message: str = ""
    recoverable: bool = False
    limit_price: Optional[float] = None
    order_send_ts: Optional[datetime] = None
    order_response_ts: Optional[datetime] = None
    actual_fill_price: Optional[float] = None
    fill_source: str = ""


@dataclass
class EntryPositionVerifyResult:
    position: Optional[PositionState] = None
    verified_flat: bool = False
    verify_error: bool = False
    reason: str = ""
    positions_count: int = 0
    positions_summary: list[dict[str, Any]] = field(default_factory=list)
    raw_positions: list[dict[str, Any]] = field(default_factory=list)
    matching_qty: int = 0
    total_qty: int = 0


@dataclass
class ReconcileResult:
    ok_for_entry: bool
    live_state: str
    message: str
    total_leaves_qty: int = 0
    matching_leaves_qty: int = 0
    positions: Optional[list[dict[str, Any]]] = None


@dataclass
class AdaptiveControlState:
    enabled: bool = ADAPTIVE_CONTROL_ENABLED
    vwap_mode: str = CURRENT_VWAP_MODE
    freeze_strat_1m_until: Optional[datetime] = None
    block_4x_until: Optional[datetime] = None

    def allow_strat(self, strategy: str, ts: datetime) -> bool:
        if not self.enabled:
            return True
        if strategy == "STRAT_1M" and self.freeze_strat_1m_until and ts < self.freeze_strat_1m_until:
            return False
        return True


@dataclass
class MonitorStatus:
    count: int = 0
    open_position: Optional[PositionState] = None
    last_entry_ts_by_side: dict[str, Optional[datetime]] = None
    reentry_block_until_by_side: dict[str, Optional[datetime]] = None
    entry_global_block_until: Optional[datetime] = None
    recovery_until: Optional[datetime] = None
    live_state: str = "FLAT"
    last_error_code: str = ""
    last_error_message: str = ""
    exit_fail_count: int = 0
    last_entry_reject_key: str = ""
    midday_written: bool = False
    pending_entry_side: Optional[str] = None
    pending_entry_ts: Optional[datetime] = None
    pending_add: bool = False
    pending_exit: bool = False
    force_market_close_sent: bool = False
    midday_order_cleanup_done: bool = False
    midday_bar_finalize_done: bool = False
    rsi20_long_watch_active: bool = False
    rsi20_long_watch_started_at: Optional[datetime] = None
    rsi20_long_watch_expires_at: Optional[datetime] = None
    rsi20_long_watch_started_rsi: Optional[float] = None
    rsi70_drop_long_watch_active: bool = False
    rsi70_drop_long_watch_started_at: Optional[datetime] = None
    rsi70_drop_long_watch_expires_at: Optional[datetime] = None
    rsi70_drop_long_watch_started_rsi: Optional[float] = None
    rsi70_drop_long_watch_started_price: Optional[float] = None
    rsi70_drop_long_watch_reason: str = ""
    rsi70_drop_long_watch_started_bar_ts: Optional[datetime] = None
    force_close_state: str = ""
    failed_close_signature: str = ""
    failed_close_signature_ts: Optional[datetime] = None
    last_manual_position_check_ts: Optional[datetime] = None
    last_recovery_flat_check_ts: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.last_entry_ts_by_side is None:
            self.last_entry_ts_by_side = {"LONG": None, "SHORT": None}
        if self.reentry_block_until_by_side is None:
            self.reentry_block_until_by_side = {"LONG": None, "SHORT": None}


MASK_KEYS = {"password", "apipassword", "api_password", "order_password", "x-api-key", "token", "authorization"}
STORAGE_KEY_ALIASES = {
    "strategy_name": "strategy",
    "signal_reason": "signal_reason",
    "order_id": "order_id",
    "side": "side",
    "symbol": "symbol",
    "entry_price": "entry_price",
    "exit_price": "exit_price",
    "qty": "qty",
    "price": "price",
}


def sanitize_for_log(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if str(k).lower() in MASK_KEYS:
                out[k] = "***MASKED***"
            else:
                out[k] = sanitize_for_log(v)
        return out
    if isinstance(value, list):
        return [sanitize_for_log(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def safe_json(value: Any) -> str:
    return json.dumps(sanitize_for_log(value), ensure_ascii=False, default=str)


def build_storage_normalized_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Keep original payload while adding a stable, DB-oriented normalized view.
    This makes analysis resilient to API field-name drift and config key naming.
    """
    normalized: dict[str, Any] = {}
    for src_key, dst_key in STORAGE_KEY_ALIASES.items():
        if src_key in payload and payload.get(src_key) is not None:
            normalized[dst_key] = payload.get(src_key)
    if isinstance(payload.get("raw_response_json"), dict):
        raw = payload["raw_response_json"]
        if "OrderId" in raw and "order_id" not in normalized:
            normalized["order_id"] = raw.get("OrderId")
        if "Result" in raw:
            normalized["api_result"] = raw.get("Result")
    out = dict(payload)
    if normalized:
        out["_normalized_storage"] = normalized
    return out


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def position_state_payload(pos: Optional[PositionState]) -> dict[str, Any]:
    if pos is None:
        return {"state": "FLAT"}
    return {
        "state": "OPEN",
        "side": pos.side,
        "strategy": pos.strategy,
        "entry_time": pos.entry_ts.isoformat(),
        "entry_price": pos.entry_price,
        "margin_trade_type": pos.margin_trade_type,
        "entry_order_id": pos.entry_order_id,
        "exit_order_id": pos.exit_order_id,
        "take_profit_order_id": pos.take_profit_order_id,
        "managed_execution_ids": list(pos.managed_execution_ids or []),
        "managed_close_positions": list(pos.managed_close_positions or []),
        "trailing_active": pos.trailing_active,
        "trailing_trigger_ticks": pos.trailing_trigger_ticks,
        "trailing_floor_ticks": pos.trailing_floor_ticks,
        "trailing_width_ticks": pos.trailing_width_ticks,
        "trailing_high": pos.trailing_high,
        "trailing_low": pos.trailing_low,
        "trailing_stop_price": pos.trailing_stop_price,
        "ma5_breach_count": pos.ma5_breach_count,
        "trailing_started_at": pos.trailing_started_at.isoformat() if pos.trailing_started_at else None,
        "trailing_ma5_bar_bucket": pos.trailing_ma5_bar_bucket.isoformat() if pos.trailing_ma5_bar_bucket else None,
        "trailing_ma5_bar_open": pos.trailing_ma5_bar_open,
        "trailing_ma5_reference": pos.trailing_ma5_reference,
        "trailing_ma5_open_relation": pos.trailing_ma5_open_relation,
        "ma5_exit_skip_count": pos.ma5_exit_skip_count,
        "last_ma5_exit_deferred_ts": pos.last_ma5_exit_deferred_ts.isoformat() if pos.last_ma5_exit_deferred_ts else None,
        "last_hold_score": pos.last_hold_score,
        "hard_stop_ticks": pos.hard_stop_ticks,
    }


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as con:
            cur = con.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS system_events(
              ts TEXT, level TEXT, event_type TEXT, message TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS structured_events(
              event_id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT, level TEXT, event_type TEXT, correlation_id TEXT, payload_json TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS execution_facts(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT,
              event_type TEXT,
              order_id TEXT,
              strategy TEXT,
              side TEXT,
              signal_reason TEXT,
              price REAL,
              qty REAL,
              fill_price REAL,
              raw_payload_json TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots(
              ts TEXT PRIMARY KEY,
              price REAL, volume REAL, vwap REAL,
              sell1_price REAL, sell1_qty REAL, buy1_price REAL, buy1_qty REAL,
              sell2_price REAL, sell2_qty REAL, buy2_price REAL, buy2_qty REAL,
              sell3_price REAL, sell3_qty REAL, buy3_price REAL, buy3_qty REAL,
              spread_ticks REAL, raw_json TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bars_1m(
              ts TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL,
              ma5 REAL, ma13 REAL, ma25 REAL, ma75 REAL, atr14 REAL
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bars_3m(
              ts TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL,
              ma5 REAL, ma13 REAL, ma25 REAL, ma75 REAL, atr14 REAL
            )""")
            self._ensure_bar_ma13_column(cur, "bars_1m")
            self._ensure_bar_ma13_column(cur, "bars_3m")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS feature_snapshot(
              ts TEXT PRIMARY KEY,
              price REAL, vwap REAL, spread_ticks REAL, obi_l1 REAL, obi_l3 REAL, vwap_gap_bps REAL,
              ret_30s REAL, ret_1m REAL, ret_3m REAL,
              close_pos_in_bar_1m REAL, close_pos_in_bar_3m REAL,
              ma_trend_score_3m REAL, pullback_quality REAL, reacceleration_score REAL,
              overextension_penalty REAL, volume_1m REAL, volume_3m REAL, trade_intensity_30s REAL,
              regime TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS prediction_snapshot(
              ts TEXT PRIMARY KEY,
              regime TEXT,
              p_up_1m REAL, p_down_1m REAL, p_up_3m REAL, p_down_3m REAL,
              signal TEXT, rsi9_value REAL, reason_1 TEXT, reason_2 TEXT, reason_3 TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades(
              trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
              entry_ts TEXT, exit_ts TEXT, entry_side TEXT, strategy TEXT,
              entry_price REAL, exit_price REAL,
              pnl_ticks REAL, holding_sec REAL, exit_reason TEXT,
              mfe_ticks REAL, mae_ticks REAL,
              exit_signal_price REAL,
              exit_order_limit_price REAL,
              exit_actual_fill_price REAL,
              exit_fill_source TEXT,
              exit_signal_ts TEXT,
              exit_order_send_ts TEXT,
              exit_order_response_ts TEXT,
              exit_decision_to_send_ms REAL,
              exit_order_id TEXT
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS gate_decisions(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT, symbol TEXT,
              raw_signal TEXT, final_signal TEXT,
              gate_mode TEXT, gate_action TEXT, gate_reason TEXT, gate_applied INTEGER,
              regime TEXT,
              price REAL, vwap REAL,
              spread_ticks REAL, spread_ratio REAL,
              volume_ratio REAL, volume_delta REAL,
              realized_vol_1m REAL, realized_vol_5m REAL, range_5m REAL,
              board_imbalance REAL, price_vs_vwap REAL,
              p_up_1m REAL, p_down_1m REAL, p_up_3m REAL, p_down_3m REAL,
              raw_features_json TEXT
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_execution_facts_order_id_ts ON execution_facts(order_id, ts)")
            self._ensure_prediction_rsi9_column(cur)
            self._ensure_execution_fill_price_column(cur)
            self._ensure_paper_trade_exit_detail_columns(cur)
            con.commit()

    def _ensure_bar_ma13_column(self, cur: sqlite3.Cursor, table: str) -> None:
        cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
        if "ma13" not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN ma13 REAL")

    def _ensure_prediction_rsi9_column(self, cur: sqlite3.Cursor) -> None:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(prediction_snapshot)").fetchall()]
        if "rsi9_value" not in cols:
            cur.execute("ALTER TABLE prediction_snapshot ADD COLUMN rsi9_value REAL")

    def _ensure_execution_fill_price_column(self, cur: sqlite3.Cursor) -> None:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(execution_facts)").fetchall()]
        if "fill_price" not in cols:
            cur.execute("ALTER TABLE execution_facts ADD COLUMN fill_price REAL")

    def _ensure_paper_trade_exit_detail_columns(self, cur: sqlite3.Cursor) -> None:
        cols = [r[1] for r in cur.execute("PRAGMA table_info(paper_trades)").fetchall()]
        wanted = {
            "exit_signal_price": "REAL",
            "exit_order_limit_price": "REAL",
            "exit_actual_fill_price": "REAL",
            "exit_fill_source": "TEXT",
            "exit_signal_ts": "TEXT",
            "exit_order_send_ts": "TEXT",
            "exit_order_response_ts": "TEXT",
            "exit_decision_to_send_ms": "REAL",
            "exit_order_id": "TEXT",
        }
        for name, typ in wanted.items():
            if name not in cols:
                cur.execute(f"ALTER TABLE paper_trades ADD COLUMN {name} {typ}")

    def log(self, level: str, event_type: str, message: str) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT INTO system_events VALUES (?,?,?,?)",
                (now_jst().isoformat(), level, event_type, message),
            )
            con.commit()

    def log_structured(
        self,
        level: str,
        event_type: str,
        payload: dict[str, Any],
        correlation_id: str = "",
        mirror_message: Optional[str] = None,
    ) -> None:
        ts = now_jst().isoformat()
        safe_payload = build_storage_normalized_payload(sanitize_for_log(payload))
        with self._connect() as con:
            con.execute(
                "INSERT INTO structured_events(ts,level,event_type,correlation_id,payload_json) VALUES (?,?,?,?,?)",
                (ts, level, event_type, correlation_id, json.dumps(safe_payload, ensure_ascii=False, default=str)),
            )
            normalized = safe_payload.get("_normalized_storage", {})
            if isinstance(normalized, dict) and normalized.get("order_id"):
                con.execute(
                    """
                    INSERT INTO execution_facts(
                      ts,event_type,order_id,strategy,side,signal_reason,price,qty,fill_price,raw_payload_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        ts,
                        event_type,
                        str(normalized.get("order_id")),
                        str(normalized.get("strategy", "")),
                        str(normalized.get("side", "")),
                        str(normalized.get("signal_reason", "")),
                        _safe_float(normalized.get("price")),
                        _safe_float(normalized.get("qty")),
                        _safe_float(normalized.get("fill_price")),
                        json.dumps(safe_payload, ensure_ascii=False, default=str),
                    ),
                )
            if mirror_message is not None:
                con.execute("INSERT INTO system_events VALUES (?,?,?,?)", (ts, level, event_type, mirror_message))
            con.commit()

    def insert_snapshot(self, s: TickSnapshot, spread_ticks: Optional[float]) -> None:
        with self._connect() as con:
            con.execute(
                """INSERT OR REPLACE INTO market_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    s.ts.isoformat(),
                    s.price,
                    s.volume,
                    s.vwap,
                    s.sell1_price,
                    s.sell1_qty,
                    s.buy1_price,
                    s.buy1_qty,
                    s.sell2_price,
                    s.sell2_qty,
                    s.buy2_price,
                    s.buy2_qty,
                    s.sell3_price,
                    s.sell3_qty,
                    s.buy3_price,
                    s.buy3_qty,
                    spread_ticks,
                    s.raw_json,
                ),
            )
            con.commit()

    def insert_bar(self, table: str, b: Bar) -> None:
        with self._connect() as con:
            con.execute(
                f"""INSERT OR REPLACE INTO {table}
                (ts,open,high,low,close,volume,vwap,ma5,ma13,ma25,ma75,atr14)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    b.ts.isoformat(),
                    b.open,
                    b.high,
                    b.low,
                    b.close,
                    b.volume,
                    b.vwap,
                    b.ma5,
                    b.ma13,
                    b.ma25,
                    b.ma75,
                    b.atr14,
                ),
            )
            con.commit()

    def insert_feature(self, f: FeatureSnapshot) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO feature_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f.ts.isoformat(),
                    f.price,
                    f.vwap,
                    f.spread_ticks,
                    f.obi_l1,
                    f.obi_l3,
                    f.vwap_gap_bps,
                    f.ret_30s,
                    f.ret_1m,
                    f.ret_3m,
                    f.close_pos_in_bar_1m,
                    f.close_pos_in_bar_3m,
                    f.ma_trend_score_3m,
                    f.pullback_quality,
                    f.reacceleration_score,
                    f.overextension_penalty,
                    f.volume_1m,
                    f.volume_3m,
                    f.trade_intensity_30s,
                    f.regime,
                ),
            )
            con.commit()

    def insert_prediction(self, p: PredictionSnapshot) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO prediction_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    p.ts.isoformat(),
                    p.regime,
                    p.p_up_1m,
                    p.p_down_1m,
                    p.p_up_3m,
                    p.p_down_3m,
                    p.signal,
                    p.rsi9_value,
                    p.reason_1,
                    p.reason_2,
                    p.reason_3,
                ),
            )
            con.commit()

    def insert_gate_decision(self, symbol: str, pred: PredictionSnapshot, decision: GateDecision) -> None:
        gf = decision.features
        with self._connect() as con:
            con.execute(
                """INSERT INTO gate_decisions(
                  ts,symbol,raw_signal,final_signal,gate_mode,gate_action,gate_reason,gate_applied,regime,
                  price,vwap,spread_ticks,spread_ratio,volume_ratio,volume_delta,realized_vol_1m,realized_vol_5m,range_5m,
                  board_imbalance,price_vs_vwap,p_up_1m,p_down_1m,p_up_3m,p_down_3m,raw_features_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    pred.ts.isoformat(),
                    symbol,
                    decision.raw_signal,
                    decision.final_signal,
                    decision.mode,
                    decision.action,
                    decision.reason,
                    1 if decision.applied else 0,
                    decision.regime,
                    gf.price,
                    gf.vwap,
                    gf.spread_ticks,
                    gf.spread_ratio,
                    gf.volume_ratio,
                    gf.volume_delta,
                    gf.realized_vol_1m,
                    gf.realized_vol_5m,
                    gf.range_5m,
                    gf.board_imbalance,
                    gf.price_vs_vwap,
                    pred.p_up_1m,
                    pred.p_down_1m,
                    pred.p_up_3m,
                    pred.p_down_3m,
                    decision_features_json(gf),
                ),
            )
            con.commit()

    def insert_execution_fill_price(self, event_type: str, order_id: str, side: str, strategy: str, signal_reason: str, fill_price: Optional[float]) -> None:
        if not order_id:
            return
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO execution_facts(
                  ts,event_type,order_id,strategy,side,signal_reason,price,qty,fill_price,raw_payload_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    now_jst().isoformat(),
                    event_type,
                    order_id,
                    strategy,
                    side,
                    signal_reason,
                    None,
                    None,
                    fill_price,
                    json.dumps({"event_type": event_type, "fill_price": fill_price}, ensure_ascii=False, default=str),
                ),
            )
            con.commit()

    def insert_trade(self, **kwargs: Any) -> None:
        with self._connect() as con:
            con.execute(
                """INSERT INTO paper_trades(
                     entry_ts,exit_ts,entry_side,strategy,entry_price,exit_price,pnl_ticks,holding_sec,exit_reason,mfe_ticks,mae_ticks,
                     exit_signal_price,exit_order_limit_price,exit_actual_fill_price,exit_fill_source,exit_signal_ts,
                     exit_order_send_ts,exit_order_response_ts,exit_decision_to_send_ms,exit_order_id
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    kwargs.get("entry_ts"),
                    kwargs.get("exit_ts"),
                    kwargs.get("entry_side"),
                    kwargs.get("strategy"),
                    kwargs.get("entry_price"),
                    kwargs.get("exit_price"),
                    kwargs.get("pnl_ticks"),
                    kwargs.get("holding_sec"),
                    kwargs.get("exit_reason"),
                    kwargs.get("mfe_ticks"),
                    kwargs.get("mae_ticks"),
                    kwargs.get("exit_signal_price"),
                    kwargs.get("exit_order_limit_price"),
                    kwargs.get("exit_actual_fill_price"),
                    kwargs.get("exit_fill_source"),
                    kwargs.get("exit_signal_ts"),
                    kwargs.get("exit_order_send_ts"),
                    kwargs.get("exit_order_response_ts"),
                    kwargs.get("exit_decision_to_send_ms"),
                    kwargs.get("exit_order_id"),
                ),
            )
            con.commit()


class RollingBars:
    def __init__(self, minutes: int) -> None:
        self.minutes = minutes
        self.current_bucket: Optional[datetime] = None
        self.rows: list[TickSnapshot] = []
        self.history: deque[Bar] = deque(maxlen=400)

    def _bucket(self, ts: datetime) -> datetime:
        minute = (ts.minute // self.minutes) * self.minutes
        return ts.replace(second=0, microsecond=0, minute=minute)

    def update(self, snap: TickSnapshot) -> Optional[Bar]:
        if snap.price is None:
            return None
        bucket = self._bucket(snap.ts)
        if self.current_bucket is None:
            self.current_bucket = bucket
        if bucket != self.current_bucket:
            bar = self._finalize_bar(self.current_bucket, self.rows)
            self.history.append(bar)
            self.current_bucket = bucket
            self.rows = [snap]
            return self._decorate_bar(bar)
        self.rows.append(snap)
        return None

    def force_finalize(self) -> Optional[Bar]:
        if self.current_bucket and self.rows:
            bar = self._finalize_bar(self.current_bucket, self.rows)
            self.history.append(bar)
            self.rows = []
            self.current_bucket = None
            return self._decorate_bar(bar)
        self.current_bucket = None
        return None

    def latest(self) -> Optional[Bar]:
        return self.history[-1] if self.history else None

    def prev(self, n: int = 1) -> Optional[Bar]:
        if len(self.history) >= n + 1:
            return list(self.history)[-1 - n]
        return None

    def _finalize_bar(self, ts: datetime, rows: list[TickSnapshot]) -> Bar:
        prices = [r.price for r in rows if r.price is not None]
        vols = [r.volume for r in rows if r.volume is not None]
        vwaps = [r.vwap for r in rows if r.vwap is not None]
        open_ = prices[0]
        high = max(prices)
        low = min(prices)
        close = prices[-1]
        volume = max((vols[-1] - vols[0]) if len(vols) >= 2 else 0.0, 0.0)
        vwap = vwaps[-1] if vwaps else close
        return Bar(ts=ts, open=open_, high=high, low=low, close=close, volume=volume, vwap=vwap)

    def _decorate_bar(self, bar: Bar) -> Bar:
        closes = [b.close for b in self.history]
        if len(closes) >= 5:
            bar.ma5 = sum(closes[-5:]) / 5.0
        if len(closes) >= 13:
            bar.ma13 = sum(closes[-13:]) / 13.0
        if len(closes) >= 25:
            bar.ma25 = sum(closes[-25:]) / 25.0
        if len(closes) >= 75:
            bar.ma75 = sum(closes[-75:]) / 75.0
        if len(self.history) >= 15:
            bars = list(self.history)
            true_ranges: list[float] = []
            for i, hist_bar in enumerate(bars):
                prev_close = bars[i - 1].close if i > 0 else hist_bar.close
                true_ranges.append(
                    max(
                        hist_bar.high - hist_bar.low,
                        abs(hist_bar.high - prev_close),
                        abs(hist_bar.low - prev_close),
                    )
                )
            if len(true_ranges) >= 14:
                bar.atr14 = sum(true_ranges[-14:]) / 14.0
        return bar



def preload_prev_day_1m_bars(outdir: str, today_db_path: str, limit: int = 120) -> list[Bar]:
    """Load recent 1m bars from the most recent previous monitor DB in outdir."""
    today_name = os.path.basename(today_db_path)
    cands: list[tuple[str, str]] = []
    for name in os.listdir(outdir):
        if not (name.startswith("monitor_1570_") and name.endswith(".db")):
            continue
        if name == today_name:
            continue
        date_part = name[len("monitor_1570_"):-len(".db")]
        if len(date_part) == 8 and date_part.isdigit():
            cands.append((date_part, os.path.join(outdir, name)))
    if not cands:
        return []
    cands.sort(key=lambda x: x[0], reverse=True)
    prev_db_path = cands[0][1]
    con = sqlite3.connect(prev_db_path)
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT ts,open,high,low,close,volume,vwap,ma5,ma13,ma25,ma75,atr14
            FROM bars_1m
            ORDER BY ts DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
    finally:
        con.close()
    bars: list[Bar] = []
    for row in reversed(rows):
        try:
            bars.append(
                Bar(
                    ts=datetime.fromisoformat(str(row[0])),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5] or 0.0),
                    vwap=float(row[6] or row[4]),
                    ma5=(float(row[7]) if row[7] is not None else None),
                    ma13=(float(row[8]) if row[8] is not None else None),
                    ma25=(float(row[9]) if row[9] is not None else None),
                    ma75=(float(row[10]) if row[10] is not None else None),
                    atr14=(float(row[11]) if row[11] is not None else None),
                )
            )
        except Exception:
            continue
    return bars

def extract_snapshot(raw: dict[str, Any]) -> TickSnapshot:
    def g(path: str) -> Any:
        cur: Any = raw
        for part in path.split("."):
            if cur is None:
                return None
            cur = cur.get(part) if isinstance(cur, dict) else None
        return cur

    def d(v: Any) -> Optional[float]:
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    ts_raw = raw.get("CurrentPriceTime") or now_jst().isoformat()
    try:
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).astimezone(JST)
    except Exception:
        ts = now_jst()

    return TickSnapshot(
        ts=ts,
        price=d(raw.get("CurrentPrice")),
        volume=d(raw.get("TradingVolume")),
        vwap=d(raw.get("VWAP")),
        sell1_price=d(g("Sell1.Price")),
        sell1_qty=d(g("Sell1.Qty")),
        buy1_price=d(g("Buy1.Price")),
        buy1_qty=d(g("Buy1.Qty")),
        sell2_price=d(g("Sell2.Price")),
        sell2_qty=d(g("Sell2.Qty")),
        buy2_price=d(g("Buy2.Price")),
        buy2_qty=d(g("Buy2.Qty")),
        sell3_price=d(g("Sell3.Price")),
        sell3_qty=d(g("Sell3.Qty")),
        buy3_price=d(g("Buy3.Price")),
        buy3_qty=d(g("Buy3.Qty")),
        raw_json=json.dumps(raw, ensure_ascii=False),
    )


def calc_spread_ticks(s: TickSnapshot) -> Optional[float]:
    if s.sell1_price is None or s.buy1_price is None or s.price is None:
        return None
    spread_price = s.sell1_price - s.buy1_price
    if spread_price < 0:
        spread_price = abs(spread_price)
    return spread_price / tick_size_for_1570(s.price)


def calc_obi(s: TickSnapshot) -> tuple[float, float]:
    b1 = s.buy1_qty or 0.0
    a1 = s.sell1_qty or 0.0
    obi_l1 = ((b1 - a1) / (b1 + a1)) if (b1 + a1) > 0 else 0.0
    bs = (s.buy1_qty or 0.0) + (s.buy2_qty or 0.0) + (s.buy3_qty or 0.0)
    ss = (s.sell1_qty or 0.0) + (s.sell2_qty or 0.0) + (s.sell3_qty or 0.0)
    obi_l3 = ((bs - ss) / (bs + ss)) if (bs + ss) > 0 else 0.0
    return obi_l1, obi_l3


def calc_close_pos_in_bar(bar: Optional[Bar]) -> float:
    if not bar:
        return 0.5
    rng = max(bar.high - bar.low, 1e-9)
    return (bar.close - bar.low) / rng


def detect_regime(
    bar1: Optional[Bar],
    bar3: Optional[Bar],
    spread_ticks: Optional[float],
    obi_l1: float,
) -> str:
    if not bar1 or not bar3:
        return "chaos"
    if bar3.ma5 is not None and bar3.ma25 is not None:
        if bar3.ma5 > bar3.ma25 and bar3.close > bar3.vwap:
            return "trend_up"
        if bar3.ma5 < bar3.ma25 and bar3.close < bar3.vwap:
            return "trend_down"
    if spread_ticks is not None and spread_ticks <= 1.0 and abs(obi_l1) < 0.05:
        return "range"
    return "chaos"


def build_features(
    ts: datetime,
    tick_buf: deque[TickSnapshot],
    bar1: Optional[Bar],
    prev1: Optional[Bar],
    bar3: Optional[Bar],
    prev3: Optional[Bar],
) -> Optional[FeatureSnapshot]:
    if not tick_buf or bar1 is None or bar3 is None:
        return None
    s = tick_buf[-1]
    if s.price is None or s.vwap is None:
        return None

    spread_ticks = calc_spread_ticks(s) or 0.0
    obi_l1, obi_l3 = calc_obi(s)
    vwap_gap_bps = ((s.price / s.vwap) - 1.0) * 10000.0 if s.vwap else 0.0

    close_30s_ago = None
    volume_30s_ago = None
    target_ts = s.ts - timedelta(seconds=30)
    for old in reversed(tick_buf):
        if old.ts <= target_ts and old.price is not None:
            close_30s_ago = old.price
            volume_30s_ago = old.volume
            break

    ret_30s = ((s.price / close_30s_ago) - 1.0) if close_30s_ago else 0.0
    ret_1m = ((bar1.close / prev1.close) - 1.0) if prev1 and prev1.close else 0.0
    ret_3m = ((bar3.close / prev3.close) - 1.0) if prev3 and prev3.close else 0.0
    close_pos_in_bar_1m = calc_close_pos_in_bar(bar1)
    close_pos_in_bar_3m = calc_close_pos_in_bar(bar3)

    ma_trend_score_3m = 0.0
    if bar3.ma5 and bar3.ma25:
        ma_trend_score_3m = (bar3.ma5 - bar3.ma25) / max(abs(bar3.ma25), 1e-9)

    pullback_quality = 0.0
    if ret_3m > 0 and ret_1m < 0 and s.price >= s.vwap:
        pullback_quality = min(abs(ret_1m) / 0.001, 1.0)
    elif ret_3m < 0 and ret_1m > 0 and s.price <= s.vwap:
        pullback_quality = min(abs(ret_1m) / 0.001, 1.0)

    reacceleration_score = 0.0
    if ret_30s > 0 and obi_l1 > 0.04:
        reacceleration_score = min(ret_30s / 0.0005, 1.0)
    elif ret_30s < 0 and obi_l1 < -0.04:
        reacceleration_score = -min(abs(ret_30s) / 0.0005, 1.0)

    regime = detect_regime(bar1, bar3, spread_ticks, obi_l1)
    threshold = vwap_threshold_for_regime(regime)
    overextension_penalty = max(0.0, abs(vwap_gap_bps) - threshold) / 10.0

    volume_1m = bar1.volume
    volume_3m = bar3.volume
    if volume_30s_ago is not None and s.volume is not None:
        trade_intensity_30s = max((s.volume - volume_30s_ago) / 30.0, 0.0)
    else:
        trade_intensity_30s = 0.0

    return FeatureSnapshot(
        ts=ts,
        price=s.price,
        vwap=s.vwap,
        spread_ticks=spread_ticks,
        obi_l1=obi_l1,
        obi_l3=obi_l3,
        vwap_gap_bps=vwap_gap_bps,
        ret_30s=ret_30s,
        ret_1m=ret_1m,
        ret_3m=ret_3m,
        close_pos_in_bar_1m=close_pos_in_bar_1m,
        close_pos_in_bar_3m=close_pos_in_bar_3m,
        ma_trend_score_3m=ma_trend_score_3m,
        pullback_quality=pullback_quality,
        reacceleration_score=reacceleration_score,
        overextension_penalty=overextension_penalty,
        volume_1m=volume_1m,
        volume_3m=volume_3m,
        trade_intensity_30s=trade_intensity_30s,
        regime=regime,
    )


def extended_feature_metrics(f: FeatureSnapshot, history: list[Bar]) -> dict[str, Any]:
    tick_size = tick_size_for_1570(f.price)
    ret1_ticks = price_to_ticks(history[-1].close - history[-2].close, f.price) if len(history) >= 2 else None
    ret5_ticks = price_to_ticks(history[-1].close - history[-6].close, f.price) if len(history) >= 6 else None
    avg20_volume = None
    volume_ratio_20 = None
    if len(history) >= 21:
        vols = [b.volume for b in history[-21:-1]]
        avg20_volume = sum(vols) / max(len(vols), 1)
        if avg20_volume > 0:
            volume_ratio_20 = history[-1].volume / avg20_volume
    last20 = history[-20:] if len(history) >= 20 else []
    last30 = history[-30:] if len(history) >= 30 else []
    range20_ticks = price_to_ticks(max(b.high for b in last20) - min(b.low for b in last20), f.price) if last20 else None
    range30_ticks = price_to_ticks(max(b.high for b in last30) - min(b.low for b in last30), f.price) if last30 else None
    ma75_slope_3m_ticks = None
    if len(history) >= 4 and history[-1].ma75 is not None and history[-4].ma75 is not None:
        ma75_slope_3m_ticks = price_to_ticks(history[-1].ma75 - history[-4].ma75, f.price)
    directional_vwap_gap_bps_long = f.vwap_gap_bps
    directional_vwap_gap_bps_short = -f.vwap_gap_bps
    abs_vwap_gap_bps = abs(f.vwap_gap_bps)
    range_pos_30 = None
    if last30:
        low30 = min(b.low for b in last30)
        high30 = max(b.high for b in last30)
        if high30 > low30:
            range_pos_30 = (f.price - low30) / (high30 - low30)
    return {
        "ret1_ticks": ret1_ticks,
        "ret5_ticks": ret5_ticks,
        "volume_ratio_20": volume_ratio_20,
        "avg20_volume": avg20_volume,
        "range20_ticks": range20_ticks,
        "range30_ticks": range30_ticks,
        "ma75_slope_3m_ticks": ma75_slope_3m_ticks,
        "directional_vwap_gap_bps_long": directional_vwap_gap_bps_long,
        "directional_vwap_gap_bps_short": directional_vwap_gap_bps_short,
        "abs_vwap_gap_bps": abs_vwap_gap_bps,
        "directional_range_position_30m_long": range_pos_30,
        "directional_range_position_30m_short": (1.0 - range_pos_30) if range_pos_30 is not None else None,
        "tick_size": tick_size,
    }


def big_trend_start_score(side: str, f: FeatureSnapshot, metrics: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    sign = 1 if side == "LONG" else -1
    directional_slope = (metrics.get("ma75_slope_3m_ticks") or 0.0) * sign
    abs_gap = metrics.get("abs_vwap_gap_bps") or 0.0
    directional_gap = metrics.get("directional_vwap_gap_bps_long") if side == "LONG" else metrics.get("directional_vwap_gap_bps_short")
    directional_range_pos = metrics.get("directional_range_position_30m_long") if side == "LONG" else metrics.get("directional_range_position_30m_short")
    components: dict[str, Any] = {}
    score = 0
    if directional_slope >= 2:
        score += 3; components["ma75_slope_ge_2"] = 3
    if directional_slope >= 5:
        score += 2; components["ma75_slope_ge_5"] = 2
    if (metrics.get("range30_ticks") or 0) >= 70:
        score += 2; components["range30_ge_70"] = 2
    if (metrics.get("range20_ticks") or 0) >= 60:
        score += 1; components["range20_ge_60"] = 1
    if 20 <= abs_gap <= 160:
        score += 2; components["abs_vwap_gap_20_160"] = 2
    if directional_gap is not None and directional_gap <= 80:
        score += 2; components["directional_vwap_gap_le_80"] = 2
    if directional_range_pos is not None and directional_range_pos >= 0.6:
        score += 1; components["directional_range_position_ge_0_6"] = 1
    if abs_gap > 180:
        score -= 2; components["abs_vwap_gap_gt_180"] = -2
    if (metrics.get("range30_ticks") or 0) < 50:
        score -= 2; components["range30_lt_50"] = -2
    if f.spread_ticks > 2:
        score -= 1; components["spread_gt_2"] = -1
    return score, components


def hold_score_for_position(pos: PositionState, f: FeatureSnapshot, metrics: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    components: dict[str, Any] = {}
    score = 0
    abs_gap = metrics.get("abs_vwap_gap_bps") or 0.0
    if abs_gap <= 10:
        score += 2; components["abs_vwap_gap_le_10"] = 2
    if (metrics.get("range20_ticks") or 0) >= 36:
        score += 2; components["range20_ge_36"] = 2
    range_pos = metrics.get("directional_range_position_30m_long") if pos.side == "LONG" else metrics.get("directional_range_position_30m_short")
    if range_pos is not None and range_pos >= 0.6:
        score += 2; components["directional_range_position_ge_0_6"] = 2
    if (metrics.get("volume_ratio_20") or 0) >= 0.8:
        score += 1; components["volume_ratio_ge_0_8"] = 1
    if f.obi_l3 >= -0.2:
        score += 1; components["obi_l3_ge_minus_0_2"] = 1
    directional_gap = metrics.get("directional_vwap_gap_bps_long") if pos.side == "LONG" else metrics.get("directional_vwap_gap_bps_short")
    if directional_gap is not None and directional_gap > 160:
        score -= 3; components["directional_vwap_gap_gt_160"] = -3
    if (metrics.get("range20_ticks") or 0) < 28:
        score -= 2; components["range20_lt_28"] = -2
    if f.spread_ticks > 2:
        score -= 1; components["spread_gt_2"] = -1
    return score, components


def can_trade_now(ts: datetime, f: FeatureSnapshot) -> tuple[bool, str]:
    t = ts.strftime("%H:%M:%S")
    if not time_in_windows(t, TRADE_WINDOWS):
        return False, "OUT_OF_TRADE_WINDOW"
    if f.spread_ticks > SPREAD_TICKS_MAX:
        return False, "SPREAD_WIDE"
    if f.price <= 0:
        return False, "NO_PRICE"
    if f.volume_1m <= 0:
        return False, "NO_VOLUME"
    if abs(f.obi_l1) < BOOK_NEUTRAL_OBI_L1_MIN and abs(f.obi_l3) < BOOK_NEUTRAL_OBI_L3_MIN:
        return False, "BOOK_NEUTRAL"
    if abs(f.obi_l1) < 0.02 and abs(f.obi_l3) < 0.01:
        return False, "BOOK_NEUTRAL"

    if abs(f.vwap_gap_bps) > vwap_threshold_for_regime(f.regime):
        return False, "OVEREXTENDED"
    return True, "OK"


def long_score_1m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m > 0:
        score += 1.3
    if f.ret_1m > 0:
        score += 1.0
    if f.price > f.vwap:
        score += 0.8
    score += 0.9 * max(f.obi_l1, 0.0)
    score += 0.7 * max(f.obi_l3, 0.0)
    score += 0.6 * max(f.close_pos_in_bar_1m - 0.5, 0.0)
    score -= 0.5 * f.overextension_penalty
    return score


def short_score_1m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m < 0:
        score += 1.3
    if f.ret_1m < 0:
        score += 1.0
    if f.price < f.vwap:
        score += 0.8
    score += 0.9 * max(-f.obi_l1, 0.0)
    score += 0.7 * max(-f.obi_l3, 0.0)
    score += 0.6 * max(0.5 - f.close_pos_in_bar_1m, 0.0)
    score -= 0.5 * f.overextension_penalty
    return score


def long_score_3m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m > 0:
        score += 1.4
    if f.ma_trend_score_3m > 0:
        score += 1.1
    score += 0.8 * max(f.pullback_quality, 0.0)
    score += 0.8 * max(f.reacceleration_score, 0.0)
    score += 0.5 * max(f.obi_l1, 0.0)
    score -= 0.4 * f.overextension_penalty
    return score


def short_score_3m(f: FeatureSnapshot) -> float:
    score = 0.0
    if f.ret_3m < 0:
        score += 1.4
    if f.ma_trend_score_3m < 0:
        score += 1.1
    score += 0.8 * max(f.pullback_quality, 0.0)
    score += 0.8 * max(-f.reacceleration_score, 0.0)
    score += 0.5 * max(-f.obi_l1, 0.0)
    score -= 0.4 * f.overextension_penalty
    return score



def _scalp_prediction(
    f: FeatureSnapshot,
    p_up_1m: float,
    p_down_1m: float,
    p_up_3m: float,
    p_down_3m: float,
) -> Optional[PredictionSnapshot]:
    if not SCALPING_ENABLED:
        return None
    t = f.ts.strftime("%H:%M:%S")
    if not time_in_windows(t, TRADE_WINDOWS):
        return None
    if f.price <= 0 or f.volume_1m <= 0:
        return None
    if f.spread_ticks > SCALP_MAX_SPREAD_TICKS:
        return None
    if abs(f.vwap_gap_bps) > SCALP_MAX_VWAP_GAP_BPS:
        return None

    common_detail = f"r30={f.ret_30s:.5f} obi={f.obi_l1:.2f}"

    # 1) Down move losing force, then a fast buy imbalance appears.
    if (
        (f.ret_1m < 0 or f.ret_3m < 0)
        and f.ret_30s >= SCALP_REBOUND_RET30_MIN
        and f.obi_l1 >= SCALP_REBOUND_OBI_L1_MIN
        and f.obi_l3 >= 0.0
    ):
        return PredictionSnapshot(
            ts=f.ts,
            regime=f.regime,
            p_up_1m=p_up_1m,
            p_down_1m=p_down_1m,
            p_up_3m=p_up_3m,
            p_down_3m=p_down_3m,
            signal="LONG_CANDIDATE",
            rsi9_value=0.0,
            reason_1="SCALP_REBOUND_LONG",
            reason_2=common_detail,
            reason_3=f"ret1={f.ret_1m:.5f} ret3={f.ret_3m:.5f}",
        )

    # 2) Price is already above VWAP, but a squeeze/buyback wave is still active.
    if (
        f.vwap_gap_bps >= SCALP_SQUEEZE_VWAP_GAP_BPS_MIN
        and f.ret_30s >= SCALP_SQUEEZE_RET30_MIN
        and f.ret_1m >= -0.0010
        and f.obi_l1 >= SCALP_SQUEEZE_OBI_L1_MIN
        and f.trade_intensity_30s >= SCALP_MIN_TRADE_INTENSITY_30S
    ):
        return PredictionSnapshot(
            ts=f.ts,
            regime=f.regime,
            p_up_1m=p_up_1m,
            p_down_1m=p_down_1m,
            p_up_3m=p_up_3m,
            p_down_3m=p_down_3m,
            signal="LONG_CANDIDATE",
            rsi9_value=0.0,
            reason_1="SCALP_SQUEEZE_LONG",
            reason_2=common_detail,
            reason_3=f"vwap_gap={f.vwap_gap_bps:.1f}bps intensity={f.trade_intensity_30s:.1f}",
        )

    # 3) VWAP-uptrend pullback: 3m is up, 1m dips, 30s turns back up.
    if (
        f.price > f.vwap
        and 0.0 <= f.vwap_gap_bps <= SCALP_PULLBACK_VWAP_GAP_BPS_MAX
        and f.ret_3m > 0
        and f.ret_1m < 0
        and f.ret_30s > 0
        and f.obi_l1 > 0.20
    ):
        return PredictionSnapshot(
            ts=f.ts,
            regime=f.regime,
            p_up_1m=p_up_1m,
            p_down_1m=p_down_1m,
            p_up_3m=p_up_3m,
            p_down_3m=p_down_3m,
            signal="LONG_CANDIDATE",
            rsi9_value=0.0,
            reason_1="SCALP_VWAP_PULLBACK_LONG",
            reason_2=common_detail,
            reason_3=f"vwap_gap={f.vwap_gap_bps:.1f}bps",
        )

    # 4) Momentum breakout: short horizon move plus buy board pressure.
    if (
        f.ret_30s >= SCALP_BREAKOUT_RET30_MIN
        and f.ret_1m > 0
        and f.obi_l1 > 0.40
        and f.close_pos_in_bar_1m > 0.65
        and f.vwap_gap_bps <= SCALP_MAX_VWAP_GAP_BPS
    ):
        return PredictionSnapshot(
            ts=f.ts,
            regime=f.regime,
            p_up_1m=p_up_1m,
            p_down_1m=p_down_1m,
            p_up_3m=p_up_3m,
            p_down_3m=p_down_3m,
            signal="LONG_CANDIDATE",
            rsi9_value=0.0,
            reason_1="SCALP_BREAKOUT_LONG",
            reason_2=common_detail,
            reason_3=f"close_pos={f.close_pos_in_bar_1m:.2f}",
        )

    # SHORT is deliberately strict: never short above VWAP.
    if (
        f.price < f.vwap
        and f.ret_3m < 0
        and f.ret_1m < 0
        and f.ret_30s <= SCALP_SHORT_RET30_MAX
        and f.obi_l1 <= SCALP_SHORT_OBI_L1_MAX
        and f.obi_l3 < 0
    ):
        return PredictionSnapshot(
            ts=f.ts,
            regime=f.regime,
            p_up_1m=p_up_1m,
            p_down_1m=p_down_1m,
            p_up_3m=p_up_3m,
            p_down_3m=p_down_3m,
            signal="SHORT_CANDIDATE",
            rsi9_value=0.0,
            reason_1="SCALP_STRICT_SHORT",
            reason_2=common_detail,
            reason_3=f"vwap_gap={f.vwap_gap_bps:.1f}bps",
        )

    return None

def build_prediction(f: FeatureSnapshot) -> PredictionSnapshot:
    p_up_1m = sigmoid(long_score_1m(f) - short_score_1m(f))
    p_down_1m = 1.0 - p_up_1m
    p_up_3m = sigmoid(long_score_3m(f) - short_score_3m(f))
    p_down_3m = 1.0 - p_up_3m

    scalp = _scalp_prediction(f, p_up_1m, p_down_1m, p_up_3m, p_down_3m)
    if scalp is not None:
        return scalp

    long_strong = p_up_1m >= PROB_UPPER_1M and p_up_3m >= PROB_UPPER_3M
    # Never allow the fallback 1m/3m model to short above VWAP.
    # Scalp shorts already have the same guard in _scalp_prediction().
    short_strong = (
        p_down_1m >= PROB_UPPER_1M
        and p_down_3m >= PROB_UPPER_3M
        and f.price < f.vwap
    )

    gate_ok, gate_reason = can_trade_now(f.ts, f)
    if not gate_ok:
        signal = "NO_ACTION"
        reasons = [gate_reason, f.regime, f"vwap_gap={f.vwap_gap_bps:.1f}bps"]
    else:
        if long_strong and not short_strong:
            signal = "LONG_CANDIDATE"
            reasons = ["ALIGN_UP", f"p1={p_up_1m:.2f}", f"p3={p_up_3m:.2f}"]
        elif short_strong and not long_strong:
            signal = "SHORT_CANDIDATE"
            reasons = ["ALIGN_DOWN", f"p1={p_down_1m:.2f}", f"p3={p_down_3m:.2f}"]
        else:
            signal = "NO_ACTION"
            reasons = ["LOW_CONFIDENCE", f"p1u={p_up_1m:.2f}", f"p3u={p_up_3m:.2f}"]

    return PredictionSnapshot(
        ts=f.ts,
        regime=f.regime,
        p_up_1m=p_up_1m,
        p_down_1m=p_down_1m,
        p_up_3m=p_up_3m,
        p_down_3m=p_down_3m,
        signal=signal,
        rsi9_value=0.0,
        reason_1=reasons[0],
        reason_2=reasons[1],
        reason_3=reasons[2],
    )


def rsi9_wilder(closes: list[float], period: int = RSI9_PERIOD) -> Optional[float]:
    if len(closes) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = max(d, 0.0)
        loss = max(-d, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))




def ma75_slope_2m(history: list[Bar]) -> Optional[float]:
    if len(history) < 3:
        return None
    current = history[-1].ma75
    two_min_ago = history[-3].ma75
    if current is None or two_min_ago is None:
        return None
    return current - two_min_ago


def ma75_gap_ratio(current_price: Optional[float], ma75: Optional[float]) -> Optional[float]:
    if current_price is None or ma75 is None:
        return None
    try:
        price = float(current_price)
        ma = float(ma75)
    except Exception:
        return None
    if price <= 0 or ma <= 0:
        return None
    return max(price, ma) / min(price, ma) - 1.0


def log_ma75_gap_block(
    storage: Optional[Storage],
    event_type: str,
    ts: datetime,
    side: str,
    rsi_now: Optional[float],
    rsi_prev: Optional[float],
    rsi_prev2: Optional[float],
    current_price: Optional[float],
    ma75: Optional[float],
    gap_ratio: Optional[float],
    original_entry_rule: str,
    blocked_reason: str,
) -> None:
    payload = {
        "ts": ts.isoformat(),
        "side": side,
        "rsi_now": rsi_now,
        "rsi_prev": rsi_prev,
        "rsi_prev2": rsi_prev2,
        "current_price": current_price,
        "ma75": ma75,
        "ma75_gap_ratio": gap_ratio,
        "ma75_gap_pct": (gap_ratio * 100.0) if gap_ratio is not None else None,
        "threshold": RSI17_DROP_MA75_GAP_THRESHOLD,
        "original_entry_rule": original_entry_rule,
        "blocked_reason": blocked_reason,
    }
    if storage is not None:
        storage.log_structured("WARN", event_type, payload)
    else:
        print(f"[WARN] {event_type} {payload}", flush=True)


def rsi17_drop_ma75_gap_block_reason(
    current_price: Optional[float],
    ma75: Optional[float],
    original_entry_rule: str,
) -> tuple[Optional[str], Optional[float]]:
    gap_ratio = ma75_gap_ratio(current_price, ma75)
    if gap_ratio is None:
        return "ma75_gap_check_unavailable_blocked", None
    if gap_ratio >= RSI17_DROP_MA75_GAP_THRESHOLD:
        return f"{original_entry_rule}_gap_blocked", gap_ratio
    return None, gap_ratio


def should_rsi9_long_add(bar1: Optional[Bar], history: list[Bar], open_pos: Optional[PositionState]) -> tuple[bool, str]:
    if bar1 is None or open_pos is None:
        return False, "NO_BAR_OR_POSITION"
    if open_pos.side != "LONG" or open_pos.strategy != "RSI9":
        return False, "NOT_LONG_RSI9"
    if open_pos.rsi10_add_done:
        return False, "ALREADY_ADDED"
    if open_pos.take_profit_order_id is not None:
        return False, "ACTIVE_TP_ORDER"
    closes=[b.close for b in history]
    if len(closes) <= RSI9_PERIOD + 1:
        return False, "INSUFFICIENT_HISTORY"
    rsi_now = rsi9_wilder(closes, RSI9_PERIOD)
    rsi_prev = rsi9_wilder(closes[:-1], RSI9_PERIOD) if len(closes) > RSI9_PERIOD + 1 else None
    if rsi_now is None or rsi_prev is None:
        return False, "RSI_UNAVAILABLE"
    in_no_entry = (bar1.ts.hour == 9 and 0 <= bar1.ts.minute <= 15)
    if in_no_entry:
        return False, "NO_ENTRY_WINDOW"
    if rsi_now <= RSI9_LONG_ADD_ENTRY and rsi_prev <= RSI9_LONG_ADD_ENTRY:
        return True, "rsi10_add"
    return False, "RSI_NOT_LOW_ENOUGH"
def clear_rsi20_long_watch(
    status: MonitorStatus,
    storage: Optional[Storage],
    event: str,
    ts: datetime,
    rsi_now: Optional[float],
    rsi_prev: Optional[float],
    reason: str,
) -> None:
    payload = {
        "ts": ts.isoformat(),
        "rsi_now": rsi_now,
        "rsi_prev": rsi_prev,
        "watch_started_at": status.rsi20_long_watch_started_at.isoformat() if status.rsi20_long_watch_started_at else None,
        "watch_expires_at": status.rsi20_long_watch_expires_at.isoformat() if status.rsi20_long_watch_expires_at else None,
        "reason": reason,
    }
    if storage is not None:
        storage.log_structured("INFO", event, payload)
    else:
        print(f"[INFO] {event} {payload}", flush=True)
    status.rsi20_long_watch_active = False
    status.rsi20_long_watch_started_at = None
    status.rsi20_long_watch_expires_at = None
    status.rsi20_long_watch_started_rsi = None


def start_rsi20_long_watch(
    status: MonitorStatus,
    storage: Optional[Storage],
    ts: datetime,
    rsi_now: float,
    rsi_prev: float,
    bar1: Optional[Bar] = None,
    in_no_entry: bool = False,
    force_close_time_reached: bool = False,
    open_pos_exists: bool = False,
) -> None:
    status.rsi20_long_watch_active = True
    status.rsi20_long_watch_started_at = ts
    status.rsi20_long_watch_expires_at = ts + timedelta(minutes=RSI20_LONG_WATCH_MINUTES)
    status.rsi20_long_watch_started_rsi = rsi_now
    payload = {
        "ts": ts.isoformat(),
        "rsi_now": rsi_now,
        "rsi_prev": rsi_prev,
        "watch_started_at": status.rsi20_long_watch_started_at.isoformat(),
        "watch_expires_at": status.rsi20_long_watch_expires_at.isoformat(),
        "reason": "RSI20_TWO_BARS",
        "ma5": bar1.ma5 if bar1 else None,
        "ma25": bar1.ma25 if bar1 else None,
        "ma75": bar1.ma75 if bar1 else None,
        "in_no_entry": in_no_entry,
        "force_close_time_reached": force_close_time_reached,
        "open_pos_exists": open_pos_exists,
    }
    if storage is not None:
        storage.log_structured("INFO", "RSI20_LONG_WATCH_START", payload)
    else:
        print(f"[INFO] RSI20_LONG_WATCH_START {payload}", flush=True)


def clear_rsi70_drop_long_watch(
    status: MonitorStatus,
    storage: Optional[Storage],
    event: str,
    ts: datetime,
    rsi_now: Optional[float],
    rsi_prev: Optional[float],
    reason: str,
) -> None:
    payload = {
        "ts": ts.isoformat(),
        "rsi_now": rsi_now,
        "rsi_prev": rsi_prev,
        "watch_started_at": status.rsi70_drop_long_watch_started_at.isoformat() if status.rsi70_drop_long_watch_started_at else None,
        "watch_expires_at": status.rsi70_drop_long_watch_expires_at.isoformat() if status.rsi70_drop_long_watch_expires_at else None,
        "started_rsi": status.rsi70_drop_long_watch_started_rsi,
        "started_price": status.rsi70_drop_long_watch_started_price,
        "watch_reason": status.rsi70_drop_long_watch_reason,
        "reason": reason,
    }
    if storage is not None:
        storage.log_structured("INFO", event, payload)
    else:
        print(f"[INFO] {event} {payload}", flush=True)
    status.rsi70_drop_long_watch_active = False
    status.rsi70_drop_long_watch_started_at = None
    status.rsi70_drop_long_watch_expires_at = None
    status.rsi70_drop_long_watch_started_rsi = None
    status.rsi70_drop_long_watch_started_price = None
    status.rsi70_drop_long_watch_reason = ""
    status.rsi70_drop_long_watch_started_bar_ts = None


def start_rsi70_drop_long_watch(
    status: MonitorStatus,
    storage: Optional[Storage],
    ts: datetime,
    rsi_now: float,
    rsi_prev: float,
    rsi_prev2: float,
    current_price: float,
    ma75: Optional[float],
    ma75_slope_2m_value: Optional[float],
    ma75_gap_ratio_value: Optional[float],
    watch_minutes: int = 10,
    original_reason: str = "short_b_drop_from_rsi70_ma75_up",
) -> None:
    status.rsi70_drop_long_watch_active = True
    status.rsi70_drop_long_watch_started_at = ts
    status.rsi70_drop_long_watch_expires_at = ts + timedelta(minutes=watch_minutes)
    status.rsi70_drop_long_watch_started_rsi = rsi_now
    status.rsi70_drop_long_watch_started_price = current_price
    status.rsi70_drop_long_watch_reason = original_reason
    status.rsi70_drop_long_watch_started_bar_ts = ts
    payload = {
        "ts": ts.isoformat(),
        "rsi_now": rsi_now,
        "rsi_prev": rsi_prev,
        "rsi_prev2": rsi_prev2,
        "rsi_drop_2m": rsi_prev2 - rsi_now,
        "ma75": ma75,
        "ma75_slope_2m": ma75_slope_2m_value,
        "current_price": current_price,
        "ma75_gap_ratio": ma75_gap_ratio_value,
        "original_reason": original_reason,
        "new_action": "LONG_WATCH",
        "watch_started_at": status.rsi70_drop_long_watch_started_at.isoformat(),
        "watch_expires_at": status.rsi70_drop_long_watch_expires_at.isoformat(),
    }
    if storage is not None:
        storage.log_structured("INFO", "RSI70_DROP_LONG_WATCH_START", payload)
        storage.log_structured("INFO", "RSI70_DROP_MA75_UP_LONG_WATCH_START", payload)
    else:
        print(f"[INFO] RSI70_DROP_LONG_WATCH_START {payload}", flush=True)


def build_rsi9_prediction(
    bar1: Optional[Bar],
    history: list[Bar],
    open_pos: Optional[PositionState],
    status: Optional[MonitorStatus] = None,
    storage: Optional[Storage] = None,
    allow_new_entry: bool = True,
    rsi70_watch_config: Optional[dict[str, Any]] = None,
    new_entry_cutoff_reached: bool = False,
) -> Optional[PredictionSnapshot]:
    if bar1 is None:
        return None
    closes = [b.close for b in history]
    if len(closes) <= RSI9_PERIOD + 1:
        return None

    # latest RSI and previous RSI (2-consecutive condition)
    rsi_now = rsi9_wilder(closes, RSI9_PERIOD)
    rsi_prev = rsi9_wilder(closes[:-1], RSI9_PERIOD) if len(closes) > RSI9_PERIOD + 1 else None
    if rsi_now is None or rsi_prev is None:
        return None

    signal = "NO_ACTION"
    side = "NEUTRAL"
    entry_rule = "none"

    # No-entry window: 09:00-09:15 JST (inclusive)
    in_no_entry = (bar1.ts.hour == 9 and 0 <= bar1.ts.minute <= 15)

    rsi20_watch_expired_this_bar = False
    if status is not None and status.rsi20_long_watch_active:
        if open_pos is not None:
            clear_rsi20_long_watch(status, storage, "RSI20_LONG_WATCH_CANCELLED", bar1.ts, rsi_now, rsi_prev, "POSITION_OPEN")
        elif status.rsi20_long_watch_expires_at and bar1.ts > status.rsi20_long_watch_expires_at:
            rsi20_watch_expired_this_bar = True
            clear_rsi20_long_watch(status, storage, "RSI20_LONG_WATCH_EXPIRED", bar1.ts, rsi_now, rsi_prev, "WATCH_TIMEOUT")
        elif in_no_entry:
            payload = {
                "ts": bar1.ts.isoformat(),
                "rsi_now": rsi_now,
                "rsi_prev": rsi_prev,
                "watch_started_at": status.rsi20_long_watch_started_at.isoformat() if status.rsi20_long_watch_started_at else None,
                "watch_expires_at": status.rsi20_long_watch_expires_at.isoformat() if status.rsi20_long_watch_expires_at else None,
                "reason": "WATCH_ACTIVE_BUT_NO_ENTRY_WINDOW",
            }
            if storage is not None:
                storage.log_structured("INFO", "WATCH_ACTIVE_BUT_NO_ENTRY_WINDOW", payload)
            else:
                print(f"[INFO] WATCH_ACTIVE_BUT_NO_ENTRY_WINDOW {payload}", flush=True)
        elif not allow_new_entry:
            payload = {
                "ts": bar1.ts.isoformat(),
                "rsi_now": rsi_now,
                "rsi_prev": rsi_prev,
                "watch_started_at": status.rsi20_long_watch_started_at.isoformat() if status.rsi20_long_watch_started_at else None,
                "watch_expires_at": status.rsi20_long_watch_expires_at.isoformat() if status.rsi20_long_watch_expires_at else None,
                "reason": "WATCH_ACTIVE_BUT_ENTRY_NOT_ALLOWED",
            }
            if storage is not None:
                storage.log_structured("INFO", "WATCH_ACTIVE_BUT_ENTRY_NOT_ALLOWED", payload)
            else:
                print(f"[INFO] WATCH_ACTIVE_BUT_ENTRY_NOT_ALLOWED {payload}", flush=True)

    rsi70_cfg = rsi70_watch_config if isinstance(rsi70_watch_config, dict) else {}
    rsi70_watch_enabled = bool(rsi70_cfg.get("enabled", True))
    rsi70_allow_same_bar_trigger = bool(rsi70_cfg.get("allow_same_bar_trigger", False))

    if status is not None and status.rsi70_drop_long_watch_active:
        if open_pos is not None:
            clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", bar1.ts, rsi_now, rsi_prev, "POSITION_OPEN")
        elif status.rsi70_drop_long_watch_expires_at and bar1.ts > status.rsi70_drop_long_watch_expires_at:
            clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_EXPIRED", bar1.ts, rsi_now, rsi_prev, "WATCH_TIMEOUT")
        elif new_entry_cutoff_reached:
            clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", bar1.ts, rsi_now, rsi_prev, "NEW_ENTRY_CUTOFF")
        elif status.live_state in {"RECOVERING", "MANUAL_POSITION_CHECK_REQUIRED"}:
            clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", bar1.ts, rsi_now, rsi_prev, f"LIVE_STATE_{status.live_state}")
        elif status.entry_global_block_until and bar1.ts < status.entry_global_block_until:
            clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", bar1.ts, rsi_now, rsi_prev, "ENTRY_GLOBAL_BLOCK")

    if open_pos is None and not in_no_entry and not new_entry_cutoff_reached:
        rsi_prev2 = rsi9_wilder(closes[:-2], RSI9_PERIOD) if len(closes) > RSI9_PERIOD + 2 else None
        ma_ok = (bar1.ma5 is not None and bar1.ma25 is not None and bar1.ma75 is not None)
        long_a_start = rsi_now <= RSI9_LONG_ENTRY and rsi_prev <= RSI9_LONG_ENTRY
        if status is not None and status.rsi70_drop_long_watch_active:
            same_bar = status.rsi70_drop_long_watch_started_bar_ts == bar1.ts
            can_trigger_same_bar = rsi70_allow_same_bar_trigger or not same_bar
            ma5_recovered = bar1.ma5 is not None and bar1.close >= bar1.ma5
            trigger_rule = ""
            if allow_new_entry and can_trigger_same_bar and bool(rsi70_cfg.get("trigger_on_rsi_turn", True)) and rsi_now > rsi_prev:
                trigger_rule = "long_watch_from_rsi70_drop_ma75_up_rsi_turn"
            elif allow_new_entry and can_trigger_same_bar and bool(rsi70_cfg.get("trigger_on_ma5_recover", True)) and ma5_recovered:
                trigger_rule = "long_watch_from_rsi70_drop_ma75_up_ma5_recover"
            if trigger_rule:
                blocked_reason, gap_ratio = rsi17_drop_ma75_gap_block_reason(bar1.close, bar1.ma75, trigger_rule)
                if blocked_reason is not None:
                    clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", bar1.ts, rsi_now, rsi_prev, "MA75_GAP_RECHECK_BLOCKED")
                else:
                    signal, side = "LONG_CANDIDATE", "LONG"
                    entry_rule = trigger_rule
                    if storage is not None:
                        storage.log_structured(
                            "INFO",
                            "RSI70_DROP_LONG_WATCH_TRIGGERED",
                            {
                                "ts": bar1.ts.isoformat(),
                                "rsi_now": rsi_now,
                                "rsi_prev": rsi_prev,
                                "watch_started_at": status.rsi70_drop_long_watch_started_at.isoformat() if status.rsi70_drop_long_watch_started_at else None,
                                "watch_expires_at": status.rsi70_drop_long_watch_expires_at.isoformat() if status.rsi70_drop_long_watch_expires_at else None,
                                "reason_3": trigger_rule,
                                "ma5_recovered": ma5_recovered,
                                "allow_same_bar_trigger": rsi70_allow_same_bar_trigger,
                            },
                        )
                    clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_TRIGGERED", bar1.ts, rsi_now, rsi_prev, trigger_rule)
            elif not allow_new_entry and storage is not None:
                storage.log_structured("INFO", "RSI70_DROP_LONG_WATCH_ACTIVE_BUT_ENTRY_NOT_ALLOWED", {"ts": bar1.ts.isoformat(), "rsi_now": rsi_now, "rsi_prev": rsi_prev})
        if signal == "NO_ACTION" and ma_ok and allow_new_entry:
            short_ma = bar1.ma5 > bar1.ma25 > bar1.ma75
            all_ma_below = (bar1.close <= (bar1.ma5 or -1e18)) and (bar1.close <= (bar1.ma25 or -1e18)) and (bar1.close <= (bar1.ma75 or -1e18))
            if rsi_prev2 is not None and (rsi_prev2 - rsi_now) >= 17.0:
                slope2m = ma75_slope_2m(history)
                ma75_current = history[-1].ma75 if len(history) >= 1 else None
                ma75_2m_ago = history[-3].ma75 if len(history) >= 3 else None
                if slope2m is not None and slope2m > 0:
                    if rsi_prev2 >= RSI9_SHORT_ENTRY:
                        original_reason = "short_b_drop_from_rsi70_ma75_up"
                        blocked_reason, gap_ratio = rsi17_drop_ma75_gap_block_reason(bar1.close, bar1.ma75, original_reason)
                        if blocked_reason is not None:
                            event_type = "ENTRY_BLOCKED_MA75_GAP_UNAVAILABLE" if gap_ratio is None else "RSI17_DROP_MA75_GAP_BLOCKED"
                            log_ma75_gap_block(storage, event_type, bar1.ts, "SHORT", rsi_now, rsi_prev, rsi_prev2, bar1.close, bar1.ma75, gap_ratio, original_reason, blocked_reason)
                            signal, side = "NO_ACTION", "NEUTRAL"
                            entry_rule = blocked_reason
                        else:
                            signal, side = "NO_ACTION", "NEUTRAL"
                            entry_rule = "short_b_drop_from_rsi70_ma75_up_disabled_long_watch"
                            if status is not None and rsi70_watch_enabled and allow_new_entry and not status.rsi70_drop_long_watch_active:
                                start_rsi70_drop_long_watch(
                                    status,
                                    storage,
                                    bar1.ts,
                                    rsi_now,
                                    rsi_prev,
                                    rsi_prev2,
                                    bar1.close,
                                    bar1.ma75,
                                    slope2m,
                                    gap_ratio,
                                    watch_minutes=int(rsi70_cfg.get("watch_minutes", 10)),
                                    original_reason=original_reason,
                                )
                            elif storage is not None:
                                storage.log_structured(
                                    "INFO",
                                    "RSI70_DROP_SHORT_DISABLED_LONG_WATCH_START",
                                    {
                                        "ts": bar1.ts.isoformat(),
                                        "rsi_now": rsi_now,
                                        "rsi_prev": rsi_prev,
                                        "rsi_prev2": rsi_prev2,
                                        "rsi_drop_2m": rsi_prev2 - rsi_now,
                                        "ma75": bar1.ma75,
                                        "ma75_slope_2m": slope2m,
                                        "current_price": bar1.close,
                                        "ma75_gap_ratio": gap_ratio,
                                        "original_reason": original_reason,
                                        "new_action": "LONG_WATCH",
                                        "watch_start_skipped_reason": "WATCH_ALREADY_ACTIVE_OR_ENTRY_NOT_ALLOWED",
                                    },
                                )
                    else:
                        signal, side = "LONG_CANDIDATE", "LONG"
                        entry_rule = "long_b_drop_ma75_up_all_ma_below_allowed" if all_ma_below else "long_b_drop_ma75_up"
                        print(f"[INFO] DROP17_MA75_SLOPE_LONG rsi_now={rsi_now:.2f} rsi_prev={rsi_prev:.2f} rsi_prev2={rsi_prev2:.2f} ma75_current={ma75_current} ma75_2m_ago={ma75_2m_ago} ma75_slope_2m={slope2m} all_ma_below={all_ma_below}", flush=True)
                elif slope2m is not None and slope2m < 0:
                    if rsi_now <= 29.9:
                        signal, side = "NO_ACTION", "NEUTRAL"
                        entry_rule = "short_b_drop_ma75_down_rsi_too_low_blocked"
                        print(f"[INFO] DROP17_MA75_SLOPE_SHORT_BLOCKED_RSI_LOW rsi_now={rsi_now:.2f} rsi_prev={rsi_prev:.2f} rsi_prev2={rsi_prev2:.2f} ma75_current={ma75_current} ma75_2m_ago={ma75_2m_ago} ma75_slope_2m={slope2m}", flush=True)
                    elif rsi_now > rsi_prev:
                        signal, side = "NO_ACTION", "NEUTRAL"
                        entry_rule = "short_b_drop_ma75_down_rsi_rebound_blocked"
                        print(f"[INFO] DROP17_MA75_SLOPE_SHORT_BLOCKED_RSI_REBOUND rsi_now={rsi_now:.2f} rsi_prev={rsi_prev:.2f} rsi_prev2={rsi_prev2:.2f} ma75_current={ma75_current} ma75_2m_ago={ma75_2m_ago} ma75_slope_2m={slope2m} reason_3=short_b_drop_ma75_down_rsi_rebound_blocked", flush=True)
                    else:
                        signal, side = "SHORT_CANDIDATE", "SHORT"
                        entry_rule = "short_b_drop_ma75_down"
                        print(f"[INFO] DROP17_MA75_SLOPE_SHORT rsi_now={rsi_now:.2f} rsi_prev={rsi_prev:.2f} rsi_prev2={rsi_prev2:.2f} ma75_current={ma75_current} ma75_2m_ago={ma75_2m_ago} ma75_slope_2m={slope2m}", flush=True)
                else:
                    signal, side = "NO_ACTION", "NEUTRAL"
                    entry_rule = "drop17_ma75_flat_or_unknown"
                    print(f"[INFO] DROP17_MA75_SLOPE_SKIP rsi_now={rsi_now:.2f} rsi_prev={rsi_prev:.2f} rsi_prev2={rsi_prev2:.2f} ma75_current={ma75_current} ma75_2m_ago={ma75_2m_ago} ma75_slope_2m={slope2m}", flush=True)
                if entry_rule in RSI17_DROP_ENTRY_RULES and entry_rule != "short_b_drop_from_rsi70_ma75_up":
                    original_entry_rule = entry_rule
                    original_side = side
                    blocked_reason, gap_ratio = rsi17_drop_ma75_gap_block_reason(bar1.close, bar1.ma75, original_entry_rule)
                    if blocked_reason is not None:
                        event_type = "ENTRY_BLOCKED_MA75_GAP_UNAVAILABLE" if gap_ratio is None else "RSI17_DROP_MA75_GAP_BLOCKED"
                        log_ma75_gap_block(
                            storage,
                            event_type,
                            bar1.ts,
                            original_side,
                            rsi_now,
                            rsi_prev,
                            rsi_prev2,
                            bar1.close,
                            bar1.ma75,
                            gap_ratio,
                            original_entry_rule,
                            blocked_reason,
                        )
                        signal, side = "NO_ACTION", "NEUTRAL"
                        entry_rule = blocked_reason
            elif False and short_ma and rsi_now >= RSI9_SHORT_ENTRY and rsi_prev >= RSI9_SHORT_ENTRY:
                signal, side = "SHORT_CANDIDATE", "SHORT"
                entry_rule = "short_frozen"

        if status is not None:
            if signal in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
                pass
            elif status.rsi20_long_watch_active and rsi_now > rsi_prev:
                if allow_new_entry:
                    signal, side = "LONG_CANDIDATE", "LONG"
                    entry_rule = "long_a_reversal_watch"
                    clear_rsi20_long_watch(status, storage, "RSI20_LONG_WATCH_TRIGGERED", bar1.ts, rsi_now, rsi_prev, "RSI_TURNED_UP")
                else:
                    payload = {
                        "ts": bar1.ts.isoformat(),
                        "rsi_now": rsi_now,
                        "rsi_prev": rsi_prev,
                        "watch_started_at": status.rsi20_long_watch_started_at.isoformat() if status.rsi20_long_watch_started_at else None,
                        "watch_expires_at": status.rsi20_long_watch_expires_at.isoformat() if status.rsi20_long_watch_expires_at else None,
                        "reason": "WATCH_ACTIVE_BUT_ENTRY_NOT_ALLOWED",
                    }
                    if storage is not None:
                        storage.log_structured("INFO", "WATCH_ACTIVE_BUT_ENTRY_NOT_ALLOWED", payload)
                    else:
                        print(f"[INFO] WATCH_ACTIVE_BUT_ENTRY_NOT_ALLOWED {payload}", flush=True)
            elif long_a_start and not status.rsi20_long_watch_active and not rsi20_watch_expired_this_bar:
                start_rsi20_long_watch(status, storage, bar1.ts, rsi_now, rsi_prev, bar1=bar1, in_no_entry=in_no_entry, force_close_time_reached=False, open_pos_exists=open_pos is not None)
    elif open_pos is not None:
        side = open_pos.side

    return PredictionSnapshot(
        ts=bar1.ts,
        regime="RSI9",
        p_up_1m=0.5,
        p_down_1m=0.5,
        p_up_3m=0.5,
        p_down_3m=0.5,
        signal=signal,
        rsi9_value=rsi_now,
        reason_1="RSI9_ONLY",
        reason_2=f"rsi9={rsi_now:.2f}",
        reason_3=entry_rule,
    )




def extract_rsi_from_pred(pred: Optional[PredictionSnapshot]) -> Optional[float]:
    if pred is None:
        return None
    if not pred.reason_2.startswith("rsi9="):
        return None
    try:
        return float(pred.reason_2.split("=", 1)[1])
    except Exception:
        return None

def can_enter(side: str, now_: datetime, state: MonitorStatus) -> tuple[bool, str]:
    if state.open_position is not None:
        return False, "ALREADY_OPEN"
    if state.entry_global_block_until and now_ < state.entry_global_block_until:
        return False, "ENTRY_GLOBAL_BLOCK"
    if state.recovery_until and now_ < state.recovery_until:
        return False, "RECOVERY_COOLDOWN"
    if state.live_state in {"RECOVERING", "MANUAL_POSITION_CHECK_REQUIRED"}:
        return False, state.live_state
    block_until = state.reentry_block_until_by_side.get(side)
    if block_until and now_ < block_until:
        return False, "REENTRY_BLOCK"
    last_entry = state.last_entry_ts_by_side.get(side)
    if last_entry and (now_ - last_entry).total_seconds() < ENTRY_COOLDOWN_SEC:
        return False, "ENTRY_COOLDOWN"
    return True, "OK"


def create_position(
    pred: PredictionSnapshot,
    f: FeatureSnapshot,
    config: dict[str, Any],
    entry_order_id: Optional[str] = None,
    side_override: Optional[str] = None,
) -> PositionState:
    if side_override in {"LONG", "SHORT"}:
        side = side_override
    else:
        is_long = pred.signal == "LONG_CANDIDATE"
        side = "LONG" if is_long else "SHORT"
    is_long = side == "LONG"
    if pred.reason_1 == "RSI9_ONLY":
        strategy = "RSI9"
        stop_ticks, take_ticks, min_hold, max_hold = 9999, 9999, 0, 3600
    elif pred.reason_1 in SCALP_EXIT_PARAMS:
        strategy = pred.reason_1
        stop_ticks, take_ticks, min_hold, max_hold = SCALP_EXIT_PARAMS[strategy]
    elif (pred.p_up_3m if is_long else pred.p_down_3m) >= (
        pred.p_up_1m if is_long else pred.p_down_1m
    ):
        strategy = "STRAT_3M"
        min_hold = MIN_HOLD_SEC_3M
        max_hold = MAX_HOLD_SEC_3M
        stop_ticks = STOP_TICKS_3M
        take_ticks = TAKE_TICKS_3M
    else:
        strategy = "STRAT_1M"
        min_hold = MIN_HOLD_SEC_1M
        max_hold = MAX_HOLD_SEC_1M
        stop_ticks = STOP_TICKS_1M
        take_ticks = TAKE_TICKS_1M

    return PositionState(
        side=side,
        strategy=strategy,
        entry_ts=f.ts,
        entry_price=f.price,
        entry_p_up_1m=pred.p_up_1m,
        entry_p_up_3m=pred.p_up_3m,
        stop_ticks=stop_ticks,
        take_ticks=take_ticks,
        min_hold_sec=min_hold,
        max_hold_sec=max_hold,
        entry_vwap_gap_bps=f.vwap_gap_bps,
        entry_regime=f.regime,
        entry_vwap_mode=CURRENT_VWAP_MODE,
        margin_trade_type=margin_trade_type_for_side(config, side),
        entry_order_id=entry_order_id,
        rsi_special_entry=(pred.reason_3 in {"long_b_drop", "long_b_drop_ma75_up", "long_b_drop_ma75_up_all_ma_below_allowed", "short_b_drop_ma75_down", "short_b_drop_from_rsi70_ma75_up", "long_a_reversal_watch"}),
        order_qty=int(config.get("order_qty",2)),
        filled_qty=0,
        remaining_qty=0,
        hard_stop_ticks=int(config.get("hard_stop_ticks", HARD_STOP_TICKS)),
    )


def current_pnl_ticks(pos: PositionState, current_price: float) -> float:
    pnl_ticks = price_to_ticks(current_price - pos.entry_price, pos.entry_price)
    return -pnl_ticks if pos.side == "SHORT" else pnl_ticks


def _trailing_payload(pos: PositionState, f: FeatureSnapshot, pnl_ticks: float) -> dict[str, Any]:
    """Common payload for trailing/MA5-exit diagnostics.

    trailing_high/trailing_low/trailing_stop_price are retained only for
    backwards-compatible position payloads; MA5-based trailing exits do not use
    those fields for decisions.
    """
    return {
        "ts": f.ts.isoformat(),
        "side": pos.side,
        "strategy": pos.strategy,
        "entry_price": pos.entry_price,
        "current_price": f.price,
        "pnl_ticks": pnl_ticks,
        "trailing_trigger_ticks": pos.trailing_trigger_ticks,
        "trailing_floor_ticks": pos.trailing_floor_ticks,
        "trailing_width_ticks": pos.trailing_width_ticks,
        "trailing_high": pos.trailing_high,
        "trailing_low": pos.trailing_low,
        "trailing_stop_price": pos.trailing_stop_price,
        "ma5_breach_count": pos.ma5_breach_count,
        "trailing_started_at": pos.trailing_started_at.isoformat() if pos.trailing_started_at else None,
        "trailing_ma5_bar_bucket": pos.trailing_ma5_bar_bucket.isoformat() if pos.trailing_ma5_bar_bucket else None,
        "trailing_ma5_bar_open": pos.trailing_ma5_bar_open,
        "trailing_ma5_reference": pos.trailing_ma5_reference,
        "trailing_ma5_open_relation": pos.trailing_ma5_open_relation,
    }


def _ma5_trailing_relation(side: str, bar_open: float, confirmed_ma5: float) -> str:
    if side == "LONG":
        return "LONG_ABOVE_MA5" if bar_open > confirmed_ma5 else "LONG_BELOW_OR_EQUAL_MA5"
    return "SHORT_BELOW_MA5" if bar_open < confirmed_ma5 else "SHORT_ABOVE_OR_EQUAL_MA5"


def _set_ma5_trailing_context(
    pos: PositionState,
    f: FeatureSnapshot,
    current_bar_bucket: datetime,
    current_bar_open: float,
    confirmed_ma5: float,
    pnl_ticks: float,
    storage: Optional[Storage],
) -> None:
    relation = _ma5_trailing_relation(pos.side, current_bar_open, confirmed_ma5)
    pos.trailing_ma5_bar_bucket = current_bar_bucket
    pos.trailing_ma5_bar_open = current_bar_open
    pos.trailing_ma5_reference = confirmed_ma5
    pos.trailing_ma5_open_relation = relation
    if storage is not None:
        storage.log_structured(
            "INFO",
            "MA5_TRAILING_BAR_CONTEXT",
            {
                "ts": f.ts.isoformat(),
                "side": pos.side,
                "strategy": pos.strategy,
                "bar_bucket": current_bar_bucket.isoformat(),
                "bar_open": current_bar_open,
                "confirmed_ma5": confirmed_ma5,
                "relation": relation,
                "trailing_active": pos.trailing_active,
                "pnl_ticks": pnl_ticks,
            },
        )


def _log_ma5_trailing_exit(
    storage: Optional[Storage],
    pos: PositionState,
    f: FeatureSnapshot,
    pnl_ticks: float,
    exit_reason: str,
    confirmed_bar_close: Optional[float] = None,
) -> None:
    if storage is None:
        return
    storage.log_structured(
        "INFO",
        "MA5_TRAILING_EXIT_SIGNAL",
        {
            "ts": f.ts.isoformat(),
            "side": pos.side,
            "strategy": pos.strategy,
            "exit_reason": exit_reason,
            "entry_price": pos.entry_price,
            "signal_price": f.price,
            "current_price": f.price,
            "current_bar_bucket": pos.trailing_ma5_bar_bucket.isoformat() if pos.trailing_ma5_bar_bucket else None,
            "current_bar_open": pos.trailing_ma5_bar_open,
            "confirmed_bar_close": confirmed_bar_close,
            "confirmed_ma5": pos.trailing_ma5_reference,
            "pnl_ticks": pnl_ticks,
            "bar_bucket": pos.trailing_ma5_bar_bucket.isoformat() if pos.trailing_ma5_bar_bucket else None,
            "trailing_ma5_open_relation": pos.trailing_ma5_open_relation,
            "relation": pos.trailing_ma5_open_relation,
            "trailing_started_at": pos.trailing_started_at.isoformat() if pos.trailing_started_at else None,
            "exit_signal_ts": f.ts.isoformat(),
            "reason_detail": {
                "long_close_below": exit_reason == "MA5_CLOSE_BELOW_TRAILING" and pos.trailing_ma5_bar_open is not None and pos.trailing_ma5_reference is not None and pos.trailing_ma5_bar_open <= pos.trailing_ma5_reference and confirmed_bar_close is not None and confirmed_bar_close < pos.trailing_ma5_reference,
                "short_close_above": exit_reason == "MA5_CLOSE_ABOVE_TRAILING" and pos.trailing_ma5_bar_open is not None and pos.trailing_ma5_reference is not None and pos.trailing_ma5_bar_open >= pos.trailing_ma5_reference and confirmed_bar_close is not None and confirmed_bar_close > pos.trailing_ma5_reference,
                "intrabar_cross": exit_reason == "MA5_INTRABAR_CROSS_TRAILING",
            },
        },
    )


def should_defer_ma5_exit_by_hold_score(
    pos: PositionState,
    f: FeatureSnapshot,
    pnl_ticks: float,
    exit_reason_original: str,
    hold_score_config: Optional[dict[str, Any]],
    analysis_config: Optional[dict[str, Any]],
    feature_metrics: Optional[dict[str, Any]],
    storage: Optional[Storage],
) -> bool:
    cfg = hold_score_config if isinstance(hold_score_config, dict) else {}
    analysis = analysis_config if isinstance(analysis_config, dict) else {}
    if not cfg and not analysis.get("log_hold_score_when_disabled", True):
        return False
    metrics = feature_metrics or {}
    score, components = hold_score_for_position(pos, f, metrics)
    pos.last_hold_score = float(score)
    if storage is not None and (bool(cfg.get("enabled", False)) or bool(analysis.get("log_hold_score_when_disabled", True))):
        storage.log_structured("INFO", "HOLD_SCORE_CALCULATED", {"ts": f.ts.isoformat(), "side": pos.side, "score": score, "components": components, "exit_reason_original": exit_reason_original, "enabled": bool(cfg.get("enabled", False))})
    if not bool(cfg.get("enabled", False)):
        return False
    if bool(cfg.get("long_only", True)) and pos.side != "LONG":
        return False
    max_skip = int(cfg.get("max_ma5_exit_skip_count", 1))
    threshold = float(cfg.get("score_threshold", 8))
    if score >= threshold and pos.ma5_exit_skip_count < max_skip:
        pos.ma5_exit_skip_count += 1
        pos.last_ma5_exit_deferred_ts = f.ts
        if storage is not None:
            storage.log_structured(
                "INFO",
                "MA5_EXIT_DEFERRED_BY_HOLD_SCORE",
                {
                    "ts": f.ts.isoformat(),
                    "side": pos.side,
                    "exit_reason_original": exit_reason_original,
                    "hold_score": score,
                    "components": components,
                    "ma5_exit_skip_count": pos.ma5_exit_skip_count,
                    "entry_price": pos.entry_price,
                    "current_price": f.price,
                    "pnl_ticks": pnl_ticks,
                },
            )
        return True
    return False


def update_trailing_exit(
    pos: PositionState,
    f: FeatureSnapshot,
    bar1_new: Optional[Bar] = None,
    storage: Optional[Storage] = None,
    current_bar_bucket: Optional[datetime] = None,
    current_bar_open: Optional[float] = None,
    confirmed_bar1: Optional[Bar] = None,
    hold_score_config: Optional[dict[str, Any]] = None,
    analysis_config: Optional[dict[str, Any]] = None,
    feature_metrics: Optional[dict[str, Any]] = None,
) -> tuple[bool, str, float]:
    pnl_ticks = current_pnl_ticks(pos, f.price)
    # HARD_STOP_LOSS is intentionally the first ordinary exit check.  It is
    # strategy-independent and supersedes legacy stop_ticks values (some of
    # which are 10 ticks) so positions are not stopped before the explicit
    # -15tick rule requested for this monitor.
    if pnl_ticks <= -pos.hard_stop_ticks:
        if storage is not None:
            storage.log_structured(
                "WARN",
                "HARD_STOP_LOSS_SIGNAL",
                {
                    "ts": f.ts.isoformat(),
                    "side": pos.side,
                    "strategy": pos.strategy,
                    "entry_price": pos.entry_price,
                    "current_price": f.price,
                    "pnl_ticks": pnl_ticks,
                    "hard_stop_ticks": pos.hard_stop_ticks,
                    "trailing_active": pos.trailing_active,
                    "trailing_high": pos.trailing_high,
                    "trailing_low": pos.trailing_low,
                    "trailing_stop_price": pos.trailing_stop_price,
                    "exit_reason": "HARD_STOP_LOSS",
                },
            )
        return True, "HARD_STOP_LOSS", pnl_ticks

    if not pos.trailing_active:
        if pnl_ticks < pos.trailing_trigger_ticks:
            return False, "HOLD", pnl_ticks
        pos.trailing_active = True
        pos.trailing_started_at = f.ts
        # The old price-trailing fields are intentionally cleared and not used
        # for exit decisions.  trailing_active now means MA5-based trailing
        # exit management has started after +10 ticks.
        pos.trailing_high = None
        pos.trailing_low = None
        pos.trailing_stop_price = None
        pos.ma5_breach_count = 0
        pos.trailing_ma5_bar_bucket = None
        pos.trailing_ma5_bar_open = None
        pos.trailing_ma5_reference = None
        pos.trailing_ma5_open_relation = ""
        if storage is not None:
            storage.log_structured(
                "INFO",
                "TRAILING_STARTED",
                {
                    **_trailing_payload(pos, f, pnl_ticks),
                    "exit_mode": "MA5_BASED_TRAILING",
                },
            )

    # First evaluate close-confirmation for the 1m bucket that just finalized,
    # using the MA5/reference captured when that bucket started.  This avoids
    # mixing the newly-started bucket with the closed bucket.
    if (
        bar1_new is not None
        and pos.trailing_ma5_bar_bucket is not None
        and bar1_new.ts == pos.trailing_ma5_bar_bucket
        and pos.trailing_ma5_reference is not None
    ):
        relation = pos.trailing_ma5_open_relation
        confirmed_ma5 = pos.trailing_ma5_reference
        if pos.side == "LONG" and relation == "LONG_BELOW_OR_EQUAL_MA5" and bar1_new.close < confirmed_ma5:
            if should_defer_ma5_exit_by_hold_score(pos, f, pnl_ticks, "MA5_CLOSE_BELOW_TRAILING", hold_score_config, analysis_config, feature_metrics, storage):
                return False, "HOLD", pnl_ticks
            _log_ma5_trailing_exit(storage, pos, f, pnl_ticks, "MA5_CLOSE_BELOW_TRAILING", confirmed_bar_close=bar1_new.close)
            return True, "MA5_CLOSE_BELOW_TRAILING", pnl_ticks
        if pos.side == "SHORT" and relation == "SHORT_ABOVE_OR_EQUAL_MA5" and bar1_new.close > confirmed_ma5:
            _log_ma5_trailing_exit(storage, pos, f, pnl_ticks, "MA5_CLOSE_ABOVE_TRAILING", confirmed_bar_close=bar1_new.close)
            return True, "MA5_CLOSE_ABOVE_TRAILING", pnl_ticks

    confirmed_ma5 = confirmed_bar1.ma5 if confirmed_bar1 is not None else None
    if (
        current_bar_bucket is not None
        and current_bar_open is not None
        and confirmed_ma5 is not None
        and pos.trailing_ma5_bar_bucket != current_bar_bucket
    ):
        _set_ma5_trailing_context(pos, f, current_bar_bucket, current_bar_open, confirmed_ma5, pnl_ticks, storage)

    if pos.trailing_ma5_reference is None or pos.trailing_ma5_bar_open is None:
        return False, "HOLD", pnl_ticks

    relation = pos.trailing_ma5_open_relation
    confirmed_ma5 = pos.trailing_ma5_reference
    if pos.side == "LONG" and relation == "LONG_ABOVE_MA5" and f.price <= confirmed_ma5:
        if should_defer_ma5_exit_by_hold_score(pos, f, pnl_ticks, "MA5_INTRABAR_CROSS_TRAILING", hold_score_config, analysis_config, feature_metrics, storage):
            return False, "HOLD", pnl_ticks
        _log_ma5_trailing_exit(storage, pos, f, pnl_ticks, "MA5_INTRABAR_CROSS_TRAILING")
        return True, "MA5_INTRABAR_CROSS_TRAILING", pnl_ticks
    if pos.side == "SHORT" and relation == "SHORT_BELOW_MA5" and f.price >= confirmed_ma5:
        _log_ma5_trailing_exit(storage, pos, f, pnl_ticks, "MA5_INTRABAR_CROSS_TRAILING")
        return True, "MA5_INTRABAR_CROSS_TRAILING", pnl_ticks

    return False, "HOLD", pnl_ticks


def should_exit(
    pos: PositionState,
    f: FeatureSnapshot,
    pred: PredictionSnapshot,
    storage: Optional[Storage] = None,
    bar1_new: Optional[Bar] = None,
    current_bar_bucket: Optional[datetime] = None,
    current_bar_open: Optional[float] = None,
    confirmed_bar1: Optional[Bar] = None,
    hold_score_config: Optional[dict[str, Any]] = None,
    analysis_config: Optional[dict[str, Any]] = None,
    feature_metrics: Optional[dict[str, Any]] = None,
) -> tuple[bool, str, float]:
    # Profit exits for STRAT_1M/STRAT_3M/RSI9 (including RSI17 special entries)
    # are managed by +10tick activation followed by confirmed-1m-MA5 exit rules.
    # This intentionally replaces fixed +10tick / +5tick staged take-profit orders
    # and the former highest/lowest-price 20tick trailing stop while preserving
    # HARD_STOP_LOSS and the external force-close flow.
    return update_trailing_exit(
        pos,
        f,
        bar1_new=bar1_new,
        storage=storage,
        current_bar_bucket=current_bar_bucket,
        current_bar_open=current_bar_open,
        confirmed_bar1=confirmed_bar1,
        hold_score_config=hold_score_config,
        analysis_config=analysis_config,
        feature_metrics=feature_metrics,
    )



def _recent_trades(
    closed_trades: list[ClosedTradeSummary],
    now_: datetime,
    minutes: int = LIGHT_BRAKE_LOOKBACK_MINUTES,
    strategy: Optional[str] = None,
) -> list[ClosedTradeSummary]:
    cutoff = now_ - timedelta(minutes=minutes)
    out = [t for t in closed_trades if t.exit_ts >= cutoff]
    if strategy:
        out = [t for t in out if t.strategy == strategy]
    return out


def _sum_pnl(trades: list[ClosedTradeSummary]) -> float:
    return float(sum(t.pnl_ticks for t in trades))


def apply_light_loss_brake(
    adaptive: AdaptiveControlState,
    closed_trades: list[ClosedTradeSummary],
    f: FeatureSnapshot,
    storage: Optional[Storage] = None,
) -> None:
    if not LIGHT_BRAKE_ENABLED or not adaptive.enabled:
        return

    now_ = f.ts
    if now_.strftime("%H:%M:%S") < LIGHT_BRAKE_EVALUATION_START:
        return

    if adaptive.block_4x_until and now_ < adaptive.block_4x_until and adaptive.vwap_mode == "4x":
        old_mode = adaptive.vwap_mode
        adaptive.vwap_mode = "2x"
        set_vwap_mode(adaptive.vwap_mode)
        if storage is not None:
            storage.log("INFO", "LIGHT_BRAKE_KEEP_4X_BLOCK", f"{old_mode}->2x until={adaptive.block_4x_until.isoformat()}")

    recent_4x = _recent_trades(closed_trades, now_)
    recent_4x = [t for t in recent_4x if t.entry_vwap_mode == "4x"]
    if len(recent_4x) >= LIGHT_BRAKE_4X_MIN_TRADES:
        pnl_4x = _sum_pnl(recent_4x)
        if pnl_4x <= LIGHT_BRAKE_4X_PNL_LIMIT:
            until = now_ + timedelta(minutes=LIGHT_BRAKE_4X_BLOCK_MINUTES)
            if adaptive.block_4x_until is None or until > adaptive.block_4x_until:
                adaptive.block_4x_until = until
                old_mode = adaptive.vwap_mode
                adaptive.vwap_mode = "2x"
                set_vwap_mode(adaptive.vwap_mode)
                if storage is not None:
                    storage.log("INFO", "LIGHT_BRAKE_BLOCK_4X", f"{old_mode}->2x until={until.isoformat()} pnl_4x={pnl_4x:.1f}")

    recent_1m = _recent_trades(closed_trades, now_, strategy="STRAT_1M")
    if len(recent_1m) >= LIGHT_BRAKE_STRAT_1M_MIN_TRADES:
        pnl_1m = _sum_pnl(recent_1m)
        if pnl_1m <= LIGHT_BRAKE_STRAT_1M_PNL_LIMIT:
            until = now_ + timedelta(minutes=LIGHT_BRAKE_STRAT_1M_FREEZE_MINUTES)
            if adaptive.freeze_strat_1m_until is None or until > adaptive.freeze_strat_1m_until:
                adaptive.freeze_strat_1m_until = until
                if storage is not None:
                    storage.log("INFO", "LIGHT_BRAKE_FREEZE_STRAT_1M", f"until={until.isoformat()} pnl_1m={pnl_1m:.1f}")

    set_vwap_mode(adaptive.vwap_mode)


def write_latest_status(
    outdir: str,
    status: MonitorStatus,
    latest_feature: Optional[FeatureSnapshot],
    latest_pred: Optional[PredictionSnapshot],
    storage: Storage,
    latest_gate: Optional[GateDecision] = None,
) -> None:
    _ = storage
    path = os.path.join(outdir, "latest_status.md")
    lines = [
        "# 1570 latest status",
        "",
        f"- 時刻: {now_jst().isoformat()}",
        f"- count: {status.count}",
        f"- live_state: {status.live_state}",
        f"- last_error_code: {status.last_error_code}",
    ]
    if latest_feature:
        lines += [
            f"- 価格: {latest_feature.price}",
            f"- VWAP: {latest_feature.vwap}",
            f"- spread_ticks: {latest_feature.spread_ticks:.2f}",
        ]
    if latest_pred:
        lines += [
            f"- p_up_1m: {latest_pred.p_up_1m:.3f}",
            f"- p_up_3m: {latest_pred.p_up_3m:.3f}",
            f"- signal: {latest_pred.signal}",
        ]
    if latest_gate:
        lines += [
            f"- gate_mode: {latest_gate.mode}",
            f"- gate_action: {latest_gate.action}",
            f"- gate_applied: {latest_gate.applied}",
            f"- final_signal: {latest_gate.final_signal}",
            f"- gate_reason: {latest_gate.reason}",
            f"- gate_regime: {latest_gate.regime}",
        ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _read_rows(con: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    columns = [desc[0] for desc in cur.description or []]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _read_rows_if_exists(con: sqlite3.Connection, table: str, order_by: str) -> list[dict[str, Any]]:
    exists = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not exists:
        return []
    return _read_rows(con, f"SELECT * FROM {table} ORDER BY {order_by}")

def _value_counts(rows: list[dict[str, Any]], column: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(column) or "")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _top_counts(rows: list[dict[str, Any]], column: str, limit: int) -> list[tuple[str, int]]:
    return sorted(_value_counts(rows, column).items(), key=lambda x: (-x[1], x[0]))[:limit]


def _numeric_values(rows: list[dict[str, Any]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        try:
            if row.get(column) is not None:
                values.append(float(row[column]))
        except (TypeError, ValueError):
            continue
    return values


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def generate_report(db_path: str, report_path: str, midday: bool = False) -> None:
    con = sqlite3.connect(db_path)
    try:
        pred = _read_rows_if_exists(con, "prediction_snapshot", "ts")
        trades = _read_rows_if_exists(con, "paper_trades", "trade_id")
        events = _read_rows_if_exists(con, "system_events", "ts")
        structured = _read_rows_if_exists(con, "structured_events", "ts")
        gate_decisions = _read_rows_if_exists(con, "gate_decisions", "ts")
        feats = _read_rows_if_exists(con, "feature_snapshot", "ts")
    finally:
        con.close()

    date_label = os.path.basename(db_path).split("monitor_1570_")[-1].split(".db")[0]
    if len(date_label) == 8:
        date_fmt = f"{date_label[:4]}-{date_label[4:6]}-{date_label[6:8]}"
    else:
        date_fmt = jst_date_str()

    lines = [
        f"# 1570 自動監視 {'前場レポート' if midday else '日次レポート'}",
        "",
        f"対象日: {date_fmt}",
        "",
    ]
    lines.append("## 1. 総括")
    lines.append(f"- スナップショット数: {len(feats)}")
    lines.append(f"- 予測回数: {len(pred)}")
    if pred:
        vc = _value_counts(pred, "signal")
        lines.append(f"- LONG候補: {int(vc.get('LONG_CANDIDATE', 0))}")
        lines.append(f"- SHORT候補: {int(vc.get('SHORT_CANDIDATE', 0))}")
        lines.append(f"- NO_ACTION: {int(vc.get('NO_ACTION', 0))}")

    lines.append("")
    lines.append("## 2. 仮想売買")
    if not trades:
        lines.append("- 完了トレード数: 0")
        lines.append("- 勝率: 0.0%")
        lines.append("- 平均損益(ティック): 0.00")
        lines.append("- 総損益(ティック): 0.00")
        lines.append("- 平均保有秒数: 0.0")
    else:
        pnl_values = _numeric_values(trades, "pnl_ticks")
        holding_values = _numeric_values(trades, "holding_sec")
        wins = sum(1 for value in pnl_values if value > 0)
        lines.append(f"- 完了トレード数: {len(trades)}")
        lines.append(f"- 勝率: {100.0 * wins / len(trades):.1f}%")
        lines.append(f"- 平均損益(ティック): {_mean(pnl_values):.2f}")
        lines.append(f"- 総損益(ティック): {sum(pnl_values):.2f}")
        lines.append(f"- 平均保有秒数: {_mean(holding_values):.1f}")
        lines.append("")
        lines.append("## 3. 戦略別件数")
        for strat, n in _top_counts(trades, "strategy", 100):
            lines.append(f"- {strat}: {int(n)}")
        lines.append("")
        lines.append("## 4. 出口理由")
        for reason, n in _top_counts(trades, "exit_reason", 100):
            lines.append(f"- {reason}: {int(n)}")

    if gate_decisions:
        lines.append("")
        lines.append("## Volatility-Regime Gate Summary")
        mode_counts = _value_counts(gate_decisions, "gate_mode")
        latest_mode = str(next((row.get("gate_mode") for row in reversed(gate_decisions) if row.get("gate_mode")), "unknown"))
        lines.append(f"- Gate enabled: {latest_mode != 'off'}")
        lines.append(f"- Gate mode: {latest_mode}")
        if mode_counts:
            lines.append("- Mode counts: " + ", ".join(f"{k}={int(v)}" for k, v in mode_counts.items()))
        action_counts = _value_counts(gate_decisions, "gate_action")
        lines.append(f"- ALLOW count: {int(action_counts.get('ALLOW', action_counts.get('PASS', 0)))}")
        lines.append(f"- BLOCK count: {int(action_counts.get('BLOCK', 0))}")
        lines.append(f"- WARN_ONLY count: {int(action_counts.get('WARN_ONLY', action_counts.get('WARN', 0)))}")
        lines.append(f"- Applied BLOCK count: {sum(_to_int(row.get('gate_applied'), 0) for row in gate_decisions)}")
        block_reasons = [row for row in gate_decisions if row.get("gate_action") == "BLOCK"]
        if block_reasons:
            reason_counts = _top_counts(block_reasons, "gate_reason", 8)
            if reason_counts:
                lines.append("- BLOCK reasons: " + ", ".join(f"{k}={int(v)}" for k, v in reason_counts))
        rc = _value_counts(gate_decisions, "regime")
        if rc:
            lines.append("- Regime counts: " + ", ".join(f"{k}={int(v)}" for k, v in rc.items()))
        raw_vc = _value_counts(gate_decisions, "raw_signal")
        lines.append(f"- Raw LONG_CANDIDATE count: {int(raw_vc.get('LONG_CANDIDATE', 0))}")
        lines.append(f"- Raw SHORT_CANDIDATE count: {int(raw_vc.get('SHORT_CANDIDATE', 0))}")
        final_vc = _value_counts(gate_decisions, "final_signal")
        lines.append(f"- Final LONG_CANDIDATE count: {int(final_vc.get('LONG_CANDIDATE', 0))}")
        lines.append(f"- Final SHORT_CANDIDATE count: {int(final_vc.get('SHORT_CANDIDATE', 0))}")
        excluded = [
            row
            for row in gate_decisions
            if row.get("raw_signal") in {"LONG_CANDIDATE", "SHORT_CANDIDATE"} and row.get("final_signal") == "NO_ACTION"
        ]
        lines.append(f"- Gateで除外された候補数: {len(excluded)}")

    err_count = sum(1 for row in events if row.get("level") in {"WARN", "ERROR"})
    lines.append("")
    lines.append(f"## {'5' if trades else '3'}. システム")
    lines.append(f"- WARN/ERROR件数: {err_count}")
    if events:
        fail_events = [
            row
            for row in events
            if any(token in str(row.get("event_type") or "") for token in ("FAIL", "ERROR", "REJECTED"))
        ]
        if fail_events:
            lines.append("")
            lines.append("### 主要エラー/失敗イベント")
            for event_type, n in _top_counts(fail_events, "event_type", 12):
                lines.append(f"- {event_type}: {int(n)}")
    if structured:
        lines.append("")
        lines.append("### 注文・復旧イベント")
        for event_type, n in _top_counts(structured, "event_type", 16):
            lines.append(f"- {event_type}: {int(n)}")
        api_counts: dict[str, int] = {}
        side_counts: dict[str, int] = {}
        strategy_counts: dict[str, int] = {}
        market_counts: dict[str, int] = {}
        for sr in structured:
            try:
                payload = json.loads(sr.get("payload_json") or "{}")
            except Exception:
                payload = {}
            code = str(payload.get("api_code") or "")
            if code:
                api_counts[code] = api_counts.get(code, 0) + 1
            if code == "100302":
                side = str(payload.get("side") or "UNKNOWN")
                strategy = str(payload.get("strategy_name") or "UNKNOWN")
                exchange = str(payload.get("exchange") or "")
                order_ex = str(payload.get("order_exchange") or "")
                side_counts[side] = side_counts.get(side, 0) + 1
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
                market_key = f"exchange={exchange} order_exchange={order_ex}"
                market_counts[market_key] = market_counts.get(market_key, 0) + 1
        if api_counts:
            lines.append("")
            lines.append("### API Code別件数")
            for code, n in sorted(api_counts.items(), key=lambda x: (-x[1], x[0]))[:12]:
                label = "ENTRY_FAIL_100302" if code == "100302" else f"API Code {code}"
                lines.append(f"- {label}: {n}")
        if side_counts or strategy_counts or market_counts:
            lines.append("")
            lines.append("### 100302内訳")
            if side_counts:
                lines.append("- side別: " + ", ".join(f"{k}={v}" for k, v in sorted(side_counts.items())))
            if strategy_counts:
                lines.append("- strategy別: " + ", ".join(f"{k}={v}" for k, v in sorted(strategy_counts.items())))
            if market_counts:
                lines.append("- market/order_exchange別: " + ", ".join(f"{k}:{v}" for k, v in sorted(market_counts.items())))
    lines.append("")
    lines.append("### 直近イベント")
    if not events:
        lines.append("- なし")
    else:
        for r in events[-8:]:
            lines.append(f"- {r.get('ts')} [{r.get('level')}] {r.get('event_type')}: {r.get('message')}")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def position_leaves_qty(position: dict[str, Any]) -> int:
    return max(_to_int(position.get("LeavesQty"), 0), 0)


def position_hold_qty(position: dict[str, Any]) -> int:
    return max(_to_int(position.get("HoldQty"), 0), 0)


def position_available_qty(position: dict[str, Any]) -> int:
    return max(position_leaves_qty(position) - position_hold_qty(position), 0)


def position_execution_id(position: dict[str, Any]) -> str:
    return str(position.get("ExecutionID") or "")


def managed_positions_for(position_state: PositionState, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    managed_set = {str(x) for x in (position_state.managed_execution_ids or []) if str(x)}
    if not managed_set:
        return positions
    return [p for p in positions if position_execution_id(p) in managed_set]


def position_quantities(positions: list[dict[str, Any]]) -> tuple[int, int, int]:
    leaves_qty = sum(position_leaves_qty(p) for p in positions)
    hold_qty = sum(min(position_hold_qty(p), position_leaves_qty(p)) for p in positions)
    available_qty = sum(position_available_qty(p) for p in positions)
    return leaves_qty, hold_qty, available_qty


def _detail_has_execution_marker(detail: dict[str, Any]) -> bool:
    if detail.get("ExecutionPrice") is not None or detail.get("ContractPrice") is not None:
        return True
    marker_keys = ("ExecutionID", "ExecutionDay", "ContractDay", "ContractQty", "ExecutionQty")
    if any(detail.get(k) not in (None, "") for k in marker_keys):
        return True
    text = " ".join(str(detail.get(k) or "") for k in ("State", "StateName", "RecType", "Type", "Description"))
    return "約定" in text or "Contract" in text or "Execution" in text


def _extract_fill_price_from_order_rows(rows: list[dict[str, Any]], order_id: str) -> tuple[Optional[float], str, Any]:
    matched = [r for r in rows if not order_id or str(r.get("ID") or r.get("OrderId") or r.get("OrderID") or "") == str(order_id)]
    if not matched:
        matched = rows
    weighted_value = 0.0
    weighted_qty = 0.0
    explicit_prices: list[float] = []
    order_limit_price_seen = False
    for row in matched:
        details = row.get("Details") or row.get("details") or []
        if isinstance(details, list):
            for detail in details:
                if not isinstance(detail, dict):
                    continue
                price = _safe_float(detail.get("ExecutionPrice") or detail.get("ContractPrice"))
                if price is None and _detail_has_execution_marker(detail):
                    price = _safe_float(detail.get("Price"))
                qty = _safe_float(detail.get("ExecutionQty") or detail.get("ContractQty") or detail.get("Qty"))
                if price is not None and price > 0:
                    explicit_prices.append(price)
                if price is not None and price > 0 and qty is not None and qty > 0:
                    weighted_value += price * qty
                    weighted_qty += qty
        for key in ("AvgPrice", "ExecutionPrice", "ContractPrice"):
            price = _safe_float(row.get(key))
            if price is not None and price > 0:
                explicit_prices.append(price)
        if _safe_float(row.get("Price")) is not None:
            order_limit_price_seen = True
    if weighted_qty > 0:
        return weighted_value / weighted_qty, "ORDER_DETAIL", matched
    if explicit_prices:
        return explicit_prices[-1], "ORDER_DETAIL", matched
    return None, "ORDER_LIMIT_PRICE_FALLBACK" if order_limit_price_seen else "UNAVAILABLE", matched


def resolve_actual_fill_price(
    client: KabuApiClient,
    storage: Optional[Storage],
    order_id: str,
    side: str,
    qty: Optional[int] = None,
    signal_price: Optional[float] = None,
    limit_price: Optional[float] = None,
) -> tuple[Optional[float], str]:
    if not order_id:
        return None, "ORDER_ID_MISSING"
    try:
        order_ids = [oid for oid in _split_order_ids(order_id) if oid]
        rows: list[dict[str, Any]] = []
        if len(order_ids) > 1:
            for oid in order_ids:
                rows.extend(client.get_orders(order_id=oid, product=2))
        else:
            rows = client.get_orders(order_id=order_ids[0] if order_ids else order_id, product=2)
        actual_fill_price, source, raw_detail = _extract_fill_price_from_order_rows(rows, order_id)
        if actual_fill_price is not None:
            if storage is not None:
                storage.log_structured(
                    "INFO",
                    "EXIT_ACTUAL_FILL_PRICE_RESOLVED",
                    {
                        "order_id": order_id,
                        "side": side,
                        "qty": qty,
                        "signal_price": signal_price,
                        "limit_price": limit_price,
                        "actual_fill_price": actual_fill_price,
                        "fill_source": source or "ORDER_DETAIL",
                        "raw_order_detail_json": raw_detail,
                    },
                )
            return actual_fill_price, source or "ORDER_DETAIL"
        if storage is not None:
            storage.log_structured(
                "WARN",
                "EXIT_ACTUAL_FILL_PRICE_UNAVAILABLE",
                {
                    "order_id": order_id,
                    "side": side,
                    "qty": qty,
                    "signal_price": signal_price,
                    "limit_price": limit_price,
                    "actual_fill_price": None,
                    "fill_source": source or "UNAVAILABLE",
                    "raw_order_detail_json": rows,
                    "error": "NO_EXECUTION_PRICE_IN_ORDER_DETAIL",
                },
            )
        return None, source or "UNAVAILABLE"
    except Exception as e:
        if storage is not None:
            storage.log_structured(
                "WARN",
                "EXIT_ACTUAL_FILL_PRICE_UNAVAILABLE",
                {
                    "order_id": order_id,
                    "side": side,
                    "qty": qty,
                    "signal_price": signal_price,
                    "limit_price": limit_price,
                    "actual_fill_price": None,
                    "fill_source": "UNAVAILABLE",
                    "error": str(e),
                },
            )
        return None, "UNAVAILABLE"


def position_identity(position: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(position.get("ExecutionID") or ""),
        str(position.get("Symbol") or ""),
        str(position.get("Side") or ""),
        str(position.get("MarginTradeType") or ""),
    )


def dedupe_positions(positions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    duplicate_count = 0
    for position in positions:
        key = position_identity(position)
        if key[0] and key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        deduped.append(position)
    return deduped, duplicate_count


def fetch_positions(client: KabuApiClient, config: dict[str, Any], storage: Optional[Storage] = None, reason: str = "") -> list[dict[str, Any]]:
    positions = client.get_positions(config["symbol"])
    if storage is not None:
        storage.log_structured(
            "INFO",
            "POSITION_SNAPSHOT",
            {
                "reason": reason,
                "symbol": config["symbol"],
                "position_exchanges_expected": position_exchanges(config),
                "order_exchange": order_exchange(config),
                "exit_exchange": exit_exchange(config),
                "positions_count": len(positions),
                "raw_positions_json": positions,
            },
        )
    deduped, duplicate_count = dedupe_positions(positions)
    if storage is not None and duplicate_count:
        storage.log_structured(
            "WARN",
            "POSITION_DEDUPED",
            {
                "reason": reason,
                "symbol": config["symbol"],
                "raw_positions_count": len(positions),
                "deduped_positions_count": len(deduped),
                "duplicate_count": duplicate_count,
            },
        )
    return deduped


def margin_trade_type_for_side(config: dict[str, Any], side: str) -> int:
    if side == "SHORT":
        return int(config.get("margin_trade_type_short", 1))
    return int(config.get("margin_trade_type_long", config.get("margin_trade_type", 3)))


def position_matches(position: dict[str, Any], side: Optional[str] = None, margin_trade_type: Optional[int] = None) -> bool:
    if side is not None:
        expected_code = "2" if side == "LONG" else "1"
        if str(position.get("Side")) != expected_code:
            return False
    if margin_trade_type is not None and int(_to_int(position.get("MarginTradeType"), -1)) != int(margin_trade_type):
        return False
    return True


def get_open_position_qty(client: KabuApiClient, config: dict[str, Any], side: str, margin_trade_type: Optional[int] = None) -> int:
    positions = fetch_positions(client, config)
    total = 0
    for p in positions:
        if not position_matches(p, side=side, margin_trade_type=margin_trade_type):
            continue
        total += position_leaves_qty(p)
    return total


def summarize_positions(
    positions: list[dict[str, Any]],
    expected_side: Optional[str] = None,
    expected_margin_trade_type: Optional[int] = None,
) -> tuple[int, int]:
    total = 0
    matching = 0
    for p in positions:
        leaves = position_leaves_qty(p)
        total += leaves
        if position_matches(p, side=expected_side, margin_trade_type=expected_margin_trade_type):
            matching += leaves
    return total, matching


def wait_for_position_qty(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    target_qty: int,
    timeout_sec: int,
    comparator: str = "ge",
    margin_trade_type: Optional[int] = None,
) -> bool:
    start = time.time()
    while time.time() - start <= timeout_sec:
        qty = get_open_position_qty(client, config, side, margin_trade_type=margin_trade_type)
        if comparator == "ge" and qty >= target_qty:
            return True
        if comparator == "eq" and qty == target_qty:
            return True
        time.sleep(0.5)
    return False


def wait_for_managed_position_qty(
    client: KabuApiClient,
    config: dict[str, Any],
    pos: PositionState,
    target_qty: int,
    timeout_sec: int | float,
    comparator: str = "eq",
    storage: Optional[Storage] = None,
    reason: str = "MANAGED_POSITION_QTY_WAIT",
) -> bool:
    start = time.time()
    while True:
        positions = fetch_positions(client, config, storage, reason=reason)
        managed_positions = managed_positions_for(pos, positions)
        leaves_qty, _, _ = position_quantities(managed_positions)
        if comparator == "ge" and leaves_qty >= target_qty:
            return True
        if comparator == "eq" and leaves_qty == target_qty:
            return True
        if time.time() - start >= timeout_sec:
            return False
        time.sleep(0.5)


def get_matching_position_quantities(
    positions: list[dict[str, Any]],
    side: str,
    margin_trade_type: Optional[int] = None,
) -> tuple[int, int, int]:
    leaves_total = 0
    hold_total = 0
    available_total = 0
    for p in positions:
        if not position_matches(p, side=side, margin_trade_type=margin_trade_type):
            continue
        leaves = position_leaves_qty(p)
        hold = min(position_hold_qty(p), leaves)
        leaves_total += leaves
        hold_total += hold
        available_total += max(leaves - hold, 0)
    return leaves_total, hold_total, available_total


def average_price_from_positions(
    positions: list[dict[str, Any]],
    side: str,
    margin_trade_type: Optional[int] = None,
) -> Optional[float]:
    prices: list[float] = []
    for p in positions:
        if not position_matches(p, side=side, margin_trade_type=margin_trade_type):
            continue
        val = p.get("Price")
        if val is None:
            continue
        try:
            px = float(val)
        except Exception:
            continue
        if px > 0:
            prices.append(px)
    if not prices:
        return None
    return float(sum(prices) / len(prices))


def weighted_average_price_from_positions(
    positions: list[dict[str, Any]],
    side: str,
    margin_trade_type: Optional[int] = None,
) -> Optional[float]:
    weighted_value = 0.0
    total_qty = 0
    for p in positions:
        if not position_matches(p, side=side, margin_trade_type=margin_trade_type):
            continue
        qty = position_leaves_qty(p)
        if qty <= 0:
            continue
        px = actual_position_price(p, 0.0)
        if px <= 0:
            continue
        weighted_value += px * qty
        total_qty += qty
    if total_qty <= 0:
        return None
    return weighted_value / total_qty


def side_from_api_position(position: dict[str, Any]) -> str:
    return "LONG" if str(position.get("Side")) == "2" else "SHORT"


def actual_position_price(position: dict[str, Any], fallback: float = 0.0) -> float:
    try:
        price = float(position.get("Price"))
        return price if price > 0 else fallback
    except Exception:
        return fallback


def positions_summary_for_log(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for pos in positions:
        item = {
            "Symbol": pos.get("Symbol"),
            "Side": pos.get("Side"),
            "LeavesQty": pos.get("LeavesQty"),
            "Price": pos.get("Price"),
            "MarginTradeType": pos.get("MarginTradeType"),
        }
        if "ExecutionID" in pos:
            item["ExecutionID"] = pos.get("ExecutionID")
        if "AccountType" in pos:
            item["AccountType"] = pos.get("AccountType")
        summary.append(item)
    return summary


def new_managed_positions_from_diff(
    positions: list[dict[str, Any]],
    before_positions: Optional[list[dict[str, Any]]],
    expected_side: str,
    expected_margin_trade_type: Optional[int],
) -> list[dict[str, Any]]:
    matched = [
        p for p in positions
        if position_matches(p, side=expected_side, margin_trade_type=expected_margin_trade_type)
        and position_leaves_qty(p) > 0
        and position_execution_id(p)
    ]
    if before_positions is None:
        return matched
    before_qty = {
        position_execution_id(p): position_leaves_qty(p)
        for p in before_positions
        if position_execution_id(p)
    }
    return [p for p in matched if position_leaves_qty(p) > before_qty.get(position_execution_id(p), 0)]


def find_matching_actual_position(
    positions: list[dict[str, Any]],
    expected_side: str,
    expected_margin_trade_type: Optional[int],
) -> Optional[dict[str, Any]]:
    candidates = [
        p for p in positions
        if position_matches(p, side=expected_side, margin_trade_type=expected_margin_trade_type)
        and position_leaves_qty(p) > 0
    ]
    if not candidates:
        return None
    return max(candidates, key=position_leaves_qty)




def enter_recovery_after_add_rebuild_failure(
    storage: Storage,
    status: MonitorStatus,
    pos: PositionState,
    ts: datetime,
    reason: str,
    payload: dict[str, Any],
) -> None:
    status.live_state = "RECOVERING"
    status.recovery_until = now_jst() + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
    status.pending_add = False
    storage.log_structured(
        "ERROR",
        "RSI9_LONG_ADD_MANAGED_STATE_REBUILD_FAILED",
        {
            "ts": ts.isoformat(),
            "reason": reason,
            "recovery_until": status.recovery_until.isoformat() if status.recovery_until else None,
            "internal_position": position_state_payload(pos),
            **payload,
        },
        mirror_message=f"reason={reason}",
    )


def rebuild_rsi9_long_add_managed_state(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    status: MonitorStatus,
    pos: PositionState,
    before_positions: list[dict[str, Any]],
    expected_total_qty: int,
    ts: datetime,
) -> bool:
    before_managed_execution_ids = [str(x) for x in (pos.managed_execution_ids or []) if str(x)]
    before_managed_set = set(before_managed_execution_ids)
    old_filled_qty = int(pos.filled_qty)
    old_entry_price = pos.entry_fill_price if pos.entry_fill_price is not None else pos.entry_price
    try:
        after_positions = fetch_positions(client, config, storage, reason="POST_ADD_REBUILD_MANAGED_POSITIONS")
    except Exception as e:
        enter_recovery_after_add_rebuild_failure(
            storage,
            status,
            pos,
            ts,
            "POSITIONS_FETCH_FAILED",
            {"before_managed_execution_ids": before_managed_execution_ids, **api_error_payload(e)},
        )
        return False

    matching_positions = [
        p for p in after_positions
        if position_matches(p, side="LONG", margin_trade_type=pos.margin_trade_type)
        and position_leaves_qty(p) > 0
        and position_execution_id(p)
    ]
    added_positions = new_managed_positions_from_diff(after_positions, before_positions, "LONG", pos.margin_trade_type)
    added_execution_ids = sorted({position_execution_id(p) for p in added_positions if position_execution_id(p) and position_execution_id(p) not in before_managed_set})
    after_managed_execution_ids = sorted(before_managed_set | set(added_execution_ids))
    managed_positions = [p for p in matching_positions if position_execution_id(p) in set(after_managed_execution_ids)]
    managed_close_positions = positions_summary_for_log(managed_positions)
    managed_qty_sum = sum(position_leaves_qty(p) for p in managed_positions)
    new_entry_price = weighted_average_price_from_positions(managed_positions, "LONG", margin_trade_type=pos.margin_trade_type)
    failure_reason = ""
    if not added_execution_ids:
        failure_reason = "ADDED_EXECUTION_IDS_NOT_DETECTED"
    elif not managed_close_positions:
        failure_reason = "MANAGED_CLOSE_POSITIONS_EMPTY"
    elif managed_qty_sum != int(expected_total_qty):
        failure_reason = "MANAGED_QTY_SUM_MISMATCH"
    elif new_entry_price is None:
        failure_reason = "MANAGED_WEIGHTED_ENTRY_PRICE_UNAVAILABLE"
    if failure_reason:
        enter_recovery_after_add_rebuild_failure(
            storage,
            status,
            pos,
            ts,
            failure_reason,
            {
                "before_managed_execution_ids": before_managed_execution_ids,
                "after_managed_execution_ids": after_managed_execution_ids,
                "added_execution_ids": added_execution_ids,
                "managed_close_positions": managed_close_positions,
                "managed_qty_sum": managed_qty_sum,
                "expected_total_qty": int(expected_total_qty),
                "old_filled_qty": old_filled_qty,
                "old_entry_price": old_entry_price,
                "raw_positions_json": after_positions,
            },
        )
        return False

    pos.managed_execution_ids = after_managed_execution_ids
    pos.managed_close_positions = managed_close_positions
    pos.filled_qty = int(managed_qty_sum)
    pos.order_qty = int(managed_qty_sum)
    pos.remaining_qty = 0
    pos.entry_price = float(new_entry_price)
    pos.entry_fill_price = float(new_entry_price)
    storage.log_structured(
        "INFO",
        "RSI9_LONG_ADD_MANAGED_STATE_REBUILT",
        {
            "ts": ts.isoformat(),
            "before_managed_execution_ids": before_managed_execution_ids,
            "after_managed_execution_ids": after_managed_execution_ids,
            "added_execution_ids": added_execution_ids,
            "managed_close_positions": managed_close_positions,
            "managed_qty_sum": managed_qty_sum,
            "expected_total_qty": int(expected_total_qty),
            "old_filled_qty": old_filled_qty,
            "new_filled_qty": pos.filled_qty,
            "old_entry_price": old_entry_price,
            "new_entry_price": pos.entry_price,
            "raw_positions_json": after_positions,
        },
    )
    return True

def create_position_from_actual_position(
    api_pos: dict[str, Any],
    template_pos: PositionState,
    expected_side: str,
    config: dict[str, Any],
    entry_order_id: str,
) -> PositionState:
    actual_side = side_from_api_position(api_pos)
    if actual_side != expected_side:
        raise ValueError(f"actual side mismatch: expected={expected_side} actual={actual_side}")
    leaves_qty = position_leaves_qty(api_pos)
    margin_trade_type = _to_int(api_pos.get("MarginTradeType"), margin_trade_type_for_side(config, expected_side))
    entry_price = actual_position_price(api_pos, template_pos.entry_price)
    execution_id = str(api_pos.get("ExecutionID") or "")
    managed_close_positions = positions_summary_for_log([api_pos]) if execution_id else []
    return PositionState(
        side=actual_side,
        strategy=template_pos.strategy,
        entry_ts=template_pos.entry_ts,
        entry_price=entry_price,
        entry_p_up_1m=template_pos.entry_p_up_1m,
        entry_p_up_3m=template_pos.entry_p_up_3m,
        stop_ticks=template_pos.stop_ticks,
        take_ticks=template_pos.take_ticks,
        min_hold_sec=template_pos.min_hold_sec,
        max_hold_sec=template_pos.max_hold_sec,
        entry_vwap_gap_bps=template_pos.entry_vwap_gap_bps,
        entry_regime=template_pos.entry_regime,
        entry_vwap_mode=template_pos.entry_vwap_mode,
        margin_trade_type=margin_trade_type,
        entry_order_id=entry_order_id,
        order_qty=leaves_qty,
        filled_qty=leaves_qty,
        remaining_qty=0,
        entry_fill_price=entry_price,
        rsi_special_entry=template_pos.rsi_special_entry,
        rsi_special_tp_stage=template_pos.rsi_special_tp_stage,
        rsi_special_tp_order_ts=template_pos.rsi_special_tp_order_ts,
        rsi10_add_done=template_pos.rsi10_add_done,
        managed_execution_ids=[execution_id] if execution_id else [],
        managed_close_positions=managed_close_positions,
        trailing_active=template_pos.trailing_active,
        trailing_trigger_ticks=template_pos.trailing_trigger_ticks,
        trailing_floor_ticks=template_pos.trailing_floor_ticks,
        trailing_width_ticks=template_pos.trailing_width_ticks,
        trailing_high=template_pos.trailing_high,
        trailing_low=template_pos.trailing_low,
        trailing_stop_price=template_pos.trailing_stop_price,
        ma5_breach_count=template_pos.ma5_breach_count,
        trailing_started_at=template_pos.trailing_started_at,
        trailing_ma5_bar_bucket=template_pos.trailing_ma5_bar_bucket,
        trailing_ma5_bar_open=template_pos.trailing_ma5_bar_open,
        trailing_ma5_reference=template_pos.trailing_ma5_reference,
        trailing_ma5_open_relation=template_pos.trailing_ma5_open_relation,
        ma5_exit_skip_count=template_pos.ma5_exit_skip_count,
        last_ma5_exit_deferred_ts=template_pos.last_ma5_exit_deferred_ts,
        last_hold_score=template_pos.last_hold_score,
        hard_stop_ticks=template_pos.hard_stop_ticks,
    )


def verify_position_before_exit(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    status: MonitorStatus,
    pos: PositionState,
    intended_exit_reason: str,
    ts: datetime,
) -> bool:
    storage.log_structured("INFO", "EXIT_POSITION_VERIFY_START", {
        "ts": ts.isoformat(),
        "intended_exit_reason": intended_exit_reason,
        "internal_position": position_state_payload(pos),
    })
    try:
        positions = fetch_positions(client, config, storage, reason="EXIT_POSITION_VERIFY")
    except Exception as e:
        status.live_state = "RECOVERING"
        status.recovery_until = ts + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
        storage.log_structured("ERROR", "POSITION_NOT_FOUND_BEFORE_EXIT", {
            "internal_side": pos.side,
            "internal_margin_trade_type": pos.margin_trade_type,
            "internal_entry_price": pos.entry_price,
            "intended_exit_reason": intended_exit_reason,
            "internal_position": position_state_payload(pos),
            **api_error_payload(e),
        })
        return False
    if pos.managed_execution_ids:
        check_positions = managed_positions_for(pos, positions)
        if not check_positions or sum(position_leaves_qty(p) for p in check_positions) <= 0:
            status.live_state = "FLAT"
            status.open_position = None
            storage.log_structured("WARN", "POSITION_MANAGED_IDS_NOT_FOUND_BEFORE_EXIT", {
                "managed_execution_ids": list(pos.managed_execution_ids),
                "intended_exit_reason": intended_exit_reason,
                "internal_position": position_state_payload(pos),
                "positions_summary": positions_summary_for_log(positions),
            })
            return False
        actual = find_matching_actual_position(check_positions, pos.side, pos.margin_trade_type)
    else:
        check_positions = positions
        actual = find_matching_actual_position(positions, pos.side, pos.margin_trade_type)
    if actual is None:
        first = next((p for p in check_positions if position_leaves_qty(p) > 0), None)
        event = "POSITION_SIDE_MISMATCH_BEFORE_EXIT" if first is not None else "POSITION_NOT_FOUND_BEFORE_EXIT"
        status.live_state = "RECOVERING"
        status.recovery_until = ts + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
        storage.log_structured("ERROR", event, {
            "internal_side": pos.side,
            "actual_side": side_from_api_position(first) if first else None,
            "internal_margin_trade_type": pos.margin_trade_type,
            "actual_margin_trade_type": _to_int(first.get("MarginTradeType"), -1) if first else None,
            "internal_entry_price": pos.entry_price,
            "actual_price": actual_position_price(first) if first else None,
            "actual_leaves_qty": position_leaves_qty(first) if first else 0,
            "intended_exit_reason": intended_exit_reason,
            "internal_position": position_state_payload(pos),
            "actual_position": first,
            "positions_summary": positions_summary_for_log(positions),
        })
        return False
    storage.log_structured("INFO", "EXIT_POSITION_VERIFY_OK", {
        "internal_side": pos.side,
        "actual_side": side_from_api_position(actual),
        "internal_margin_trade_type": pos.margin_trade_type,
        "actual_margin_trade_type": _to_int(actual.get("MarginTradeType"), -1),
        "internal_entry_price": pos.entry_price,
        "actual_price": actual_position_price(actual),
        "actual_leaves_qty": position_leaves_qty(actual),
        "intended_exit_reason": intended_exit_reason,
        "internal_position": position_state_payload(pos),
        "actual_position": actual,
    })
    return True


def verify_entry_position_after_order(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    template_pos: PositionState,
    expected_side: str,
    order_id: str,
    pred: PredictionSnapshot,
    status: MonitorStatus,
    ts: datetime,
    before_positions: Optional[list[dict[str, Any]]] = None,
) -> EntryPositionVerifyResult:
    expected_margin = template_pos.margin_trade_type
    expected_qty = int(config.get("order_qty", 2))
    retry_max = ENTRY_POSITION_VERIFY_RETRY_MAX
    retry_interval_sec = ENTRY_POSITION_VERIFY_RETRY_INTERVAL_SEC
    base_payload = {
        "ts": ts.isoformat(),
        "order_id": order_id,
        "expected_side": expected_side,
        "expected_qty": expected_qty,
        "expected_margin_trade_type": expected_margin,
        "retry_max": retry_max,
        "retry_interval_sec": retry_interval_sec,
        "pred_signal": pred.signal,
        "pred_reason_1": pred.reason_1,
        "pred_reason_2": pred.reason_2,
        "pred_reason_3": pred.reason_3,
        "live_state": status.live_state,
        "pending_entry_side": status.pending_entry_side,
        "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None,
        "internal_position": position_state_payload(template_pos),
        "before_positions_summary": positions_summary_for_log(before_positions or []),
    }
    storage.log_structured("INFO", "ENTRY_POSITION_VERIFY_START", base_payload)

    last_positions: list[dict[str, Any]] = []
    last_reason = "NO_MATCHING_POSITION"
    last_error_payload: dict[str, Any] = {}
    for attempt in range(1, retry_max + 1):
        try:
            positions = fetch_positions(client, config, storage, reason=f"ENTRY_POSITION_VERIFY_ATTEMPT_{attempt}")
            last_positions = positions
            last_error_payload = {}
        except Exception as e:
            positions = []
            last_positions = []
            last_reason = "FETCH_POSITIONS_ERROR"
            last_error_payload = api_error_payload(e)
            if attempt < retry_max:
                storage.log_structured("WARN", "ENTRY_POSITION_VERIFY_RETRY", {
                    **base_payload,
                    "attempt": attempt,
                    "reason": last_reason,
                    "positions_count": 0,
                    "positions_summary": [],
                    **last_error_payload,
                })
                time.sleep(retry_interval_sec)
                continue
            break

        actual = find_matching_actual_position(positions, expected_side, expected_margin)
        positions_summary = positions_summary_for_log(positions)
        if actual is not None:
            actual_pos = create_position_from_actual_position(actual, template_pos, expected_side, config, order_id)
            managed_positions = new_managed_positions_from_diff(positions, before_positions, expected_side, expected_margin)
            if not managed_positions and before_positions is None:
                managed_positions = [actual] if position_execution_id(actual) else []
            actual_pos.managed_execution_ids = [position_execution_id(p) for p in managed_positions]
            actual_pos.managed_close_positions = positions_summary_for_log(managed_positions)
            actual_pos.filled_qty = sum(position_leaves_qty(p) for p in managed_positions) or actual_pos.filled_qty
            actual_pos.order_qty = actual_pos.filled_qty
            storage.log_structured("INFO", "ENTRY_POSITION_VERIFY_OK", {
                **base_payload,
                "actual_side": actual_pos.side,
                "actual_qty": actual_pos.filled_qty,
                "actual_leaves_qty": actual_pos.filled_qty,
                "actual_price": actual_pos.entry_fill_price,
                "actual_margin_trade_type": actual_pos.margin_trade_type,
                "attempt": attempt,
                "positions_count": len(positions),
                "positions_summary": positions_summary,
                "actual_position": positions_summary_for_log([actual])[0] if actual else None,
                "internal_position": position_state_payload(actual_pos),
            })
            storage.log_structured("INFO", "POSITION_STATE_CREATED_FROM_ACTUAL", {
                "ts": ts.isoformat(),
                "order_id": order_id,
                "expected_side": expected_side,
                "attempt": attempt,
                "actual_position": positions_summary_for_log([actual])[0] if actual else None,
                "internal_position": position_state_payload(actual_pos),
            })
            return EntryPositionVerifyResult(
                position=actual_pos,
                verified_flat=False,
                verify_error=False,
                reason="MATCHED_POSITION",
                positions_count=len(positions),
                positions_summary=positions_summary,
                raw_positions=positions,
                matching_qty=actual_pos.filled_qty,
                total_qty=sum(position_leaves_qty(p) for p in positions),
            )

        last_reason = "NO_MATCHING_POSITION"
        if attempt < retry_max:
            storage.log_structured("WARN", "ENTRY_POSITION_VERIFY_RETRY", {
                **base_payload,
                "attempt": attempt,
                "reason": last_reason,
                "positions_count": len(positions),
                "positions_summary": positions_summary,
            })
            time.sleep(retry_interval_sec)

    failed_summary = positions_summary_for_log(last_positions)
    total_qty, matching_qty = summarize_positions(last_positions, expected_side, expected_margin)
    verified_flat = not last_error_payload and total_qty <= 0 and matching_qty <= 0
    final_reason = "VERIFIED_FLAT" if verified_flat else last_reason
    storage.log_structured("ERROR" if last_error_payload else "WARN", "ENTRY_POSITION_VERIFY_FAILED", {
        **base_payload,
        "reason": final_reason,
        "verified_flat": verified_flat,
        "verify_error": bool(last_error_payload),
        "positions_count": len(last_positions),
        "positions_summary": failed_summary,
        "total_qty": total_qty,
        "matching_qty": matching_qty,
        **last_error_payload,
    })
    return EntryPositionVerifyResult(
        position=None,
        verified_flat=verified_flat,
        verify_error=bool(last_error_payload),
        reason=final_reason,
        positions_count=len(last_positions),
        positions_summary=failed_summary,
        raw_positions=last_positions,
        matching_qty=matching_qty,
        total_qty=total_qty,
    )

def wait_for_position_unlocked_or_flat(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    margin_trade_type: Optional[int] = None,
    timeout_sec: float = 5.0,
    managed_execution_ids: Optional[list[str]] = None,
) -> tuple[bool, bool, int, int, int, list[dict[str, Any]]]:
    start = time.time()
    last_positions: list[dict[str, Any]] = []
    last_leaves = 0
    last_hold = 0
    last_available = 0
    while time.time() - start <= timeout_sec:
        last_positions = fetch_positions(client, config)
        if managed_execution_ids:
            managed_set = {str(x) for x in managed_execution_ids if str(x)}
            scoped_positions = [p for p in last_positions if position_execution_id(p) in managed_set]
            last_leaves, last_hold, last_available = position_quantities(scoped_positions)
        else:
            last_leaves, last_hold, last_available = get_matching_position_quantities(
                last_positions,
                side,
                margin_trade_type=margin_trade_type,
            )
        if last_leaves <= 0:
            return True, True, last_leaves, last_hold, last_available, last_positions
        if last_hold <= 0 and last_available > 0:
            return True, False, last_leaves, last_hold, last_available, last_positions
        time.sleep(0.5)
    return False, False, last_leaves, last_hold, last_available, last_positions


def build_entry_order_payload(
    config: dict[str, Any],
    side: str,
    front_order_type: Optional[int] = None,
    price: Optional[float] = None,
    qty_override: Optional[int] = None,
) -> dict[str, Any]:
    order_password = config["order_password"]
    qty = int(qty_override if qty_override is not None else config["order_qty"])
    side_code = "2" if side == "LONG" else "1"
    payload: dict[str, Any] = {
        "Password": order_password,
        "Symbol": config["symbol"],
        "Exchange": order_exchange(config),
        "SecurityType": 1,
        "Side": side_code,
        "CashMargin": int(config["entry_cash_margin"]),
        "MarginTradeType": margin_trade_type_for_side(config, side),
        "DelivType": int(config["entry_deliv_type"]),
        "AccountType": int(config["account_type"]),
        "Qty": qty,
        "FrontOrderType": int(front_order_type if front_order_type is not None else config["entry_front_order_type"]),
        "Price": float(price if price is not None else config.get("entry_price", 0)),
        "ExpireDay": int(config.get("expire_day", 0)),
    }
    overrides = config.get("live_entry_overrides_long" if side == "LONG" else "live_entry_overrides_short", {})
    if isinstance(overrides, dict):
        payload.update(overrides)
    return payload


def build_exit_order_payload(
    config: dict[str, Any],
    side: str,
    close_positions: list[dict[str, Any]],
    qty: int,
    front_order_type: Optional[int] = None,
    price: Optional[float] = None,
    margin_trade_type: Optional[int] = None,
    exchange: Optional[int] = None,
) -> dict[str, Any]:
    order_password = config["order_password"]
    exit_side_code = "1" if side == "LONG" else "2"
    payload: dict[str, Any] = {
        "Password": order_password,
        "Symbol": config["symbol"],
        "Exchange": int(exchange if exchange is not None else exit_exchange(config)),
        "SecurityType": 1,
        "Side": exit_side_code,
        "CashMargin": int(config["exit_cash_margin"]),
        "MarginTradeType": int(margin_trade_type if margin_trade_type is not None else margin_trade_type_for_side(config, side)),
        "DelivType": int(config["exit_deliv_type"]),
        "AccountType": int(config["account_type"]),
        "Qty": qty,
        "ClosePositions": close_positions,
        "FrontOrderType": int(front_order_type if front_order_type is not None else config["exit_front_order_type"]),
        "Price": float(price if price is not None else config.get("exit_price", 0)),
        "ExpireDay": int(config.get("expire_day", 0)),
    }
    overrides = config.get("live_exit_overrides", {})
    if isinstance(overrides, dict):
        payload.update(overrides)
    if exchange is not None:
        payload["Exchange"] = int(exchange)
    return payload


def entry_reject_key(config: dict[str, Any], side: str, strategy: str, pred: Optional[PredictionSnapshot], api_code: str) -> str:
    ts = pred.ts.isoformat() if pred else ""
    reason = pred.reason_1 if pred else ""
    return f"{config['symbol']}|{side}|{strategy}|{ts}|{reason}|{api_code}"


def short_ma_guard_pass(bar_1m: Optional[Bar], price: Optional[float]) -> bool:
    if bar_1m is None or price is None:
        return False
    if bar_1m.ma5 is None or bar_1m.ma13 is None or bar_1m.ma25 is None:
        return False
    return price <= bar_1m.ma5 and price <= bar_1m.ma13 and price <= bar_1m.ma25


def order_context(config: dict[str, Any], side: str, candidate: Optional[PositionState] = None, pred: Optional[PredictionSnapshot] = None, status: Optional[MonitorStatus] = None) -> dict[str, Any]:
    return {
        "symbol": config["symbol"],
        "side": side,
        "exchange": config.get("exchange"),
        "order_exchange": order_exchange(config),
        "exit_exchange": exit_exchange(config),
        "strategy_name": candidate.strategy if candidate else "",
        "signal_reason": pred.reason_1 if pred else "",
        "internal_position_state": position_state_payload(status.open_position if status else None),
    }


def entry_execution_config(config: dict[str, Any]) -> dict[str, Any]:
    cfg = config.get("entry_execution", {})
    return cfg if isinstance(cfg, dict) else {}


def use_limit_entry(config: dict[str, Any]) -> bool:
    cfg = entry_execution_config(config)
    return bool(cfg.get("enabled", False)) and str(cfg.get("mode", "market")) == "limit_with_timeout"


def entry_limit_price(side: str, snap: Optional[TickSnapshot], limit_mode: str) -> Optional[float]:
    if snap is None:
        return None
    best_ask = snap.sell1_price
    best_bid = snap.buy1_price
    if side == "LONG":
        return best_bid if limit_mode == "passive_best" else best_ask
    return best_ask if limit_mode == "passive_best" else best_bid


def marketable_exit_limit_price(pos: PositionState, snap: Optional[TickSnapshot]) -> Optional[float]:
    if snap is None:
        return None
    if pos.side == "LONG":
        return snap.buy1_price
    return snap.sell1_price


def take_profit_limit_price(pos: PositionState) -> float:
    tick_size = tick_size_for_1570(pos.entry_price)
    delta = pos.take_ticks * tick_size
    return pos.entry_price + delta if pos.side == "LONG" else pos.entry_price - delta


def take_profit_filled_ticks(pos: PositionState) -> float:
    if pos.entry_fill_price is None or pos.exit_fill_price is None:
        return float(pos.take_ticks)
    pnl_ticks = price_to_ticks(pos.exit_fill_price - pos.entry_fill_price, pos.entry_fill_price)
    return pnl_ticks if pos.side == "LONG" else -pnl_ticks


def take_profit_execution_config(config: dict[str, Any]) -> dict[str, Any]:
    cfg = config.get("take_profit_execution", {})
    return cfg if isinstance(cfg, dict) else {}


def take_profit_fallback_after_signal_sec(config: dict[str, Any]) -> float:
    cfg = take_profit_execution_config(config)
    try:
        wait_sec = float(cfg.get("fallback_market_after_signal_sec", 2.0))
    except Exception:
        wait_sec = 2.0
    return max(wait_sec, 0.0)


def cancel_pending_take_profit_order(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    pos: PositionState,
    context: dict[str, Any],
) -> tuple[bool, bool]:
    order_ids = list(pos.take_profit_order_ids or [])
    if not order_ids and pos.take_profit_order_id:
        order_ids = [pos.take_profit_order_id]
    if not order_ids:
        return True, False
    any_fail = False
    for order_id in order_ids:
        cancel_payload = {"OrderId": order_id}
        storage.log_structured("WARN", "TAKE_PROFIT_CANCEL_SENT", {**context, "order_id": order_id, "request_json": cancel_payload})
        try:
            cres = client.cancel_order(order_id, config["order_password"])
            storage.log_structured("WARN", "TAKE_PROFIT_CANCEL_RESPONSE", {**context, "order_id": order_id, "raw_response_json": cres})
        except Exception as e:
            ep = api_error_payload(e)
            any_fail = True
            storage.log_structured("WARN", "TAKE_PROFIT_CANCEL_FAIL", {**context, "order_id": order_id, **ep})
    pos.take_profit_order_id = None
    pos.take_profit_order_ids = []
    unlocked, filled, leaves, hold, available, positions = wait_for_position_unlocked_or_flat(
        client,
        config,
        pos.side,
        margin_trade_type=pos.margin_trade_type,
        timeout_sec=float(config.get("take_profit_cancel_verify_sec", 5.0)),
        managed_execution_ids=pos.managed_execution_ids,
    )
    if any_fail:
        storage.log_structured("WARN", "TAKE_PROFIT_CANCEL_PARTIAL_FAILED", {**context, "leaves_qty": leaves, "positions_json": positions})
    return unlocked, filled

def place_take_profit_limit_order(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    pos: PositionState,
    pred: PredictionSnapshot,
    status: MonitorStatus,
) -> LiveOrderResult:
    _ = client
    context = order_context(config, pos.side, pos, pred, status)
    storage.log_structured(
        "INFO",
        "TAKE_PROFIT_LIMIT_DISABLED_TRAILING_EXIT",
        {
            **context,
            "reason": "trailing_exit_replaces_fixed_take_profit",
            "trailing_trigger_ticks": pos.trailing_trigger_ticks,
            "trailing_floor_ticks": pos.trailing_floor_ticks,
            "trailing_width_ticks": pos.trailing_width_ticks,
            "internal_position": position_state_payload(pos),
        },
    )
    return LiveOrderResult(False, "TAKE_PROFIT_LIMIT_DISABLED_TRAILING_EXIT", recoverable=False)


def verify_position_after_entry_cancel(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    target_qty: int,
    storage: Storage,
    context: dict[str, Any],
    order_id: str,
    before_positions: Optional[list[dict[str, Any]]] = None,
) -> bool:
    positions = fetch_positions(client, config, storage, reason="ENTRY_CANCEL_VERIFY")
    margin_trade_type = margin_trade_type_for_side(config, side)
    total_qty, matching_qty = summarize_positions(positions, expected_side=side, expected_margin_trade_type=margin_trade_type)
    new_positions = new_managed_positions_from_diff(positions, before_positions, side, margin_trade_type) if before_positions is not None else []
    new_qty = sum(position_leaves_qty(p) for p in new_positions)
    ok = new_qty >= max(target_qty, 1) if before_positions is not None else matching_qty >= max(target_qty, 1)
    storage.log_structured(
        "INFO",
        "ENTRY_POSITION_AFTER_CANCEL",
        {
            **context,
            "order_id": order_id,
            "target_qty": target_qty,
            "total_leaves_qty": total_qty,
            "matching_leaves_qty": matching_qty,
            "new_matching_leaves_qty": new_qty,
            "before_positions_summary": positions_summary_for_log(before_positions or []),
            "new_positions_summary": positions_summary_for_log(new_positions),
            "raw_positions_json": positions,
        },
    )
    return ok


def execute_live_entry(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    storage: Storage,
    candidate: PositionState,
    pred: PredictionSnapshot,
    status: MonitorStatus,
    latest_snapshot: Optional[TickSnapshot] = None,
    target_total_qty_override: Optional[int] = None,
    qty_override: Optional[int] = None,
) -> LiveOrderResult:
    entry_exec = entry_execution_config(config)
    limit_entry = use_limit_entry(config)
    retries = max(int(config.get("live_retry_max", LIVE_RETRY_MAX)), 0)
    if limit_entry:
        retries = max(int(entry_exec.get("max_reprice_attempts", 0)), 0)
    target_qty = int(target_total_qty_override if target_total_qty_override is not None else config.get("entry_min_fill_qty", config["order_qty"]))
    timeout_sec = int(config.get("live_entry_timeout_sec", LIVE_ENTRY_TIMEOUT_SEC))
    if limit_entry:
        timeout_sec = max(float(entry_exec.get("timeout_sec", timeout_sec)), 0.5)
    context = {
        **order_context(config, side, candidate, pred, status),
        "entry_execution_mode": entry_exec.get("mode", "market"),
        "entry_limit_mode": entry_exec.get("limit_mode", ""),
    }

    last = LiveOrderResult(False, "ENTRY_UNKNOWN_ERROR")
    entry_before_positions = fetch_positions(client, config, storage, reason="ENTRY_BEFORE_POSITIONS")
    for attempt in range(retries + 1):
        limit_price = None
        front_order_type = None
        if limit_entry:
            limit_mode = str(entry_exec.get("limit_mode", "marketable_best"))
            if limit_mode not in {"marketable_best", "passive_best"}:
                limit_mode = "marketable_best"
            limit_price = entry_limit_price(side, latest_snapshot, limit_mode)
            front_order_type = 20
            if limit_price is None or limit_price <= 0:
                last = LiveOrderResult(False, "ENTRY_LIMIT_PRICE_UNAVAILABLE", recoverable=True)
                storage.log_structured(
                    "WARN",
                    "ENTRY_LIMIT_PRICE_UNAVAILABLE",
                    {
                        **context,
                        "attempt": attempt + 1,
                        "best_ask": latest_snapshot.sell1_price if latest_snapshot else None,
                        "best_bid": latest_snapshot.buy1_price if latest_snapshot else None,
                    },
                    mirror_message=f"side={side} limit_mode={limit_mode}",
                )
                return last
        payload = build_entry_order_payload(config, side, front_order_type=front_order_type, price=limit_price, qty_override=qty_override)
        storage.log_structured(
            "INFO",
            "ENTRY_ORDER_REQUEST",
            {
                **context,
                "attempt": attempt + 1,
                "request_json": payload,
                "limit_price": limit_price,
                "best_ask": latest_snapshot.sell1_price if latest_snapshot else None,
                "best_bid": latest_snapshot.buy1_price if latest_snapshot else None,
            },
        )
        try:
            res = client.send_order(payload)
        except Exception as e:
            ep = api_error_payload(e)
            code = str(ep.get("api_code") or "")
            event_type = "ENTRY_FAIL_100302" if code == "100302" else "ENTRY_ORDER_FAIL"
            msg = "ENTRY_REJECTED_100302" if code == "100302" else "ENTRY_RESTRICTED_100368" if code == "100368" else "ENTRY_SEND_ERROR"
            storage.log_structured("ERROR", event_type, {**context, "attempt": attempt + 1, **ep, "request_json": payload}, mirror_message=f"{msg}: {ep.get('raw_error')}")
            last = LiveOrderResult(False, f"{msg}: {ep.get('raw_error')}", api_code=code, api_message=str(ep.get("api_message") or ""), recoverable=(code in {"100302"}))
            if code in {"100302", "100368"}:
                return last
            continue
        order_id = str(res.get("OrderId") or res.get("OrderID") or "")
        storage.log_structured("INFO", "ENTRY_ORDER_RESPONSE", {**context, "attempt": attempt + 1, "order_id": order_id, "raw_response_json": res})
        if not order_id:
            last = LiveOrderResult(False, f"ENTRY_ORDER_ID_MISSING: {res}")
            continue
        if wait_for_position_qty(client, config, side, target_qty=max(target_qty, 1), timeout_sec=timeout_sec, comparator="ge", margin_trade_type=margin_trade_type_for_side(config, side)):
            return LiveOrderResult(True, order_id, order_id=order_id)
        last = LiveOrderResult(False, f"ENTRY_NOT_FILLED_TIMEOUT order_id={order_id}", order_id=order_id, recoverable=True)
        cancel_payload = {"OrderId": order_id}
        storage.log_structured("WARN", "CANCEL_ORDER_REQUEST", {**context, "order_id": order_id, "request_json": cancel_payload})
        try:
            cres = client.cancel_order(order_id, config["order_password"])
            storage.log_structured("WARN", "CANCEL_ORDER_RESPONSE", {**context, "order_id": order_id, "raw_response_json": cres})
            if bool(entry_exec.get("verify_position_after_cancel", True)) and verify_position_after_entry_cancel(
                client, config, side, target_qty, storage, context, order_id, before_positions=entry_before_positions
            ):
                return LiveOrderResult(True, order_id, order_id=order_id)
        except Exception as e:
            ep = api_error_payload(e)
            storage.log_structured("WARN", "CANCEL_ORDER_FAIL", {**context, "order_id": order_id, **ep}, mirror_message=f"ENTRY_CANCEL_FAIL order_id={order_id} Code={ep.get('api_code')} Message={ep.get('api_message')}")
            if str(ep.get("api_code") or "") == "43" and verify_position_after_entry_cancel(
                client, config, side, target_qty, storage, context, order_id, before_positions=entry_before_positions
            ):
                return LiveOrderResult(True, order_id, order_id=order_id)
            if bool(entry_exec.get("verify_position_after_cancel", True)) and verify_position_after_entry_cancel(
                client, config, side, target_qty, storage, context, order_id, before_positions=entry_before_positions
            ):
                return LiveOrderResult(True, order_id, order_id=order_id)
    return last


def close_position_groups_for_side(
    positions: list[dict[str, Any]],
    side: str,
    margin_trade_type: Optional[int] = None,
    available_only: bool = False,
    default_exchange: int = 0,
    managed_execution_ids: Optional[list[str]] = None,
    storage: Optional[Storage] = None,
) -> list[tuple[int, list[dict[str, Any]], int]]:
    managed_set = {str(x) for x in (managed_execution_ids or []) if str(x)}
    qty_by_exchange_hold_id: dict[int, dict[str, int]] = {}
    for p in positions:
        if not position_matches(p, side=side, margin_trade_type=margin_trade_type):
            continue
        hold_id = position_execution_id(p)
        if not hold_id:
            continue
        if managed_set and hold_id not in managed_set:
            if storage is not None:
                storage.log_structured("WARN", "UNMANAGED_POSITION_SKIPPED", {"ExecutionID": hold_id, "position": positions_summary_for_log([p])[0]})
            continue
        leaves = position_leaves_qty(p)
        hold = min(position_hold_qty(p), leaves)
        qty = position_available_qty(p) if available_only else leaves
        if qty <= 0:
            if storage is not None and leaves > 0 and hold > 0:
                storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_SKIP_LOCKED_POSITION", {"ExecutionID": hold_id, "LeavesQty": leaves, "HoldQty": hold, "AvailableQty": position_available_qty(p), "position": positions_summary_for_log([p])[0]})
            continue
        position_exchange = _to_int(p.get("Exchange"), default_exchange)
        if position_exchange <= 0:
            position_exchange = default_exchange
        qty_by_hold_id = qty_by_exchange_hold_id.setdefault(position_exchange, {})
        qty_by_hold_id[hold_id] = max(qty_by_hold_id.get(hold_id, 0), qty)

    groups: list[tuple[int, list[dict[str, Any]], int]] = []
    for position_exchange in sorted(qty_by_exchange_hold_id):
        qty_by_hold_id = qty_by_exchange_hold_id[position_exchange]
        close_positions = [{"HoldID": hold_id, "Qty": qty} for hold_id, qty in qty_by_hold_id.items()]
        total_qty = sum(qty_by_hold_id.values())
        groups.append((position_exchange, close_positions, total_qty))
    return groups




def close_position_groups_from_managed_state(
    pos: PositionState,
    config: dict[str, Any],
    default_exchange: Optional[int] = None,
) -> tuple[Optional[list[tuple[int, list[dict[str, Any]], int]]], str]:
    """Build ClosePositions from bot-managed cached execution ids without a /positions round trip.

    This fast path is intentionally conservative: if we cannot determine per-HoldID
    quantity from managed_close_positions (or unambiguously from filled_qty for a
    single managed id), callers must fall back to a fresh positions fetch.
    """
    managed_ids = [str(x) for x in (pos.managed_execution_ids or []) if str(x)]
    if not managed_ids:
        return None, "MANAGED_EXECUTION_IDS_MISSING"
    exchange = int(default_exchange if default_exchange is not None else exit_exchange(config))
    qty_by_hold_id: dict[str, int] = {}
    exchange_by_hold_id: dict[str, int] = {}
    for row in pos.managed_close_positions or []:
        if not isinstance(row, dict):
            continue
        hold_id = str(row.get("ExecutionID") or row.get("HoldID") or "")
        if not hold_id or hold_id not in managed_ids:
            continue
        raw_qty = row.get("AvailableQty")
        if raw_qty is None:
            raw_qty = row.get("LeavesQty")
        if raw_qty is None:
            raw_qty = row.get("Qty")
        qty = _to_int(raw_qty, 0)
        if qty <= 0:
            continue
        qty_by_hold_id[hold_id] = max(qty_by_hold_id.get(hold_id, 0), qty)
        row_exchange = _to_int(row.get("Exchange"), exchange)
        exchange_by_hold_id[hold_id] = row_exchange if row_exchange > 0 else exchange

    missing_ids = [hold_id for hold_id in managed_ids if qty_by_hold_id.get(hold_id, 0) <= 0]
    if missing_ids:
        if len(managed_ids) == 1 and pos.filled_qty > 0:
            hold_id = managed_ids[0]
            qty_by_hold_id[hold_id] = int(pos.filled_qty)
            exchange_by_hold_id[hold_id] = exchange_by_hold_id.get(hold_id, exchange)
            missing_ids = []
        else:
            return None, f"MANAGED_QTY_UNKNOWN:{','.join(missing_ids)}"

    qty_by_exchange: dict[int, dict[str, int]] = {}
    for hold_id, qty in qty_by_hold_id.items():
        if qty <= 0:
            continue
        qty_by_exchange.setdefault(exchange_by_hold_id.get(hold_id, exchange), {})[hold_id] = qty
    groups: list[tuple[int, list[dict[str, Any]], int]] = []
    for position_exchange in sorted(qty_by_exchange):
        qty_map = qty_by_exchange[position_exchange]
        close_positions = [{"HoldID": hold_id, "Qty": qty} for hold_id, qty in qty_map.items()]
        groups.append((position_exchange, close_positions, sum(qty_map.values())))
    if not groups:
        return None, "NO_MANAGED_CLOSE_QTY"
    total_group_qty = sum(group_total_qty for _, _, group_total_qty in groups)
    if pos.filled_qty > 0 and total_group_qty != int(pos.filled_qty):
        return None, "FAST_PATH_QTY_MISMATCH"
    return groups, "MANAGED_STATE"

def close_positions_for_side(
    positions: list[dict[str, Any]],
    side: str,
    margin_trade_type: Optional[int] = None,
    available_only: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    close_positions: list[dict[str, Any]] = []
    total_qty = 0
    for _, group_close_positions, group_total_qty in close_position_groups_for_side(
        positions,
        side,
        margin_trade_type=margin_trade_type,
        available_only=available_only,
    ):
        close_positions.extend(group_close_positions)
        total_qty += group_total_qty
    return close_positions, total_qty


def execute_live_exit(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    storage: Storage,
    pos: PositionState,
    pred: Optional[PredictionSnapshot],
    status: MonitorStatus,
    latest_snapshot: Optional[TickSnapshot] = None,
    force_marketable_limit: bool = False,
    force_market_order: bool = False,
    exit_signal_ts: Optional[datetime] = None,
    signal_price: Optional[float] = None,
    pnl_ticks: Optional[float] = None,
    ma5_exit_context: Optional[dict[str, Any]] = None,
) -> LiveOrderResult:
    exit_exec = config.get("exit_execution", {}) if isinstance(config.get("exit_execution", {}), dict) else {}
    retries = max(int(exit_exec.get("max_reprice_attempts", config.get("live_retry_max", LIVE_RETRY_MAX))), 0)
    timeout_sec = int(config.get("live_exit_timeout_sec", LIVE_EXIT_TIMEOUT_SEC))
    context = order_context(config, side, pos, pred, status)
    decision_ts = exit_signal_ts or now_jst()
    last = LiveOrderResult(False, "EXIT_UNKNOWN_ERROR", recoverable=True)

    fast_path_groups: Optional[list[tuple[int, list[dict[str, Any]], int]]] = None
    fast_path_reason = ""
    fast_exit_available = bool(
        pos.managed_execution_ids
        and pos.filled_qty > 0
        and status.live_state == "OPEN"
        and pos.exit_order_id is None
        and not status.pending_exit
        and not force_market_order
    )
    if fast_exit_available:
        fast_path_groups, fast_path_reason = close_position_groups_from_managed_state(pos, config, default_exchange=exit_exchange(config))
        if fast_path_groups:
            storage.log_structured(
                "INFO",
                "EXIT_FAST_PATH_CLOSE_POSITIONS_USED",
                {
                    **context,
                    "decision_ts": decision_ts.isoformat(),
                    "managed_execution_ids": list(pos.managed_execution_ids or []),
                    "managed_close_positions": pos.managed_close_positions,
                    "close_position_groups": fast_path_groups,
                    "total_qty": sum(group_total_qty for _, _, group_total_qty in fast_path_groups),
                    "fast_path_reason": fast_path_reason,
                },
            )
        else:
            storage.log_structured(
                "WARN",
                "EXIT_FAST_PATH_FALLBACK_TO_POSITIONS_FETCH",
                {
                    **context,
                    "decision_ts": decision_ts.isoformat(),
                    "managed_execution_ids": list(pos.managed_execution_ids or []),
                    "managed_close_positions": pos.managed_close_positions,
                    "filled_qty": pos.filled_qty,
                    "fallback_reason": fast_path_reason,
                },
            )

    if fast_path_groups is None:
        if not verify_position_before_exit(
            client,
            config,
            storage,
            status,
            pos,
            "FORCE_MARKET_ORDER" if force_market_order else "LIVE_EXIT",
            now_jst(),
        ):
            if status.open_position is None and status.live_state == "FLAT":
                return LiveOrderResult(True, "POSITION_ALREADY_CLOSED")
            return LiveOrderResult(False, "POSITION_VERIFY_FAILED_BEFORE_EXIT", recoverable=True)
    else:
        storage.log_structured("INFO", "EXIT_POSITION_VERIFY_FAST_PATH", {**context, "decision_ts": decision_ts.isoformat(), "reason": "managed_execution_ids_available", "managed_execution_ids": list(pos.managed_execution_ids or [])})

    for attempt in range(retries + 1):
        positions: list[dict[str, Any]] = []
        if fast_path_groups is not None and attempt == 0:
            close_position_groups = fast_path_groups
            total_qty = sum(group_total_qty for _, _, group_total_qty in close_position_groups)
            leaves_qty = total_qty
            hold_qty = 0
            available_qty = total_qty
        else:
            if fast_path_groups is not None and attempt > 0:
                storage.log_structured("WARN", "EXIT_FAST_PATH_FALLBACK_TO_POSITIONS_FETCH", {**context, "decision_ts": decision_ts.isoformat(), "attempt": attempt + 1, "fallback_reason": "REPRICE_AFTER_FAST_PATH_TIMEOUT"})
            positions = fetch_positions(client, config, storage, reason="EXIT_BUILD_CLOSE_POSITIONS")
            if force_market_order:
                storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_POSITIONS_SNAPSHOT", {**context, "positions_summary": positions_summary_for_log(positions), "managed_execution_ids": list(pos.managed_execution_ids or [])})
            if not pos.managed_execution_ids and not bool(config.get("allow_unmanaged_force_close", False)):
                storage.log_structured("ERROR", "MANAGED_POSITION_IDS_MISSING", {**context, "positions_summary": positions_summary_for_log(positions), "internal_position": position_state_payload(pos)})
                return LiveOrderResult(False, "MANAGED_POSITION_IDS_MISSING", recoverable=True)
            close_position_groups = close_position_groups_for_side(
                positions,
                side,
                margin_trade_type=pos.margin_trade_type,
                available_only=True,
                default_exchange=exit_exchange(config),
                managed_execution_ids=pos.managed_execution_ids,
                storage=storage,
            )
            total_qty = sum(group_total_qty for _, _, group_total_qty in close_position_groups)
            if force_market_order:
                storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_AVAILABLE_CLOSE_POSITIONS", {**context, "close_position_groups": close_position_groups, "total_qty": total_qty})
            managed_positions = managed_positions_for(pos, positions) if pos.managed_execution_ids else positions
            leaves_qty, hold_qty, available_qty = position_quantities(managed_positions)
            if not positions:
                return LiveOrderResult(False, "NO_POSITIONS_FOR_EXIT", recoverable=True)
            if leaves_qty <= 0:
                storage.log_structured("INFO", "POSITION_ALREADY_CLOSED", {**context, "managed_execution_ids": list(pos.managed_execution_ids or []), "positions_summary": positions_summary_for_log(positions)})
                status.open_position = None
                status.live_state = "FLAT"
                return LiveOrderResult(True, "POSITION_ALREADY_CLOSED")
        if total_qty <= 0 or not close_position_groups:
            storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_WAIT_UNLOCK", {**context, "leaves_qty": leaves_qty, "hold_qty": hold_qty, "available_qty": available_qty, "positions_summary": positions_summary_for_log(positions)})
            return LiveOrderResult(
                False,
                f"NO_AVAILABLE_OPEN_POSITION leaves={leaves_qty} hold={hold_qty} available={available_qty}",
                recoverable=True,
            )

        close_signature = safe_json({"side": side, "margin_trade_type": pos.margin_trade_type, "groups": close_position_groups, "force_market_order": force_market_order})
        if status.failed_close_signature == close_signature and status.failed_close_signature_ts is not None:
            if (now_jst() - status.failed_close_signature_ts).total_seconds() < 30:
                storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_FAILED_RETRY", {**context, "reason": "BACKOFF_SAME_CLOSE_SIGNATURE", "close_signature": close_signature, "failed_close_signature_ts": status.failed_close_signature_ts.isoformat(), "positions_summary": positions_summary_for_log(positions)})
                return LiveOrderResult(False, "CLOSE_SIGNATURE_BACKOFF", recoverable=True)

        initial_leaves_qty = leaves_qty
        order_ids: list[str] = []
        for position_exchange, close_positions, group_total_qty in close_position_groups:
            if force_market_order:
                front_order_type = 10
                limit_price = 0.0
                exit_order_mode = "market_1520_force_close"
            elif force_marketable_limit or pos.strategy == "RSI9":
                front_order_type = 20
                refreshed_snapshot = latest_snapshot
                try:
                    board_fetch_start_ts = now_jst()
                    raw_board = client.get_board(str(config.get("symbol", SYMBOL_DEFAULT)), int(config.get("exchange", EXCHANGE_DEFAULT)))
                    board_fetch_ts = now_jst()
                    refreshed_snapshot = extract_snapshot(raw_board)
                    latest_snapshot = refreshed_snapshot
                except Exception as e:
                    board_fetch_ts = now_jst()
                    storage.log("WARN", "EXIT_BOARD_REFRESH_FAILED", f"attempt={attempt+1} side={side} strategy={pos.strategy} error={e}")
                limit_price = marketable_exit_limit_price(pos, refreshed_snapshot)
                exit_order_mode = "marketable_limit"
                if limit_price is None or limit_price <= 0:
                    return LiveOrderResult(False, "MARKETABLE_EXIT_LIMIT_PRICE_UNAVAILABLE", recoverable=True)
                storage.log_structured(
                    "INFO",
                    "EXIT_BOARD_SNAPSHOT_FOR_ORDER",
                    {
                        **context,
                        "decision_ts": decision_ts.isoformat(),
                        "board_fetch_ts": board_fetch_ts.isoformat(),
                        "side": side,
                        "best_bid": refreshed_snapshot.buy1_price if refreshed_snapshot else None,
                        "best_ask": refreshed_snapshot.sell1_price if refreshed_snapshot else None,
                        "current_price": refreshed_snapshot.price if refreshed_snapshot else None,
                        "chosen_limit_price": limit_price,
                        "limit_price_reason": exit_order_mode,
                        "decision_to_board_ms": (board_fetch_ts - decision_ts).total_seconds() * 1000.0,
                    },
                )
            else:
                front_order_type = None
                limit_price = None
                exit_order_mode = "config_default"
            payload = build_exit_order_payload(
                config,
                side,
                close_positions=close_positions,
                qty=group_total_qty,
                front_order_type=front_order_type,
                price=limit_price,
                margin_trade_type=pos.margin_trade_type,
                exchange=position_exchange,
            )
            storage.log_structured(
                "INFO",
                "FORCE_MARKET_CLOSE_1520_ORDER_SENT" if force_market_order else "EXIT_ORDER_REQUEST",
                {
                    **context,
                    "attempt": attempt + 1,
                    "position_exchange": position_exchange,
                    "exit_order_mode": exit_order_mode,
                    "limit_price": limit_price,
                    "force_market_order": force_market_order,
                    "force_marketable_limit": force_marketable_limit,
                    "request_json": payload,
                    "positions_json": positions,
                },
            )
            try:
                order_send_ts = now_jst()
                res = client.send_order(payload)
                order_response_ts = now_jst()
            except Exception as e:
                order_response_ts = now_jst()
                ep = api_error_payload(e)
                code = str(ep.get("api_code") or "")
                storage.log_structured(
                    "ERROR",
                    "EXIT_ORDER_FAIL",
                    {
                        **context,
                        "attempt": attempt + 1,
                        "position_exchange": position_exchange,
                        **ep,
                        "request_json": payload,
                        "positions_json": positions,
                    },
                    mirror_message=f"EXIT_SEND_ERROR Code={code} Message={ep.get('api_message')}",
                )
                if code == "8":
                    status.failed_close_signature = close_signature
                    status.failed_close_signature_ts = now_jst()
                return LiveOrderResult(False, f"EXIT_SEND_ERROR Code={code} Message={ep.get('api_message')}: {ep.get('raw_error')}", api_code=code, api_message=str(ep.get("api_message") or ""), recoverable=(code == "8"))
            order_id = str(res.get("OrderId") or res.get("OrderID") or "")
            storage.log_structured(
                "INFO",
                "EXIT_ORDER_SEND_TIMING",
                {
                    **context,
                    "decision_ts": decision_ts.isoformat(),
                    "order_send_ts": order_send_ts.isoformat(),
                    "order_response_ts": order_response_ts.isoformat(),
                    "decision_to_send_ms": (order_send_ts - decision_ts).total_seconds() * 1000.0,
                    "send_to_response_ms": (order_response_ts - order_send_ts).total_seconds() * 1000.0,
                    "decision_to_response_ms": (order_response_ts - decision_ts).total_seconds() * 1000.0,
                    "order_id": order_id,
                    "side": side,
                    "exit_reason": ma5_exit_context.get("exit_reason") if isinstance(ma5_exit_context, dict) else None,
                    "signal_price": signal_price,
                    "limit_price": limit_price,
                },
            )
            storage.log_structured(
                "INFO",
                "EXIT_ORDER_RESPONSE",
                {**context, "attempt": attempt + 1, "position_exchange": position_exchange, "order_id": order_id, "raw_response_json": res},
            )
            if not order_id:
                last = LiveOrderResult(False, f"EXIT_ORDER_ID_MISSING: {res}", recoverable=True)
                break
            order_ids.append(order_id)
            last = LiveOrderResult(False, "EXIT_ORDER_SENT", order_id=order_id, limit_price=limit_price, order_send_ts=order_send_ts, order_response_ts=order_response_ts, recoverable=True)
        if not order_ids:
            continue
        combined_order_id = ",".join(order_ids)
        if wait_for_managed_position_qty(client, config, pos, target_qty=0, timeout_sec=timeout_sec, comparator="eq", storage=storage, reason="EXIT_WAIT_MANAGED_FLAT"):
            fill_price, fill_source = resolve_actual_fill_price(client, storage, combined_order_id, side, qty=initial_leaves_qty, signal_price=signal_price, limit_price=last.limit_price)
            return LiveOrderResult(True, combined_order_id, order_id=combined_order_id, limit_price=last.limit_price, order_send_ts=last.order_send_ts, order_response_ts=last.order_response_ts, actual_fill_price=fill_price, fill_source=fill_source)
        last = LiveOrderResult(False, f"EXIT_NOT_FILLED_TIMEOUT order_id={combined_order_id}", order_id=combined_order_id, recoverable=True)
        for order_id in order_ids:
            cancel_payload = {"OrderId": order_id}
            storage.log_structured("WARN", "CANCEL_ORDER_REQUEST", {**context, "order_id": order_id, "request_json": cancel_payload})
            try:
                cres = client.cancel_order(order_id, config["order_password"])
                storage.log_structured("WARN", "CANCEL_ORDER_RESPONSE", {**context, "order_id": order_id, "raw_response_json": cres})
            except Exception as e:
                ep = api_error_payload(e)
                code = str(ep.get("api_code") or "")
                storage.log_structured("WARN", "CANCEL_ORDER_FAIL", {**context, "order_id": order_id, **ep}, mirror_message=f"EXIT_CANCEL_FAIL order_id={order_id} Code={code} Message={ep.get('api_message')}")
                if code == "43":
                    if wait_for_managed_position_qty(client, config, pos, target_qty=0, timeout_sec=timeout_sec, comparator="eq", storage=storage, reason="EXIT_CANCEL_WAIT_MANAGED_FLAT"):
                        fill_price, fill_source = resolve_actual_fill_price(client, storage, order_id, side, qty=initial_leaves_qty, signal_price=signal_price, limit_price=last.limit_price)
                        return LiveOrderResult(True, order_id, order_id=order_id, api_code=code, api_message=str(ep.get("api_message") or ""), limit_price=last.limit_price, order_send_ts=last.order_send_ts, order_response_ts=last.order_response_ts, actual_fill_price=fill_price, fill_source=fill_source)
                    last = LiveOrderResult(False, f"EXIT_CANCEL_ALREADY_FILLED_VERIFY_POSITION order_id={order_id}", order_id=order_id, api_code=code, api_message=str(ep.get("api_message") or ""), recoverable=True)
                    break
        remaining_positions = fetch_positions(client, config, storage, reason="EXIT_REPRICE_REMAINING_QTY")
        remaining_managed_positions = managed_positions_for(pos, remaining_positions) if pos.managed_execution_ids else remaining_positions
        remaining_leaves_qty, _, _ = position_quantities(remaining_managed_positions)
        storage.log_structured(
            "INFO",
            "EXIT_REPRICE_LOOP",
            {**context, "attempt": attempt + 1, "initial_leaves_qty": initial_leaves_qty, "remaining_leaves_qty": remaining_leaves_qty, "order_ids": order_ids},
        )
        if remaining_leaves_qty <= 0:
            fill_price, fill_source = resolve_actual_fill_price(client, storage, combined_order_id, side, qty=initial_leaves_qty, signal_price=signal_price, limit_price=last.limit_price)
            return LiveOrderResult(True, combined_order_id, order_id=combined_order_id, limit_price=last.limit_price, order_send_ts=last.order_send_ts, order_response_ts=last.order_response_ts, actual_fill_price=fill_price, fill_source=fill_source)
    return last


def force_close_open_position(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    status: MonitorStatus,
    reason: str,
    ts: datetime,
    last_pred: Optional[PredictionSnapshot],
    latest_snapshot: Optional[TickSnapshot] = None,
    use_market_order: bool = False,
) -> None:
    pos = status.open_position
    if pos is None:
        return
    if not config.get("live_mode"):
        storage.log("WARN", "FORCE_EXIT_PAPER_CLEAR", f"reason={reason} side={pos.side}")
        status.open_position = None
        status.live_state = "FLAT"
        return

    if latest_snapshot is None and not use_market_order:
        try:
            raw_board = client.get_board(str(config.get("symbol", SYMBOL_DEFAULT)), int(config.get("exchange", EXCHANGE_DEFAULT)))
            latest_snapshot = extract_snapshot(raw_board)
            storage.log("INFO", "FORCE_CLOSE_SNAPSHOT_REFRESHED", f"reason={reason} side={pos.side} strategy={pos.strategy}")
        except Exception as e:
            storage.log("ERROR", "FORCE_CLOSE_SNAPSHOT_REFRESH_FAILED", f"reason={reason} side={pos.side} strategy={pos.strategy} error={e}")

    context = order_context(config, pos.side, pos, last_pred, status)
    context["force_exit_reason"] = reason
    storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_START", {**context, "reason": reason, "force_close_state": status.force_close_state, "internal_position": position_state_payload(pos)})
    if pos.take_profit_order_id:
        storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_CANCEL_TP_SENT", {**context, "order_id": pos.take_profit_order_id})
        cancel_ok, filled_during_cancel = cancel_pending_take_profit_order(client, config, storage, pos, context)
        storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_CANCEL_TP_RESPONSE", {**context, "order_id": pos.take_profit_order_id, "cancel_ok": cancel_ok, "filled_during_cancel": filled_during_cancel})
        if filled_during_cancel:
            status.exit_fail_count = 0
            status.live_state = "FLAT"
            status.open_position = None
            storage.log("WARN", "FORCE_EXIT_TAKE_PROFIT_FILLED", f"reason={reason} side={pos.side}")
            return
        if not cancel_ok:
            status.live_state = "RECOVERING"
            status.force_close_state = "FORCE_CLOSE_WAIT_UNLOCK"
            storage.log("ERROR", "FORCE_EXIT_CANCEL_TAKE_PROFIT_FAIL", f"reason={reason} side={pos.side}; continue to available market close")

    status.live_state = "EXIT_SENT"
    status.force_close_state = "FORCE_CLOSE_PENDING"
    result = execute_live_exit(
        client,
        config,
        pos.side,
        storage,
        pos,
        last_pred,
        status,
        latest_snapshot=latest_snapshot,
        force_marketable_limit=(not use_market_order),
        force_market_order=use_market_order,
    )
    if result.ok:
        pos.exit_order_id = result.order_id
        status.exit_fail_count = 0
        status.live_state = "FLAT"
        status.open_position = None
        status.force_market_close_sent = True
        status.force_close_state = "FORCE_CLOSE_CONFIRMED_FLAT"
        storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_CONFIRMED_FLAT", {**context, "reason": reason, "order_id": result.order_id})
        storage.log("WARN", "FORCE_EXIT_OK", f"reason={reason} side={pos.side} order_id={result.order_id}")
        return

    status.exit_fail_count += 1
    status.last_error_code = result.api_code
    status.last_error_message = result.message
    status.live_state = "EXIT_VERIFYING"
    status.force_close_state = "FORCE_CLOSE_FAILED_RETRY"
    storage.log("ERROR", "FORCE_EXIT_FAIL", f"reason={reason} side={pos.side} message={result.message}")
    storage.log_structured(
        "ERROR",
        "FORCE_CLOSE_FAILED",
        {
            "reason": reason,
            "side": pos.side,
            "strategy": pos.strategy,
            "message": result.message,
            "api_code": result.api_code,
            "internal_position": position_state_payload(pos),
        },
        mirror_message=f"reason={reason} side={pos.side} message={result.message}",
    )
    if not use_market_order and status.open_position is not None:
        storage.log("ERROR", "MANUAL_POSITION_CHECK_REQUIRED", f"reason={reason} side={pos.side} strategy={pos.strategy} message={result.message}")
    rec = reconcile_live_position(
        client,
        config,
        status,
        storage,
        expected_side=pos.side,
        reason=f"FORCE_EXIT_FAIL:{reason}:{result.message}",
        ts=ts,
        expected_margin_trade_type=pos.margin_trade_type,
    )
    if rec.total_leaves_qty == 0 and status.open_position is None:
        storage.log("WARN", "FORCE_EXIT_SYNC_FLAT", f"reason={reason}")
    elif rec.message == "MATCHED_OPEN":
        status.live_state = "OPEN"
    else:
        status.live_state = "RECOVERING"


def reconcile_live_position(
    client: KabuApiClient,
    config: dict[str, Any],
    status: MonitorStatus,
    storage: Storage,
    expected_side: Optional[str],
    reason: str,
    ts: datetime,
    expected_margin_trade_type: Optional[int] = None,
) -> ReconcileResult:
    before = {"live_state": status.live_state, "internal_position": position_state_payload(status.open_position)}
    storage.log_structured("INFO", "POSITION_RECONCILE_START", {"reason": reason, "expected_side": expected_side, "before_state": before})
    try:
        positions = fetch_positions(client, config, storage, reason=reason)
    except Exception as e:
        ep = api_error_payload(e)
        status.live_state = "RECOVERING"
        status.last_error_code = str(ep.get("api_code") or "")
        status.last_error_message = str(ep.get("raw_error") or "")
        storage.log_structured("ERROR", "RECOVERY_ENTER", {"reason": reason, "before_state": before, **ep}, mirror_message=f"position reconcile failed: {ep.get('raw_error')}")
        return ReconcileResult(False, status.live_state, "POSITION_RECONCILE_ERROR")

    expected_margin = expected_margin_trade_type
    if expected_margin is None and status.open_position is not None:
        expected_margin = status.open_position.margin_trade_type
    managed_execution_ids = list(status.open_position.managed_execution_ids or []) if status.open_position is not None else []
    scoped_positions: list[dict[str, Any]] = []
    if status.open_position is not None and managed_execution_ids:
        scoped_positions = managed_positions_for(status.open_position, positions)
        scoped_total_qty, scoped_hold_qty, scoped_available_qty = position_quantities(scoped_positions)
        total_qty = scoped_total_qty
        matching_qty = scoped_total_qty
    else:
        total_qty, matching_qty = summarize_positions(
            positions,
            expected_side or (status.open_position.side if status.open_position else None),
            expected_margin_trade_type=expected_margin,
        )
        scoped_hold_qty = 0
        scoped_available_qty = 0
    payload_base = {
        "reason": reason,
        "expected_side": expected_side,
        "expected_margin_trade_type": expected_margin,
        "managed_execution_ids": managed_execution_ids,
        "scoped_positions_summary": positions_summary_for_log(scoped_positions),
        "scoped_hold_qty": scoped_hold_qty,
        "scoped_available_qty": scoped_available_qty,
        "positions_count": len(positions),
        "total_leaves_qty": total_qty,
        "matching_leaves_qty": matching_qty,
        "raw_positions_json": positions,
        "positions_json": positions,
        "before_state": before,
    }

    if total_qty == 0:
        if status.open_position is not None:
            status.open_position = None
            status.live_state = "FLAT"
            status.exit_fail_count = 0
            cooldown = max(int(config.get("recovery_cooldown_sec", RECOVERY_COOLDOWN_SEC)), 0)
            status.recovery_until = ts + timedelta(seconds=cooldown) if cooldown else None
            after = {"live_state": status.live_state, "internal_position": position_state_payload(status.open_position), "recovery_until": status.recovery_until}
            storage.log_structured("WARN", "INTERNAL_STATE_SYNC", {**payload_base, "after_state": after, "action": "STALE_INTERNAL_POSITION_CLEARED"}, mirror_message="STALE_INTERNAL_POSITION_CLEARED actual_qty=0")
            storage.log_structured("WARN", "INTERNAL_POSITION_CLEARED_AFTER_CONFIRMED_FLAT", {**payload_base, "after_state": after})
            return ReconcileResult(True, status.live_state, "STALE_INTERNAL_POSITION_CLEARED", total_qty, matching_qty, positions)
        status.live_state = "FLAT"
        storage.log_structured("INFO", "POSITION_RECONCILE_RESULT", {**payload_base, "after_state": {"live_state": status.live_state}, "action": "FLAT_CONFIRMED"})
        return ReconcileResult(True, status.live_state, "FLAT_CONFIRMED", total_qty, matching_qty, positions)

    if status.open_position is None:
        status.live_state = "MANUAL_POSITION_CHECK_REQUIRED"
        status.pending_entry_side = None
        status.pending_entry_ts = None
        status.pending_add = False
        status.recovery_until = ts + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
        after = {
            "live_state": status.live_state,
            "recovery_until": status.recovery_until.isoformat() if status.recovery_until else None,
            "pending_entry_side": status.pending_entry_side,
            "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None,
        }
        storage.log_structured("ERROR", "ORPHAN_API_POSITION_BLOCK_ENTRY", {**payload_base, "after_state": after, "action": "ORPHAN_API_POSITION"}, mirror_message="ORPHAN_API_POSITION actual position exists while internal state is FLAT")
        storage.log_structured("ERROR", "RECOVERY_ENTER", {**payload_base, "after_state": after, "action": "ORPHAN_API_POSITION"}, mirror_message="ORPHAN_API_POSITION actual position exists while internal state is FLAT")
        return ReconcileResult(False, status.live_state, "ORPHAN_API_POSITION", total_qty, matching_qty, positions)

    if matching_qty <= 0:
        status.live_state = "MANUAL_POSITION_CHECK_REQUIRED"
        status.pending_entry_side = None
        status.pending_entry_ts = None
        status.pending_add = False
        status.recovery_until = ts + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
        after = {
            "live_state": status.live_state,
            "recovery_until": status.recovery_until.isoformat() if status.recovery_until else None,
            "pending_entry_side": status.pending_entry_side,
            "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None,
        }
        storage.log_structured("ERROR", "SIDE_MISMATCH_BLOCK_ENTRY", {**payload_base, "after_state": after, "action": "SIDE_MISMATCH"}, mirror_message="SIDE_MISMATCH actual position side does not match internal state")
        storage.log_structured("ERROR", "RECOVERY_ENTER", {**payload_base, "after_state": after, "action": "SIDE_MISMATCH"}, mirror_message="SIDE_MISMATCH actual position side does not match internal state")
        return ReconcileResult(False, status.live_state, "SIDE_MISMATCH", total_qty, matching_qty, positions)

    status.live_state = "OPEN"
    status.recovery_until = None
    storage.log_structured("INFO", "RECOVERY_EXIT", {**payload_base, "after_state": {"live_state": status.live_state}, "action": "MATCHED_OPEN"})
    return ReconcileResult(True, status.live_state, "MATCHED_OPEN", total_qty, matching_qty, positions)




def _split_order_ids(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def mark_entry_not_filled_resume_flat(
    status: MonitorStatus,
    storage: Storage,
    config: dict[str, Any],
    side: str,
    ts: datetime,
    order_id: str,
    pred: PredictionSnapshot,
    verify_result: Optional[EntryPositionVerifyResult] = None,
) -> None:
    cooldown_sec = max(int(config.get("entry_not_filled_cooldown_sec", 30)), 0)
    cooldown_until = ts + timedelta(seconds=cooldown_sec) if cooldown_sec else None
    status.open_position = None
    status.live_state = "FLAT"
    status.recovery_until = None
    status.pending_entry_side = None
    status.pending_entry_ts = None
    status.pending_add = False
    status.pending_exit = False
    if cooldown_until is not None:
        status.reentry_block_until_by_side[side] = cooldown_until
    after_state = {
        "live_state": status.live_state,
        "recovery_until": status.recovery_until,
        "pending_entry_side": status.pending_entry_side,
        "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None,
        "pending_add": status.pending_add,
        "pending_exit": status.pending_exit,
    }
    storage.log_structured(
        "WARN",
        "ENTRY_NOT_FILLED_RESUME_FLAT",
        {
            "ts": ts.isoformat(),
            "order_id": order_id,
            "side": side,
            "expected_qty": int(config.get("order_qty", 2)),
            "actual_qty": 0,
            "pred_signal": pred.signal,
            "pred_reason_1": pred.reason_1,
            "pred_reason_2": pred.reason_2,
            "pred_reason_3": pred.reason_3,
            "after_state": after_state,
            "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
            "verified_flat": verify_result.verified_flat if verify_result else True,
            "verify_reason": verify_result.reason if verify_result else "VERIFIED_FLAT",
            "positions_count": verify_result.positions_count if verify_result else 0,
            "positions_summary": verify_result.positions_summary if verify_result else [],
        },
    )


def mark_entry_verify_uncertain_keep_recovering(
    status: MonitorStatus,
    storage: Storage,
    side: str,
    ts: datetime,
    order_id: str,
    pred: PredictionSnapshot,
    verify_result: EntryPositionVerifyResult,
) -> None:
    status.open_position = None
    status.live_state = "RECOVERING"
    status.recovery_until = ts + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
    status.pending_entry_side = None
    status.pending_entry_ts = None
    status.pending_add = False
    status.pending_exit = False
    payload = {
        "ts": ts.isoformat(),
        "order_id": order_id,
        "side": side,
        "actual_qty": verify_result.matching_qty,
        "total_qty": verify_result.total_qty,
        "verified_flat": verify_result.verified_flat,
        "verify_error": verify_result.verify_error,
        "verify_reason": verify_result.reason,
        "positions_count": verify_result.positions_count,
        "positions_summary": verify_result.positions_summary,
        "pred_signal": pred.signal,
        "pred_reason_1": pred.reason_1,
        "pred_reason_2": pred.reason_2,
        "pred_reason_3": pred.reason_3,
        "after_state": {
            "live_state": status.live_state,
            "recovery_until": status.recovery_until.isoformat() if status.recovery_until else None,
            "pending_entry_side": status.pending_entry_side,
            "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None,
            "pending_add": status.pending_add,
            "pending_exit": status.pending_exit,
        },
    }
    storage.log_structured(
        "WARN",
        "ENTRY_VERIFY_UNCERTAIN_KEEP_RECOVERING",
        payload,
        mirror_message=f"side={side} reason={verify_result.reason}; keep RECOVERING",
    )



def handle_entry_not_filled_without_internal_position(
    status: MonitorStatus,
    storage: Storage,
    config: dict[str, Any],
    side: str,
    ts: datetime,
    order_id: str,
    pred: PredictionSnapshot,
    verify_result: EntryPositionVerifyResult,
) -> None:
    base_payload = {
        "ts": ts.isoformat(),
        "order_id": order_id,
        "expected_side": side,
        "expected_qty": int(config.get("order_qty", 2)),
        "actual_qty": verify_result.matching_qty,
        "total_qty": verify_result.total_qty,
        "verified_flat": verify_result.verified_flat,
        "verify_error": verify_result.verify_error,
        "verify_reason": verify_result.reason,
        "positions_count": verify_result.positions_count,
        "positions_summary": verify_result.positions_summary,
        "pred_signal": pred.signal,
        "pred_reason_1": pred.reason_1,
        "pred_reason_2": pred.reason_2,
        "pred_reason_3": pred.reason_3,
    }
    if verify_result.verified_flat:
        mark_entry_not_filled_resume_flat(status, storage, config, side, ts, order_id, pred, verify_result)
        storage.log_structured(
            "WARN",
            "ENTRY_NOT_FILLED_NO_INTERNAL_POSITION",
            {
                **base_payload,
                "actual_qty": 0,
                "after_state": {
                    "live_state": status.live_state,
                    "recovery_until": status.recovery_until,
                    "pending_entry_side": status.pending_entry_side,
                    "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None,
                    "pending_add": status.pending_add,
                    "pending_exit": status.pending_exit,
                },
            },
        )
        storage.log("WARN", "ENTRY_NOT_FILLED", f"side={side} filled_qty=0")
    else:
        mark_entry_verify_uncertain_keep_recovering(status, storage, side, ts, order_id, pred, verify_result)
        storage.log_structured(
            "WARN",
            "ENTRY_NOT_FILLED_VERIFY_UNCERTAIN",
            {
                **base_payload,
                "after_state": {
                    "live_state": status.live_state,
                    "recovery_until": status.recovery_until.isoformat() if status.recovery_until else None,
                    "pending_entry_side": status.pending_entry_side,
                    "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None,
                    "pending_add": status.pending_add,
                    "pending_exit": status.pending_exit,
                },
            },
        )



def manual_position_clear_resume_check(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    status: MonitorStatus,
    ts: datetime,
) -> None:
    if not config.get("live_mode"):
        return
    if status.live_state != "MANUAL_POSITION_CHECK_REQUIRED" or status.open_position is not None:
        return
    interval_sec = max(float(config.get("manual_position_check_interval_sec", 5.0)), 1.0)
    if status.last_manual_position_check_ts is not None:
        if (ts - status.last_manual_position_check_ts).total_seconds() < interval_sec:
            return
    status.last_manual_position_check_ts = ts
    try:
        positions = fetch_positions(client, config, storage, reason="MANUAL_POSITION_CLEAR_CHECK")
        total_qty, matching_qty = summarize_positions(positions)
        payload = {
            "ts": ts.isoformat(),
            "total_qty": total_qty,
            "matching_qty": matching_qty,
            "positions_summary": positions_summary_for_log(positions),
            "raw_positions_json": positions,
            "live_state": status.live_state,
        }
        if total_qty <= 0:
            status.live_state = "FLAT"
            status.recovery_until = None
            status.pending_entry_side = None
            status.pending_entry_ts = None
            status.pending_add = False
            status.pending_exit = False
            storage.log_structured(
                "INFO",
                "MANUAL_POSITION_CLEARED_RESUME_AUTO_TRADE",
                {**payload, "after_state": {"live_state": status.live_state}},
                mirror_message="manual/API positions are flat; auto trading resumed",
            )
        else:
            storage.log_structured("INFO", "MANUAL_POSITION_STILL_OPEN", payload)
    except Exception as e:
        storage.log_structured(
            "WARN",
            "MANUAL_POSITION_CLEAR_CHECK_ERROR",
            {"ts": ts.isoformat(), "live_state": status.live_state, **api_error_payload(e)},
            mirror_message=str(e),
        )


def recovery_flat_resume_check(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    status: MonitorStatus,
    ts: datetime,
) -> None:
    if not config.get("live_mode"):
        return
    if status.live_state != "RECOVERING" or status.open_position is not None:
        return
    if status.recovery_until is not None and ts < status.recovery_until:
        return
    interval_sec = max(float(config.get("recovery_flat_check_interval_sec", 5.0)), 1.0)
    if status.last_recovery_flat_check_ts is not None:
        if (ts - status.last_recovery_flat_check_ts).total_seconds() < interval_sec:
            return
    status.last_recovery_flat_check_ts = ts
    try:
        positions = fetch_positions(client, config, storage, reason="RECOVERY_FLAT_RESUME_CHECK")
        total_qty, matching_qty = summarize_positions(positions)
        payload = {
            "ts": ts.isoformat(),
            "total_qty": total_qty,
            "matching_qty": matching_qty,
            "positions_summary": positions_summary_for_log(positions),
            "raw_positions_json": positions,
            "live_state": status.live_state,
        }
        if total_qty <= 0:
            status.live_state = "FLAT"
            status.recovery_until = None
            status.pending_entry_side = None
            status.pending_entry_ts = None
            status.pending_add = False
            status.pending_exit = False
            storage.log_structured(
                "INFO",
                "RECOVERY_FLAT_CONFIRMED_RESUME_AUTO_TRADE",
                {**payload, "after_state": {"live_state": status.live_state, "recovery_until": status.recovery_until}},
                mirror_message="recovery state cleared after confirming flat positions",
            )
        else:
            storage.log_structured("WARN", "RECOVERY_FLAT_POSITION_EXISTS", payload)
    except Exception as e:
        storage.log_structured(
            "WARN",
            "RECOVERY_FLAT_CHECK_ERROR",
            {"ts": ts.isoformat(), "live_state": status.live_state, **api_error_payload(e)},
            mirror_message=str(e),
        )


def midday_order_cleanup(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    status: MonitorStatus,
    ts: datetime,
) -> None:
    pos = status.open_position
    kept_payload = {
        "kept_take_profit_order_id": pos.take_profit_order_id if pos else None,
        "kept_take_profit_order_ids": list(pos.take_profit_order_ids or []) if pos else [],
        "kept_exit_order_id": pos.exit_order_id if pos else None,
    }
    base_payload = {
        "ts": ts.isoformat(),
        "live_state": status.live_state,
        "pending_entry_side": status.pending_entry_side,
        "open_position": position_state_payload(pos),
        **kept_payload,
    }
    storage.log_structured("INFO", "MIDDAY_ENTRY_ORDER_CLEANUP_START", base_payload)

    entry_order_ids: list[str] = []
    entry_order_state = status.live_state in {"ENTRY_SENT", "ENTRY_PARTIAL", "ENTRY_RETRY"}
    pending_entry_state = status.pending_entry_side is not None
    if pos is not None and (entry_order_state or pending_entry_state):
        entry_order_ids.extend(_split_order_ids(pos.entry_order_id))

    dedup: list[str] = []
    seen: set[str] = set()
    for oid in entry_order_ids:
        if oid and oid not in seen:
            seen.add(oid)
            dedup.append(oid)

    if not dedup:
        storage.log_structured("INFO", "MIDDAY_ENTRY_ORDER_CLEANUP_SKIPPED_NO_ENTRY_ORDER", base_payload)

    had_cancel_fail = False
    cancelled_any = False
    for oid in dedup:
        req = {"OrderId": oid}
        storage.log_structured("WARN", "MIDDAY_ENTRY_ORDER_CANCEL_REQUEST", {**base_payload, "order_id": oid, "request_json": req})
        try:
            res = client.cancel_order(oid, config["order_password"])
            cancelled_any = True
            storage.log_structured("WARN", "MIDDAY_ENTRY_ORDER_CANCEL_RESPONSE", {**base_payload, "order_id": oid, "raw_response_json": res})
        except Exception as e:
            had_cancel_fail = True
            storage.log_structured("ERROR", "MIDDAY_ENTRY_ORDER_CANCEL_FAIL", {**base_payload, "order_id": oid, **api_error_payload(e)})

    positions = fetch_positions(client, config, storage, reason="MIDDAY_ENTRY_ORDER_CLEANUP_POST_CHECK") if config.get("live_mode") and (cancelled_any or had_cancel_fail) else []
    rec_ok = True
    rec_message = ""
    if config.get("live_mode") and pos is not None and (cancelled_any or had_cancel_fail):
        rec = reconcile_live_position(
            client,
            config,
            status,
            storage,
            expected_side=pos.side,
            reason="MIDDAY_ENTRY_ORDER_CLEANUP_POST_CHECK",
            ts=ts,
            expected_margin_trade_type=pos.margin_trade_type,
        )
        rec_ok = rec.ok_for_entry
        rec_message = rec.message

    if had_cancel_fail and (not rec_ok):
        status.live_state = "RECOVERING"
        storage.log_structured("WARN", "MIDDAY_ENTRY_ORDER_CLEANUP_RECOVERING", {**base_payload, "positions_json": positions, "live_state": status.live_state, "reason": rec_message})

    status.pending_entry_side = None
    status.pending_entry_ts = None
    status.pending_add = False
    status.midday_order_cleanup_done = True
    done_payload = {
        **base_payload,
        "cancelled_entry_order_ids": dedup,
        "cancelled_any": cancelled_any,
        "had_cancel_fail": had_cancel_fail,
        "positions_json": positions,
        "live_state": status.live_state,
        "open_position": position_state_payload(status.open_position),
    }
    storage.log_structured("INFO", "MIDDAY_ENTRY_ORDER_CLEANUP_DONE", done_payload)

def run_monitor(config: dict[str, Any]) -> tuple[str, str]:
    outdir = config["outdir"]
    ensure_dir(outdir)
    db_path = os.path.join(outdir, f"monitor_1570_{jst_date_compact()}.db")
    daily_report_path = os.path.join(outdir, f"daily_report_{jst_date_str()}.md")
    midday_report_path = os.path.join(outdir, f"midday_report_{jst_date_str()}.md")
    storage = Storage(db_path)
    storage.log("INFO", "START", "monitor start")
    storage.log_structured("INFO", "STARTUP_CONFIG", {"live_mode": config.get("live_mode"), "order_qty": config.get("order_qty"), "entry_min_fill_qty": config.get("entry_min_fill_qty"), "order_exchange": config.get("order_exchange"), "exit_order_exchange": config.get("exit_order_exchange")})

    client = KabuApiClient(config["base_url"])
    status = MonitorStatus()
    token_ok = False
    start_errors: list[str] = []
    for _ in range(3):
        try:
            client.get_token(config["api_password"])
            storage.log("INFO", "TOKEN_OK", "token acquired")
            client.register_symbol(config["symbol"], config["exchange"])
            storage.log("INFO", "REGISTER_OK", "register ok")
            token_ok = True
            break
        except Exception as e:
            err = str(e)
            start_errors.append(err)
            print(f"[ERROR] startup auth/register failed: {err}")
            storage.log("ERROR", "TOKEN_FAIL", err)
            time.sleep(1)
    if config.get("live_mode"):
        try:
            sp = fetch_positions(client, config, storage, reason="STARTUP_POSITION_CHECK")
            tq,mq = summarize_positions(sp)
            storage.log_structured("INFO", "STARTUP_POSITION_CHECK", {"total_qty": tq, "matching_qty": mq, "positions_json": sp})
            if tq > 0:
                status.live_state = "MANUAL_POSITION_CHECK_REQUIRED"
                storage.log("WARN", "MANUAL_POSITION_CHECK_REQUIRED", "startup detected existing positions; new entry paused")
        except Exception as e:
            storage.log("ERROR", "STARTUP_POSITION_CHECK", str(e))
    if not token_ok:
        generate_report(db_path, daily_report_path)
        detail = start_errors[-1] if start_errors else "unknown startup error"
        raise RuntimeError(
            "monitor startup failed (token/register). "
            f"last_error={detail}. "
            "Check api_password/order_password and exchange in config."
        )

    tick_buf: deque[TickSnapshot] = deque(maxlen=2000)
    rb1 = RollingBars(1)
    rb3 = RollingBars(3)
    try:
        warm_bars = preload_prev_day_1m_bars(outdir, db_path, limit=int(config.get("prev_day_warmup_bars", 120)))
        if warm_bars:
            for b in warm_bars:
                rb1.history.append(b)
            storage.log("INFO", "WARMUP_1M_PREV_DB", f"loaded={len(warm_bars)}")
        else:
            storage.log("INFO", "WARMUP_1M_PREV_DB", "loaded=0")
    except Exception as e:
        storage.log("WARN", "WARMUP_1M_PREV_DB_FAIL", str(e))
    adaptive = AdaptiveControlState(enabled=bool(config.get("adaptive_control", ADAPTIVE_CONTROL_ENABLED)))
    volatility_gate = VolatilityRegimeGate(config.get("volatility_regime_gate", {}))
    set_vwap_mode(str(config.get("initial_vwap_mode", adaptive.vwap_mode)))
    adaptive.vwap_mode = CURRENT_VWAP_MODE
    closed_trades: list[ClosedTradeSummary] = []
    start_ts = now_jst()
    next_status_write = start_ts
    next_console_status = start_ts
    runtime_minutes = config.get("runtime_minutes")
    end_ts = start_ts + timedelta(minutes=runtime_minutes) if runtime_minutes else None

    last_feature: Optional[FeatureSnapshot] = None
    last_pred: Optional[PredictionSnapshot] = None
    last_gate: Optional[GateDecision] = None
    last_snapshot: Optional[TickSnapshot] = None
    mfe_ticks = 0.0
    mae_ticks = 0.0

    while True:
        now_ = now_jst()
        tstr = now_.strftime("%H:%M:%S")
        if end_ts and now_ >= end_ts:
            storage.log("INFO", "STOP_RUNTIME", "runtime end reached")
            force_close_open_position(client, config, storage, status, "STOP_RUNTIME", now_, last_pred, latest_snapshot=last_snapshot, use_market_order=True)
            break
        if tstr >= STOP_AFTER:
            storage.log("INFO", "STOP_AFTER_SESSION", "session end reached")
            force_close_open_position(client, config, storage, status, "STOP_AFTER_SESSION", now_, last_pred, latest_snapshot=last_snapshot, use_market_order=True)
            break
        try:
            raw = client.get_board(config["symbol"], config["exchange"])
            snap = extract_snapshot(raw)
            last_snapshot = snap
            tick_buf.append(snap)
            spread_ticks = calc_spread_ticks(snap)
            storage.insert_snapshot(snap, spread_ticks)
            status.count += 1

            force_close_time_reached_top = tstr >= str(config.get("force_close_after", FORCE_CLOSE_AFTER))
            if force_close_time_reached_top:
                storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_START", {
                    "ts": now_.isoformat(),
                    "live_state": status.live_state,
                    "open_position": position_state_payload(status.open_position),
                    "force_close_state": status.force_close_state,
                })
                if status.pending_entry_side is not None:
                    storage.log_structured("WARN", "FORCE_MARKET_CLOSE_1520_CLEAR_PENDING_ENTRY", {"side": status.pending_entry_side, "pending_entry_ts": status.pending_entry_ts.isoformat() if status.pending_entry_ts else None})
                status.pending_entry_side = None
                status.pending_entry_ts = None
                status.pending_add = False
                if status.rsi20_long_watch_active:
                    clear_rsi20_long_watch(status, storage, "RSI20_LONG_WATCH_CANCELLED", now_, None, None, "FORCE_CLOSE_TIME_REACHED")
                if status.rsi70_drop_long_watch_active:
                    clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", now_, None, None, "FORCE_CLOSE_TIME_REACHED")
                if status.open_position is not None:
                    force_close_open_position(
                        client,
                        config,
                        storage,
                        status,
                        reason="FORCE_MARKET_CLOSE_1520",
                        ts=now_,
                        last_pred=last_pred,
                        latest_snapshot=snap,
                        use_market_order=True,
                    )
                time.sleep(config["poll_interval_sec"])
                continue

            manual_position_clear_resume_check(client, config, storage, status, now_)
            recovery_flat_resume_check(client, config, storage, status, now_)

            bar1_new = rb1.update(snap)
            if bar1_new:
                storage.insert_bar("bars_1m", bar1_new)
            current_bar_bucket_1m = rb1.current_bucket
            current_bar_open_1m = rb1.rows[0].price if rb1.rows and rb1.rows[0].price is not None else None
            confirmed_bar1 = rb1.latest()
            bar3_new = rb3.update(snap)
            if bar3_new:
                storage.insert_bar("bars_3m", bar3_new)

            midday_cancel_start = str(config.get("midday_order_cancel_start", MIDDAY_ORDER_CANCEL_START))
            midday_cancel_end = str(config.get("midday_order_cancel_end", MIDDAY_ORDER_CANCEL_END))
            if (
                not status.midday_order_cleanup_done
                and midday_cancel_start <= tstr < midday_cancel_end
            ):
                midday_order_cleanup(client, config, storage, status, now_)

            if (
                not status.midday_bar_finalize_done
                and "11:30:00" <= tstr < "11:31:00"
            ):
                b1f = rb1.force_finalize()
                if b1f:
                    storage.insert_bar("bars_1m", b1f)
                b3f = rb3.force_finalize()
                if b3f:
                    storage.insert_bar("bars_3m", b3f)
                status.midday_bar_finalize_done = True

            set_vwap_mode(adaptive.vwap_mode)
            f = build_features(snap.ts, tick_buf, rb1.latest(), rb1.prev(1), rb3.latest(), rb3.prev(1))
            if f is not None:
                storage.insert_feature(f)
                # 15:20強制決済は上位ブロックで処理済み。ここでは新規停止フラグとして扱わない。
                force_close_handled_above = False
                entry_cutoff_reached = new_entry_cutoff_reached(config, tstr)
                allow_new_entry = (
                    time_in_windows(tstr, TRADE_WINDOWS)
                    and not force_close_handled_above
                    and not entry_cutoff_reached
                    and status.pending_entry_side is None
                    and status.live_state not in {"RECOVERING", "MANUAL_POSITION_CHECK_REQUIRED", "ENTRY_SENT", "EXIT_SENT", "EXIT_VERIFYING"}
                )
                p = build_rsi9_prediction(
                    rb1.latest(),
                    list(rb1.history),
                    status.open_position,
                    status=status,
                    storage=storage,
                    allow_new_entry=allow_new_entry,
                    rsi70_watch_config=config.get("rsi70_drop_long_watch", {}),
                    new_entry_cutoff_reached=entry_cutoff_reached,
                )
                if p is None:
                    continue
                metrics = extended_feature_metrics(f, list(rb1.history))
                analysis_cfg = config.get("analysis_signals", {}) if isinstance(config.get("analysis_signals", {}), dict) else {}
                feature_cfg = config.get("feature_entries", {}) if isinstance(config.get("feature_entries", {}), dict) else {}
                feature_enabled = bool(feature_cfg.get("enabled", False))
                feature_candidates: list[PredictionSnapshot] = []
                if not entry_cutoff_reached and status.open_position is None and allow_new_entry:
                    if bool(feature_cfg.get("long_vwap_volume_momentum", True)):
                        if (
                            metrics.get("abs_vwap_gap_bps") is not None
                            and metrics.get("volume_ratio_20") is not None
                            and metrics.get("ret5_ticks") is not None
                            and metrics["abs_vwap_gap_bps"] < 20
                            and metrics["volume_ratio_20"] >= 1.3
                            and metrics["ret5_ticks"] >= 5
                        ):
                            feature_candidates.append(PredictionSnapshot(f.ts, "FEATURE_ENTRY", 0.5, 0.5, 0.5, 0.5, "LONG_CANDIDATE", p.rsi9_value, "FEATURE_ENTRY", f"ret5_ticks={metrics['ret5_ticks']:.2f}", "long_feature_vwap_volume_momentum"))
                    if bool(feature_cfg.get("short_vwap_extended_fail", True)):
                        if (
                            metrics.get("abs_vwap_gap_bps") is not None
                            and metrics.get("ret5_ticks") is not None
                            and metrics.get("ret1_ticks") is not None
                            and metrics["abs_vwap_gap_bps"] > 60
                            and metrics["ret5_ticks"] >= 5
                            and metrics["ret1_ticks"] <= -5
                        ):
                            feature_candidates.append(PredictionSnapshot(f.ts, "FEATURE_ENTRY", 0.5, 0.5, 0.5, 0.5, "SHORT_CANDIDATE", p.rsi9_value, "FEATURE_ENTRY", f"ret1_ticks={metrics['ret1_ticks']:.2f}", "short_feature_vwap_extended_fail"))
                if feature_candidates:
                    if feature_enabled and p.signal == "NO_ACTION":
                        p = feature_candidates[0]
                        storage.log_structured(
                            "INFO",
                            "FEATURE_ENTRY_TRIGGERED",
                            {
                                "ts": f.ts.isoformat(),
                                "signal": p.signal,
                                "reason_1": p.reason_1,
                                "reason_2": p.reason_2,
                                "reason_3": p.reason_3,
                                "metrics": metrics,
                                "feature_entries_enabled": feature_enabled,
                                "entry_cutoff_reached": entry_cutoff_reached,
                                "allow_new_entry": allow_new_entry,
                                "live_state": status.live_state,
                            },
                        )
                    elif bool(analysis_cfg.get("log_feature_candidates_when_disabled", True)):
                        for fp in feature_candidates:
                            storage.log_structured("INFO", "FEATURE_ENTRY_CANDIDATE_LOG_ONLY", {"ts": f.ts.isoformat(), "signal": fp.signal, "reason_1": fp.reason_1, "reason_3": fp.reason_3, "metrics": metrics, "feature_entries_enabled": feature_enabled})
                big_trend_cfg = config.get("big_trend_start_score", {}) if isinstance(config.get("big_trend_start_score", {}), dict) else {}
                log_big_trend_score = bool(big_trend_cfg.get("enabled", False)) or bool(analysis_cfg.get("log_big_trend_score_when_disabled", False))
                if log_big_trend_score and bar1_new is not None:
                    for score_side in ("LONG", "SHORT"):
                        score, components = big_trend_start_score(score_side, f, metrics)
                        storage.log_structured("INFO", "BIG_TREND_START_SCORE_CALCULATED", {"ts": f.ts.isoformat(), "side": score_side, "score": score, "components": components, "log_reason": "enabled" if bool(big_trend_cfg.get("enabled", False)) else "disabled_log_1m_only", **metrics})
                storage.insert_prediction(p)
                gate_features = volatility_gate.compute_features(tick_buf, f)
                gate_decision = volatility_gate.evaluate(p.signal, gate_features, current_position=status.open_position)
                if bool(volatility_gate.logging.get("save_gate_decision", True)):
                    storage.insert_gate_decision(config["symbol"], p, gate_decision)
                last_feature = f
                last_pred = p
                last_gate = gate_decision
                effective_signal = p.signal
                current_rsi = extract_rsi_from_pred(p)

                # RSI threshold hit on closed 1m bar -> execute on next 1m bar open (first tick)
                if (not force_close_handled_above) and bar1_new is not None and status.pending_entry_side is None and status.open_position is None:
                    if effective_signal in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
                        pending_side = "LONG" if effective_signal == "LONG_CANDIDATE" else "SHORT"
                        if entry_cutoff_reached:
                            log_new_entry_cutoff_block(storage, config, f.ts, effective_signal, p.reason_3, pending_side)
                        else:
                            status.pending_entry_side = pending_side
                            status.pending_entry_ts = f.ts
                            storage.log("INFO", "RSI_PENDING_ENTRY", f"side={status.pending_entry_side} signal_ts={f.ts.isoformat()}")

                if (not force_close_handled_above) and bar1_new is not None and status.open_position is not None and status.live_state == "OPEN":
                    add_ok, add_reason = should_rsi9_long_add(rb1.latest(), list(rb1.history), status.open_position)
                    if status.pending_exit:
                        storage.log("INFO", "RSI9_LONG_ADD_SKIP", "reason=PENDING_EXIT")
                    elif add_ok and entry_cutoff_reached:
                        status.pending_add = False
                        log_new_entry_cutoff_block(storage, config, f.ts, "LONG_CANDIDATE", add_reason, "LONG")
                        storage.log("INFO", "RSI9_LONG_ADD_SKIP", "reason=NEW_ENTRY_CUTOFF")
                    elif add_ok and not status.pending_add:
                        status.pending_add = True
                        storage.log("INFO", "RSI9_LONG_ADD_PENDING", f"reason={add_reason}")
                    elif not add_ok:
                        storage.log("INFO", "RSI9_LONG_ADD_SKIP", f"reason={add_reason}")

                if status.pending_add and status.open_position is not None and status.open_position.side == "LONG":
                    pos_add = status.open_position
                    if pos_add.rsi10_add_done:
                        status.pending_add = False
                        storage.log("INFO", "RSI9_LONG_ADD_SKIP", "reason=ALREADY_ADDED_PENDING_CLEARED")
                    elif pos_add.take_profit_order_id is not None:
                        status.pending_add = False
                        storage.log("INFO", "RSI9_LONG_ADD_SKIP", "reason=ACTIVE_TP_ORDER_PENDING_CLEARED")
                    elif status.live_state != "OPEN":
                        status.pending_add = False
                        storage.log("INFO", "RSI9_LONG_ADD_SKIP", f"reason=LIVE_STATE_{status.live_state}_PENDING_CLEARED")
                    elif force_close_handled_above or entry_cutoff_reached:
                        status.pending_add = False
                        if entry_cutoff_reached:
                            log_new_entry_cutoff_block(storage, config, f.ts, "LONG_CANDIDATE", "rsi10_add", "LONG")
                    else:
                        current_qty = get_open_position_qty(client, config, "LONG", margin_trade_type=pos_add.margin_trade_type) if config.get("live_mode") else int(pos_add.filled_qty)
                        if config.get("live_mode") and current_qty <= 0:
                            status.pending_add = False
                            status.live_state = "RECOVERING"
                            storage.log("WARN", "RSI9_LONG_ADD_FAIL", "reason=INCONSISTENT_NO_POSITION existing_qty=0")
                        else:
                            add_qty = int(config.get("order_qty", 2))
                            target_total_qty = int(current_qty + add_qty)
                            add_before_positions: list[dict[str, Any]] = []
                            add_before_positions_ok = True
                            if config.get("live_mode"):
                                try:
                                    add_before_positions = fetch_positions(client, config, storage, reason="PRE_ADD_REBUILD_MANAGED_POSITIONS")
                                except Exception as e:
                                    add_before_positions_ok = False
                                    status.pending_add = False
                                    enter_recovery_after_add_rebuild_failure(
                                        storage,
                                        status,
                                        pos_add,
                                        f.ts,
                                        "PRE_ADD_POSITIONS_FETCH_FAILED",
                                        {
                                            "before_managed_execution_ids": list(pos_add.managed_execution_ids or []),
                                            "current_qty": current_qty,
                                            "add_qty": add_qty,
                                            "target_total_qty": target_total_qty,
                                            **api_error_payload(e),
                                        },
                                    )
                            if not add_before_positions_ok:
                                add_res = LiveOrderResult(False, "PRE_ADD_POSITIONS_FETCH_FAILED", recoverable=True)
                            else:
                                add_pred = PredictionSnapshot(ts=f.ts, regime="RSI9", p_up_1m=0.5,p_down_1m=0.5,p_up_3m=0.5,p_down_3m=0.5, signal="LONG_CANDIDATE", rsi9_value=current_rsi, reason_1="RSI9_ADD", reason_2=f"rsi9={current_rsi if current_rsi is not None else 0:.2f}", reason_3="rsi10_add")
                                add_res = execute_live_entry(client, config, "LONG", storage, pos_add, add_pred, status, latest_snapshot=snap, target_total_qty_override=target_total_qty, qty_override=add_qty)
                            if add_res.ok:
                                status.pending_add = False
                                pos_add.rsi10_add_done = True
                                if config.get("live_mode"):
                                    rebuild_ok = rebuild_rsi9_long_add_managed_state(
                                        client,
                                        config,
                                        storage,
                                        status,
                                        pos_add,
                                        add_before_positions,
                                        target_total_qty,
                                        f.ts,
                                    )
                                    if not rebuild_ok:
                                        storage.log("ERROR", "RSI9_LONG_ADD_FAIL", f"reason=MANAGED_STATE_REBUILD_FAILED existing_qty={current_qty} add_qty={add_qty} target_total_qty={target_total_qty}")
                                    else:
                                        storage.log("INFO", "RSI9_LONG_ADD_OK", f"existing_qty={current_qty} add_qty={add_qty} target_total_qty={target_total_qty} managed_qty={pos_add.filled_qty}")
                                else:
                                    pos_add.filled_qty = int(target_total_qty)
                                    pos_add.order_qty = int(target_total_qty)
                                    pos_add.remaining_qty = 0
                                    storage.log("INFO", "RSI9_LONG_ADD_OK", f"existing_qty={current_qty} add_qty={add_qty} target_total_qty={target_total_qty}")
                            else:
                                status.pending_add = False
                                storage.log("WARN", "RSI9_LONG_ADD_FAIL", f"existing_qty={current_qty} add_qty={add_qty} target_total_qty={target_total_qty} message={add_res.message}")

                if status.pending_entry_side and status.open_position is None:
                    if force_close_handled_above:
                        status.pending_entry_side = None
                        status.pending_entry_ts = None
                    elif entry_cutoff_reached:
                        side = status.pending_entry_side
                        log_new_entry_cutoff_block(storage, config, f.ts, p.signal, p.reason_3, side or "")
                        status.pending_entry_side = None
                        status.pending_entry_ts = None
                    else:
                        side = status.pending_entry_side
                        enter_ok, _ = can_enter(side, f.ts, status)
                        if enter_ok:
                            if p.reason_3 in RSI17_DROP_ENTRY_RULES:
                                latest_bar_for_gap = rb1.latest()
                                send_price = snap.price if snap.price is not None else f.price
                                send_ma75 = latest_bar_for_gap.ma75 if latest_bar_for_gap is not None else None
                                blocked_reason, gap_ratio = rsi17_drop_ma75_gap_block_reason(send_price, send_ma75, p.reason_3)
                                if blocked_reason is not None:
                                    event_type = "ENTRY_BLOCKED_MA75_GAP_UNAVAILABLE" if gap_ratio is None else "ENTRY_BLOCKED_MA75_GAP_AT_SEND"
                                    log_ma75_gap_block(
                                        storage,
                                        event_type,
                                        f.ts,
                                        side,
                                        current_rsi,
                                        None,
                                        None,
                                        send_price,
                                        send_ma75,
                                        gap_ratio,
                                        p.reason_3,
                                        blocked_reason,
                                    )
                                    status.pending_entry_side = None
                                    status.pending_entry_ts = None
                                    continue
                            candidate_pos = create_position(p, f, config, side_override=side)
                            if candidate_pos.side != side:
                                storage.log_structured(
                                    "ERROR",
                                    "ENTRY_SIDE_MISMATCH_BLOCK",
                                    {
                                        "expected_side": side,
                                        "candidate_pos_side": candidate_pos.side,
                                        "pred_signal": p.signal,
                                        "pred_reason_1": p.reason_1,
                                        "pred_reason_2": p.reason_2,
                                        "pred_reason_3": p.reason_3,
                                        "ts": f.ts.isoformat(),
                                    },
                                    mirror_message=f"expected_side={side} candidate_pos_side={candidate_pos.side}",
                                )
                                status.pending_entry_side = None
                                status.pending_entry_ts = None
                                status.pending_add = False
                                status.live_state = "RECOVERING"
                                status.recovery_until = now_jst() + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
                            elif not adaptive.allow_strat(candidate_pos.strategy, f.ts):
                                storage.log("INFO", "ADAPTIVE_SKIP_ENTRY", f"side={candidate_pos.side} strategy={candidate_pos.strategy} reason=STRAT_1M_FROZEN until={adaptive.freeze_strat_1m_until}")
                                status.pending_entry_side = None
                            elif config["live_mode"]:
                                rec = reconcile_live_position(client, config, status, storage, expected_side=side, reason="PRE_ENTRY", ts=f.ts, expected_margin_trade_type=margin_trade_type_for_side(config, side))
                                if not rec.ok_for_entry or status.open_position is not None:
                                    storage.log_structured(
                                        "WARN",
                                        "ENTRY_SKIP_RECOVERY",
                                        {
                                            "side": side,
                                            "strategy_name": candidate_pos.strategy,
                                            "signal_reason": p.reason_1,
                                            "reconcile_result": asdict(rec),
                                            "internal_state": position_state_payload(status.open_position),
                                        },
                                        mirror_message=f"side={side} reason={rec.message}",
                                    )
                                else:
                                    reject_key = entry_reject_key(config, side, candidate_pos.strategy, p, status.last_error_code)
                                    if status.last_entry_reject_key and status.last_entry_reject_key == reject_key:
                                        storage.log("INFO", "ENTRY_SKIP_DUPLICATE_REJECT", reject_key)
                                        status.pending_entry_side = None
                                    else:
                                        status.live_state = "ENTRY_SENT"
                                        result = execute_live_entry(client, config, side, storage, candidate_pos, p, status, latest_snapshot=snap)
                                        verify_result = verify_entry_position_after_order(
                                            client,
                                            config,
                                            storage,
                                            candidate_pos,
                                            side,
                                            result.order_id or "",
                                            p,
                                            status,
                                            f.ts,
                                            before_positions=rec.positions,
                                        )
                                        verified_pos = verify_result.position
                                        if not result.ok:
                                            if verified_pos is not None and verified_pos.filled_qty > 0:
                                                candidate_pos = verified_pos
                                                status.open_position = candidate_pos
                                                status.live_state = "OPEN"
                                                status.pending_entry_side = None
                                                status.pending_entry_ts = None
                                                status.pending_add = False
                                                status.last_entry_ts_by_side[side] = f.ts
                                                storage.log("WARN", "PARTIAL_ENTRY_FILLED", f"side={side} filled_qty={candidate_pos.filled_qty} remaining_qty={candidate_pos.remaining_qty}")
                                            else:
                                                handle_entry_not_filled_without_internal_position(
                                                    status, storage, config, side, f.ts, result.order_id or "", p, verify_result
                                                )
                                            status.last_error_code = result.api_code
                                            status.last_error_message = result.message
                                            storage.log("ERROR", "LIVE_ENTRY_FAIL", result.message)
                                            if result.api_code:
                                                status.last_entry_reject_key = entry_reject_key(config, side, candidate_pos.strategy, p, result.api_code)
                                            if result.message.startswith("ENTRY_RESTRICTED_100368"):
                                                block_sec = int(config.get("entry_error_block_sec", ENTRY_ERROR_BLOCK_SEC))
                                                status.entry_global_block_until = f.ts + timedelta(seconds=max(block_sec, 1))
                                                storage.log("WARN", "ENTRY_GLOBAL_BLOCK", f"reason={result.message} until={status.entry_global_block_until.isoformat()}")
                                        else:
                                            storage.log("INFO", "LIVE_ENTRY_OK", f"{side} order_id={result.order_id}")
                                            if verified_pos is None or verified_pos.filled_qty <= 0:
                                                handle_entry_not_filled_without_internal_position(
                                                    status, storage, config, side, f.ts, result.order_id or "", p, verify_result
                                                )
                                            else:
                                                candidate_pos = verified_pos
                                                storage.insert_execution_fill_price(
                                                    "ENTRY_FILL_PRICE",
                                                    result.order_id,
                                                    side,
                                                    candidate_pos.strategy,
                                                    p.reason_1,
                                                    candidate_pos.entry_fill_price,
                                                )
                                                status.open_position = candidate_pos
                                                status.live_state = "OPEN"
                                                if status.rsi20_long_watch_active:
                                                    clear_rsi20_long_watch(status, storage, "RSI20_LONG_WATCH_CANCELLED", f.ts, extract_rsi_from_pred(p), None, "ENTRY_FILLED_BY_OTHER_SIGNAL")
                                                if status.rsi70_drop_long_watch_active:
                                                    clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", f.ts, extract_rsi_from_pred(p), None, "ENTRY_FILLED_BY_OTHER_SIGNAL")
                                                if candidate_pos.filled_qty >= int(config.get("order_qty", 2)):
                                                    storage.log("INFO", "ENTRY_FULLY_FILLED", f"side={side} filled_qty={candidate_pos.filled_qty}")
                                                else:
                                                    storage.log("WARN", "PARTIAL_ENTRY_FILLED", f"side={side} filled_qty={candidate_pos.filled_qty} remaining_qty={candidate_pos.remaining_qty}")
                                                storage.log_structured(
                                                    "INFO",
                                                    "TRAILING_EXIT_MANAGED_NO_TP_LIMIT",
                                                    {
                                                        "side": candidate_pos.side,
                                                        "strategy": candidate_pos.strategy,
                                                        "rsi_special_entry": candidate_pos.rsi_special_entry,
                                                        "entry_rule": p.reason_3,
                                                        "signal_reason": p.reason_1,
                                                        "entry_price": candidate_pos.entry_price,
                                                        "order_id": result.order_id,
                                                        "trailing_trigger_ticks": candidate_pos.trailing_trigger_ticks,
                                                        "trailing_floor_ticks": candidate_pos.trailing_floor_ticks,
                                                        "trailing_width_ticks": candidate_pos.trailing_width_ticks,
                                                    },
                                                    mirror_message=f"side={candidate_pos.side} strategy={candidate_pos.strategy} trailing_exit_managed=True",
                                                )
                                                status.last_entry_reject_key = ""
                                                status.last_entry_ts_by_side[side] = f.ts
                                                status.pending_entry_side = None
                                                status.pending_entry_ts = None
                                                status.pending_add = False
                                                mfe_ticks = 0.0
                                                mae_ticks = 0.0
                            else:
                                status.open_position = candidate_pos
                                status.live_state = "OPEN"
                                if status.rsi20_long_watch_active:
                                    clear_rsi20_long_watch(status, storage, "RSI20_LONG_WATCH_CANCELLED", f.ts, extract_rsi_from_pred(p), None, "ENTRY_FILLED_BY_OTHER_SIGNAL")
                                if status.rsi70_drop_long_watch_active:
                                    clear_rsi70_drop_long_watch(status, storage, "RSI70_DROP_LONG_WATCH_CANCELLED", f.ts, extract_rsi_from_pred(p), None, "ENTRY_FILLED_BY_OTHER_SIGNAL")
                                status.last_entry_ts_by_side[side] = f.ts
                                status.pending_entry_side = None
                                mfe_ticks = 0.0
                                mae_ticks = 0.0
                elif status.open_position is not None:
                    skip_exit_eval = False
                    if config["live_mode"] and status.live_state == "RECOVERING":
                        rec = reconcile_live_position(client, config, status, storage, expected_side=status.open_position.side, reason="RECOVERING_POLL", ts=f.ts, expected_margin_trade_type=status.open_position.margin_trade_type)
                        if rec.total_leaves_qty == 0 and status.open_position is None:
                            storage.log("WARN", "RECOVERY_EXIT_FLAT", rec.message)
                            skip_exit_eval = True
                        elif rec.message == "MATCHED_OPEN":
                            status.live_state = "OPEN"
                            storage.log("INFO", "RECOVERY_EXIT", "matched live position; resume exit evaluation")
                        else:
                            status.live_state = "RECOVERING"
                            storage.log("WARN", "EXIT_SKIP_RECOVERING", rec.message)
                            skip_exit_eval = True

                    if status.open_position is not None and not skip_exit_eval:
                        pos = status.open_position
                        cur_pnl_ticks = price_to_ticks(f.price - pos.entry_price, pos.entry_price)
                        if pos.side == "SHORT":
                            cur_pnl_ticks = -cur_pnl_ticks
                        mfe_ticks = max(mfe_ticks, cur_pnl_ticks)
                        mae_ticks = min(mae_ticks, cur_pnl_ticks)
                        live_tp_already_filled = False

                        # RSI9 special entries now use the same trailing exit manager as paper mode.
                        # Do not place +10tick/+5tick staged TP limit orders; any existing TP
                        # order is only observed/cancelled by the generic exit path below.

                        if live_tp_already_filled:
                            ex = True
                            ex_reason = "TAKE_PROFIT_LIMIT_FILLED"
                            if pos.exit_fill_price is None:
                                pos.exit_fill_price = take_profit_limit_price(pos)
                            pnl_ticks = take_profit_filled_ticks(pos)
                        elif config["live_mode"] and pos.take_profit_order_id and wait_for_managed_position_qty(client, config, pos, target_qty=0, timeout_sec=0, comparator="eq", storage=storage, reason="TP_WAIT_MANAGED_FILLED"):
                            pos.exit_fill_price = take_profit_limit_price(pos)
                            ex, ex_reason, pnl_ticks = True, "TAKE_PROFIT_LIMIT_FILLED", take_profit_filled_ticks(pos)
                            live_tp_already_filled = True
                        else:
                            ex, ex_reason, pnl_ticks = should_exit(
                                pos,
                                f,
                                p,
                                storage=storage,
                                bar1_new=bar1_new,
                                current_bar_bucket=current_bar_bucket_1m,
                                current_bar_open=current_bar_open_1m,
                                confirmed_bar1=confirmed_bar1,
                                hold_score_config=config.get("hold_score_extension", {}),
                                analysis_config=analysis_cfg,
                                feature_metrics=metrics,
                            )

                        if config["live_mode"] and ex and ex_reason == "TAKE_PROFIT" and pos.take_profit_order_id and not live_tp_already_filled:
                            if pos.strategy == "RSI9":
                                context = order_context(config, pos.side, pos, p, status)
                                cancel_ok, filled_during_cancel = cancel_pending_take_profit_order(client, config, storage, pos, context)
                                if filled_during_cancel:
                                    live_tp_already_filled = True
                                    ex_reason = "TAKE_PROFIT_LIMIT_FILLED"
                                    if pos.exit_fill_price is None:
                                        pos.exit_fill_price = take_profit_limit_price(pos)
                                    pnl_ticks = take_profit_filled_ticks(pos)
                                elif not cancel_ok:
                                    ex = False
                                    status.live_state = "RECOVERING"
                                    status.recovery_until = f.ts + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
                                    storage.log_structured(
                                        "ERROR",
                                        "RSI9_SPECIAL_TP_CANCEL_FAILED" if pos.rsi_special_entry else "RSI9_OLD_TP_CANCEL_FAILED",
                                        {
                                            "ts": f.ts.isoformat(),
                                            "side": pos.side,
                                            "strategy": pos.strategy,
                                            "rsi_special_entry": pos.rsi_special_entry,
                                            "entry_rule": p.reason_3,
                                            "order_id": pos.take_profit_order_id,
                                            "internal_position": position_state_payload(pos),
                                        },
                                        mirror_message=f"side={pos.side} strategy={pos.strategy} order_id={pos.take_profit_order_id}",
                                    )
                                else:
                                    pos.take_profit_order_id = None
                                    storage.log("INFO", "RSI9_OLD_TP_CANCELLED_BEFORE_EXIT", f"side={pos.side} strategy={pos.strategy}")
                            if ex and not live_tp_already_filled and pos.strategy == "RSI9":
                                pass
                            elif pos.strategy == "RSI9":
                                # do not enter TAKE_PROFIT_LIMIT_WAIT flow for RSI9
                                pass
                            else:
                                holding_sec_now = (f.ts - pos.entry_ts).total_seconds()
                                if pos.take_profit_trigger_ts is None:
                                    pos.take_profit_trigger_ts = f.ts
                                signal_wait_sec = (f.ts - pos.take_profit_trigger_ts).total_seconds()
                                fallback_wait_sec = take_profit_fallback_after_signal_sec(config)
                                if holding_sec_now >= pos.max_hold_sec:
                                    ex_reason = "TIME_STOP"
                                    pnl_ticks = cur_pnl_ticks
                                    storage.log("WARN", "TAKE_PROFIT_LIMIT_TIMEOUT", f"{pos.side} order_id={pos.take_profit_order_id} target={take_profit_limit_price(pos):.1f} holding_sec={holding_sec_now:.1f}")
                                elif signal_wait_sec >= fallback_wait_sec:
                                    ex_reason = "TAKE_PROFIT_MARKET_FALLBACK"
                                    pnl_ticks = cur_pnl_ticks
                                    storage.log("WARN", "TAKE_PROFIT_LIMIT_FALLBACK", f"{pos.side} order_id={pos.take_profit_order_id} target={take_profit_limit_price(pos):.1f} signal_wait_sec={signal_wait_sec:.1f} fallback_wait_sec={fallback_wait_sec:.1f}")
                                else:
                                    storage.log("INFO", "TAKE_PROFIT_LIMIT_WAIT", f"{pos.side} order_id={pos.take_profit_order_id} target={take_profit_limit_price(pos):.1f} signal_wait_sec={signal_wait_sec:.1f} fallback_wait_sec={fallback_wait_sec:.1f}")
                                    ex = False
                        elif not (ex and ex_reason == "TAKE_PROFIT"):
                            pos.take_profit_trigger_ts = None

                        if ex:
                            exit_confirmed = True
                            result: Optional[LiveOrderResult] = None
                            exit_fill_source = "SIGNAL_PRICE_FALLBACK"
                            exit_actual_fill_price: Optional[float] = None
                            exit_signal_ts = f.ts
                            exit_signal_price = f.price
                            ma5_exit_context = {
                                "exit_reason": ex_reason,
                                "current_bar_bucket": current_bar_bucket_1m.isoformat() if current_bar_bucket_1m else None,
                                "current_bar_open": current_bar_open_1m,
                                "confirmed_ma5": pos.trailing_ma5_reference,
                                "confirmed_bar_close": bar1_new.close if bar1_new is not None else None,
                                "trailing_ma5_open_relation": pos.trailing_ma5_open_relation,
                            }
                            storage.log_structured(
                                "INFO",
                                "EXIT_SIGNAL_DECISION",
                                {
                                    "decision_ts": exit_signal_ts.isoformat(),
                                    "side": pos.side,
                                    "strategy": pos.strategy,
                                    "exit_reason": ex_reason,
                                    "signal_price": exit_signal_price,
                                    "pnl_ticks": pnl_ticks,
                                    "entry_price": pos.entry_price,
                                    "position_state": position_state_payload(pos),
                                    "ma5_exit_context": ma5_exit_context,
                                },
                            )
                            if config["live_mode"]:
                                if pos.take_profit_order_id and ex_reason not in {"TAKE_PROFIT", "TAKE_PROFIT_LIMIT_FILLED"}:
                                    context = order_context(config, pos.side, pos, p, status)
                                    cancel_ok, filled_during_cancel = cancel_pending_take_profit_order(client, config, storage, pos, context)
                                    if filled_during_cancel:
                                        live_tp_already_filled = True
                                        ex_reason = "TAKE_PROFIT_LIMIT_FILLED"
                                        if pos.exit_fill_price is None:
                                            pos.exit_fill_price = take_profit_limit_price(pos)
                                        pnl_ticks = take_profit_filled_ticks(pos)
                                    elif not cancel_ok:
                                        exit_confirmed = False
                                        status.live_state = "RECOVERING"
                                        storage.log("ERROR", "LIVE_EXIT_FAIL", "TAKE_PROFIT_CANCEL_FAILED before protective market exit")

                                if exit_confirmed and live_tp_already_filled:
                                    status.exit_fail_count = 0
                                    status.live_state = "FLAT"
                                    status.open_position = None
                                    status.pending_exit = False
                                    status.pending_add = False
                                    storage.log("INFO", "LIVE_TAKE_PROFIT_FILLED", f"{pos.side} order_id={pos.take_profit_order_id or pos.exit_order_id}")
                                    storage.insert_execution_fill_price(
                                        "EXIT_FILL_PRICE",
                                        pos.take_profit_order_id or pos.exit_order_id or "",
                                        pos.side,
                                        pos.strategy,
                                        ex_reason,
                                        pos.exit_fill_price,
                                    )
                                elif exit_confirmed:
                                    result = execute_live_exit(
                                        client,
                                        config,
                                        pos.side,
                                        storage,
                                        pos,
                                        p,
                                        status,
                                        latest_snapshot=snap,
                                        force_marketable_limit=(pos.strategy == "RSI9"),
                                        force_market_order=False,
                                        exit_signal_ts=exit_signal_ts,
                                        signal_price=exit_signal_price,
                                        pnl_ticks=pnl_ticks,
                                        ma5_exit_context=ma5_exit_context,
                                    )
                                    if not result.ok:
                                        exit_confirmed = False
                                        status.exit_fail_count += 1
                                        status.last_error_code = result.api_code
                                        status.last_error_message = result.message
                                        storage.log("ERROR", "LIVE_EXIT_FAIL", result.message)
                                        status.live_state = "EXIT_VERIFYING"
                                        rec = reconcile_live_position(client, config, status, storage, expected_side=pos.side, reason=f"EXIT_FAIL:{result.message}", ts=f.ts, expected_margin_trade_type=pos.margin_trade_type)
                                        if rec.total_leaves_qty == 0 and status.open_position is None:
                                            storage.log("WARN", "LIVE_EXIT_SYNC_FLAT", f"reason={result.message}")
                                        elif rec.message == "MATCHED_OPEN":
                                            status.live_state = "OPEN"
                                            storage.log("WARN", "LIVE_EXIT_RETRY_READY", f"reason={result.message} actual_qty={rec.matching_leaves_qty}")
                                        else:
                                            status.live_state = "RECOVERING"
                                            storage.log_structured(
                                                "ERROR",
                                                "RECOVERY_ENTER",
                                                {
                                                    "reason": "EXIT_FAIL_UNRECONCILED",
                                                    "exit_fail_count": status.exit_fail_count,
                                                    "last_error_code": status.last_error_code,
                                                    "last_error_message": status.last_error_message,
                                                    "reconcile_result": asdict(rec),
                                                    "internal_state": position_state_payload(status.open_position),
                                                },
                                                mirror_message=f"exit_fail_count={status.exit_fail_count} reason={result.message}",
                                            )
                                    else:
                                        pos.exit_order_id = result.order_id
                                        exit_actual_fill_price = result.actual_fill_price
                                        if result.actual_fill_price is not None:
                                            pos.exit_fill_price = result.actual_fill_price
                                            exit_fill_source = result.fill_source or "ORDER_DETAIL"
                                        else:
                                            pos.exit_fill_price = f.price
                                            exit_fill_source = "SIGNAL_PRICE_FALLBACK" if result.fill_source in {"UNAVAILABLE", "ORDER_LIMIT_PRICE_FALLBACK", "ORDER_ID_MISSING", ""} else f"{result.fill_source}_FALLBACK"
                                        status.exit_fail_count = 0
                                        storage.log("INFO", "LIVE_EXIT_OK", f"{pos.side} order_id={result.order_id}")
                                        rem_qty = get_open_position_qty(client, config, pos.side, margin_trade_type=pos.margin_trade_type)
                                        if rem_qty <= 0:
                                            status.live_state = "FLAT"
                                            storage.log("INFO", "EXIT_FULLY_FILLED", f"side={pos.side}")
                                        else:
                                            pos.filled_qty = int(rem_qty)
                                            pos.remaining_qty = 0
                                            status.open_position = pos
                                            status.live_state = "RECOVERING"
                                            exit_confirmed = False
                                            storage.log("WARN", "EXIT_PARTIAL_REMAINING", f"side={pos.side} remaining_qty={rem_qty}")
                                        storage.insert_execution_fill_price(
                                            "EXIT_FILL_PRICE",
                                            result.order_id,
                                            pos.side,
                                            pos.strategy,
                                            ex_reason,
                                            exit_actual_fill_price,
                                        )
                                        storage.log_structured(
                                            "INFO",
                                            "EXIT_FILL_DETAIL_RECORDED",
                                            {
                                                "order_id": result.order_id,
                                                "exit_signal_price": exit_signal_price,
                                                "exit_order_limit_price": result.limit_price,
                                                "exit_actual_fill_price": exit_actual_fill_price,
                                                "exit_fill_source": exit_fill_source,
                                                "exit_signal_ts": exit_signal_ts.isoformat(),
                                                "exit_order_send_ts": result.order_send_ts.isoformat() if result.order_send_ts else None,
                                                "exit_order_response_ts": result.order_response_ts.isoformat() if result.order_response_ts else None,
                                            },
                                        )
                                else:
                                    exit_fill_source = "PAPER_SIGNAL_PRICE"
                                    pos.exit_fill_price = f.price

                            if exit_confirmed:
                                holding_sec = (f.ts - pos.entry_ts).total_seconds()
                                if pos.entry_fill_price is not None and pos.exit_fill_price is not None:
                                    actual_pnl_ticks = price_to_ticks(
                                        pos.exit_fill_price - pos.entry_fill_price,
                                        pos.entry_fill_price,
                                    )
                                    pnl_ticks = actual_pnl_ticks if pos.side == "LONG" else -actual_pnl_ticks
                                storage.insert_trade(
                                    entry_ts=pos.entry_ts.isoformat(),
                                    exit_ts=f.ts.isoformat(),
                                    entry_side=pos.side,
                                    strategy=pos.strategy,
                                    entry_price=pos.entry_fill_price if pos.entry_fill_price is not None else pos.entry_price,
                                    exit_price=pos.exit_fill_price if pos.exit_fill_price is not None else f.price,
                                    pnl_ticks=pnl_ticks,
                                    holding_sec=holding_sec,
                                    exit_reason=ex_reason,
                                    mfe_ticks=mfe_ticks,
                                    mae_ticks=mae_ticks,
                                    exit_signal_price=exit_signal_price,
                                    exit_order_limit_price=result.limit_price if result is not None else None,
                                    exit_actual_fill_price=exit_actual_fill_price if config["live_mode"] else None,
                                    exit_fill_source=exit_fill_source,
                                    exit_signal_ts=exit_signal_ts.isoformat(),
                                    exit_order_send_ts=result.order_send_ts.isoformat() if result is not None and result.order_send_ts else None,
                                    exit_order_response_ts=result.order_response_ts.isoformat() if result is not None and result.order_response_ts else None,
                                    exit_decision_to_send_ms=(result.order_send_ts - exit_signal_ts).total_seconds() * 1000.0 if result is not None and result.order_send_ts else None,
                                    exit_order_id=result.order_id if result is not None else None,
                                )
                                closed_trades.append(
                                    ClosedTradeSummary(
                                        exit_ts=f.ts,
                                        side=pos.side,
                                        strategy=pos.strategy,
                                        pnl_ticks=pnl_ticks,
                                        exit_reason=ex_reason,
                                        entry_vwap_mode=pos.entry_vwap_mode,
                                    )
                                )
                                apply_light_loss_brake(adaptive, closed_trades, f, storage)
                                if ex_reason in {"STOP_LOSS", "EDGE_BREAK_HARD", "HARD_STOP_LOSS"}:
                                    status.reentry_block_until_by_side[pos.side] = f.ts + timedelta(
                                        seconds=REENTRY_AFTER_STOP_SEC
                                    )
                                else:
                                    status.reentry_block_until_by_side[pos.side] = f.ts + timedelta(
                                        seconds=ENTRY_COOLDOWN_SEC
                                    )
                                status.open_position = None


            if not status.midday_written and tstr >= "11:30:00":
                generate_report(db_path, midday_report_path, midday=True)
                storage.log(
                    "INFO",
                    "MIDDAY_REPORT",
                    f"midday report written: {os.path.basename(midday_report_path)}",
                )
                status.midday_written = True

            if now_ >= next_status_write:
                write_latest_status(outdir, status, last_feature, last_pred, storage, last_gate)
                next_status_write = now_ + timedelta(minutes=10)

            if now_ >= next_console_status:
                if last_feature and last_pred:
                    gate_text = ""
                    if last_gate and bool(volatility_gate.logging.get("print_gate_reason", True)):
                        gf = last_gate.features
                        gate_text = (
                            f" gate={last_gate.action} applied={last_gate.applied} final={last_gate.final_signal} "
                            f"regime={last_gate.regime} gate_reason={last_gate.reason} "
                            f"spread_ticks={gf.spread_ticks:.2f} vol_ratio={gf.volume_ratio:.2f} "
                            f"rv1m={gf.realized_vol_1m:.6f} rv5m={gf.realized_vol_5m:.6f} "
                            f"board={gf.board_imbalance:.2f} price_vs_vwap={gf.price_vs_vwap:.5f}"
                        )
                    print(
                        f"[{now_.strftime('%H:%M:%S')}] count={status.count} "
                        f"price={last_feature.price} p_up_1m={last_pred.p_up_1m*100:.1f}% "
                        f"p_up_3m={last_pred.p_up_3m*100:.1f}% signal={last_pred.signal} "
                        f"reason={last_pred.reason_1}{gate_text}",
                        flush=True,
                    )
                else:
                    print(
                        f"[{now_.strftime('%H:%M:%S')}] count={status.count} waiting_for_features",
                        flush=True,
                    )
                next_console_status = now_ + timedelta(
                    seconds=max(float(config.get("console_status_interval_sec", CONSOLE_STATUS_INTERVAL_SEC)), 1.0)
                )

            time.sleep(config["poll_interval_sec"])
        except KeyboardInterrupt:
            storage.log("INFO", "STOP", "keyboard interrupt")
            force_close_open_position(client, config, storage, status, "KEYBOARD_INTERRUPT", now_jst(), last_pred, latest_snapshot=last_snapshot, use_market_order=True)
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            storage.log("ERROR", "LOOP_ERROR", str(e))
            if "401" in str(e) or "Unauthorized" in str(e):
                try:
                    client.get_token(config["api_password"])
                    storage.log("INFO", "TOKEN_REFRESH", "token refreshed")
                    client.register_symbol(config["symbol"], config["exchange"])
                except Exception as e2:
                    storage.log("ERROR", "TOKEN_REFRESH_FAIL", str(e2))
            time.sleep(3)

    b1 = rb1.force_finalize()
    if b1:
        storage.insert_bar("bars_1m", b1)
    b3 = rb3.force_finalize()
    if b3:
        storage.insert_bar("bars_3m", b3)
    storage.log("INFO", "STOP", "monitor stop requested")
    generate_report(db_path, daily_report_path, midday=False)
    return db_path, daily_report_path


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    apply_runtime_threshold_overrides(cfg)
    config = {
        "api_password": args.api_password or cfg.get("api_password") or API_PASSWORD_HARDCODED,
        "live_mode": (False if args.paper_mode else bool(args.live_mode or cfg.get("live_mode", True))),
        "order_password": args.order_password or cfg.get("order_password") or args.api_password or cfg.get("api_password") or API_PASSWORD_HARDCODED,
        "order_qty": int(args.order_qty if args.order_qty is not None else cfg.get("order_qty", 2)),
        "entry_min_fill_qty": int(cfg.get("entry_min_fill_qty", (args.order_qty if args.order_qty is not None else cfg.get("order_qty", 2)))),
        "account_type": int(args.account_type if args.account_type is not None else cfg.get("account_type", 4)),
        "margin_trade_type": int(args.margin_trade_type if args.margin_trade_type is not None else cfg.get("margin_trade_type", 3)),
        "margin_trade_type_long": int(cfg.get("margin_trade_type_long", args.margin_trade_type if args.margin_trade_type is not None else cfg.get("margin_trade_type", 3))),
        "margin_trade_type_short": int(cfg.get("margin_trade_type_short", 1)),
        "entry_cash_margin": int(args.entry_cash_margin if args.entry_cash_margin is not None else cfg.get("entry_cash_margin", 2)),
        "exit_cash_margin": int(args.exit_cash_margin if args.exit_cash_margin is not None else cfg.get("exit_cash_margin", 3)),
        "entry_deliv_type": int(args.entry_deliv_type if args.entry_deliv_type is not None else cfg.get("entry_deliv_type", 0)),
        "exit_deliv_type": int(args.exit_deliv_type if args.exit_deliv_type is not None else cfg.get("exit_deliv_type", 2)),
        "entry_front_order_type": int(cfg.get("entry_front_order_type", 10)),
        "exit_front_order_type": int(cfg.get("exit_front_order_type", 10)),
        "entry_price": float(cfg.get("entry_price", 0)),
        "exit_price": float(cfg.get("exit_price", 0)),
        "expire_day": int(cfg.get("expire_day", 0)),
        "live_entry_timeout_sec": int(args.live_entry_timeout_sec if args.live_entry_timeout_sec is not None else cfg.get("live_entry_timeout_sec", LIVE_ENTRY_TIMEOUT_SEC)),
        "live_exit_timeout_sec": int(args.live_exit_timeout_sec if args.live_exit_timeout_sec is not None else cfg.get("live_exit_timeout_sec", LIVE_EXIT_TIMEOUT_SEC)),
        "live_retry_max": int(args.live_retry_max if args.live_retry_max is not None else cfg.get("live_retry_max", LIVE_RETRY_MAX)),
        "entry_error_block_sec": int(cfg.get("entry_error_block_sec", ENTRY_ERROR_BLOCK_SEC)),
        "recovery_cooldown_sec": int(cfg.get("recovery_cooldown_sec", RECOVERY_COOLDOWN_SEC)),
        "adaptive_control": bool(cfg.get("adaptive_control", True)) and not bool(args.disable_adaptive_control),
        "initial_vwap_mode": args.initial_vwap_mode or cfg.get("initial_vwap_mode", "2x"),
        "outdir": args.outdir or cfg.get("outdir", "monitor_output"),
        "runtime_minutes": args.runtime_minutes if args.runtime_minutes is not None else cfg.get("runtime_minutes"),
        "base_url": args.base_url or cfg.get("base_url", API_BASE_DEFAULT),
        "symbol": cfg.get("symbol", SYMBOL_DEFAULT),
        "exchange": int(cfg.get("exchange", EXCHANGE_DEFAULT)),
        "order_exchange": int(cfg.get("order_exchange", cfg.get("exchange", EXCHANGE_DEFAULT))),
        "exit_order_exchange": int(cfg.get("exit_order_exchange", cfg.get("exchange", EXCHANGE_DEFAULT))),
        "margin_entry_exchange": cfg.get("margin_entry_exchange"),
        "poll_interval_sec": float(cfg.get("poll_interval_sec", POLL_INTERVAL_SEC)),
        "console_status_interval_sec": float(cfg.get("console_status_interval_sec", CONSOLE_STATUS_INTERVAL_SEC)),
        "live_entry_overrides_long": cfg.get("live_entry_overrides_long", {}),
        "live_entry_overrides_short": cfg.get("live_entry_overrides_short", {}),
        "live_exit_overrides": cfg.get("live_exit_overrides", {}),
        "volatility_regime_gate": cfg.get("volatility_regime_gate", {}),
        "entry_execution": cfg.get("entry_execution", {}),
        "take_profit_execution": cfg.get("take_profit_execution", {"enabled": True, "fallback_market_after_signal_sec": 5.0}),
        "scalping": cfg.get("scalping", {"enabled": SCALPING_ENABLED}),
    }
    afternoon_trade_start = str(cfg.get("afternoon_trade_start", "12:30:00"))
    globals()["TRADE_WINDOWS"] = [
        ("09:03:00", "11:25:00"),
        (afternoon_trade_start, "15:20:00"),
    ]

    if not config["api_password"]:
        raise SystemExit(
            "API password is required. Set API_PASSWORD_HARDCODED at the top, "
            "use --api-password, or config file."
        )
    if config["live_mode"] and not config["order_password"]:
        raise SystemExit("order_password is required in live_mode.")
    if config["live_mode"] and int(config["order_qty"]) <= 0:
        raise SystemExit("order_qty must be > 0 in live_mode.")
    if config["live_mode"] and int(config["entry_min_fill_qty"]) <= 0:
        raise SystemExit("entry_min_fill_qty must be > 0 in live_mode.")
    if config["live_mode"] and int(config["entry_min_fill_qty"]) > int(config["order_qty"]):
        raise SystemExit("entry_min_fill_qty must be <= order_qty in live_mode.")
    if config["live_mode"] and int(config["order_qty"]) != 2:
        raise SystemExit("2lot-ready mode requires order_qty == 2 in live_mode.")
    if config["live_mode"] and int(config["entry_min_fill_qty"]) != 2:
        raise SystemExit("2lot-ready mode requires entry_min_fill_qty == 2 in live_mode.")
    if config["live_mode"] and int(config["live_entry_timeout_sec"]) <= 0:
        raise SystemExit("live_entry_timeout_sec must be > 0 in live_mode.")
    if config["live_mode"] and int(config["live_exit_timeout_sec"]) <= 0:
        raise SystemExit("live_exit_timeout_sec must be > 0 in live_mode.")
    if config["live_mode"] and int(config["entry_front_order_type"]) <= 0:
        raise SystemExit("entry_front_order_type must be > 0 in live_mode.")
    if config["live_mode"] and int(config["exit_front_order_type"]) <= 0:
        raise SystemExit("exit_front_order_type must be > 0 in live_mode.")
    normalize_exchange_for_margin(config)
    ensure_dir(config["outdir"])
    print(
        f"Starting monitor: config={args.config or '(none)'} symbol={config['symbol']} exchange={config['exchange']} "
        f"order_exchange={order_exchange(config)} exit_exchange={exit_exchange(config)} "
        f"outdir={config['outdir']} live_mode={config['live_mode']} order_qty={config['order_qty']} "
        f"adaptive_control={config['adaptive_control']} initial_vwap_mode={config['initial_vwap_mode']} "
        f"prob_upper_1m={PROB_UPPER_1M:.2f} prob_upper_3m={PROB_UPPER_3M:.2f} "
        f"scalping={'on' if SCALPING_ENABLED else 'off'} "
        f"gate_mode={config.get('volatility_regime_gate', {}).get('mode', 'off')} "
        f"entry_exec={config.get('entry_execution', {}).get('mode', 'market') if config.get('entry_execution', {}).get('enabled', False) else 'market'}",
        flush=True,
    )
    db_path, report_path = run_monitor(config)
    print(f"DB saved: {os.path.relpath(db_path)}")
    print(f"Report saved: {os.path.relpath(report_path)}")


if __name__ == "__main__":
    main()
