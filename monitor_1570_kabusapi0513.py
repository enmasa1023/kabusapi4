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
from dataclasses import asdict, dataclass
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

# ===== user-editable direct settings =====
API_PASSWORD_HARDCODED = "enmasa1023"  # ここにAPIパスワードを入れる
# =======================================


TRADE_WINDOWS = [
    ("09:03:00", "11:25:00"),
    ("12:35:00", "15:20:00"),
]
STOP_AFTER = "15:30:00"
FORCE_CLOSE_AFTER = "15:20:00"

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
    take_profit_trigger_ts: Optional[datetime] = None
    entry_fill_price: Optional[float] = None
    exit_fill_price: Optional[float] = None
    rsi_special_entry: bool = False
    rsi_special_tp_stage: int = 0
    rsi_special_tp_order_ts: Optional[datetime] = None


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
              mfe_ticks REAL, mae_ticks REAL
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
                """INSERT INTO paper_trades(entry_ts,exit_ts,entry_side,strategy,entry_price,exit_price,pnl_ticks,holding_sec,exit_reason,mfe_ticks,mae_ticks)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
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
            return self._decorate_bar(bar)
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


def build_rsi9_prediction(bar1: Optional[Bar], history: list[Bar], open_pos: Optional[PositionState]) -> Optional[PredictionSnapshot]:
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

    if open_pos is None and not in_no_entry:
        ma_ok = (bar1.ma5 is not None and bar1.ma25 is not None and bar1.ma75 is not None)
        if ma_ok:
            long_ma = bar1.ma75 > bar1.ma25 > bar1.ma5
            short_ma = bar1.ma5 > bar1.ma25 > bar1.ma75
            if long_ma and rsi_now <= RSI9_LONG_ENTRY and rsi_prev <= RSI9_LONG_ENTRY:
                signal, side = "LONG_CANDIDATE", "LONG"
                entry_rule = "long_a"
            rsi_prev2 = rsi9_wilder(closes[:-2], RSI9_PERIOD) if len(closes) > RSI9_PERIOD + 2 else None
            all_ma_below = (bar1.close <= (bar1.ma5 or -1e18)) and (bar1.close <= (bar1.ma25 or -1e18)) and (bar1.close <= (bar1.ma75 or -1e18))
            if rsi_prev2 is not None and (rsi_prev2 - rsi_now) >= 17.0 and (not all_ma_below):
                signal, side = "LONG_CANDIDATE", "LONG"
                entry_rule = "long_b_drop"
            elif False and short_ma and rsi_now >= RSI9_SHORT_ENTRY and rsi_prev >= RSI9_SHORT_ENTRY:
                signal, side = "SHORT_CANDIDATE", "SHORT"
                entry_rule = "short_frozen"
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
    if state.live_state == "RECOVERING":
        return False, "RECOVERING"
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
) -> PositionState:
    is_long = pred.signal == "LONG_CANDIDATE"
    side = "LONG" if is_long else "SHORT"
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
        rsi_special_entry=(pred.reason_3 == "long_b_drop"),
    )


def should_exit(pos: PositionState, f: FeatureSnapshot, pred: PredictionSnapshot) -> tuple[bool, str, float]:
    elapsed = (f.ts - pos.entry_ts).total_seconds()
    if pos.strategy == "RSI9":
        rsi = None
        if pred.reason_2.startswith("rsi9="):
            try:
                rsi = float(pred.reason_2.split("=", 1)[1])
            except Exception:
                rsi = None
        if rsi is not None:
            if pos.side == "LONG":
                if (not pos.rsi_special_entry) and rsi >= RSI9_LONG_TP:
                    return True, "TAKE_PROFIT", 0.0
            else:
                if rsi <= RSI9_SHORT_TP:
                    return True, "TAKE_PROFIT", 0.0
        # RSI9 is TP-only by design: disable all generic market-stop/time-stop exits.
        return False, "HOLD", 0.0

    pnl_ticks = price_to_ticks(f.price - pos.entry_price, pos.entry_price)
    if pos.side == "SHORT":
        pnl_ticks = -pnl_ticks

    if pnl_ticks <= -pos.stop_ticks:
        return True, "STOP_LOSS", pnl_ticks
    if pnl_ticks >= pos.take_ticks:
        return True, "TAKE_PROFIT", pnl_ticks

    if elapsed < pos.min_hold_sec:
        if (
            pos.side == "LONG"
            and pnl_ticks <= -pos.stop_ticks
            and f.price < f.vwap
            and f.obi_l1 < -0.18
            and pred.p_down_1m > 0.67
        ):
            return True, "EDGE_BREAK_HARD", pnl_ticks
        if (
            pos.side == "SHORT"
            and pnl_ticks <= -pos.stop_ticks
            and f.price > f.vwap
            and f.obi_l1 > 0.18
            and pred.p_up_1m > 0.67
        ):
            return True, "EDGE_BREAK_HARD", pnl_ticks
        return False, "MIN_HOLD", pnl_ticks

    if pos.side == "LONG":
        if (
            pnl_ticks >= 1
            and pred.p_up_1m < PROB_EXIT_EDGE - 0.02
            and f.price < f.vwap
            and f.obi_l1 < -0.03
        ):
            return True, "EDGE_DECAY", pnl_ticks
    else:
        if (
            pnl_ticks >= 1
            and pred.p_down_1m < PROB_EXIT_EDGE - 0.02
            and f.price > f.vwap
            and f.obi_l1 > 0.03
        ):
            return True, "EDGE_DECAY", pnl_ticks

    if elapsed >= pos.max_hold_sec:
        return True, "TIME_STOP", pnl_ticks

    return False, "HOLD", pnl_ticks


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


def wait_for_position_unlocked_or_flat(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    margin_trade_type: Optional[int] = None,
    timeout_sec: float = 5.0,
) -> tuple[bool, bool, int, int, int, list[dict[str, Any]]]:
    start = time.time()
    last_positions: list[dict[str, Any]] = []
    last_leaves = 0
    last_hold = 0
    last_available = 0
    while time.time() - start <= timeout_sec:
        last_positions = fetch_positions(client, config)
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
) -> dict[str, Any]:
    order_password = config["order_password"]
    qty = int(config["order_qty"])
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
    order_id = pos.take_profit_order_id
    if not order_id:
        return True, False
    cancel_payload = {"OrderId": order_id}
    storage.log_structured("WARN", "TAKE_PROFIT_CANCEL_REQUEST", {**context, "order_id": order_id, "request_json": cancel_payload})
    try:
        cres = client.cancel_order(order_id, config["order_password"])
        storage.log_structured("WARN", "TAKE_PROFIT_CANCEL_RESPONSE", {**context, "order_id": order_id, "raw_response_json": cres})
        pos.take_profit_order_id = None
        unlocked, filled, leaves, hold, available, positions = wait_for_position_unlocked_or_flat(
            client,
            config,
            pos.side,
            margin_trade_type=pos.margin_trade_type,
            timeout_sec=float(config.get("take_profit_cancel_verify_sec", 5.0)),
        )
        storage.log_structured(
            "INFO" if unlocked else "WARN",
            "TAKE_PROFIT_CANCEL_VERIFY",
            {
                **context,
                "order_id": order_id,
                "unlocked": unlocked,
                "filled": filled,
                "leaves_qty": leaves,
                "hold_qty": hold,
                "available_qty": available,
                "positions_json": positions,
            },
        )
        return unlocked, filled
    except Exception as e:
        ep = api_error_payload(e)
        code = str(ep.get("api_code") or "")
        storage.log_structured("WARN", "TAKE_PROFIT_CANCEL_FAIL", {**context, "order_id": order_id, **ep}, mirror_message=f"TAKE_PROFIT_CANCEL_FAIL order_id={order_id} Code={code} Message={ep.get('api_message')}")
        unlocked, filled, leaves, hold, available, positions = wait_for_position_unlocked_or_flat(
            client,
            config,
            pos.side,
            margin_trade_type=pos.margin_trade_type,
            timeout_sec=float(config.get("take_profit_cancel_verify_sec", 5.0)),
        )
        storage.log_structured(
            "INFO" if unlocked else "WARN",
            "TAKE_PROFIT_CANCEL_FAIL_VERIFY",
            {
                **context,
                "order_id": order_id,
                "api_code": code,
                "unlocked": unlocked,
                "filled": filled,
                "leaves_qty": leaves,
                "hold_qty": hold,
                "available_qty": available,
                "positions_json": positions,
            },
        )
        if unlocked:
            pos.take_profit_order_id = None
            return True, filled
        return False, False


def place_take_profit_limit_order(
    client: KabuApiClient,
    config: dict[str, Any],
    storage: Storage,
    pos: PositionState,
    pred: PredictionSnapshot,
    status: MonitorStatus,
) -> LiveOrderResult:
    context = order_context(config, pos.side, pos, pred, status)
    positions = fetch_positions(client, config, storage, reason="TAKE_PROFIT_BUILD_CLOSE_POSITIONS")
    close_position_groups = close_position_groups_for_side(
        positions,
        pos.side,
        margin_trade_type=pos.margin_trade_type,
        default_exchange=exit_exchange(config),
    )
    if not close_position_groups:
        return LiveOrderResult(False, "TAKE_PROFIT_NO_MATCHING_OPEN_POSITION", recoverable=True)
    if len(close_position_groups) > 1:
        storage.log_structured(
            "WARN",
            "TAKE_PROFIT_MULTI_EXCHANGE_UNSUPPORTED",
            {
                **context,
                "close_position_groups": [
                    {"exchange": exchange, "close_positions": close_positions, "total_qty": total_qty}
                    for exchange, close_positions, total_qty in close_position_groups
                ],
                "positions_json": positions,
            },
        )
        return LiveOrderResult(False, "TAKE_PROFIT_MULTI_EXCHANGE_UNSUPPORTED", recoverable=True)
    position_exchange, close_positions, total_qty = close_position_groups[0]
    if total_qty <= 0 or not close_positions:
        return LiveOrderResult(False, "TAKE_PROFIT_NO_MATCHING_OPEN_POSITION", recoverable=True)
    limit_price = take_profit_limit_price(pos)
    payload = build_exit_order_payload(
        config,
        pos.side,
        close_positions=close_positions,
        qty=total_qty,
        front_order_type=20,
        price=limit_price,
        margin_trade_type=pos.margin_trade_type,
        exchange=position_exchange,
    )
    storage.log_structured(
        "INFO",
        "TAKE_PROFIT_ORDER_REQUEST",
        {**context, "position_exchange": position_exchange, "request_json": payload, "limit_price": limit_price, "positions_json": positions},
    )
    try:
        res = client.send_order(payload)
    except Exception as e:
        ep = api_error_payload(e)
        code = str(ep.get("api_code") or "")
        storage.log_structured("ERROR", "TAKE_PROFIT_ORDER_FAIL", {**context, **ep, "request_json": payload, "positions_json": positions}, mirror_message=f"TAKE_PROFIT_SEND_ERROR Code={code} Message={ep.get('api_message')}")
        return LiveOrderResult(False, f"TAKE_PROFIT_SEND_ERROR Code={code} Message={ep.get('api_message')}: {ep.get('raw_error')}", api_code=code, api_message=str(ep.get("api_message") or ""), recoverable=True)
    order_id = str(res.get("OrderId") or res.get("OrderID") or "")
    storage.log_structured("INFO", "TAKE_PROFIT_ORDER_RESPONSE", {**context, "order_id": order_id, "raw_response_json": res})
    if not order_id:
        return LiveOrderResult(False, f"TAKE_PROFIT_ORDER_ID_MISSING: {res}", recoverable=True)
    return LiveOrderResult(True, order_id, order_id=order_id)


def verify_position_after_entry_cancel(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    target_qty: int,
    storage: Storage,
    context: dict[str, Any],
    order_id: str,
) -> bool:
    positions = fetch_positions(client, config, storage, reason="ENTRY_CANCEL_VERIFY")
    total_qty, matching_qty = summarize_positions(positions, expected_side=side, expected_margin_trade_type=margin_trade_type_for_side(config, side))
    storage.log_structured(
        "INFO",
        "ENTRY_POSITION_AFTER_CANCEL",
        {
            **context,
            "order_id": order_id,
            "target_qty": target_qty,
            "total_leaves_qty": total_qty,
            "matching_leaves_qty": matching_qty,
            "raw_positions_json": positions,
        },
    )
    return matching_qty >= max(target_qty, 1)


def execute_live_entry(
    client: KabuApiClient,
    config: dict[str, Any],
    side: str,
    storage: Storage,
    candidate: PositionState,
    pred: PredictionSnapshot,
    status: MonitorStatus,
    latest_snapshot: Optional[TickSnapshot] = None,
) -> LiveOrderResult:
    entry_exec = entry_execution_config(config)
    limit_entry = use_limit_entry(config)
    retries = max(int(config.get("live_retry_max", LIVE_RETRY_MAX)), 0)
    if limit_entry:
        retries = max(int(entry_exec.get("max_reprice_attempts", 0)), 0)
    target_qty = int(config.get("entry_min_fill_qty", config["order_qty"]))
    timeout_sec = int(config.get("live_entry_timeout_sec", LIVE_ENTRY_TIMEOUT_SEC))
    if limit_entry:
        timeout_sec = max(float(entry_exec.get("timeout_sec", timeout_sec)), 0.5)
    context = {
        **order_context(config, side, candidate, pred, status),
        "entry_execution_mode": entry_exec.get("mode", "market"),
        "entry_limit_mode": entry_exec.get("limit_mode", ""),
    }

    last = LiveOrderResult(False, "ENTRY_UNKNOWN_ERROR")
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
        payload = build_entry_order_payload(config, side, front_order_type=front_order_type, price=limit_price)
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
                client, config, side, target_qty, storage, context, order_id
            ):
                return LiveOrderResult(True, order_id, order_id=order_id)
        except Exception as e:
            ep = api_error_payload(e)
            storage.log_structured("WARN", "CANCEL_ORDER_FAIL", {**context, "order_id": order_id, **ep}, mirror_message=f"ENTRY_CANCEL_FAIL order_id={order_id} Code={ep.get('api_code')} Message={ep.get('api_message')}")
            if str(ep.get("api_code") or "") == "43" and wait_for_position_qty(client, config, side, target_qty=max(target_qty, 1), timeout_sec=timeout_sec, comparator="ge", margin_trade_type=margin_trade_type_for_side(config, side)):
                return LiveOrderResult(True, order_id, order_id=order_id)
            if bool(entry_exec.get("verify_position_after_cancel", True)) and verify_position_after_entry_cancel(
                client, config, side, target_qty, storage, context, order_id
            ):
                return LiveOrderResult(True, order_id, order_id=order_id)
    return last


def close_position_groups_for_side(
    positions: list[dict[str, Any]],
    side: str,
    margin_trade_type: Optional[int] = None,
    available_only: bool = False,
    default_exchange: int = 0,
) -> list[tuple[int, list[dict[str, Any]], int]]:
    qty_by_exchange_hold_id: dict[int, dict[str, int]] = {}
    for p in positions:
        if not position_matches(p, side=side, margin_trade_type=margin_trade_type):
            continue
        leaves = position_leaves_qty(p)
        hold = min(position_hold_qty(p), leaves)
        qty = max(leaves - hold, 0) if available_only else leaves
        if qty <= 0:
            continue
        hold_id = str(p.get("ExecutionID") or "")
        if not hold_id:
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
) -> LiveOrderResult:
    retries = max(int(config.get("live_retry_max", LIVE_RETRY_MAX)), 0)
    timeout_sec = int(config.get("live_exit_timeout_sec", LIVE_EXIT_TIMEOUT_SEC))
    context = order_context(config, side, pos, pred, status)
    last = LiveOrderResult(False, "EXIT_UNKNOWN_ERROR", recoverable=True)

    for attempt in range(retries + 1):
        positions = fetch_positions(client, config, storage, reason="EXIT_BUILD_CLOSE_POSITIONS")
        close_position_groups = close_position_groups_for_side(
            positions,
            side,
            margin_trade_type=pos.margin_trade_type,
            available_only=True,
            default_exchange=exit_exchange(config),
        )
        total_qty = sum(group_total_qty for _, _, group_total_qty in close_position_groups)
        leaves_qty, hold_qty, available_qty = get_matching_position_quantities(
            positions,
            side,
            margin_trade_type=pos.margin_trade_type,
        )
        if not positions:
            return LiveOrderResult(False, "NO_POSITIONS_FOR_EXIT", recoverable=True)
        if leaves_qty <= 0:
            return LiveOrderResult(False, "NO_MATCHING_OPEN_POSITION", recoverable=True)
        if total_qty <= 0 or not close_position_groups:
            return LiveOrderResult(
                False,
                f"NO_AVAILABLE_OPEN_POSITION leaves={leaves_qty} hold={hold_qty} available={available_qty}",
                recoverable=True,
            )

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
                    raw_board = client.get_board(str(config.get("symbol", SYMBOL_DEFAULT)), int(config.get("exchange", EXCHANGE_DEFAULT)))
                    refreshed_snapshot = extract_snapshot(raw_board)
                    latest_snapshot = refreshed_snapshot
                except Exception as e:
                    storage.log("WARN", "EXIT_BOARD_REFRESH_FAILED", f"attempt={attempt+1} side={side} strategy={pos.strategy} error={e}")
                limit_price = marketable_exit_limit_price(pos, refreshed_snapshot)
                exit_order_mode = "marketable_limit"
                if limit_price is None or limit_price <= 0:
                    return LiveOrderResult(False, "MARKETABLE_EXIT_LIMIT_PRICE_UNAVAILABLE", recoverable=True)
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
                "EXIT_ORDER_REQUEST",
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
                res = client.send_order(payload)
            except Exception as e:
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
                return LiveOrderResult(False, f"EXIT_SEND_ERROR Code={code} Message={ep.get('api_message')}: {ep.get('raw_error')}", api_code=code, api_message=str(ep.get("api_message") or ""), recoverable=(code == "8"))
            order_id = str(res.get("OrderId") or res.get("OrderID") or "")
            storage.log_structured(
                "INFO",
                "EXIT_ORDER_RESPONSE",
                {**context, "attempt": attempt + 1, "position_exchange": position_exchange, "order_id": order_id, "raw_response_json": res},
            )
            if not order_id:
                last = LiveOrderResult(False, f"EXIT_ORDER_ID_MISSING: {res}", recoverable=True)
                break
            order_ids.append(order_id)
        if not order_ids:
            continue
        combined_order_id = ",".join(order_ids)
        if wait_for_position_qty(client, config, side, target_qty=0, timeout_sec=timeout_sec, comparator="eq", margin_trade_type=pos.margin_trade_type):
            return LiveOrderResult(True, combined_order_id, order_id=combined_order_id)
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
                    if wait_for_position_qty(client, config, side, target_qty=0, timeout_sec=timeout_sec, comparator="eq", margin_trade_type=pos.margin_trade_type):
                        return LiveOrderResult(True, order_id, order_id=order_id, api_code=code, api_message=str(ep.get("api_message") or ""))
                    last = LiveOrderResult(False, f"EXIT_CANCEL_ALREADY_FILLED_VERIFY_POSITION order_id={order_id}", order_id=order_id, api_code=code, api_message=str(ep.get("api_message") or ""), recoverable=True)
                    break
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
    if pos.take_profit_order_id:
        cancel_ok, filled_during_cancel = cancel_pending_take_profit_order(client, config, storage, pos, context)
        if filled_during_cancel:
            status.exit_fail_count = 0
            status.live_state = "FLAT"
            status.open_position = None
            storage.log("WARN", "FORCE_EXIT_TAKE_PROFIT_FILLED", f"reason={reason} side={pos.side}")
            return
        if not cancel_ok:
            status.live_state = "RECOVERING"
            storage.log("ERROR", "FORCE_EXIT_CANCEL_TAKE_PROFIT_FAIL", f"reason={reason} side={pos.side}")
            return

    status.live_state = "EXIT_SENT"
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
        storage.log("WARN", "FORCE_EXIT_OK", f"reason={reason} side={pos.side} order_id={result.order_id}")
        return

    status.exit_fail_count += 1
    status.last_error_code = result.api_code
    status.last_error_message = result.message
    status.live_state = "EXIT_VERIFYING"
    storage.log("ERROR", "FORCE_EXIT_FAIL", f"reason={reason} side={pos.side} message={result.message}")
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
    total_qty, matching_qty = summarize_positions(
        positions,
        expected_side or (status.open_position.side if status.open_position else None),
        expected_margin_trade_type=expected_margin,
    )
    payload_base = {
        "reason": reason,
        "expected_side": expected_side,
        "expected_margin_trade_type": expected_margin,
        "positions_count": len(positions),
        "total_leaves_qty": total_qty,
        "matching_leaves_qty": matching_qty,
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
            return ReconcileResult(True, status.live_state, "STALE_INTERNAL_POSITION_CLEARED", total_qty, matching_qty, positions)
        status.live_state = "FLAT"
        storage.log_structured("INFO", "POSITION_RECONCILE_RESULT", {**payload_base, "after_state": {"live_state": status.live_state}, "action": "FLAT_CONFIRMED"})
        return ReconcileResult(True, status.live_state, "FLAT_CONFIRMED", total_qty, matching_qty, positions)

    if status.open_position is None:
        status.live_state = "RECOVERING"
        status.recovery_until = None
        storage.log_structured("ERROR", "RECOVERY_ENTER", {**payload_base, "after_state": {"live_state": status.live_state}, "action": "ORPHAN_API_POSITION"}, mirror_message="ORPHAN_API_POSITION actual position exists while internal state is FLAT")
        return ReconcileResult(False, status.live_state, "ORPHAN_API_POSITION", total_qty, matching_qty, positions)

    if matching_qty <= 0:
        status.live_state = "RECOVERING"
        status.recovery_until = None
        storage.log_structured("ERROR", "RECOVERY_ENTER", {**payload_base, "after_state": {"live_state": status.live_state}, "action": "SIDE_MISMATCH"}, mirror_message="SIDE_MISMATCH actual position side does not match internal state")
        return ReconcileResult(False, status.live_state, "SIDE_MISMATCH", total_qty, matching_qty, positions)

    status.live_state = "OPEN"
    status.recovery_until = None
    storage.log_structured("INFO", "RECOVERY_EXIT", {**payload_base, "after_state": {"live_state": status.live_state}, "action": "MATCHED_OPEN"})
    return ReconcileResult(True, status.live_state, "MATCHED_OPEN", total_qty, matching_qty, positions)


def run_monitor(config: dict[str, Any]) -> tuple[str, str]:
    outdir = config["outdir"]
    ensure_dir(outdir)
    db_path = os.path.join(outdir, f"monitor_1570_{jst_date_compact()}.db")
    daily_report_path = os.path.join(outdir, f"daily_report_{jst_date_str()}.md")
    midday_report_path = os.path.join(outdir, f"midday_report_{jst_date_str()}.md")
    storage = Storage(db_path)
    storage.log("INFO", "START", "monitor start")

    client = KabuApiClient(config["base_url"])
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
    status = MonitorStatus()
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
            force_close_open_position(client, config, storage, status, "STOP_RUNTIME", now_, last_pred, latest_snapshot=last_snapshot, use_market_order=False)
            break
        if tstr >= STOP_AFTER:
            storage.log("INFO", "STOP_AFTER_SESSION", "session end reached")
            force_close_open_position(client, config, storage, status, "STOP_AFTER_SESSION", now_, last_pred, latest_snapshot=last_snapshot, use_market_order=False)
            break
        try:
            raw = client.get_board(config["symbol"], config["exchange"])
            snap = extract_snapshot(raw)
            last_snapshot = snap
            tick_buf.append(snap)
            spread_ticks = calc_spread_ticks(snap)
            storage.insert_snapshot(snap, spread_ticks)
            status.count += 1

            bar1_new = rb1.update(snap)
            if bar1_new:
                storage.insert_bar("bars_1m", bar1_new)
            bar3_new = rb3.update(snap)
            if bar3_new:
                storage.insert_bar("bars_3m", bar3_new)

            set_vwap_mode(adaptive.vwap_mode)
            f = build_features(snap.ts, tick_buf, rb1.latest(), rb1.prev(1), rb3.latest(), rb3.prev(1))
            if f is not None:
                storage.insert_feature(f)
                p = build_rsi9_prediction(rb1.latest(), list(rb1.history), status.open_position)
                if p is None:
                    continue
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
                force_close_time_reached = tstr >= str(config.get("force_close_after", FORCE_CLOSE_AFTER))

                if force_close_time_reached:
                    effective_signal = "NO_ACTION"
                    if status.pending_entry_side is not None:
                        storage.log("WARN", "PENDING_ENTRY_CLEARED_FORCE_CLOSE_TIME", f"side={status.pending_entry_side}")
                        status.pending_entry_side = None
                        status.pending_entry_ts = None
                    if (
                        status.open_position is not None
                        and not status.force_market_close_sent
                        and status.live_state not in {"EXIT_SENT", "EXIT_VERIFYING", "RECOVERING"}
                    ):
                        storage.log("WARN", "FORCE_MARKET_CLOSE_1520", f"time={tstr} side={status.open_position.side} strategy={status.open_position.strategy}")
                        status.force_market_close_sent = True
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

                # RSI threshold hit on closed 1m bar -> execute on next 1m bar open (first tick)
                if (not force_close_time_reached) and bar1_new is not None and status.pending_entry_side is None and status.open_position is None:
                    if effective_signal in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
                        status.pending_entry_side = "LONG" if effective_signal == "LONG_CANDIDATE" else "SHORT"
                        status.pending_entry_ts = f.ts
                        storage.log("INFO", "RSI_PENDING_ENTRY", f"side={status.pending_entry_side} signal_ts={f.ts.isoformat()}")

                if status.pending_entry_side and status.open_position is None:
                    if force_close_time_reached:
                        status.pending_entry_side = None
                        status.pending_entry_ts = None
                    else:
                        side = status.pending_entry_side
                        enter_ok, _ = can_enter(side, f.ts, status)
                        if enter_ok:
                            candidate_pos = create_position(p, f, config)
                            if not adaptive.allow_strat(candidate_pos.strategy, f.ts):
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
                                        if not result.ok:
                                            status.live_state = "FLAT"
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
                                            candidate_pos.entry_order_id = result.order_id
                                            try:
                                                entry_positions = fetch_positions(client, config, storage, reason="POST_ENTRY_FILL_PRICE")
                                                candidate_pos.entry_fill_price = average_price_from_positions(
                                                    entry_positions,
                                                    side,
                                                    margin_trade_type=candidate_pos.margin_trade_type,
                                                )
                                            except Exception:
                                                candidate_pos.entry_fill_price = None
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
                                            take_profit_cfg = config.get("take_profit_execution", {})
                                            if candidate_pos.strategy == "RSI9":
                                                storage.log("INFO", "RSI9_TAKE_PROFIT_LIMIT_SKIP", f"side={candidate_pos.side} strategy=RSI9")
                                            elif not isinstance(take_profit_cfg, dict) or bool(take_profit_cfg.get("enabled", True)):
                                                tp_result = place_take_profit_limit_order(client, config, storage, candidate_pos, p, status)
                                                if tp_result.ok:
                                                    candidate_pos.take_profit_order_id = tp_result.order_id
                                                    storage.log("INFO", "TAKE_PROFIT_LIMIT_OK", f"{side} order_id={tp_result.order_id} price={take_profit_limit_price(candidate_pos):.1f}")
                                                else:
                                                    storage.log("WARN", "TAKE_PROFIT_LIMIT_FAIL", tp_result.message)
                                            status.last_entry_reject_key = ""
                                            status.last_entry_ts_by_side[side] = f.ts
                                            status.pending_entry_side = None
                                            mfe_ticks = 0.0
                                            mae_ticks = 0.0
                            else:
                                status.open_position = candidate_pos
                                status.live_state = "OPEN"
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

                        if pos.strategy == "RSI9" and pos.rsi_special_entry and config["live_mode"] and pos.entry_fill_price is not None:
                            elapsed_special = (f.ts - (pos.rsi_special_tp_order_ts or pos.entry_ts)).total_seconds() if pos.rsi_special_tp_order_ts else 0
                            target_ticks = 10 if pos.rsi_special_tp_stage == 0 else 5
                            if pos.take_profit_order_id is None:
                                pos.take_ticks = target_ticks
                                tp_res = place_take_profit_limit_order(client, config, storage, pos, p, status)
                                if tp_res.ok:
                                    pos.take_profit_order_id = tp_res.order_id
                                    pos.rsi_special_tp_order_ts = f.ts
                            elif pos.rsi_special_tp_stage == 0 and elapsed_special >= 300:
                                context = order_context(config, pos.side, pos, p, status)
                                cancel_ok, filled = cancel_pending_take_profit_order(client, config, storage, pos, context)
                                if filled:
                                    live_tp_already_filled = True
                                    ex, ex_reason, pnl_ticks = True, "TAKE_PROFIT_LIMIT_FILLED", take_profit_filled_ticks(pos)
                                elif cancel_ok:
                                    pos.rsi_special_tp_stage = 1
                                    pos.take_ticks = 5
                                    tp_res2 = place_take_profit_limit_order(client, config, storage, pos, p, status)
                                    if tp_res2.ok:
                                        pos.take_profit_order_id = tp_res2.order_id
                                        pos.rsi_special_tp_order_ts = f.ts

                        if config["live_mode"] and pos.take_profit_order_id and wait_for_position_qty(client, config, pos.side, target_qty=0, timeout_sec=0, comparator="eq", margin_trade_type=pos.margin_trade_type):
                            pos.exit_fill_price = take_profit_limit_price(pos)
                            ex, ex_reason, pnl_ticks = True, "TAKE_PROFIT_LIMIT_FILLED", take_profit_filled_ticks(pos)
                            live_tp_already_filled = True
                        else:
                            ex, ex_reason, pnl_ticks = should_exit(pos, f, p)

                        if config["live_mode"] and ex and ex_reason == "TAKE_PROFIT" and pos.take_profit_order_id and not live_tp_already_filled:
                            if pos.strategy == "RSI9":
                                context = order_context(config, pos.side, pos, p, status)
                                cancel_ok, filled_during_cancel = cancel_pending_take_profit_order(client, config, storage, pos, context)
                                if filled_during_cancel:
                                    live_tp_already_filled = True
                                    ex_reason = "TAKE_PROFIT_LIMIT_FILLED"
                                    pnl_ticks = take_profit_filled_ticks(pos)
                                elif not cancel_ok:
                                    ex = False
                                    status.live_state = "RECOVERING"
                                    status.recovery_until = f.ts + timedelta(seconds=RECOVERY_COOLDOWN_SEC)
                                    storage.log("ERROR", "RSI9_OLD_TP_CANCEL_FAILED", f"side={pos.side} strategy={pos.strategy} order_id={pos.take_profit_order_id}")
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
                            if config["live_mode"]:
                                if pos.take_profit_order_id and ex_reason not in {"TAKE_PROFIT", "TAKE_PROFIT_LIMIT_FILLED"}:
                                    context = order_context(config, pos.side, pos, p, status)
                                    cancel_ok, filled_during_cancel = cancel_pending_take_profit_order(client, config, storage, pos, context)
                                    if filled_during_cancel:
                                        live_tp_already_filled = True
                                        ex_reason = "TAKE_PROFIT_LIMIT_FILLED"
                                        pnl_ticks = take_profit_filled_ticks(pos)
                                    elif not cancel_ok:
                                        exit_confirmed = False
                                        status.live_state = "RECOVERING"
                                        storage.log("ERROR", "LIVE_EXIT_FAIL", "TAKE_PROFIT_CANCEL_FAILED before protective market exit")

                                if exit_confirmed and live_tp_already_filled:
                                    status.exit_fail_count = 0
                                    status.live_state = "FLAT"
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
                                    status.live_state = "EXIT_SENT"
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
                                        pos.exit_fill_price = f.price
                                        status.exit_fail_count = 0
                                        status.live_state = "FLAT"
                                        storage.log("INFO", "LIVE_EXIT_OK", f"{pos.side} order_id={result.order_id}")
                                        storage.insert_execution_fill_price(
                                            "EXIT_FILL_PRICE",
                                            result.order_id,
                                            pos.side,
                                            pos.strategy,
                                            ex_reason,
                                            pos.exit_fill_price,
                                        )

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
                                if ex_reason in {"STOP_LOSS", "EDGE_BREAK_HARD"}:
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
            force_close_open_position(client, config, storage, status, "KEYBOARD_INTERRUPT", now_jst(), last_pred, latest_snapshot=last_snapshot, use_market_order=False)
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
        "live_mode": bool(args.live_mode or cfg.get("live_mode", False)),
        "order_password": args.order_password or cfg.get("order_password") or args.api_password or cfg.get("api_password") or API_PASSWORD_HARDCODED,
        "order_qty": int(args.order_qty if args.order_qty is not None else cfg.get("order_qty", 1)),
        "entry_min_fill_qty": int(cfg.get("entry_min_fill_qty", cfg.get("order_qty", 1))),
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
