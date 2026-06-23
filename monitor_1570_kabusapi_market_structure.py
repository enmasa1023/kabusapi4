#!/usr/bin/env python3
"""1570 market-structure-only monitor.

合言葉: 戦略は単純化 / 執行・安全装置は維持 / DBは後追い記録 / 判断はメモリで即時実行。

This engine intentionally excludes the legacy RSI50/RSI35/scalping/feature-entry
execution paths.  It keeps market-data collection, operational safety wrappers,
and asynchronous DB logging while evaluating only `market_structure_strategy`.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import queue
import sqlite3
import statistics
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dtime, timezone
from pathlib import Path
from typing import Any, Deque, Optional

JST = timezone(timedelta(hours=9))
API_BASE_DEFAULT = "http://localhost:18080/kabusapi"
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"
ACTION_ENTRY = "ENTRY"
ACTION_EXIT = "EXIT"
MORNING_START = dtime(9, 0)
MORNING_END = dtime(11, 25)
AFTERNOON_START = dtime(12, 30)
AFTERNOON_END = dtime(15, 20)


def now_jst() -> datetime:
    return datetime.now(JST)


def parse_hms(value: str) -> dtime:
    h, m, s = [int(x) for x in value.split(":")]
    return dtime(h, m, s)


def floor_minute(ts: datetime, minutes: int = 1) -> datetime:
    minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=minute, second=0, microsecond=0)


def iso_dt(value: Any) -> Optional[str]:
    return value.isoformat() if isinstance(value, datetime) else value


def tick_size_for_1570(price: float) -> float:
    # 1570 normally trades in 10-yen ticks in the target price band; keep a
    # conservative fallback for lower test prices.
    return 10.0 if price >= 3000 else 1.0


def is_data_collection_only(config: dict[str, Any]) -> bool:
    cfg = config.get("data_collection_only", {})
    return bool(isinstance(cfg, dict) and cfg.get("enabled", False))


def orders_enabled(config: dict[str, Any]) -> bool:
    return bool(
        config.get("live_mode", False)
        and not is_data_collection_only(config)
        and config.get("entry_execution", {}).get("enabled", False)
    )


def entry_orders_enabled(config: dict[str, Any], state: RuntimeState, snap: TickSnapshot) -> bool:
    return bool(
        config.get("live_mode", False) is True
        and not is_data_collection_only(config)
        and config.get("entry_execution", {}).get("enabled") is True
        and config.get("market_structure_strategy", {}).get("enabled") is True
        and state.open_position is None
        and (state.recovery_until is None or now_jst() >= state.recovery_until)
        and market_open_for_new_entry(snap.ts, parse_hms(config.get("new_entry_cutoff_time", "15:10:00")))
    )


def exit_orders_enabled(config: dict[str, Any], state: RuntimeState, _snap: TickSnapshot) -> bool:
    return bool(
        config.get("live_mode", False) is True
        and not is_data_collection_only(config)
        and config.get("market_structure_strategy", {}).get("enabled") is True
        and state.open_position is not None
    )


def market_open_for_new_entry(ts: datetime, cutoff: dtime) -> bool:
    t = ts.timetz().replace(tzinfo=None)
    in_window = (MORNING_START <= t <= MORNING_END) or (AFTERNOON_START <= t <= AFTERNOON_END)
    return in_window and t < cutoff


def market_open_for_exit(ts: datetime) -> bool:
    t = ts.timetz().replace(tzinfo=None)
    return (MORNING_START <= t <= MORNING_END) or (AFTERNOON_START <= t <= AFTERNOON_END)


@dataclass
class TickSnapshot:
    ts: datetime
    price: float
    sell1_price: Optional[float] = None
    sell1_qty: Optional[float] = None
    buy1_price: Optional[float] = None
    buy1_qty: Optional[float] = None
    cumulative_volume: Optional[float] = None
    volume_delta: float = 0.0
    vwap: Optional[float] = None
    buy_depth_10: Optional[float] = None
    sell_depth_10: Optional[float] = None
    ws_seq: Optional[int] = None


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    vwap: Optional[float] = None
    snapshots: int = 0
    ma5: Optional[float] = None
    ma13: Optional[float] = None
    ema13: Optional[float] = None
    ma25: Optional[float] = None
    ma75: Optional[float] = None
    rsi9: Optional[float] = None


@dataclass
class PendingSignal:
    signal_bar_ts: datetime
    execute_not_before_minute: datetime
    side: str
    action: str
    signal_name: str
    reason: str
    features: dict[str, Any]


@dataclass
class PositionState:
    side: str
    qty: int
    entry_price: float
    strategy: str
    entry_ts: datetime
    order_id: str = ""
    execution_ids: list[str] = field(default_factory=list)
    margin_trade_type: int = 0
    cash_margin: int = 2


@dataclass
class RuntimeState:
    bars_1m_buffer: Deque[Bar] = field(default_factory=lambda: deque(maxlen=180))
    bars_3m_buffer: Deque[Bar] = field(default_factory=lambda: deque(maxlen=180))
    market_structure_feature_buffer: Deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=180))
    snapshot_buffer: Deque[TickSnapshot] = field(default_factory=lambda: deque(maxlen=2000))
    pending_signal: Optional[PendingSignal] = None
    open_position: Optional[PositionState] = None
    last_entry_bar_ts: Optional[datetime] = None
    recovery_until: Optional[datetime] = None
    force_close_logged: bool = False
    rest_fallback_count: int = 0
    last_cumulative_volume: Optional[float] = None


class AsyncDbWriter:
    def __init__(self, db_path: str, sqlite_cfg: dict[str, Any], max_queue_size: int = 10000, batch_size: int = 100, flush_interval_sec: float = 0.5):
        self.db_path = db_path
        self.sqlite_cfg = sqlite_cfg
        self.batch_size = int(batch_size)
        self.flush_interval_sec = float(flush_interval_sec)
        self.q: queue.Queue[tuple[str, Any, bool]] = queue.Queue(maxsize=int(max_queue_size))
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="AsyncDbWriter", daemon=True)
        self.thread.start()

    def enqueue(self, op: str, payload: Any, important: bool = False) -> bool:
        try:
            self.q.put_nowait((op, payload, important))
            return True
        except queue.Full:
            if important:
                self.q.put((op, payload, important), timeout=1.0)
                return True
            return False

    def log_structured(self, level: str, event_type: str, payload: dict[str, Any], important: bool = False) -> None:
        self.enqueue("structured", {"ts": now_jst().isoformat(), "level": level, "event_type": event_type, "payload": payload}, important)

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=5)

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=max(1.0, self.sqlite_cfg.get("busy_timeout_ms", 3000) / 1000.0))
        con.execute(f"PRAGMA journal_mode={self.sqlite_cfg.get('journal_mode', 'WAL')}")
        con.execute(f"PRAGMA synchronous={self.sqlite_cfg.get('synchronous', 'NORMAL')}")
        con.execute(f"PRAGMA busy_timeout={int(self.sqlite_cfg.get('busy_timeout_ms', 3000))}")
        con.execute(f"PRAGMA temp_store={self.sqlite_cfg.get('temp_store', 'MEMORY')}")
        con.execute(f"PRAGMA wal_autocheckpoint={int(self.sqlite_cfg.get('wal_autocheckpoint', 1000))}")
        self._init_schema(con)
        return con

    def _init_schema(self, con: sqlite3.Connection) -> None:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS structured_events(
              id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, level TEXT, event_type TEXT, payload_json TEXT
            );
            CREATE TABLE IF NOT EXISTS execution_facts(
              id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, event_type TEXT, side TEXT, qty REAL, price REAL, order_id TEXT, payload_json TEXT
            );
            CREATE TABLE IF NOT EXISTS paper_trades(
              id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, side TEXT, qty REAL, price REAL, reason TEXT, payload_json TEXT
            );
            CREATE TABLE IF NOT EXISTS bars_1m(
              ts TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL,
              ma5 REAL, ma13 REAL, ema13 REAL, ma25 REAL, ma75 REAL, rsi9 REAL
            );
            CREATE TABLE IF NOT EXISTS bars_3m(
              ts TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL
            );
            CREATE TABLE IF NOT EXISTS market_structure_features_1m(
              ts TEXT PRIMARY KEY, symbol TEXT, session TEXT,
              open REAL, high REAL, low REAL, close REAL, volume REAL, vwap REAL,
              ma5 REAL, ma13 REAL, ema13 REAL, ma25 REAL, ma75 REAL, rsi9 REAL,
              high_prev_3m REAL, low_prev_3m REAL, high_prev_5m REAL, low_prev_5m REAL,
              high_prev_10m REAL, low_prev_10m REAL, high_prev_20m REAL, low_prev_20m REAL,
              high_prev_30m REAL, low_prev_30m REAL, high_prev_60m REAL, low_prev_60m REAL,
              break_high_3m INTEGER, break_low_3m INTEGER, break_high_5m INTEGER, break_low_5m INTEGER,
              break_high_10m INTEGER, break_low_10m INTEGER, break_high_20m INTEGER, break_low_20m INTEGER,
              break_high_30m INTEGER, break_low_30m INTEGER, break_high_60m INTEGER, break_low_60m INTEGER,
              failed_break_high_5m INTEGER, failed_break_low_5m INTEGER, failed_break_high_20m INTEGER, failed_break_low_20m INTEGER,
              range_pos_3m REAL, range_pos_5m REAL, range_pos_10m REAL, range_pos_20m REAL, range_pos_30m REAL, range_pos_60m REAL,
              day_high_before REAL, day_low_before REAL, day_high_so_far REAL, day_low_so_far REAL,
              day_range_pos REAL, break_day_high INTEGER, break_day_low INTEGER, failed_break_day_high INTEGER, failed_break_day_low INTEGER,
              distance_to_day_high_ticks REAL, distance_to_day_low_ticks REAL,
              opening_high_5m REAL, opening_low_5m REAL, opening_high_10m REAL, opening_low_10m REAL,
              opening_range_width_5m REAL, opening_range_width_10m REAL,
              break_opening_high_5m INTEGER, break_opening_low_5m INTEGER, break_opening_high_10m INTEGER, break_opening_low_10m INTEGER,
              failed_break_opening_high_5m INTEGER, failed_break_opening_low_5m INTEGER, failed_break_opening_high_10m INTEGER, failed_break_opening_low_10m INTEGER,
              afternoon_open_high_5m REAL, afternoon_open_low_5m REAL, afternoon_open_high_10m REAL, afternoon_open_low_10m REAL,
              afternoon_open_range_width_5m REAL, afternoon_open_range_width_10m REAL,
              break_afternoon_open_high_5m INTEGER, break_afternoon_open_low_5m INTEGER, break_afternoon_open_high_10m INTEGER, break_afternoon_open_low_10m INTEGER,
              failed_break_afternoon_open_high_5m INTEGER, failed_break_afternoon_open_low_5m INTEGER, failed_break_afternoon_open_high_10m INTEGER, failed_break_afternoon_open_low_10m INTEGER,
              volume_delta_1m REAL, volume_delta_3m REAL, volume_median_20m REAL, volume_ratio_1m REAL, volume_ratio_3m REAL, volume_z_20m REAL,
              price_change_1m REAL, price_change_3m REAL, price_change_5m REAL, price_change_10m REAL, price_change_20m REAL, price_change_30m REAL, price_change_60m REAL,
              volume_price_efficiency_1m REAL, volume_price_efficiency_3m REAL, volume_price_efficiency_5m REAL,
              rsi9_slope_1m REAL, rsi9_slope_3m REAL, ma5_slope_3m REAL, ma25_slope_5m REAL, ema13_slope_4m REAL,
              close_above_ma5 INTEGER, close_above_ma25 INTEGER, close_above_ma75 INTEGER, close_above_vwap INTEGER,
              buy_depth_10 REAL, sell_depth_10 REAL, spread_ticks REAL, obi_l1 REAL, obi_l3 REAL, obi_l10 REAL, obi_l3_delta_3m REAL, obi_l10_delta_3m REAL, microprice REAL, micro_gap_ticks REAL,
              raw_context_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_market_structure_features_1m_ts ON market_structure_features_1m(ts);
            CREATE TABLE IF NOT EXISTS market_structure_signal_candidates(
              id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, signal_name TEXT, side TEXT, action TEXT,
              confidence REAL, reason TEXT, blocked_by_data_collection_only INTEGER, feature_json TEXT,
              UNIQUE(ts, signal_name)
            );
            """
        )
        con.commit()

    def _run(self) -> None:
        con = self._connect()
        batch: list[tuple[str, Any, bool]] = []
        last_flush = time.monotonic()
        while not self.stop_event.is_set() or not self.q.empty() or batch:
            timeout = max(0.05, self.flush_interval_sec - (time.monotonic() - last_flush))
            try:
                batch.append(self.q.get(timeout=timeout))
            except queue.Empty:
                pass
            if batch and (len(batch) >= self.batch_size or time.monotonic() - last_flush >= self.flush_interval_sec or self.stop_event.is_set()):
                self._flush(con, batch)
                batch.clear()
                last_flush = time.monotonic()
        con.close()

    def _flush(self, con: sqlite3.Connection, batch: list[tuple[str, Any, bool]]) -> None:
        for op, p, _important in batch:
            if op == "structured":
                con.execute("INSERT INTO structured_events(ts,level,event_type,payload_json) VALUES(?,?,?,?)", (p["ts"], p["level"], p["event_type"], json.dumps(p["payload"], ensure_ascii=False, default=str)))
            elif op == "bar1":
                con.execute("INSERT OR REPLACE INTO bars_1m VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", p)
            elif op == "bar3":
                con.execute("INSERT OR REPLACE INTO bars_3m VALUES(?,?,?,?,?,?,?)", p)
            elif op == "feature":
                keys = list(p.keys())
                con.execute(f"INSERT OR REPLACE INTO market_structure_features_1m({','.join(keys)}) VALUES({','.join(['?']*len(keys))})", [p[k] for k in keys])
            elif op == "candidate":
                con.execute("INSERT OR IGNORE INTO market_structure_signal_candidates(ts,symbol,signal_name,side,action,confidence,reason,blocked_by_data_collection_only,feature_json) VALUES(?,?,?,?,?,?,?,?,?)", p)
            elif op == "execution":
                con.execute("INSERT INTO execution_facts(ts,event_type,side,qty,price,order_id,payload_json) VALUES(?,?,?,?,?,?,?)", p)
        con.commit()


class KabuApiClient:
    def __init__(self, base_url: str, api_password: str, order_password: str):
        self.base_url = base_url.rstrip("/")
        self.api_password = api_password
        self.order_password = order_password
        self.token: Optional[str] = None

    def _request(self, method: str, path: str, payload: Optional[dict[str, Any]] = None, timeout: float = 5.0) -> Any:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.base_url + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("X-API-KEY", self.token)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}

    def get_token(self) -> str:
        res = self._request("POST", "/token", {"APIPassword": self.api_password})
        self.token = str(res.get("Token") or res.get("token") or "")
        if not self.token:
            raise RuntimeError("kabu API token response did not include Token")
        return self.token

    def register_symbol(self, symbol: str, exchange: int) -> Any:
        return self._request("PUT", "/register", {"Symbols": [{"Symbol": symbol, "Exchange": exchange}]})

    def get_board(self, symbol: str, exchange: int) -> Any:
        return self._request("GET", f"/board/{symbol}@{exchange}")

    def send_order(self, payload: dict[str, Any]) -> Any:
        return self._request("POST", "/sendorder", payload)

    def cancel_order(self, order_id: str) -> Any:
        return self._request("PUT", "/cancelorder", {"OrderId": order_id})

    def get_positions(self, symbol: str) -> Any:
        return self._request("GET", f"/positions?product=2&symbol={symbol}&addinfo=true")

    def get_orders(self, symbol: str, order_id: Optional[str] = None, details: bool = True) -> Any:
        suffix = f"&id={order_id}" if order_id else ""
        detail = "&details=true" if details else ""
        return self._request("GET", f"/orders?product=2{suffix}{detail}")


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def extract_snapshot(raw: dict[str, Any], ws_seq: Optional[int] = None) -> Optional[TickSnapshot]:
    price = _to_float(raw.get("CurrentPrice") or raw.get("Price") or raw.get("price"))
    if price is None:
        return None
    ts_raw = raw.get("CurrentPriceTime") or raw.get("Time") or raw.get("ts")
    if ts_raw:
        try:
            ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).astimezone(JST)
        except ValueError:
            ts = now_jst()
    else:
        ts = now_jst()
    buy_depth_10 = sell_depth_10 = 0.0
    buy1p = buy1q = sell1p = sell1q = None
    for i in range(1, 11):
        b = raw.get(f"Buy{i}") or {}
        s = raw.get(f"Sell{i}") or {}
        bp = _to_float(b.get("Price") if isinstance(b, dict) else None)
        bq = _to_float(b.get("Qty") if isinstance(b, dict) else None)
        sp = _to_float(s.get("Price") if isinstance(s, dict) else None)
        sq = _to_float(s.get("Qty") if isinstance(s, dict) else None)
        if i == 1:
            buy1p, buy1q, sell1p, sell1q = bp, bq, sp, sq
        buy_depth_10 += bq or 0.0
        sell_depth_10 += sq or 0.0
    return TickSnapshot(ts=ts, price=price, buy1_price=buy1p, buy1_qty=buy1q, sell1_price=sell1p, sell1_qty=sell1q, cumulative_volume=_to_float(raw.get("TradingVolume")), vwap=_to_float(raw.get("VWAP")), buy_depth_10=buy_depth_10 or None, sell_depth_10=sell_depth_10 or None, ws_seq=ws_seq)


def attach_volume_delta(state: RuntimeState, snap: TickSnapshot, storage: Optional[AsyncDbWriter] = None) -> TickSnapshot:
    """Convert kabu API daily cumulative TradingVolume to per-snapshot delta."""
    current = snap.cumulative_volume
    prev = state.last_cumulative_volume
    delta = 0.0
    if current is not None and prev is not None and current >= prev:
        delta = current - prev
        if delta > 5_000_000 and storage:
            storage.log_structured("WARN", "SNAPSHOT_VOLUME_DELTA_ABNORMAL", {"snapshot_ts": snap.ts.isoformat(), "current_cumulative_volume": current, "previous_cumulative_volume": prev, "volume_delta": delta}, False)
    elif current is not None and prev is not None and current < prev and storage:
        storage.log_structured("WARN", "SNAPSHOT_VOLUME_CUMULATIVE_RESET", {"snapshot_ts": snap.ts.isoformat(), "current_cumulative_volume": current, "previous_cumulative_volume": prev}, False)
    snap.volume_delta = max(delta, 0.0)
    if current is not None:
        state.last_cumulative_volume = current
    return snap


class WebSocketMarketDataFeed:
    def __init__(self, config: dict[str, Any], symbol: str, exchange: int, storage: AsyncDbWriter):
        self.config = config
        self.symbol = symbol
        self.exchange = exchange
        self.storage = storage
        self._queue: Deque[TickSnapshot] = deque(maxlen=int(config.get("market_data_source", {}).get("ws_queue_maxlen", 5000)))
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._ws_seq = 0
        self._latest: Optional[TickSnapshot] = None
        self.available = False
        self.last_error = ""
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self) -> None:
        try:
            import websocket  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            self.last_error = f"WEBSOCKET_MODULE_UNAVAILABLE: {exc}"
            self.storage.log_structured("WARN", "WEBSOCKET_MARKET_DATA_UNAVAILABLE", {"reason": self.last_error}, True)
            return
        url = str(self.config.get("websocket_url") or "ws://localhost:18080/kabusapi/websocket")
        def run() -> None:
            def on_message(_ws: Any, message: str) -> None:
                try:
                    raw = json.loads(message)
                    with self._lock:
                        self._ws_seq += 1
                        seq = self._ws_seq
                    snap = extract_snapshot(raw, seq)
                    if snap:
                        with self._lock:
                            self._latest = snap
                            self._queue.append(snap)
                            self.available = True
                            self._event.set()
                except Exception as exc:
                    self.last_error = str(exc)
            def on_error(_ws: Any, error: Any) -> None:
                self.available = False
                self.last_error = str(error)
            def on_close(_ws: Any, *_args: Any) -> None:
                self.available = False
            ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
            while not self._stop.is_set():
                try:
                    ws.run_forever(ping_interval=20, ping_timeout=5)
                except Exception as exc:
                    self.last_error = str(exc)
                self.available = False
                time.sleep(1)
        self._thread = threading.Thread(target=run, name="WebSocketMarketDataFeed", daemon=True)
        self._thread.start()
        self.storage.log_structured("INFO", "WEBSOCKET_MARKET_DATA_START", {"symbol": self.symbol, "exchange": self.exchange}, True)

    def drain_snapshots_after(self, last_ws_seq: Optional[int]) -> list[TickSnapshot]:
        with self._lock:
            items = list(self._queue)
            self._queue.clear()
            self._event.clear()
        out = [s for s in items if last_ws_seq is None or (s.ws_seq is not None and s.ws_seq > last_ws_seq)]
        out.sort(key=lambda s: (s.ws_seq or 0, s.ts))
        return out

    def wait_for_data(self, timeout_sec: float) -> bool:
        return self._event.wait(timeout=timeout_sec)

    def latest_snapshot(self) -> Optional[TickSnapshot]:
        with self._lock:
            return self._latest

    def stop(self) -> None:
        self._stop.set()


class RollingBars:
    def __init__(self, minutes: int):
        self.minutes = minutes
        self.current: Optional[Bar] = None

    def update(self, snap: TickSnapshot) -> Optional[Bar]:
        bucket = floor_minute(snap.ts, self.minutes)
        if self.current is None:
            self.current = Bar(bucket, snap.price, snap.price, snap.price, snap.price, snap.volume_delta, snap.vwap, 1)
            return None
        if bucket > self.current.ts:
            finished = self.current
            self.current = Bar(bucket, snap.price, snap.price, snap.price, snap.price, snap.volume_delta, snap.vwap, 1)
            return finished
        self.current.high = max(self.current.high, snap.price)
        self.current.low = min(self.current.low, snap.price)
        self.current.close = snap.price
        self.current.volume += snap.volume_delta
        self.current.vwap = snap.vwap if snap.vwap is not None else self.current.vwap
        self.current.snapshots += 1
        return None

    def force_finalize_completed_bucket(self, now_ts: datetime, finalize_delay_ms: int) -> Optional[Bar]:
        if self.current is None:
            return None
        end_ts = self.current.ts + timedelta(minutes=self.minutes, milliseconds=finalize_delay_ms)
        if now_ts >= end_ts:
            finished = self.current
            self.current = None
            return finished
        return None


def simple_ma(values: list[float], period: int) -> Optional[float]:
    return sum(values[-period:]) / period if len(values) >= period else None


def ema(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi_wilder(values: list[float], period: int = 9) -> Optional[float]:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for a, b in zip(values, values[1:]):
        d = b - a
        gains.append(max(d, 0.0)); losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for g, l in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def decorate_bar(bar: Bar, history: Deque[Bar]) -> None:
    closes = [b.close for b in history] + [bar.close]
    bar.ma5 = simple_ma(closes, 5)
    bar.ma13 = simple_ma(closes, 13)
    bar.ema13 = ema(closes, 13)
    bar.ma25 = simple_ma(closes, 25)
    bar.ma75 = simple_ma(closes, 75)
    bar.rsi9 = rsi_wilder(closes, 9)


def prev_high_low(history: list[Bar], n: int) -> tuple[Optional[float], Optional[float]]:
    bars = history[-n:] if len(history) >= n else []
    if not bars:
        return None, None
    return max(b.high for b in bars), min(b.low for b in bars)


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b


def board_context(snap: Optional[TickSnapshot], features: Deque[dict[str, Any]]) -> dict[str, Any]:
    if not snap:
        return {"buy_depth_10": None, "sell_depth_10": None, "spread_ticks": None, "obi_l10": None, "obi_l10_delta_3m": None, "microprice": None, "micro_gap_ticks": None}
    tick = tick_size_for_1570(snap.price)
    buy10 = snap.buy_depth_10
    sell10 = snap.sell_depth_10
    obi10 = safe_div((buy10 or 0) - (sell10 or 0), (buy10 or 0) + (sell10 or 0)) if buy10 is not None and sell10 is not None else None
    prev_obi = features[-3].get("obi_l10") if len(features) >= 3 else None
    bid = snap.buy1_price; ask = snap.sell1_price; bq = snap.buy1_qty; aq = snap.sell1_qty
    micro = gap = spread = None
    if bid is not None and ask is not None:
        spread = (ask - bid) / tick
        mid = (ask + bid) / 2
        if bq and aq and bq + aq:
            micro = (ask * bq + bid * aq) / (bq + aq)
            gap = (micro - mid) / tick
    return {"buy_depth_10": buy10, "sell_depth_10": sell10, "spread_ticks": spread, "obi_l10": obi10, "obi_l10_delta_3m": (obi10 - prev_obi) if obi10 is not None and prev_obi is not None else None, "microprice": micro, "micro_gap_ticks": gap}


def session_name(ts: datetime) -> str:
    t = ts.timetz().replace(tzinfo=None)
    if MORNING_START <= t <= MORNING_END:
        return "morning"
    if AFTERNOON_START <= t <= AFTERNOON_END:
        return "afternoon"
    return "off_session"


def opening_range(history_with_current: list[Bar], start: dtime, minutes: int, current_ts: datetime) -> tuple[Optional[float], Optional[float]]:
    complete_at = (datetime.combine(current_ts.date(), start, JST) + timedelta(minutes=minutes)).timetz().replace(tzinfo=None)
    if current_ts.timetz().replace(tzinfo=None) < complete_at:
        return None, None
    bars = [b for b in history_with_current if b.ts.date() == current_ts.date() and start <= b.ts.timetz().replace(tzinfo=None) < complete_at]
    if len(bars) < minutes:
        return None, None
    return max(b.high for b in bars), min(b.low for b in bars)


def compute_market_structure_features(symbol: str, bar: Bar, state: RuntimeState, latest_snap: Optional[TickSnapshot]) -> dict[str, Any]:
    history = list(state.bars_1m_buffer)  # excludes current until caller appends later
    hist_current = history + [bar]
    closes = [b.close for b in hist_current]
    vols_prev = [b.volume for b in history[-20:]]
    vol_median = statistics.median(vols_prev) if vols_prev else None
    vol3 = sum(b.volume for b in hist_current[-3:]) if hist_current else None
    day_bars_before = [b for b in history if b.ts.date() == bar.ts.date()]
    day_bars_so_far = day_bars_before + [bar]
    f: dict[str, Any] = {"ts": bar.ts.isoformat(), "symbol": symbol, "session": session_name(bar.ts), "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume, "vwap": bar.vwap, "ma5": bar.ma5, "ma13": bar.ma13, "ema13": bar.ema13, "ma25": bar.ma25, "ma75": bar.ma75, "rsi9": bar.rsi9}
    for n in [3,5,10,20,30,60]:
        hi, lo = prev_high_low(history, n)
        f[f"high_prev_{n}m"] = hi; f[f"low_prev_{n}m"] = lo
        f[f"break_high_{n}m"] = int(hi is not None and bar.close > hi)
        f[f"break_low_{n}m"] = int(lo is not None and bar.close < lo)
        f[f"range_pos_{n}m"] = safe_div(bar.close - lo, hi - lo) if hi is not None and lo is not None and hi != lo else None
        f[f"price_change_{n}m"] = bar.close - history[-n].close if len(history) >= n else None
    for n in [5,20]:
        hi = f.get(f"high_prev_{n}m"); lo = f.get(f"low_prev_{n}m")
        f[f"failed_break_high_{n}m"] = int(hi is not None and bar.high > hi and bar.close <= hi)
        f[f"failed_break_low_{n}m"] = int(lo is not None and bar.low < lo and bar.close >= lo)
    day_high_before = max((b.high for b in day_bars_before), default=None)
    day_low_before = min((b.low for b in day_bars_before), default=None)
    day_high = max(b.high for b in day_bars_so_far); day_low = min(b.low for b in day_bars_so_far)
    f.update({"day_high_before": day_high_before, "day_low_before": day_low_before, "day_high_so_far": day_high, "day_low_so_far": day_low,
              "day_range_pos": safe_div(bar.close - day_low, day_high - day_low) if day_high != day_low else None,
              "break_day_high": int(day_high_before is not None and bar.close > day_high_before), "break_day_low": int(day_low_before is not None and bar.close < day_low_before),
              "failed_break_day_high": int(day_high_before is not None and bar.high > day_high_before and bar.close <= day_high_before),
              "failed_break_day_low": int(day_low_before is not None and bar.low < day_low_before and bar.close >= day_low_before),
              "distance_to_day_high_ticks": safe_div(day_high_before - bar.close, tick_size_for_1570(bar.close)) if day_high_before else None,
              "distance_to_day_low_ticks": safe_div(bar.close - day_low_before, tick_size_for_1570(bar.close)) if day_low_before else None})
    for prefix, start in [("opening", MORNING_START), ("afternoon_open", AFTERNOON_START)]:
        for n in [5,10]:
            hi, lo = opening_range(hist_current, start, n, bar.ts)
            f[f"{prefix}_high_{n}m"] = hi; f[f"{prefix}_low_{n}m"] = lo; f[f"{prefix}_range_width_{n}m"] = (hi - lo) if hi is not None and lo is not None else None
            f[f"break_{prefix}_high_{n}m"] = int(hi is not None and bar.close > hi)
            f[f"break_{prefix}_low_{n}m"] = int(lo is not None and bar.close < lo)
            f[f"failed_break_{prefix}_high_{n}m"] = int(hi is not None and bar.high > hi and bar.close <= hi)
            f[f"failed_break_{prefix}_low_{n}m"] = int(lo is not None and bar.low < lo and bar.close >= lo)
    f.update({"volume_delta_1m": bar.volume, "volume_delta_3m": vol3, "volume_median_20m": vol_median,
              "volume_ratio_1m": safe_div(bar.volume, vol_median), "volume_ratio_3m": safe_div(vol3, (vol_median * 3) if vol_median else None)})
    if vols_prev and len(vols_prev) > 1:
        sd = statistics.pstdev(vols_prev)
        f["volume_z_20m"] = safe_div(bar.volume - statistics.mean(vols_prev), sd) if sd else None
    else:
        f["volume_z_20m"] = None
    f["price_change_1m"] = bar.close - history[-1].close if history else None
    f["volume_price_efficiency_1m"] = abs(f["price_change_1m"]) / max(bar.volume, 1) if f["price_change_1m"] is not None else None
    f["volume_price_efficiency_3m"] = abs(f["price_change_3m"]) / max(vol3 or 0, 1) if f.get("price_change_3m") is not None else None
    f["volume_price_efficiency_5m"] = abs(f["price_change_5m"]) / max(sum(b.volume for b in hist_current[-5:]), 1) if f.get("price_change_5m") is not None else None
    f["rsi9_slope_1m"] = bar.rsi9 - history[-1].rsi9 if history and bar.rsi9 is not None and history[-1].rsi9 is not None else None
    f["rsi9_slope_3m"] = bar.rsi9 - history[-3].rsi9 if len(history) >= 3 and bar.rsi9 is not None and history[-3].rsi9 is not None else None
    f["ma5_slope_3m"] = bar.ma5 - history[-3].ma5 if len(history) >= 3 and bar.ma5 is not None and history[-3].ma5 is not None else None
    f["ma25_slope_5m"] = bar.ma25 - history[-5].ma25 if len(history) >= 5 and bar.ma25 is not None and history[-5].ma25 is not None else None
    f["ema13_slope_4m"] = bar.ema13 - history[-4].ema13 if len(history) >= 4 and bar.ema13 is not None and history[-4].ema13 is not None else None
    f["close_above_ma5"] = int(bar.ma5 is not None and bar.close > bar.ma5); f["close_above_ma25"] = int(bar.ma25 is not None and bar.close > bar.ma25); f["close_above_ma75"] = int(bar.ma75 is not None and bar.close > bar.ma75); f["close_above_vwap"] = int(bar.vwap is not None and bar.close > bar.vwap)
    f.update(board_context(latest_snap, state.market_structure_feature_buffer))
    f["obi_l1"] = None; f["obi_l3"] = None; f["obi_l3_delta_3m"] = None
    f["raw_context_json"] = json.dumps({"signed_volume_price_efficiency_1m": safe_div(f.get("price_change_1m"), max(bar.volume, 1)), "current_bar_excluded_from_prev_ranges": True}, ensure_ascii=False)
    return f


def evaluate_market_structure_strategy(feature: dict[str, Any], cfg: dict[str, Any], state: RuntimeState) -> tuple[list[dict[str, Any]], Optional[PendingSignal]]:
    strategy = cfg.get("market_structure_strategy", {})
    if not strategy.get("enabled", True):
        return [], None
    candidates: list[dict[str, Any]] = []
    pending: Optional[PendingSignal] = None
    close = feature.get("close"); ma5 = feature.get("ma5"); ma25 = feature.get("ma25"); rsi9 = feature.get("rsi9"); volr = feature.get("volume_ratio_1m")
    low5 = feature.get("low_prev_5m"); high5 = feature.get("high_prev_5m"); high10 = feature.get("high_prev_10m"); high20 = feature.get("high_prev_20m"); vwap = feature.get("vwap")
    short_cfg = strategy.get("short", {})
    long_cfg = strategy.get("long", {})
    def add(name: str, side: str, reason: str, confidence: float = 1.0) -> None:
        candidates.append({"ts": feature["ts"], "symbol": feature.get("symbol"), "signal_name": name, "side": side, "action": "LOG_ONLY" if is_data_collection_only(cfg) else "PENDING", "confidence": confidence, "reason": reason, "feature_json": json.dumps(feature, ensure_ascii=False, default=str)})
    short_allowed = bool(not state.open_position and strategy.get("allow_short", True) and short_cfg.get("enabled", True) and close is not None and low5 is not None and ma5 is not None and ma25 is not None and rsi9 is not None and volr is not None and close < low5 and close < ma5 and close < ma25 and rsi9 < float(short_cfg.get("rsi9_max", 50)) and volr >= float(short_cfg.get("volume_ratio_min", 1.2)))
    bar_ts = datetime.fromisoformat(feature["ts"])
    time_str = bar_ts.strftime("%H:%M:%S")
    if short_allowed:
        signal = "opening_range_failure_short" if "09:05:00" <= time_str <= "09:20:00" else "strict_failed_pullback_short"
        add(signal, SIDE_SELL, "close_below_prev5_low_ma5_ma25_rsi_volume")
        feature.setdefault("signal_detected_at", now_jst().isoformat())
        feature.setdefault("pending_signal_created_at", now_jst().isoformat())
        pending = PendingSignal(signal_bar_ts=bar_ts, execute_not_before_minute=bar_ts + timedelta(minutes=1), side=SIDE_SELL, action=ACTION_ENTRY, signal_name=signal, reason="strict_market_structure_short", features=feature)
    if high5 is not None and close is not None and close > high5:
        add("short_exit_reversal_candidate", SIDE_BUY, "close_above_high_prev_5m", 0.8)
        if state.open_position and state.open_position.side == SIDE_SELL:
            feature.setdefault("signal_detected_at", now_jst().isoformat())
            feature.setdefault("pending_signal_created_at", now_jst().isoformat())
            pending = PendingSignal(signal_bar_ts=bar_ts, execute_not_before_minute=bar_ts + timedelta(minutes=1), side=SIDE_BUY, action=ACTION_EXIT, signal_name="short_exit_reversal_candidate", reason="close_above_high_prev_5m", features=feature)
    if high10 is not None and close is not None and close > high10:
        add("short_exit_confirmed", SIDE_BUY, "close_above_high_prev_10m", 1.0)
    long_allowed = bool(strategy.get("allow_long", False) and long_cfg.get("enabled", False) and close is not None and high5 is not None and high20 is not None and ma5 is not None and ma25 is not None and vwap is not None and rsi9 is not None and volr is not None and close > high5 and close > high20 and close > ma5 and close > ma25 and close > vwap and rsi9 >= float(long_cfg.get("rsi9_min", 50)) and volr >= float(long_cfg.get("volume_ratio_min", 1.2)))
    if long_allowed and pending is None:
        add("strict_reversal_long", SIDE_BUY, "close_above_prev5_prev20_ma_vwap_rsi_volume")
        if not long_cfg.get("require_next_bar_hold", True):
            feature.setdefault("signal_detected_at", now_jst().isoformat())
            feature.setdefault("pending_signal_created_at", now_jst().isoformat())
            pending = PendingSignal(signal_bar_ts=bar_ts, execute_not_before_minute=bar_ts + timedelta(minutes=1), side=SIDE_BUY, action=ACTION_ENTRY, signal_name="strict_reversal_long", reason="strict_market_structure_long", features=feature)
    return candidates, pending


def block_order(storage: AsyncDbWriter, signal: Optional[PendingSignal], side: str, price: Optional[float], strategy: str, action: str) -> None:
    storage.log_structured("INFO", "DATA_COLLECTION_ONLY_ORDER_BLOCKED", {"ts": now_jst().isoformat(), "reason": "data_collection_only_enabled", "blocked_action": action, "signal": signal.signal_name if signal else None, "side": side, "price": price, "strategy": strategy}, True)


def spread_ticks(snap: TickSnapshot) -> Optional[float]:
    if snap.buy1_price is None or snap.sell1_price is None:
        return None
    return (snap.sell1_price - snap.buy1_price) / tick_size_for_1570(snap.price)


def normalize_limit_price(price: float, side: str, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    units = price / tick_size
    if abs(units - round(units)) < 1e-9:
        return float(round(units) * tick_size)
    # 買い指値は下へ丸めず、売り指値は上へ丸めない。
    return float((math.ceil(units) if side == SIDE_BUY else math.floor(units)) * tick_size)


def best_limit_price_for_order(side: str, action: str, snap: TickSnapshot) -> tuple[Optional[float], str]:
    if side == SIDE_SELL:
        return snap.buy1_price, "best_bid_for_sell"
    if side == SIDE_BUY:
        return snap.sell1_price, "best_ask_for_buy"
    return None, "unknown_side"


def validate_best_quote(side: str, action: str, snap: TickSnapshot) -> tuple[Optional[float], dict[str, Any]]:
    price, source = best_limit_price_for_order(side, action, snap)
    sp = spread_ticks(snap)
    reason = ""
    if price is None:
        reason = "missing_best_bid_for_sell" if side == SIDE_SELL else "missing_best_ask_for_buy"
    elif price <= 0:
        reason = "best_quote_non_positive"
    elif sp is not None and (sp < 0 or sp > 50):
        reason = "spread_ticks_abnormal"
    if reason:
        return None, {"price_source": source, "reason": reason, "source_best_bid": snap.buy1_price, "source_best_ask": snap.sell1_price, "spread_ticks": sp}
    limit_price = normalize_limit_price(float(price), side, tick_size_for_1570(snap.price))
    return limit_price, {"price_source": source, "reason": "", "source_best_bid": snap.buy1_price, "source_best_ask": snap.sell1_price, "spread_ticks": sp, "limit_price": limit_price}


def log_no_valid_best_quote(storage: AsyncDbWriter, sig: PendingSignal, snap: TickSnapshot, quote_meta: dict[str, Any]) -> None:
    storage.log_structured("WARN", "ORDER_BLOCKED_NO_VALID_BEST_QUOTE", {"signal_name": sig.signal_name, "action": sig.action, "side": sig.side, "current_price": snap.price, "buy1_price": snap.buy1_price, "sell1_price": snap.sell1_price, "spread_ticks": quote_meta.get("spread_ticks"), "reason": quote_meta.get("reason")}, True)


def order_execution_config(config: dict[str, Any], action: str) -> dict[str, Any]:
    return config.get("entry_execution", {}) if action == ACTION_ENTRY else config.get("exit_execution", config.get("entry_execution", {}))


def build_execution_detail(config: dict[str, Any], action: str, front_order_type: int, quote_meta: dict[str, Any]) -> dict[str, Any]:
    exe = order_execution_config(config, action)
    return {"execution_mode": exe.get("mode", "limit_with_timeout"), "limit_mode": exe.get("limit_mode", "marketable_best"), "front_order_type": front_order_type, "limit_price": quote_meta.get("limit_price"), "source_best_bid": quote_meta.get("source_best_bid"), "source_best_ask": quote_meta.get("source_best_ask"), "spread_ticks": quote_meta.get("spread_ticks"), "price_source": quote_meta.get("price_source")}


def build_entry_order_payload(config: dict[str, Any], side: str, snap: TickSnapshot) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    is_short = side == SIDE_SELL
    limit_price, quote_meta = validate_best_quote(side, ACTION_ENTRY, snap)
    if limit_price is None:
        return None, quote_meta
    front_order_type = 20 if config.get("entry_execution", {}).get("mode") == "limit_with_timeout" else config.get("entry_front_order_type", 20)
    payload = {"Password": config.get("order_password", ""), "Symbol": config.get("symbol", "1570"), "Exchange": config.get("margin_entry_exchange", config.get("order_exchange", 9)), "SecurityType": 1, "Side": "1" if is_short else "2", "CashMargin": config.get("entry_cash_margin", 2), "MarginTradeType": config.get("margin_trade_type_short" if is_short else "margin_trade_type_long", 1 if is_short else 3), "DelivType": config.get("entry_deliv_type", 0), "AccountType": config.get("account_type", 4), "Qty": config.get("order_qty", 2), "FrontOrderType": front_order_type, "Price": limit_price, "ExpireDay": config.get("expire_day", 0)}
    quote_meta.update(build_execution_detail(config, ACTION_ENTRY, front_order_type, quote_meta))
    return payload, quote_meta


def build_exit_order_payload(config: dict[str, Any], position: PositionState, snap: TickSnapshot, qty: Optional[int] = None) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    # Credit repayments use CashMargin=3 and either ClosePositionOrder or
    # ClosePositions, never both.  The simple, yaml-compatible default is
    # ClosePositionOrder=0.
    exit_side = "2" if position.side == SIDE_SELL else "1"
    limit_price, quote_meta = validate_best_quote(SIDE_BUY if exit_side == "2" else SIDE_SELL, ACTION_EXIT, snap)
    if limit_price is None:
        return None, quote_meta
    front_order_type = 20 if config.get("exit_execution", {}).get("mode", "limit_with_timeout") == "limit_with_timeout" else config.get("exit_front_order_type", 20)
    payload = {"Password": config.get("order_password", ""), "Symbol": config.get("symbol", "1570"), "Exchange": config.get("exit_order_exchange", 1), "SecurityType": 1, "Side": exit_side, "CashMargin": config.get("exit_cash_margin", 3), "DelivType": config.get("exit_deliv_type", 2), "AccountType": config.get("account_type", 4), "Qty": qty or position.qty, "FrontOrderType": front_order_type, "Price": limit_price, "ExpireDay": config.get("expire_day", 0), "ClosePositionOrder": 0}
    quote_meta.update(build_execution_detail(config, ACTION_EXIT, front_order_type, quote_meta))
    return payload, quote_meta


def order_id_from_response(response: Any) -> str:
    if not isinstance(response, dict):
        return ""
    return str(response.get("OrderId") or response.get("OrderID") or "")


def order_cum_qty(order: Any) -> float:
    if isinstance(order, list):
        return max((order_cum_qty(x) for x in order), default=0.0)
    if not isinstance(order, dict):
        return 0.0
    cum = _to_float(order.get("CumQty"))
    if cum is not None:
        return cum
    details = order.get("Details") or order.get("details") or []
    total = 0.0
    for d in details if isinstance(details, list) else []:
        if isinstance(d, dict) and str(d.get("RecType")) == "8":
            total += _to_float(d.get("Qty") or d.get("ExecutionQty") or d.get("CumQty")) or 0.0
    return total


def order_terminal(order: Any) -> bool:
    if isinstance(order, list):
        return any(order_terminal(x) for x in order)
    if not isinstance(order, dict):
        return False
    state = str(order.get("State") or order.get("OrderState") or "")
    return state == "5"


def normalize_positions(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, dict):
        raw = raw.get("Positions") or raw.get("positions") or raw.get("Result") or []
    return raw if isinstance(raw, list) else []


def position_from_api(raw_positions: Any, side: str, strategy: str, order_id: str, config: dict[str, Any]) -> Optional[PositionState]:
    target_side = "1" if side == SIDE_SELL else "2"
    positions = [p for p in normalize_positions(raw_positions) if isinstance(p, dict) and str(p.get("Side")) == target_side and (_to_float(p.get("LeavesQty")) or 0) > 0]
    if not positions:
        return None
    qty = int(sum(_to_float(p.get("LeavesQty")) or 0 for p in positions))
    weighted = sum((_to_float(p.get("Price")) or 0) * (_to_float(p.get("LeavesQty")) or 0) for p in positions)
    entry_price = weighted / qty if qty else 0.0
    execution_ids = [str(p.get("ExecutionID") or p.get("HoldID") or "") for p in positions if p.get("ExecutionID") or p.get("HoldID")]
    margin_trade_type = int(_to_float(positions[0].get("MarginTradeType")) or config.get("margin_trade_type_short" if side == SIDE_SELL else "margin_trade_type_long", 1 if side == SIDE_SELL else 3))
    return PositionState(side=side, qty=qty, entry_price=entry_price, strategy=strategy, entry_ts=now_jst(), order_id=order_id, execution_ids=execution_ids, margin_trade_type=margin_trade_type, cash_margin=2)


def reconcile_positions(config: dict[str, Any], client: KabuApiClient, storage: AsyncDbWriter, state: RuntimeState, expected_side: Optional[str] = None, strategy: str = "market_structure_strategy", order_id: str = "") -> Optional[PositionState]:
    started = now_jst()
    positions = client.get_positions(str(config.get("symbol", "1570")))
    side = expected_side or (state.open_position.side if state.open_position else SIDE_SELL)
    pos = position_from_api(positions, side, strategy, order_id, config)
    state.open_position = pos
    storage.log_structured("INFO" if pos else "WARN", "POSITION_RECONCILE_RESULT", {"started_at": started.isoformat(), "finished_at": now_jst().isoformat(), "expected_side": expected_side, "position_found": bool(pos), "qty": pos.qty if pos else 0, "order_id": order_id}, True)
    if is_data_collection_only(config) and pos:
        storage.log_structured("WARN", "DATA_COLLECTION_ONLY_LIVE_POSITION_DETECTED", {"side": pos.side, "qty": pos.qty, "entry_price": pos.entry_price, "order_id": pos.order_id}, True)
    return pos


def wait_order_fill(config: dict[str, Any], client: KabuApiClient, storage: AsyncDbWriter, order_id: str, min_qty: float, action: str = ACTION_ENTRY) -> tuple[float, bool]:
    timeout = float(order_execution_config(config, action).get("timeout_sec", 2.0))
    deadline = time.monotonic() + timeout
    last_order: Any = None
    while time.monotonic() <= deadline:
        last_order = client.get_orders(str(config.get("symbol", "1570")), order_id, True)
        cum = order_cum_qty(last_order)
        if cum >= min_qty:
            return cum, True
        if order_terminal(last_order) and cum > 0:
            return cum, cum >= min_qty
        time.sleep(0.25)
    storage.log_structured("WARN", "ORDER_FILL_TIMEOUT", {"order_id": order_id, "cum_qty": order_cum_qty(last_order), "min_qty": min_qty}, True)
    return order_cum_qty(last_order), False


def confirm_cancel(config: dict[str, Any], client: KabuApiClient, order_id: str) -> bool:
    for _ in range(8):
        order = client.get_orders(str(config.get("symbol", "1570")), order_id, True)
        if order_terminal(order):
            return True
        time.sleep(0.25)
    return False


def refresh_order_snapshot(config: dict[str, Any], client: KabuApiClient, storage: AsyncDbWriter, fallback_snap: TickSnapshot) -> TickSnapshot:
    try:
        raw = client.get_board(str(config.get("symbol", "1570")), int(config.get("exchange", 1)))
        snap = extract_snapshot(raw)
        if snap:
            return snap
    except Exception as exc:
        storage.log_structured("WARN", "ORDER_SNAPSHOT_REFRESH_FAILED", {"error": str(exc), "fallback_ts": fallback_snap.ts.isoformat()}, True)
    return fallback_snap


def latency_payload(sig: PendingSignal, extra: dict[str, Any]) -> dict[str, Any]:
    payload = {"signal_bar_ts": sig.signal_bar_ts.isoformat(), "signal_detected_at": sig.features.get("signal_detected_at"), "pending_signal_created_at": sig.features.get("pending_signal_created_at"), "next_bar_first_tick_at": extra.get("next_bar_first_tick_at"), "order_decision_at": extra.get("order_decision_at"), "order_send_started_at": extra.get("order_send_started_at"), "order_response_at": extra.get("order_response_at"), "order_verify_started_at": extra.get("order_verify_started_at"), "order_verify_finished_at": extra.get("order_verify_finished_at"), "position_reconcile_started_at": extra.get("position_reconcile_started_at"), "position_reconcile_finished_at": extra.get("position_reconcile_finished_at"), "db_enqueue_at": now_jst().isoformat(), "signal_to_decision_ms": extra.get("signal_to_decision_ms"), "decision_to_order_send_ms": extra.get("decision_to_order_send_ms"), "order_send_to_response_ms": extra.get("order_send_to_response_ms"), "order_response_to_verify_ms": extra.get("order_response_to_verify_ms"), "signal_to_position_confirm_ms": extra.get("signal_to_position_confirm_ms"), "db_queue_lag_ms": 0, "signal": sig.signal_name, "action": sig.action, "side": sig.side}
    for key in ("execution_mode", "limit_mode", "front_order_type", "limit_price", "source_best_bid", "source_best_ask", "spread_ticks", "price_source", "reprice_attempt"):
        if key in extra:
            payload[key] = extra[key]
    return payload


def execute_signal_order(config: dict[str, Any], client: KabuApiClient, storage: AsyncDbWriter, state: RuntimeState, sig: PendingSignal, snap: TickSnapshot) -> None:
    decision_at = now_jst()
    if is_data_collection_only(config):
        event = "DATA_COLLECTION_ONLY_EXIT_BLOCKED" if sig.action == ACTION_EXIT else "DATA_COLLECTION_ONLY_ORDER_BLOCKED"
        storage.log_structured("INFO", event, {"ts": decision_at.isoformat(), "reason": "data_collection_only_enabled", "blocked_action": "send_order", "signal": sig.signal_name, "side": sig.side, "price": snap.price, "strategy": "market_structure_strategy"}, True)
        storage.log_structured("INFO", "ORDER_LATENCY_TRACE", latency_payload(sig, {"next_bar_first_tick_at": snap.ts.isoformat(), "order_decision_at": decision_at.isoformat(), "signal_to_decision_ms": (decision_at - sig.signal_bar_ts).total_seconds() * 1000}), True)
        return
    if sig.action == ACTION_ENTRY and not entry_orders_enabled(config, state, snap):
        storage.log_structured("WARN", "ORDER_BLOCKED_BY_RUNTIME_GUARD", {"live_mode": config.get("live_mode"), "entry_execution_enabled": config.get("entry_execution", {}).get("enabled"), "data_collection_only": is_data_collection_only(config), "strategy_enabled": config.get("market_structure_strategy", {}).get("enabled"), "has_open_position": state.open_position is not None, "recovery_until": iso_dt(state.recovery_until), "within_entry_window": market_open_for_new_entry(snap.ts, parse_hms(config.get("new_entry_cutoff_time", "15:10:00"))), "signal": sig.signal_name, "action": sig.action}, True)
        return
    if sig.action == ACTION_EXIT and not exit_orders_enabled(config, state, snap):
        storage.log_structured("WARN", "ORDER_BLOCKED_BY_RUNTIME_GUARD", {"live_mode": config.get("live_mode"), "data_collection_only": is_data_collection_only(config), "strategy_enabled": config.get("market_structure_strategy", {}).get("enabled"), "has_open_position": state.open_position is not None, "signal": sig.signal_name, "action": sig.action}, True)
        return
    if sig.action == ACTION_EXIT and state.open_position is None:
        storage.log_structured("WARN", "EXIT_BLOCKED_NO_OPEN_POSITION", {"signal": sig.signal_name, "side": sig.side}, True)
        return
    exe_cfg = order_execution_config(config, sig.action)
    max_reprice = int(exe_cfg.get("max_reprice_attempts", 0)) if sig.action == ACTION_EXIT and sig.signal_name in {"HARD_STOP", "FORCE_CLOSE_1520"} else 0
    attempt = 0
    current_snap = snap
    last_latency: dict[str, Any] = {}
    while True:
        if sig.action == ACTION_ENTRY:
            payload, quote_meta = build_entry_order_payload(config, sig.side, current_snap)
        else:
            payload, quote_meta = build_exit_order_payload(config, state.open_position, current_snap, state.open_position.qty if state.open_position else None)
        if payload is None:
            log_no_valid_best_quote(storage, sig, current_snap, quote_meta)
            return
        execution_detail = build_execution_detail(config, sig.action, int(payload["FrontOrderType"]), quote_meta)
        send_started = now_jst()
        response = client.send_order(payload)
        response_at = now_jst()
        order_id = order_id_from_response(response)
        if not order_id:
            state.recovery_until = now_jst() + timedelta(seconds=30)
            storage.log_structured("CRITICAL", "ORDER_RESPONSE_MISSING_ORDER_ID", {"response": response, "signal": sig.signal_name, **execution_detail}, True)
            return
        verify_started = now_jst()
        min_qty = config.get("entry_min_fill_qty", config.get("order_qty", 1)) if sig.action == ACTION_ENTRY else 1
        cum_qty, fill_ok = wait_order_fill(config, client, storage, order_id, float(min_qty), sig.action)
        verify_finished = now_jst()
        if not fill_ok and exe_cfg.get("cancel_on_timeout", True):
            client.cancel_order(order_id)
            canceled = confirm_cancel(config, client, order_id)
            storage.log_structured("WARN", "ORDER_CANCEL_AFTER_TIMEOUT", {"order_id": order_id, "cum_qty": cum_qty, "cancel_confirmed": canceled, "attempt": attempt, **execution_detail}, True)
        rec_started = now_jst()
        pos = reconcile_positions(config, client, storage, state, sig.side if sig.action == ACTION_ENTRY else None, "market_structure_strategy", order_id)
        rec_finished = now_jst()
        if sig.action == ACTION_EXIT and (not pos or pos.qty == 0):
            state.open_position = None
        elif sig.action == ACTION_ENTRY and fill_ok and not pos:
            state.recovery_until = now_jst() + timedelta(seconds=60)
            storage.log_structured("CRITICAL", "POSITION_CONFIRM_FAILED_AFTER_ENTRY_FILL", {"order_id": order_id, "cum_qty": cum_qty, **execution_detail}, True)
        storage.enqueue("execution", (now_jst().isoformat(), "ORDER_REQUEST", sig.side, cum_qty, current_snap.price, order_id, json.dumps({"signal": sig.signal_name, "action": sig.action, "payload": payload, "response": response, "execution_detail": execution_detail, "attempt": attempt}, ensure_ascii=False, default=str)), True)
        last_latency = {"next_bar_first_tick_at": current_snap.ts.isoformat(), "order_decision_at": decision_at.isoformat(), "order_send_started_at": send_started.isoformat(), "order_response_at": response_at.isoformat(), "order_verify_started_at": verify_started.isoformat(), "order_verify_finished_at": verify_finished.isoformat(), "position_reconcile_started_at": rec_started.isoformat(), "position_reconcile_finished_at": rec_finished.isoformat(), "signal_to_decision_ms": (decision_at - sig.signal_bar_ts).total_seconds() * 1000, "decision_to_order_send_ms": (send_started - decision_at).total_seconds() * 1000, "order_send_to_response_ms": (response_at - send_started).total_seconds() * 1000, "order_response_to_verify_ms": (verify_finished - response_at).total_seconds() * 1000, "signal_to_position_confirm_ms": (rec_finished - sig.signal_bar_ts).total_seconds() * 1000, **execution_detail, "reprice_attempt": attempt}
        if sig.action == ACTION_ENTRY or fill_ok or state.open_position is None or attempt >= max_reprice:
            break
        attempt += 1
        storage.log_structured("WARN", "EXIT_REPRICE_ATTEMPT", {"signal": sig.signal_name, "attempt": attempt, "max_reprice_attempts": max_reprice, "remaining_qty": state.open_position.qty if state.open_position else 0, **execution_detail}, True)
        current_snap = refresh_order_snapshot(config, client, storage, current_snap)
    if sig.action == ACTION_EXIT and state.open_position is not None and not fill_ok:
        storage.log_structured("CRITICAL", "EXIT_REPRICE_EXHAUSTED_POSITION_REMAINS", {"signal": sig.signal_name, "remaining_qty": state.open_position.qty, "max_reprice_attempts": max_reprice}, True)
    storage.log_structured("INFO", "ORDER_LATENCY_TRACE", latency_payload(sig, last_latency), True)


def handle_pending_signal(config: dict[str, Any], client: KabuApiClient, storage: AsyncDbWriter, state: RuntimeState, snap: TickSnapshot) -> None:
    sig = state.pending_signal
    if not sig or snap.ts < sig.execute_not_before_minute:
        return
    if sig.action == ACTION_ENTRY:
        if state.open_position is not None:
            storage.log_structured("WARN", "ENTRY_BLOCKED_BY_OPEN_POSITION", {"signal": sig.signal_name, "side": sig.side, "signal_bar_ts": sig.signal_bar_ts.isoformat()}, True)
            state.pending_signal = None
            return
        if state.last_entry_bar_ts == sig.signal_bar_ts:
            storage.log_structured("WARN", "ENTRY_BLOCKED_BY_SAME_BAR_DUPLICATE", {"signal": sig.signal_name, "side": sig.side, "signal_bar_ts": sig.signal_bar_ts.isoformat()}, True)
            state.pending_signal = None
            return
        if not market_open_for_new_entry(snap.ts, parse_hms(config.get("new_entry_cutoff_time", "15:10:00"))):
            state.pending_signal = None
            storage.log_structured("INFO", "MARKET_STRUCTURE_PENDING_SIGNAL_EXPIRED", {"signal": sig.signal_name, "action": sig.action, "bar_ts": sig.signal_bar_ts.isoformat(), "reason": "outside_entry_window"}, True)
            return
    elif sig.action == ACTION_EXIT:
        if state.open_position is None:
            storage.log_structured("WARN", "EXIT_BLOCKED_NO_OPEN_POSITION", {"signal": sig.signal_name, "side": sig.side, "signal_bar_ts": sig.signal_bar_ts.isoformat()}, True)
            state.pending_signal = None
            return
        # EXITには新規IN用の15:10 cutoffを適用しない。取引可能時間外は
        # 通常EXITだけ止めるが、FORCE_CLOSE_1520 / HARD_STOPは即時経路で処理する。
        if not market_open_for_exit(snap.ts) and sig.signal_name not in {"FORCE_CLOSE_1520", "HARD_STOP"}:
            storage.log_structured("INFO", "MARKET_STRUCTURE_PENDING_SIGNAL_EXPIRED", {"signal": sig.signal_name, "action": sig.action, "bar_ts": sig.signal_bar_ts.isoformat(), "reason": "outside_exit_window"}, True)
            state.pending_signal = None
            return
    execute_signal_order(config, client, storage, state, sig, snap)
    if sig.action == ACTION_ENTRY:
        state.last_entry_bar_ts = sig.signal_bar_ts
    state.pending_signal = None


def check_hard_stop_and_force_close(config: dict[str, Any], client: KabuApiClient, storage: AsyncDbWriter, state: RuntimeState, snap: TickSnapshot) -> None:
    pos = state.open_position
    if not pos:
        return
    hard_ticks = float(config.get("market_structure_strategy", {}).get("hard_stop_ticks", 20))
    tick = tick_size_for_1570(pos.entry_price)
    hard = (pos.side == SIDE_SELL and snap.price >= pos.entry_price + hard_ticks * tick) or (pos.side == SIDE_BUY and snap.price <= pos.entry_price - hard_ticks * tick)
    force = snap.ts.timetz().replace(tzinfo=None) >= parse_hms(config.get("force_close_after", "15:20:00"))
    if not (hard or force):
        return
    reason = "HARD_STOP" if hard else "FORCE_CLOSE_1520"
    if not exit_orders_enabled(config, state, snap):
        storage.log_structured("WARN", "DATA_COLLECTION_ONLY_LIVE_POSITION_DETECTED", {"reason": reason, "side": pos.side, "qty": pos.qty, "entry_price": pos.entry_price, "current_price": snap.price}, True)
        if is_data_collection_only(config):
            storage.log_structured("INFO", "DATA_COLLECTION_ONLY_EXIT_BLOCKED", {"reason": reason, "blocked_action": "send_order", "side": SIDE_BUY if pos.side == SIDE_SELL else SIDE_SELL, "price": snap.price, "strategy": pos.strategy}, True)
        else:
            storage.log_structured("WARN", "EXIT_BLOCKED_BY_RUNTIME_GUARD", {"reason": reason, "live_mode": config.get("live_mode"), "strategy_enabled": config.get("market_structure_strategy", {}).get("enabled"), "has_open_position": state.open_position is not None}, True)
        return
    sig = PendingSignal(signal_bar_ts=snap.ts, execute_not_before_minute=snap.ts, side=SIDE_BUY if pos.side == SIDE_SELL else SIDE_SELL, action=ACTION_EXIT, signal_name=reason, reason=reason, features={"signal_detected_at": now_jst().isoformat(), "pending_signal_created_at": now_jst().isoformat()})
    execute_signal_order(config, client, storage, state, sig, snap)


def save_bar_and_features(config: dict[str, Any], storage: AsyncDbWriter, state: RuntimeState, bar: Bar, latest_snap: Optional[TickSnapshot], is_1m: bool) -> None:
    if is_1m:
        decorate_bar(bar, state.bars_1m_buffer)
        feature = compute_market_structure_features(str(config.get("symbol", "1570")), bar, state, latest_snap)
        candidates, pending = evaluate_market_structure_strategy(feature, config, state)
        if pending and (pending.action == ACTION_EXIT or not state.open_position):
            state.pending_signal = pending
            storage.log_structured("INFO", "MARKET_STRUCTURE_PENDING_SIGNAL_CREATED", {"signal": pending.signal_name, "side": pending.side, "signal_bar_ts": pending.signal_bar_ts.isoformat(), "execute_not_before_minute": pending.execute_not_before_minute.isoformat()}, True)
            if is_data_collection_only(config):
                if pending.action == ACTION_EXIT:
                    storage.log_structured("INFO", "DATA_COLLECTION_ONLY_EXIT_BLOCKED", {"reason": pending.reason, "blocked_action": "send_order", "side": pending.side, "price": latest_snap.price if latest_snap else None, "strategy": "market_structure_strategy"}, True)
                else:
                    block_order(storage, pending, pending.side, latest_snap.price if latest_snap else None, "market_structure_strategy", "send_order")
        for c in candidates:
            c["blocked_by_data_collection_only"] = int(is_data_collection_only(config))
            storage.enqueue("candidate", (c["ts"], c["symbol"], c["signal_name"], c["side"], "LOG_ONLY" if is_data_collection_only(config) else c["action"], c["confidence"], c["reason"], c["blocked_by_data_collection_only"], c["feature_json"]), False)
        state.market_structure_feature_buffer.append(feature)
        state.bars_1m_buffer.append(bar)
        storage.enqueue("bar1", (bar.ts.isoformat(), bar.open, bar.high, bar.low, bar.close, bar.volume, bar.vwap, bar.ma5, bar.ma13, bar.ema13, bar.ma25, bar.ma75, bar.rsi9), False)
        storage.enqueue("feature", feature, False)
    else:
        state.bars_3m_buffer.append(bar)
        storage.enqueue("bar3", (bar.ts.isoformat(), bar.open, bar.high, bar.low, bar.close, bar.volume, bar.vwap), False)


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config_1570_market_structure.json")
    ap.add_argument("--runtime-minutes", type=float, default=None)
    args = ap.parse_args()
    config = load_config(args.config)
    if args.runtime_minutes is not None:
        config["runtime_minutes"] = args.runtime_minutes
    outdir = Path(config.get("outdir", "monitor_output_market_structure")); outdir.mkdir(parents=True, exist_ok=True)
    db_path = str(outdir / f"market_structure_{now_jst().strftime('%Y%m%d')}.db")
    db_cfg = config.get("async_db_writer", {})
    storage = AsyncDbWriter(db_path, config.get("sqlite", {}), db_cfg.get("max_queue_size", 10000), db_cfg.get("batch_size", 100), db_cfg.get("flush_interval_sec", 0.5))
    storage.log_structured("INFO", "ASYNC_DB_WRITER_STARTED", {"db_path": db_path}, True)
    startup = {"live_mode": config.get("live_mode"), "strategy_mode": config.get("strategy_mode"), "data_collection_only": config.get("data_collection_only"), "entry_execution": config.get("entry_execution"), "exit_execution": config.get("exit_execution"), "market_structure_strategy": config.get("market_structure_strategy"), "entry_front_order_type": config.get("entry_front_order_type"), "exit_front_order_type": config.get("exit_front_order_type"), "legacy_long_rsi50_enabled": False, "legacy_long_rsi35_enabled": False, "legacy_scalping_enabled": False, "legacy_feature_entries_enabled": False, "legacy_big_trend_enabled": False, "legacy_hold_score_enabled": False}
    storage.log_structured("INFO", "MARKET_STRUCTURE_ENGINE_STARTED", startup, True)
    storage.log_structured("INFO", "DATA_COLLECTION_ONLY_ENABLED", startup, True)
    storage.log_structured("INFO", "ORDER_DISABLED_CONFIRMATION", {"orders_enabled": orders_enabled(config), "live_mode": config.get("live_mode"), "data_collection_only": is_data_collection_only(config), "entry_execution_enabled": config.get("entry_execution", {}).get("enabled"), "exit_execution": config.get("exit_execution"), "entry_front_order_type": config.get("entry_front_order_type"), "exit_front_order_type": config.get("exit_front_order_type")}, True)
    storage.log_structured("INFO", "LEGACY_STRATEGIES_DISABLED", {k: startup[k] for k in startup if k.startswith("legacy_")}, True)
    state = RuntimeState()
    client = KabuApiClient(config.get("base_url") or config.get("api_base_url") or API_BASE_DEFAULT, config.get("api_password", ""), config.get("order_password", ""))
    try:
        client.get_token()
        client.register_symbol(str(config.get("symbol", "1570")), int(config.get("exchange", 1)))
        if config.get("data_collection_only", {}).get("allow_position_polling", True):
            # 起動時照合。data_collection_only中に建玉があっても自動決済はしない。
            reconcile_positions(config, client, storage, state, None, "market_structure_strategy", "")
    except Exception as exc:
        storage.log_structured("ERROR", "KABU_API_STARTUP_FAILED", {"error": str(exc), "data_collection_only": is_data_collection_only(config)}, True)
        storage.stop()
        raise
    rb1 = RollingBars(1); rb3 = RollingBars(3)
    ws = WebSocketMarketDataFeed(config, str(config.get("symbol", "1570")), int(config.get("exchange", 1)), storage)
    if config.get("market_data_source", {}).get("mode") == "websocket":
        ws.start()
    started = time.monotonic(); runtime = config.get("runtime_minutes")
    last_ws_seq: Optional[int] = None; last_rest = 0.0; latest: Optional[TickSnapshot] = None
    md_cfg = config.get("market_data_source", {})
    try:
        while runtime is None or (time.monotonic() - started) < float(runtime) * 60:
            loop_now = now_jst()
            snaps = ws.drain_snapshots_after(last_ws_seq) if md_cfg.get("mode") == "websocket" else []
            if not snaps and md_cfg.get("fallback_to_rest", True) and time.monotonic() - last_rest >= float(md_cfg.get("rest_fallback_min_interval_sec", 2.0)):
                try:
                    board = client.get_board(str(config.get("symbol", "1570")), int(config.get("exchange", 1)))
                    snap = extract_snapshot(board)
                    if snap:
                        snaps = [snap]
                        last_rest = time.monotonic(); state.rest_fallback_count += 1
                        storage.log_structured("INFO", "REST_SNAPSHOT_USED", {"snapshot_ts": snap.ts.isoformat(), "price": snap.price}, False)
                except Exception as exc:
                    storage.log_structured("WARN", "REST_FALLBACK_FAILED", {"error": str(exc)}, False)
            for snap in snaps:
                attach_volume_delta(state, snap, storage)
                latest = snap; state.snapshot_buffer.append(snap)
                if snap.ws_seq is not None:
                    last_ws_seq = snap.ws_seq
                check_hard_stop_and_force_close(config, client, storage, state, snap)
                handle_pending_signal(config, client, storage, state, snap)
                b1 = rb1.update(snap); b3 = rb3.update(snap)
                if b1: save_bar_and_features(config, storage, state, b1, latest, True)
                if b3: save_bar_and_features(config, storage, state, b3, latest, False)
            b1t = rb1.force_finalize_completed_bucket(loop_now, int(md_cfg.get("bar_finalize_delay_ms", 300)))
            b3t = rb3.force_finalize_completed_bucket(loop_now, int(md_cfg.get("bar_finalize_delay_ms", 300)))
            if b1t: save_bar_and_features(config, storage, state, b1t, latest, True)
            if b3t: save_bar_and_features(config, storage, state, b3t, latest, False)
            time.sleep(float(md_cfg.get("websocket_loop_sleep_sec", 0.1)) if md_cfg.get("mode") == "websocket" else 1.0)
    finally:
        ws.stop(); storage.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
