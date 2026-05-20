#!/usr/bin/env python3
from __future__ import annotations
import argparse
import sqlite3
from dataclasses import dataclass
from statistics import median

@dataclass
class Trade:
    entry_ts: str
    exit_ts: str
    side: str
    entry_price: float
    exit_price: float
    entry_rsi: float
    exit_rsi: float
    reason: str
    qty: int
    added: bool
    pnl: float


def wilder_rsi(closes: list[float], period: int = 9) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= period:
        return out
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = (avg_gain / avg_loss) if avg_loss else 1e18
    out[period] = 100.0 - (100.0 / (1.0 + rs))
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = max(d, 0.0)
        loss = max(-d, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        rs = (avg_gain / avg_loss) if avg_loss else 1e18
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def load_bars(db_path: str) -> tuple[list[str], list[float], list[float], list[float]]:
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT ts, open, high, low, close FROM bars_1m ORDER BY ts").fetchall()
    con.close()
    if not rows:
        raise RuntimeError("bars_1m is empty")
    ts = [r[0] for r in rows]
    opens = [float(r[1]) for r in rows]
    highs = [float(r[2]) for r in rows]
    lows = [float(r[3]) for r in rows]
    closes = [float(r[4]) for r in rows]
    return ts, opens, highs, lows, closes


def run_rule(ts: list[str], opens: list[float], closes: list[float]) -> list[Trade]:
    rsi = wilder_rsi(closes, 9)
    trades: list[Trade] = []

    pos = 0  # 0 flat, 1 long, -1 short
    qty = 0
    added = False
    entry_price = 0.0
    entry_ts = ""
    entry_rsi = 0.0

    for i in range(len(closes) - 1):
        cur_rsi = rsi[i]
        if cur_rsi is None:
            continue
        next_open = opens[i + 1]
        next_ts = ts[i + 1]

        if pos == 0:
            if cur_rsi <= 20:
                pos, qty, added = 1, 1, False
                entry_price, entry_ts, entry_rsi = next_open, next_ts, cur_rsi
            elif cur_rsi >= 80:
                pos, qty, added = -1, 1, False
                entry_price, entry_ts, entry_rsi = next_open, next_ts, cur_rsi
            continue

        if pos == 1:
            if (not added) and cur_rsi <= 10:
                entry_price = (entry_price * qty + next_open) / (qty + 1)
                qty += 1
                added = True
                continue
            tp = 60.0 if added else 70.0
            if cur_rsi >= tp:
                pnl = (next_open - entry_price) * qty
                trades.append(Trade(entry_ts, next_ts, "LONG", entry_price, next_open, entry_rsi, cur_rsi, "TP", qty, added, pnl))
                pos = 0
                qty = 0

        elif pos == -1:
            if (not added) and cur_rsi >= 90:
                entry_price = (entry_price * qty + next_open) / (qty + 1)
                qty += 1
                added = True
                continue
            tp = 40.0 if added else 30.0
            if cur_rsi <= tp:
                pnl = (entry_price - next_open) * qty
                trades.append(Trade(entry_ts, next_ts, "SHORT", entry_price, next_open, entry_rsi, cur_rsi, "TP", qty, added, pnl))
                pos = 0
                qty = 0

    if pos != 0:
        p = closes[-1]
        t = ts[-1]
        pnl = ((p - entry_price) if pos == 1 else (entry_price - p)) * max(qty, 1)
        trades.append(Trade(entry_ts, t, "LONG" if pos == 1 else "SHORT", entry_price, p, entry_rsi, float(rsi[-1] or entry_rsi), "EOD", max(qty, 1), added, pnl))

    return trades


def summarize(trades: list[Trade]) -> str:
    if not trades:
        return "No trades"
    pnls = [t.pnl for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    eq = peak = mdd = 0.0
    for p in pnls:
        eq += p
        peak = max(peak, eq)
        mdd = min(mdd, eq - peak)
    add_count = sum(1 for t in trades if t.added)
    return "\n".join([
        f"trades={len(trades)}",
        f"wins={wins}",
        f"win_rate={wins/len(trades)*100:.2f}%",
        f"total_pnl={sum(pnls):.2f}",
        f"avg_pnl={sum(pnls)/len(pnls):.2f}",
        f"median_pnl={median(pnls):.2f}",
        f"max_drawdown={mdd:.2f}",
        f"added_positions={add_count}",
    ])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="monitor_1570_20260520.db")
    args = ap.parse_args()
    ts, opens, _highs, _lows, closes = load_bars(args.db)
    trades = run_rule(ts, opens, closes)
    print(summarize(trades))


if __name__ == "__main__":
    main()
