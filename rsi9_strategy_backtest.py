#!/usr/bin/env python3
"""Simple RSI(9) rule backtest on bars_1m in monitor DB.

Rule set (as requested):
- Long entry: RSI <= 30
- Long exit TP: RSI >= 70
- Long exit SL: RSI <= 20
- Short entry: RSI >= 80
- Short exit TP: RSI <= 40
- Short exit SL: RSI >= 90
"""

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
    pnl: float


def wilder_rsi(closes: list[float], period: int = 9) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= period:
        return out

    gains: list[float] = []
    losses: list[float] = []
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


def load_bars(db_path: str) -> tuple[list[str], list[float]]:
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT ts, close FROM bars_1m ORDER BY ts").fetchall()
    con.close()
    if not rows:
        raise RuntimeError("bars_1m is empty")
    ts = [r[0] for r in rows]
    closes = [float(r[1]) for r in rows]
    return ts, closes


def run_rule(ts: list[str], closes: list[float], force_eod_exit: bool = True) -> list[Trade]:
    rsi = wilder_rsi(closes, 9)
    trades: list[Trade] = []

    pos = 0
    ep = 0.0
    et = ""
    er = 0.0

    for i, (t, p, r) in enumerate(zip(ts, closes, rsi)):
        if r is None:
            continue

        if pos == 0:
            if r <= 30.0:
                pos = 1
                ep, et, er = p, t, r
            elif r >= 80.0:
                pos = -1
                ep, et, er = p, t, r
            continue

        if pos == 1:
            if r >= 70.0 or r <= 20.0:
                reason = "TP" if r >= 70.0 else "SL"
                trades.append(Trade(et, t, "LONG", ep, p, er, r, reason, p - ep))
                pos = 0
        else:
            if r <= 40.0 or r >= 90.0:
                reason = "TP" if r <= 40.0 else "SL"
                trades.append(Trade(et, t, "SHORT", ep, p, er, r, reason, ep - p))
                pos = 0

    if force_eod_exit and pos != 0:
        p = closes[-1]
        t = ts[-1]
        side = "LONG" if pos == 1 else "SHORT"
        pnl = (p - ep) if pos == 1 else (ep - p)
        last_rsi = rsi[-1] if rsi[-1] is not None else er
        trades.append(Trade(et, t, side, ep, p, er, float(last_rsi), "EOD", pnl))

    return trades


def summarize(trades: list[Trade]) -> str:
    if not trades:
        return "No trades"
    pnls = [t.pnl for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    eq = 0.0
    peak = 0.0
    mdd = 0.0
    for p in pnls:
        eq += p
        if eq > peak:
            peak = eq
        mdd = min(mdd, eq - peak)
    return "\n".join(
        [
            f"trades={len(trades)}",
            f"wins={wins}",
            f"win_rate={wins/len(trades)*100:.2f}%",
            f"total_pnl={sum(pnls):.2f}",
            f"avg_pnl={sum(pnls)/len(pnls):.2f}",
            f"median_pnl={median(pnls):.2f}",
            f"max_drawdown={mdd:.2f}",
        ]
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="monitor_1570_20260520.db")
    args = ap.parse_args()

    ts, closes = load_bars(args.db)
    trades = run_rule(ts, closes)
    print(summarize(trades))


if __name__ == "__main__":
    main()
