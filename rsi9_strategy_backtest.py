#!/usr/bin/env python3
from __future__ import annotations
import argparse
import sqlite3
from dataclasses import dataclass
from statistics import median

RSI_PERIOD = 9
LONG_ENTRY_RSI = 20.0
LONG_TP_RSI = 50.0
SHORT_ENTRY_RSI = 70.0  # currently frozen in monitor; kept for optional compare
SHORT_TP_RSI = 40.0

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


def wilder_rsi(closes: list[float], period: int = RSI_PERIOD) -> list[float | None]:
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


def sma(series: list[float], n: int) -> list[float | None]:
    out: list[float | None] = [None] * len(series)
    if n <= 0:
        return out
    run = 0.0
    for i, v in enumerate(series):
        run += v
        if i >= n:
            run -= series[i - n]
        if i >= n - 1:
            out[i] = run / n
    return out


def load_bars(db_path: str) -> tuple[list[str], list[float], list[float]]:
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT ts, open, close FROM bars_1m ORDER BY ts").fetchall()
    con.close()
    if not rows:
        raise RuntimeError("bars_1m is empty")
    ts = [r[0] for r in rows]
    opens = [float(r[1]) for r in rows]
    closes = [float(r[2]) for r in rows]
    return ts, opens, closes


def run_rule(ts: list[str], opens: list[float], closes: list[float], freeze_short: bool = True) -> list[Trade]:
    rsi = wilder_rsi(closes, RSI_PERIOD)
    ma5 = sma(closes, 5)
    ma25 = sma(closes, 25)
    ma75 = sma(closes, 75)

    trades: list[Trade] = []
    pos = 0
    entry_price = 0.0
    entry_ts = ""
    entry_rsi = 0.0

    for i in range(len(closes) - 1):
        r = rsi[i]
        if r is None:
            continue
        next_open = opens[i + 1]
        next_ts = ts[i + 1]

        if pos == 0:
            # 09:00-09:15 no entry
            hms = ts[i][11:19] if len(ts[i]) >= 19 else ""
            in_no_entry = ("09:00:00" <= hms <= "09:15:59")
            if in_no_entry:
                continue
            if i >= 2 and rsi[i - 1] is not None and rsi[i - 2] is not None and ma5[i] and ma25[i] and ma75[i]:
                long_ma = ma75[i] > ma25[i] > ma5[i]
                short_ma = ma5[i] > ma25[i] > ma75[i]
                long_base = long_ma and r <= LONG_ENTRY_RSI and rsi[i - 1] <= LONG_ENTRY_RSI
                long_special = (rsi[i - 2] - r) >= 17.0 and closes[i] > ma5[i] and closes[i] > ma25[i] and closes[i] > ma75[i]
                if long_base or long_special:
                    pos = 1
                    entry_price, entry_ts, entry_rsi = next_open, next_ts, r
                elif (not freeze_short) and short_ma and r >= SHORT_ENTRY_RSI and rsi[i - 1] >= SHORT_ENTRY_RSI:
                    pos = -1
                    entry_price, entry_ts, entry_rsi = next_open, next_ts, r
            continue

        if pos == 1 and r >= LONG_TP_RSI:
            trades.append(Trade(entry_ts, next_ts, "LONG", entry_price, next_open, entry_rsi, r, "TP_RSI", next_open - entry_price))
            pos = 0
        elif pos == -1 and r <= SHORT_TP_RSI:
            trades.append(Trade(entry_ts, next_ts, "SHORT", entry_price, next_open, entry_rsi, r, "TP_RSI", entry_price - next_open))
            pos = 0

    if pos != 0:
        p = closes[-1]
        t = ts[-1]
        pnl = (p - entry_price) if pos == 1 else (entry_price - p)
        trades.append(Trade(entry_ts, t, "LONG" if pos == 1 else "SHORT", entry_price, p, entry_rsi, float(rsi[-1] or entry_rsi), "EOD", pnl))

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
    return "\n".join([
        f"trades={len(trades)}",
        f"wins={wins}",
        f"win_rate={wins/len(trades)*100:.2f}%",
        f"total_pnl={sum(pnls):.2f}",
        f"avg_pnl={sum(pnls)/len(pnls):.2f}",
        f"median_pnl={median(pnls):.2f}",
        f"max_drawdown={mdd:.2f}",
    ])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="monitor_1570_20260520.db")
    ap.add_argument("--enable-short", action="store_true", help="Enable short-entry path for comparison (monitor currently freezes short entry)")
    args = ap.parse_args()
    ts, opens, closes = load_bars(args.db)
    trades = run_rule(ts, opens, closes, freeze_short=not args.enable_short)
    print(summarize(trades))


if __name__ == "__main__":
    main()
