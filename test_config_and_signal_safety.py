from argparse import Namespace
from datetime import datetime, timedelta, timezone

from monitor_1570_kabusapi0513_2lot_ready import (
    Bar,
    FeatureSnapshot,
    MonitorStatus,
    RollingBars,
    build_runtime_config,
    build_long_rsi50_trend_hold_prediction,
    fill_missing_ema13,
    load_config,
    PredictionSnapshot,
    short_b_drop_ma75_down_structure_decision,
    should_exit,
    start_rsi17_bullish_pullback_long_watch,
    startup_config_effective_payload,
    TickSnapshot,
    PositionState,
    HARD_STOP_TICKS,
    TRADE_WINDOWS,
    time_in_windows,
)

JST = timezone(timedelta(hours=9))


def _args(config_path="config_1570_live_prod.json"):
    return Namespace(
        config=config_path,
        api_password=None,
        live_mode=False,
        paper_mode=False,
        order_password=None,
        order_qty=None,
        account_type=None,
        margin_trade_type=None,
        entry_cash_margin=None,
        exit_cash_margin=None,
        entry_deliv_type=None,
        exit_deliv_type=None,
        live_entry_timeout_sec=None,
        live_exit_timeout_sec=None,
        live_retry_max=None,
        disable_adaptive_control=False,
        initial_vwap_mode=None,
        outdir=None,
        runtime_minutes=None,
        base_url=None,
    )


def _bar(ts, close, ma5, ma25, ma75, vwap, ema13=None):
    return Bar(ts=ts, open=close, high=close + 10, low=close - 10, close=close, volume=1000, vwap=vwap, ma5=ma5, ma25=ma25, ma75=ma75, ema13=ema13)


def _feature(ts, price, vwap, regime):
    return FeatureSnapshot(
        ts=ts,
        price=price,
        vwap=vwap,
        spread_ticks=1.0,
        obi_l1=0.0,
        obi_l3=0.0,
        vwap_gap_bps=((price - vwap) / vwap) * 10000.0,
        ret_30s=0.0,
        ret_1m=0.0,
        ret_3m=0.0,
        close_pos_in_bar_1m=0.5,
        close_pos_in_bar_3m=0.5,
        ma_trend_score_3m=0.0,
        pullback_quality=0.0,
        reacceleration_score=0.0,
        overextension_penalty=0.0,
        volume_1m=1000,
        volume_3m=3000,
        trade_intensity_30s=10.0,
        regime=regime,
    )


def test_runtime_config_preserves_enabled_nested_settings():
    cfg = load_config("config_1570_live_prod.json")
    runtime = build_runtime_config(cfg, _args())
    payload = startup_config_effective_payload(runtime)
    assert runtime["strategy_mode"] == "long_rsi50_trend_hold_only"
    assert runtime["long_rsi50_trend_hold"]["enabled"] is True
    assert runtime["long_rsi50_trend_hold"]["entry_rsi9_min"] == 50
    assert runtime["long_rsi50_trend_hold"]["exit_rsi9_max"] == 49
    assert runtime["long_rsi50_trend_hold"]["use_ema13_trend_filter"] is True
    assert runtime["long_rsi50_trend_hold"]["ema13_period"] == 13
    assert runtime["long_rsi50_trend_hold"]["ema13_lookback_bars"] == 4
    assert runtime["long_rsi50_trend_hold"]["ema13_min_rise_ticks"] == 5
    assert runtime["market_data_source"]["mode"] == "websocket"
    assert runtime["market_data_source"]["fallback_to_rest"] is True
    assert runtime["market_data_source"]["bar_finalize_delay_ms"] == 300
    assert runtime["entry_reference_close_guard"]["enabled"] is True
    assert runtime["entry_reference_close_guard"]["max_abs_deviation_ticks"] == 2
    assert runtime["entry_execution"]["limit_mode"] == "marketable_best"
    assert runtime["entry_execution"]["fallback_to_market"] is False
    assert runtime["feature_entries"]["enabled"] is False
    assert runtime["feature_entries"]["long_rsi_pullback_scalp"] is False
    assert runtime["feature_entries"]["short_extended_ma5_fail_scalp"] is False
    assert runtime["scalp_feature_entries"]["long_rsi_pullback_scalp"]["take_ticks"] == 10
    assert runtime["scalp_feature_entries"]["long_rsi_pullback_scalp"]["stop_ticks"] == 15
    assert runtime["scalp_feature_entries"]["short_extended_ma5_fail_scalp"]["stop_ticks"] == 20
    assert HARD_STOP_TICKS == 20
    assert runtime["hard_stop_ticks"] == 20
    assert runtime["big_trend_start_score"]["enabled"] is False
    assert runtime["hold_score_extension"]["enabled"] is False
    assert runtime["rsi17_drop_bullish_pullback_long_watch"]["enabled"] is False
    assert payload["strategy_mode"] == "long_rsi50_trend_hold_only"
    assert payload["long_rsi50_trend_hold_enabled"] is True
    assert payload["long_rsi50_trend_hold_entry_rsi9_min"] == 50
    assert payload["long_rsi50_trend_hold_exit_rsi9_max"] == 49
    assert payload["long_rsi50_trend_hold_use_ema13_trend_filter"] is True
    assert payload["long_rsi50_trend_hold_ema13_period"] == 13
    assert payload["long_rsi50_trend_hold_ema13_lookback_bars"] == 4
    assert payload["long_rsi50_trend_hold_ema13_min_rise_ticks"] == 5
    assert payload["market_data_source_mode"] == "websocket"
    assert payload["market_data_source_fallback_to_rest"] is True
    assert payload["bar_finalize_delay_ms"] == 300
    assert payload["entry_reference_close_guard_enabled"] is True
    assert payload["entry_reference_close_guard_max_abs_deviation_ticks"] == 2
    assert payload["feature_entries_enabled"] is False
    assert payload["feature_long_rsi_pullback_scalp"] is False
    assert payload["feature_short_extended_ma5_fail_scalp"] is False
    assert payload["hard_stop_ticks"] == 20
    assert payload["long_rsi_pullback_scalp_take_ticks"] == 10
    assert payload["long_rsi_pullback_scalp_stop_ticks"] == 15
    assert payload["short_extended_ma5_fail_scalp_stop_ticks"] == 20
    assert payload["big_trend_start_score_enabled"] is False
    assert payload["hold_score_extension_enabled"] is False
    assert payload["new_entry_cutoff_time"] == "15:10:00"


def test_short_b_drop_ma75_down_blocked_in_bullish_pullback_structure():
    ts = datetime(2026, 6, 9, 10, 30, tzinfo=JST)
    history = [
        _bar(ts - timedelta(minutes=2), 67150, 67220, 67120, 67000, 67100),
        _bar(ts - timedelta(minutes=1), 67120, 67200, 67140, 66980, 67105),
        _bar(ts, 67100, 67180, 67130, 66970, 67090),
    ]
    feature = _feature(ts, 67100, 67090, "trend_up")
    decision = short_b_drop_ma75_down_structure_decision(history[-1], history, feature, rsi_now=45.0, rsi_prev=47.0, rsi_prev2=64.0, ma75_slope_2m_value=-30.0)
    assert decision["short_allowed"] is False
    assert decision["bullish_structure"] is True
    assert decision["block_reason"] == "short_b_drop_ma75_down_bullish_structure_blocked"
    status = MonitorStatus()
    start_rsi17_bullish_pullback_long_watch(status, None, ts, 45.0, 47.0, 64.0, history[-1])
    assert status.rsi17_bullish_pullback_long_watch_active is True
    assert status.rsi17_bullish_pullback_long_watch_started_bar_ts == ts


def test_short_b_drop_ma75_down_allowed_in_bearish_structure():
    ts = datetime(2026, 6, 9, 10, 30, tzinfo=JST)
    history = [
        _bar(ts - timedelta(minutes=2), 66900, 67020, 67120, 67200, 67000),
        _bar(ts - timedelta(minutes=1), 66880, 66980, 67080, 67160, 66980),
        _bar(ts, 66850, 66940, 67020, 67120, 66950),
    ]
    feature = _feature(ts, 66850, 66950, "trend_down")
    decision = short_b_drop_ma75_down_structure_decision(history[-1], history, feature, rsi_now=45.0, rsi_prev=47.0, rsi_prev2=64.0, ma75_slope_2m_value=-80.0)
    assert decision["short_allowed"] is True
    assert decision["block_reason"] == ""


def _history_for_rsi20_watch(ts, close, vwap):
    bars = []
    for i in range(12):
        bars.append(_bar(ts - timedelta(minutes=11 - i), close, close + 20, close + 10, close - 10, vwap))
    return bars


def test_long_a_reversal_watch_blocks_when_price_below_vwap(monkeypatch):
    import monitor_1570_kabusapi0513_2lot_ready as m

    ts = datetime(2026, 6, 10, 10, 0, tzinfo=JST)
    history = _history_for_rsi20_watch(ts, 69000, 69050)
    status = MonitorStatus(rsi20_long_watch_active=True, rsi20_long_watch_started_at=ts - timedelta(minutes=1), rsi20_long_watch_expires_at=ts + timedelta(minutes=9), rsi20_long_watch_started_rsi=20.0)
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: {12: 21.0, 11: 20.0, 10: 25.0}.get(len(closes), 21.0))
    pred = m.build_rsi9_prediction(history[-1], history, None, status=status, storage=None, allow_new_entry=True)
    assert pred.signal == "NO_ACTION"
    assert pred.reason_3 == "long_a_reversal_watch_vwap_blocked"


def test_long_a_reversal_watch_allows_when_price_at_or_above_vwap(monkeypatch):
    import monitor_1570_kabusapi0513_2lot_ready as m

    ts = datetime(2026, 6, 10, 10, 0, tzinfo=JST)
    history = _history_for_rsi20_watch(ts, 69050, 69050)
    status = MonitorStatus(rsi20_long_watch_active=True, rsi20_long_watch_started_at=ts - timedelta(minutes=1), rsi20_long_watch_expires_at=ts + timedelta(minutes=9), rsi20_long_watch_started_rsi=20.0)
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: {12: 21.0, 11: 20.0, 10: 25.0}.get(len(closes), 21.0))
    pred = m.build_rsi9_prediction(history[-1], history, None, status=status, storage=None, allow_new_entry=True)
    assert pred.signal == "LONG_CANDIDATE"
    assert pred.reason_3 == "long_a_reversal_watch"


def _base_runtime_config():
    return build_runtime_config(load_config("config_1570_live_prod.json"), _args())


def _feature_enabled_runtime_config():
    cfg = _base_runtime_config()
    cfg["strategy_mode"] = "legacy"
    cfg["feature_entries"]["enabled"] = True
    cfg["feature_entries"]["long_vwap_volume_momentum"] = True
    cfg["feature_entries"]["short_vwap_extended_fail"] = True
    cfg["feature_entries"]["long_rsi_pullback_scalp"] = True
    cfg["feature_entries"]["short_extended_ma5_fail_scalp"] = True
    return cfg


def test_existing_feature_position_uses_config_hard_stop_20():
    from monitor_1570_kabusapi0513_2lot_ready import PredictionSnapshot, create_position

    ts = datetime(2026, 6, 11, 10, 0, tzinfo=JST)
    cfg = _base_runtime_config()
    pred = PredictionSnapshot(ts, "FEATURE_ENTRY", 0.5, 0.5, 0.5, 0.5, "LONG_CANDIDATE", 50.0, "FEATURE_ENTRY", "ret5_ticks=5", "long_feature_vwap_volume_momentum")
    pos = create_position(pred, _feature(ts, 67000, 66900, "trend_up"), cfg)
    assert pos.strategy in {"STRAT_1M", "STRAT_3M"}
    assert pos.entry_rule == "long_feature_vwap_volume_momentum"
    assert pos.hard_stop_ticks == 20


def test_long_rsi_pullback_scalp_triggers_and_applies_exit_ticks():
    from monitor_1570_kabusapi0513_2lot_ready import build_scalp_feature_candidates, create_position

    ts = datetime(2026, 6, 11, 10, 0, tzinfo=JST)
    cfg = _feature_enabled_runtime_config()
    feature = _feature(ts, 67000, 66900, "trend_up")
    candidates = build_scalp_feature_candidates(
        feature,
        {},
        cfg,
        35.0,
        38.0,
        _bar(ts, 67000, 66950, 66920, 66800, 66900),
        allow_new_entry=True,
        entry_cutoff_reached=False,
        open_position_exists=False,
    )
    assert candidates[0].signal == "LONG_CANDIDATE"
    assert candidates[0].reason_1 == "SCALP_FEATURE_LONG"
    assert candidates[0].reason_3 == "long_rsi_pullback_scalp"
    pos = create_position(candidates[0], feature, cfg)
    assert pos.strategy == "SCALP_FEATURE_LONG"
    assert pos.take_ticks == 10
    assert pos.stop_ticks == 15
    assert pos.hard_stop_ticks == 15


def test_long_rsi_pullback_scalp_blocks_below_vwap_or_rsi_rising():
    from monitor_1570_kabusapi0513_2lot_ready import build_scalp_feature_candidates

    ts = datetime(2026, 6, 11, 10, 0, tzinfo=JST)
    cfg = _feature_enabled_runtime_config()
    latest = _bar(ts, 67000, 66950, 66920, 66800, 66900)
    below_vwap = build_scalp_feature_candidates(
        _feature(ts, 66800, 66900, "range"), {}, cfg, 35.0, 38.0, latest,
        allow_new_entry=True, entry_cutoff_reached=False, open_position_exists=False,
    )
    assert all(c.reason_3 != "long_rsi_pullback_scalp" for c in below_vwap)
    rsi_rising = build_scalp_feature_candidates(
        _feature(ts, 67000, 66900, "range"), {}, cfg, 38.0, 35.0, latest,
        allow_new_entry=True, entry_cutoff_reached=False, open_position_exists=False,
    )
    assert all(c.reason_3 != "long_rsi_pullback_scalp" for c in rsi_rising)


def test_short_extended_ma5_fail_scalp_triggers_and_applies_exit_ticks():
    from monitor_1570_kabusapi0513_2lot_ready import build_scalp_feature_candidates, create_position

    ts = datetime(2026, 6, 11, 10, 0, tzinfo=JST)
    cfg = _feature_enabled_runtime_config()
    feature = _feature(ts, 67000, 65000, "range")
    feature.vwap_gap_bps = 200.0
    metrics = {"abs_vwap_gap_bps": 200.0, "ret5_ticks": 10.0, "ret1_ticks": -6.0}
    latest = _bar(ts, 67000, 67050, 67060, 66900, 65000)
    candidates = build_scalp_feature_candidates(
        feature, metrics, cfg, 55.0, 56.0, latest,
        allow_new_entry=True, entry_cutoff_reached=False, open_position_exists=False,
    )
    assert candidates[0].signal == "SHORT_CANDIDATE"
    assert candidates[0].reason_1 == "SCALP_FEATURE_SHORT"
    assert candidates[0].reason_3 == "short_extended_ma5_fail_scalp"
    pos = create_position(candidates[0], feature, cfg)
    assert pos.strategy == "SCALP_FEATURE_SHORT"
    assert pos.take_ticks == 15
    assert pos.stop_ticks == 20
    assert pos.hard_stop_ticks == 20


def test_short_extended_ma5_fail_scalp_blocks_ma5_above_or_gap_insufficient_or_open_position():
    from monitor_1570_kabusapi0513_2lot_ready import build_scalp_feature_candidates

    ts = datetime(2026, 6, 11, 10, 0, tzinfo=JST)
    cfg = _feature_enabled_runtime_config()
    feature = _feature(ts, 67000, 65000, "range")
    feature.vwap_gap_bps = 200.0
    metrics = {"abs_vwap_gap_bps": 200.0, "ret5_ticks": 10.0, "ret1_ticks": -6.0}
    ma5_above = build_scalp_feature_candidates(
        feature, metrics, cfg, 55.0, 56.0, _bar(ts, 67100, 67050, 67060, 66900, 65000),
        allow_new_entry=True, entry_cutoff_reached=False, open_position_exists=False,
    )
    assert all(c.reason_3 != "short_extended_ma5_fail_scalp" for c in ma5_above)
    feature.vwap_gap_bps = 100.0
    gap_short = build_scalp_feature_candidates(
        feature, {"abs_vwap_gap_bps": 100.0, "ret5_ticks": 10.0, "ret1_ticks": -6.0}, cfg, 55.0, 56.0, _bar(ts, 67000, 67050, 67060, 66900, 65000),
        allow_new_entry=True, entry_cutoff_reached=False, open_position_exists=False,
    )
    assert all(c.reason_3 != "short_extended_ma5_fail_scalp" for c in gap_short)
    with_position = build_scalp_feature_candidates(
        feature, metrics, cfg, 35.0, 38.0, _bar(ts, 67000, 67050, 67060, 66900, 65000),
        allow_new_entry=True, entry_cutoff_reached=False, open_position_exists=True,
    )
    assert with_position == []


def _rsi50_history(ts):
    bars = []
    for i in range(15):
        bars.append(_bar(ts - timedelta(minutes=14 - i), 67000 + i, 67000, 67000, 67000, 67000, ema13=67000.0 + i * 20.0))
    return bars


def test_long_rsi50_trend_hold_entry_at_50_and_no_entry_below(monkeypatch):
    import monitor_1570_kabusapi0513_2lot_ready as m

    ts = datetime(2026, 6, 12, 10, 0, tzinfo=JST)
    cfg = _base_runtime_config()
    status = MonitorStatus()
    feature = _feature(ts, 67000, 66900, "trend_up")
    snap = TickSnapshot(ts, 67000, 1000, 66900, 67010, 10, 67000, 10)
    history = _rsi50_history(ts)
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: 50.0)
    pred = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap, cfg, allow_new_entry=True)
    assert pred.signal == "LONG_CANDIDATE"
    assert pred.reason_1 == "LONG_RSI50_TREND_HOLD"
    assert pred.reason_3 == "long_rsi50_trend_hold"
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: 49.99)
    pred2 = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap, cfg, allow_new_entry=True)
    assert pred2.signal == "NO_ACTION"


def test_ema13_calculation_seed_and_recursive_update():
    ts = datetime(2026, 6, 12, 10, 0, tzinfo=JST)
    rb = RollingBars(1)
    for i in range(12):
        bar = Bar(ts=ts + timedelta(minutes=i), open=100 + i, high=100 + i, low=100 + i, close=100 + i, volume=1000, vwap=100 + i)
        rb.history.append(bar)
        rb._decorate_bar(bar)
        assert bar.ema13 is None
    bar13 = Bar(ts=ts + timedelta(minutes=12), open=112, high=112, low=112, close=112, volume=1000, vwap=112)
    rb.history.append(bar13)
    rb._decorate_bar(bar13)
    assert bar13.ema13 == sum(range(100, 113)) / 13.0
    bar14 = Bar(ts=ts + timedelta(minutes=13), open=113, high=113, low=113, close=113, volume=1000, vwap=113)
    rb.history.append(bar14)
    rb._decorate_bar(bar14)
    alpha = 2.0 / 14.0
    assert bar14.ema13 == alpha * 113 + (1.0 - alpha) * bar13.ema13


def test_fill_missing_ema13_for_prev_day_warmup():
    ts = datetime(2026, 6, 12, 10, 0, tzinfo=JST)
    bars = [Bar(ts=ts + timedelta(minutes=i), open=100 + i, high=100 + i, low=100 + i, close=100 + i, volume=1000, vwap=100 + i) for i in range(14)]
    filled = fill_missing_ema13(bars)
    assert filled[11].ema13 is None
    assert filled[12].ema13 == sum(range(100, 113)) / 13.0
    assert filled[13].ema13 is not None


def test_long_rsi50_trend_hold_blocks_when_ema13_not_rising(monkeypatch):
    import monitor_1570_kabusapi0513_2lot_ready as m

    ts = datetime(2026, 6, 12, 10, 0, tzinfo=JST)
    cfg = _base_runtime_config()
    status = MonitorStatus()
    feature = _feature(ts, 67000, 66900, "trend_up")
    snap = TickSnapshot(ts, 67000, 1000, 66900, 67010, 10, 67000, 10)
    history = _rsi50_history(ts)
    history[-1].ema13 = history[-5].ema13
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: 55.0)
    pred = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap, cfg, allow_new_entry=True)
    assert pred.signal == "NO_ACTION"


def test_long_rsi50_trend_hold_requires_ema13_rise_5_ticks(monkeypatch):
    import monitor_1570_kabusapi0513_2lot_ready as m

    ts = datetime(2026, 6, 12, 10, 0, tzinfo=JST)
    cfg = _base_runtime_config()
    status = MonitorStatus()
    feature = _feature(ts, 67000, 66900, "trend_up")
    snap = TickSnapshot(ts, 67000, 1000, 66900, 67010, 10, 67000, 10)
    history = _rsi50_history(ts)
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: 55.0)

    history[-5].ema13 = 67000.0
    history[-1].ema13 = 67049.9
    pred_short = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap, cfg, allow_new_entry=True)
    assert pred_short.signal == "NO_ACTION"

    history[-1].ema13 = 67050.0
    pred_ok = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap, cfg, allow_new_entry=True)
    assert pred_ok.signal == "LONG_CANDIDATE"
    assert pred_ok.reason_3 == "long_rsi50_trend_hold"


def test_long_rsi50_trend_hold_reference_close_guard(monkeypatch):
    import monitor_1570_kabusapi0513_2lot_ready as m

    ts = datetime(2026, 6, 12, 10, 0, tzinfo=JST)
    cfg = _base_runtime_config()
    status = MonitorStatus()
    feature = _feature(ts, 67000, 66900, "trend_up")
    history = _rsi50_history(ts)
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: 55.0)

    snap_within = TickSnapshot(ts, 67000, 1000, 66900, history[-1].close + 20, 10, 67000, 10)
    pred_within = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap_within, cfg, allow_new_entry=True)
    assert pred_within.signal == "LONG_CANDIDATE"

    snap_far = TickSnapshot(ts, 67000, 1000, 66900, history[-1].close + 30, 10, 67000, 10)
    pred_far = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap_far, cfg, allow_new_entry=True)
    assert pred_far.signal == "NO_ACTION"


def test_long_rsi50_trend_hold_allows_0900_0915_window(monkeypatch):
    import monitor_1570_kabusapi0513_2lot_ready as m

    ts = datetime(2026, 6, 12, 9, 5, tzinfo=JST)
    cfg = _base_runtime_config()
    status = MonitorStatus()
    feature = _feature(ts, 67000, 66900, "trend_up")
    snap = TickSnapshot(ts, 67000, 1000, 66900, 67010, 10, 67000, 10)
    history = _rsi50_history(ts)
    monkeypatch.setattr(m, "rsi9_wilder", lambda closes, period: 50.0)
    pred = build_long_rsi50_trend_hold_prediction(history[-1], history, None, status, feature, snap, cfg, allow_new_entry=True)
    assert pred.signal == "LONG_CANDIDATE"
    assert pred.reason_3 == "long_rsi50_trend_hold"
    assert time_in_windows("09:00:00", TRADE_WINDOWS) is True
    assert time_in_windows("09:01:00", TRADE_WINDOWS) is True


def test_long_rsi50_mode_ignores_existing_feature_candidates():
    cfg = _feature_enabled_runtime_config()
    cfg["strategy_mode"] = "long_rsi50_trend_hold_only"
    assert cfg["feature_entries"]["enabled"] is True
    # Existing feature helpers may still identify candidates when invoked
    # directly, but run_monitor bypasses them in long_rsi50_trend_hold_only mode.
    assert cfg["strategy_mode"] == "long_rsi50_trend_hold_only"


def _long_rsi50_position(ts):
    return PositionState(
        side="LONG",
        strategy="LONG_RSI50_TREND_HOLD",
        entry_ts=ts - timedelta(minutes=5),
        entry_price=67000.0,
        entry_p_up_1m=0.5,
        entry_p_up_3m=0.5,
        stop_ticks=20,
        take_ticks=0,
        min_hold_sec=0,
        max_hold_sec=3600,
        entry_rule="long_rsi50_trend_hold",
        hard_stop_ticks=20,
    )


def test_long_rsi50_trend_hold_exit_at_49_and_holds_above():
    ts = datetime(2026, 6, 12, 10, 10, tzinfo=JST)
    cfg = _base_runtime_config()
    pos = _long_rsi50_position(ts)
    feature = _feature(ts, 67050, 66900, "trend_up")
    pred = PredictionSnapshot(ts, "LONG_RSI50_TREND_HOLD", 0.5, 0.5, 0.5, 0.5, "NO_ACTION", 49.0, "LONG_RSI50_TREND_HOLD", "rsi9=49.00", "none")
    ex, reason, _ = should_exit(pos, feature, pred, bar1_new=_bar(ts, 67050, 67000, 67000, 67000, 66900), config=cfg, latest_snapshot=TickSnapshot(ts, 67050, 1000, 66900, 67060, 10, 67050, 10))
    assert ex is True
    assert reason == "RSI9_LE_49_EXIT"
    pred_hold = PredictionSnapshot(ts, "LONG_RSI50_TREND_HOLD", 0.5, 0.5, 0.5, 0.5, "NO_ACTION", 49.1, "LONG_RSI50_TREND_HOLD", "rsi9=49.10", "none")
    ex2, reason2, _ = should_exit(pos, feature, pred_hold, bar1_new=_bar(ts, 67050, 67000, 67000, 67000, 66900), config=cfg)
    assert ex2 is False
    assert reason2 == "HOLD"
    pred_hold_ema_down = PredictionSnapshot(ts, "LONG_RSI50_TREND_HOLD", 0.5, 0.5, 0.5, 0.5, "NO_ACTION", 55.0, "LONG_RSI50_TREND_HOLD", "rsi9=55.00", "none")
    ex3, reason3, _ = should_exit(pos, feature, pred_hold_ema_down, bar1_new=_bar(ts, 67050, 67000, 67000, 67000, 66900, ema13=90.0), config=cfg)
    assert ex3 is False
    assert reason3 == "HOLD"


def test_long_rsi50_trend_hold_hard_stop_uses_20_ticks():
    ts = datetime(2026, 6, 12, 10, 10, tzinfo=JST)
    cfg = _base_runtime_config()
    pos = _long_rsi50_position(ts)
    feature = _feature(ts, 66800, 66900, "trend_up")
    pred = PredictionSnapshot(ts, "LONG_RSI50_TREND_HOLD", 0.5, 0.5, 0.5, 0.5, "NO_ACTION", 55.0, "LONG_RSI50_TREND_HOLD", "rsi9=55.00", "none")
    ex, reason, pnl = should_exit(pos, feature, pred, bar1_new=_bar(ts, 66800, 67000, 67000, 67000, 66900), config=cfg)
    assert pnl <= -20
    assert ex is True
    assert reason == "HARD_STOP_LOSS"
