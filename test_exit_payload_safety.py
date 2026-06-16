from datetime import datetime

from monitor_1570_kabusapi0513_2lot_ready import (
    ApiHttpError,
    PositionState,
    PredictionSnapshot,
    build_exit_order_payload,
    close_position_groups_from_managed_state,
    execute_live_entry,
    entry_limit_price,
    TickSnapshot,
    Bar,
    FeatureSnapshot,
    execute_live_exit,
    update_trailing_exit,
    validate_exit_payload,
)


class FakeStorage:
    def __init__(self):
        self.events = []
    def log_structured(self, level, event_type, payload, mirror_message=None):
        self.events.append((level, event_type, payload))
    def log(self, level, event_type, message):
        self.events.append((level, event_type, {"message": message}))


class FakeClient:
    def __init__(self, positions):
        self.positions = positions
        self.sent_payloads = []
    def get_positions(self, symbol):
        return self.positions
    def get_board(self, symbol, exchange):
        return {
            "CurrentPrice": 69005,
            "VWAP": 69000,
            "Buy1": {"Price": 69000, "Qty": 10},
            "Sell1": {"Price": 69010, "Qty": 10},
        }
    def send_order(self, payload):
        self.sent_payloads.append(payload)
        if len(self.sent_payloads) == 1:
            raise ApiHttpError(400, "Bad Request", '{"Code":8,"Message":"決済指定内容に誤りがあります"}')
        return {"OrderId": "retry-ok"}
    def get_orders(self, order_id="", product=2):
        return []


class EntryFakeClient:
    def __init__(self, board):
        self.board = board
        self.sent_payloads = []
    def get_positions(self, symbol):
        return []
    def get_board(self, symbol, exchange):
        return self.board
    def send_order(self, payload):
        self.sent_payloads.append(payload)
        return {"OrderId": "entry-ok"}


class Status:
    live_state = "OPEN"
    pending_exit = False
    failed_close_signature = ""
    failed_close_signature_ts = None
    open_position = None


def base_config():
    return {
        "symbol": "1570",
        "exchange": 1,
        "order_exchange": 1,
        "exit_order_exchange": 1,
        "order_password": "pw",
        "exit_cash_margin": 3,
        "exit_deliv_type": 2,
        "account_type": 4,
        "exit_front_order_type": 10,
        "exit_price": 0,
        "expire_day": 0,
        "margin_trade_type": 3,
        "margin_trade_type_long": 3,
        "live_exit_timeout_sec": 0,
        "live_retry_max": 0,
    }


def entry_config():
    cfg = base_config()
    cfg.update(
        {
            "live_retry_max": 0,
            "live_entry_timeout_sec": 0,
            "order_qty": 2,
            "entry_min_fill_qty": 2,
            "entry_cash_margin": 2,
            "entry_deliv_type": 0,
            "entry_front_order_type": 10,
            "entry_price": 0,
            "strategy_mode": "long_rsi50_trend_hold_only",
            "entry_execution": {
                "enabled": True,
                "mode": "limit_with_timeout",
                "limit_mode": "marketable_best",
                "timeout_sec": 0.5,
                "max_reprice_attempts": 0,
                "fallback_to_market": False,
            },
            "entry_reference_close_guard": {
                "enabled": True,
                "max_abs_deviation_ticks": 2,
            },
            "long_rsi50_trend_hold": {
                "enabled": True,
                "entry_rsi9_min": 50,
                "exit_rsi9_max": 49,
                "hard_stop_ticks": 20,
                "use_ema13_trend_filter": True,
                "ema13_lookback_bars": 4,
                "ema13_min_rise_ticks": 5,
            },
        }
    )
    return cfg


def base_position(exchange=27, qty=2):
    return PositionState(
        side="LONG",
        strategy="RSI9",
        entry_ts=datetime(2026, 6, 8, 9, 20),
        entry_price=67040.0,
        entry_p_up_1m=0,
        entry_p_up_3m=0,
        stop_ticks=0,
        take_ticks=0,
        min_hold_sec=0,
        max_hold_sec=0,
        margin_trade_type=3,
        filled_qty=qty,
        order_qty=qty,
        managed_execution_ids=["E1"],
        managed_close_positions=[{"ExecutionID": "E1", "HoldID": "E1", "Qty": qty, "LeavesQty": qty, "HoldQty": 0, "Side": "2", "Symbol": "1570", "Exchange": exchange, "MarginTradeType": 3, "Price": 67040.0}],
    )


def long_rsi50_entry_position():
    return PositionState(
        side="LONG",
        strategy="LONG_RSI50_TREND_HOLD",
        entry_ts=datetime(2026, 6, 12, 9, 1),
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


def long_rsi50_pred():
    return PredictionSnapshot(
        ts=datetime(2026, 6, 12, 9, 0),
        regime="LONG_RSI50_TREND_HOLD",
        p_up_1m=0.5,
        p_down_1m=0.5,
        p_up_3m=0.5,
        p_down_3m=0.5,
        signal="LONG_CANDIDATE",
        rsi9_value=55.0,
        reason_1="LONG_RSI50_TREND_HOLD",
        reason_2="rsi9=55.00",
        reason_3="long_rsi50_trend_hold",
    )


def test_live_entry_final_pre_send_reference_close_guard_blocks_order():
    cfg = entry_config()
    client = EntryFakeClient(
        {
            "CurrentPrice": 67050,
            "CurrentPriceTime": "2026-06-12T09:01:00+09:00",
            "VWAP": 67000,
            "Buy1": {"Price": 67040, "Qty": 10},
            "Sell1": {"Price": 67050, "Qty": 10},
        }
    )
    storage = FakeStorage()
    result = execute_live_entry(
        client,
        cfg,
        "LONG",
        storage,
        long_rsi50_entry_position(),
        long_rsi50_pred(),
        Status(),
        latest_snapshot=TickSnapshot(datetime(2026, 6, 12, 9, 1), 67000, 1000, 66990, 67010, 10, 66990, 10),
        entry_reference_bar=Bar(datetime(2026, 6, 12, 9, 0), 67000, 67000, 67000, 67000, 1000, 67000, ema13=67050),
        entry_ema13_lookback_value=67000,
        entry_ema13_delta_ticks=5,
    )
    assert result.ok is False
    assert result.message == "FINAL_PRE_SEND_REFERENCE_CLOSE_DEVIATION_TOO_LARGE"
    assert client.sent_payloads == []
    assert any(e[1] == "LONG_RSI50_TREND_HOLD_ENTRY_BLOCKED_BY_REFERENCE_CLOSE_DEVIATION_FINAL_PRE_SEND" for e in storage.events)


def test_live_entry_final_pre_send_reference_close_guard_allows_order():
    cfg = entry_config()
    client = EntryFakeClient(
        {
            "CurrentPrice": 67010,
            "CurrentPriceTime": "2026-06-12T09:01:00+09:00",
            "VWAP": 67000,
            "Buy1": {"Price": 67000, "Qty": 10},
            "Sell1": {"Price": 67020, "Qty": 10},
        }
    )
    storage = FakeStorage()
    result = execute_live_entry(
        client,
        cfg,
        "LONG",
        storage,
        long_rsi50_entry_position(),
        long_rsi50_pred(),
        Status(),
        latest_snapshot=TickSnapshot(datetime(2026, 6, 12, 9, 1), 67000, 1000, 66990, 67010, 10, 66990, 10),
        entry_reference_bar=Bar(datetime(2026, 6, 12, 9, 0), 67000, 67000, 67000, 67000, 1000, 67000, ema13=67050),
        entry_ema13_lookback_value=67000,
        entry_ema13_delta_ticks=5,
    )
    assert result.ok is False  # fake client never creates a verified position, but send_order must be reached
    assert client.sent_payloads
    assert client.sent_payloads[0]["FrontOrderType"] == 20
    assert client.sent_payloads[0]["Price"] == 67020
    entry_requests = [e for e in storage.events if e[1] == "ENTRY_ORDER_REQUEST"]
    assert entry_requests
    assert entry_requests[0][2]["final_pre_send_best_ask"] == 67020
    assert entry_requests[0][2]["final_pre_send_deviation_ticks"] == 2


def test_fast_path_uses_actual_exchange_27_not_config_1():
    cfg = base_config()
    pos = base_position(exchange=27, qty=2)
    groups, reason = close_position_groups_from_managed_state(pos, cfg, default_exchange=1)
    assert reason == "MANAGED_STATE_WITH_EXCHANGE"
    assert groups == [(27, [{"HoldID": "E1", "Qty": 2}], 2)]
    payload = build_exit_order_payload(cfg, "LONG", groups[0][1], qty=2, exchange=groups[0][0], margin_trade_type=3)
    assert payload["Exchange"] == 27
    assert validate_exit_payload(payload, pos.managed_close_positions, pos, 2)[0]
    payload_with_mixed_close_order = dict(payload)
    payload_with_mixed_close_order["ClosePositionOrder"] = 0
    ok, reason = validate_exit_payload(payload_with_mixed_close_order, pos.managed_close_positions, pos, 2)
    assert ok is False
    assert reason == "CLOSE_POSITIONS_AND_ORDER_MIXED"


def test_fast_path_fallback_when_exchange_missing():
    cfg = base_config()
    pos = base_position(exchange=None, qty=2)
    pos.managed_close_positions[0].pop("Exchange", None)
    groups, reason = close_position_groups_from_managed_state(pos, cfg, default_exchange=1)
    assert groups is None
    assert reason == "MISSING_EXCHANGE"


def test_fast_path_fallback_when_qty_mismatch():
    cfg = base_config()
    pos = base_position(exchange=27, qty=1)
    pos.filled_qty = 2
    groups, reason = close_position_groups_from_managed_state(pos, cfg, default_exchange=1)
    assert groups is None
    assert reason == "QTY_MISMATCH"


def test_code8_rebuild_resends_with_actual_exchange_27():
    cfg = base_config()
    pos = base_position(exchange=1, qty=2)
    pos.strategy = "TEST"
    status = Status()
    status.open_position = pos
    positions = [{"ExecutionID": "E1", "LeavesQty": 2, "HoldQty": 0, "Side": "2", "Symbol": "1570", "Exchange": 27, "MarginTradeType": 3, "Price": 67040.0}]
    client = FakeClient(positions)
    storage = FakeStorage()
    result = execute_live_exit(client, cfg, "LONG", storage, pos, None, status, force_marketable_limit=False, force_market_order=False, signal_price=67580.0, pnl_ticks=52.0, ma5_exit_context={"exit_reason": "MA5_INTRABAR_CROSS_TRAILING"})
    assert len(client.sent_payloads) >= 2
    assert client.sent_payloads[0]["Exchange"] == 1
    assert client.sent_payloads[1]["Exchange"] == 27
    assert result.order_id == "retry-ok" or result.ok is False
    event_types = [e[1] for e in storage.events]
    assert "EXIT_ORDER_SEND_FAIL_CODE8_REBUILD" in event_types
    assert "EXIT_ORDER_SEND_SUCCESS_AFTER_CODE8_REBUILD" in event_types


def test_hard_stop_code8_uses_emergency_rebuild_not_backoff():
    cfg = base_config()
    pos = base_position(exchange=1, qty=2)
    pos.strategy = "TEST"
    status = Status()
    status.open_position = pos
    positions = [{"ExecutionID": "E1", "LeavesQty": 2, "HoldQty": 0, "Side": "2", "Symbol": "1570", "Exchange": 27, "MarginTradeType": 3, "Price": 67040.0}]
    client = FakeClient(positions)
    storage = FakeStorage()
    execute_live_exit(client, cfg, "LONG", storage, pos, None, status, force_marketable_limit=False, force_market_order=False, signal_price=66890.0, pnl_ticks=-15.0, ma5_exit_context={"exit_reason": "HARD_STOP_LOSS"})
    assert len(client.sent_payloads) >= 2
    assert client.sent_payloads[1]["Exchange"] == 27
    event_types = [e[1] for e in storage.events]
    assert "HARD_STOP_CODE8_EMERGENCY_REBUILD" in event_types
    assert "EXIT_PENDING_STATE_RESET_AFTER_SEND_FAIL" in event_types


def test_ma5_intrabar_exit_uses_best_bid_limit_even_when_config_market():
    cfg = base_config()
    pos = base_position(exchange=27, qty=2)
    pos.strategy = "TEST"
    status = Status()
    status.open_position = pos
    client = FakeClient(pos.managed_close_positions)
    # This test is about the first payload, so make the first send succeed.
    client.send_order = lambda payload: (client.sent_payloads.append(payload) or {"OrderId": "ok"})
    storage = FakeStorage()
    result = execute_live_exit(
        client,
        cfg,
        "LONG",
        storage,
        pos,
        None,
        status,
        force_marketable_limit=False,
        force_market_order=False,
        signal_price=69000.0,
        pnl_ticks=52.0,
        ma5_exit_context={"exit_reason": "MA5_INTRABAR_CROSS_TRAILING", "confirmed_ma5": 69006.0},
    )
    assert result.order_id == "ok"
    payload = client.sent_payloads[0]
    assert payload["FrontOrderType"] == 20
    assert payload["Price"] == 69000
    assert payload["CashMargin"] == 3
    assert payload["DelivType"] == 2
    assert payload.get("ClosePositions")
    assert "ClosePositionOrder" not in payload
    assert any(e[1] == "MA5_EXIT_LIMIT_PRICE_SELECTED" for e in storage.events)


def _feature_for_exit(ts, price):
    return FeatureSnapshot(ts, price, 69000.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 1000, 3000, 10.0, "trend_up")


def test_ma5_close_below_uses_finalized_bar_ma5_immediately():
    pos = base_position(exchange=27, qty=2)
    pos.trailing_active = True
    pos.trailing_ma5_bar_bucket = datetime(2026, 6, 10, 9, 31)
    pos.trailing_ma5_bar_open = 69000.0
    pos.trailing_ma5_reference = 68980.0
    pos.trailing_ma5_open_relation = "LONG_ABOVE_MA5"
    storage = FakeStorage()
    finalized = Bar(pos.trailing_ma5_bar_bucket, 69000.0, 69020.0, 68980.0, 69000.0, 1000, 69000.0, ma5=69006.0)
    ex, reason, _ = update_trailing_exit(pos, _feature_for_exit(finalized.ts, 69000.0), bar1_new=finalized, storage=storage)
    assert ex is True
    assert reason == "MA5_CLOSE_BELOW_TRAILING"
    event = next(e for e in storage.events if e[1] == "MA5_CLOSE_EXIT_EVALUATED_WITH_FINALIZED_MA5")
    assert event[2]["finalized_ma5"] == 69006.0
    assert event[2]["used_ma5_source"] == "finalized_bar_ma5"


def test_ma5_close_above_uses_finalized_bar_ma5_immediately_for_short():
    pos = base_position(exchange=27, qty=2)
    pos.side = "SHORT"
    pos.entry_price = 69160.0
    pos.trailing_active = True
    pos.trailing_ma5_bar_bucket = datetime(2026, 6, 10, 9, 31)
    pos.trailing_ma5_bar_open = 69000.0
    pos.trailing_ma5_reference = 69020.0
    pos.trailing_ma5_open_relation = "SHORT_BELOW_MA5"
    storage = FakeStorage()
    finalized = Bar(pos.trailing_ma5_bar_bucket, 69000.0, 69040.0, 68980.0, 69010.0, 1000, 69000.0, ma5=69006.0)
    ex, reason, _ = update_trailing_exit(pos, _feature_for_exit(finalized.ts, 69010.0), bar1_new=finalized, storage=storage)
    assert ex is True
    assert reason == "MA5_CLOSE_ABOVE_TRAILING"
    event = next(e for e in storage.events if e[1] == "MA5_CLOSE_EXIT_EVALUATED_WITH_FINALIZED_MA5")
    assert event[2]["finalized_ma5"] == 69006.0


def _scalp_position(side: str, strategy: str, entry_rule: str):
    pos = base_position(exchange=27, qty=2)
    pos.side = side
    pos.strategy = strategy
    pos.entry_rule = entry_rule
    pos.take_ticks = 10 if side == "LONG" else 15
    pos.stop_ticks = 15 if side == "LONG" else 20
    pos.hard_stop_ticks = pos.stop_ticks
    if side == "SHORT":
        pos.managed_close_positions[0]["Side"] = "1"
    return pos


def _assert_scalp_exit_best_quote_limit(side: str, strategy: str, entry_rule: str, exit_reason: str, expected_price: float):
    cfg = base_config()
    pos = _scalp_position(side, strategy, entry_rule)
    status = Status()
    status.open_position = pos
    client = FakeClient(pos.managed_close_positions)
    client.send_order = lambda payload: (client.sent_payloads.append(payload) or {"OrderId": "ok"})
    storage = FakeStorage()
    result = execute_live_exit(
        client,
        cfg,
        side,
        storage,
        pos,
        None,
        status,
        force_marketable_limit=False,
        force_market_order=False,
        signal_price=69005.0,
        pnl_ticks=10.0 if exit_reason == "TAKE_PROFIT" else -15.0,
        ma5_exit_context={"exit_reason": exit_reason},
    )
    assert result.order_id == "ok"
    payload = client.sent_payloads[0]
    assert payload["FrontOrderType"] == 20
    assert payload["Price"] == expected_price
    assert payload["Price"] > 0
    assert any(e[1] == "SCALP_FEATURE_EXIT_LIMIT_PRICE_SELECTED" for e in storage.events)


def test_scalp_feature_long_take_profit_and_stop_loss_use_best_bid_limit():
    _assert_scalp_exit_best_quote_limit("LONG", "SCALP_FEATURE_LONG", "long_rsi_pullback_scalp", "TAKE_PROFIT", 69000)
    _assert_scalp_exit_best_quote_limit("LONG", "SCALP_FEATURE_LONG", "long_rsi_pullback_scalp", "STOP_LOSS", 69000)


def test_scalp_feature_short_take_profit_and_stop_loss_use_best_ask_limit():
    _assert_scalp_exit_best_quote_limit("SHORT", "SCALP_FEATURE_SHORT", "short_extended_ma5_fail_scalp", "TAKE_PROFIT", 69010)
    _assert_scalp_exit_best_quote_limit("SHORT", "SCALP_FEATURE_SHORT", "short_extended_ma5_fail_scalp", "STOP_LOSS", 69010)


def test_long_rsi50_exit_uses_best_bid_limit_not_market():
    cfg = base_config()
    pos = base_position(exchange=27, qty=2)
    pos.strategy = "LONG_RSI50_TREND_HOLD"
    pos.entry_rule = "long_rsi50_trend_hold"
    status = Status()
    status.open_position = pos
    client = FakeClient(pos.managed_close_positions)
    client.send_order = lambda payload: (client.sent_payloads.append(payload) or {"OrderId": "ok"})
    storage = FakeStorage()
    result = execute_live_exit(
        client,
        cfg,
        "LONG",
        storage,
        pos,
        None,
        status,
        force_marketable_limit=False,
        force_market_order=False,
        signal_price=69005.0,
        pnl_ticks=10.0,
        ma5_exit_context={"exit_reason": "RSI9_LE_49_EXIT"},
    )
    assert result.order_id == "ok"
    payload = client.sent_payloads[0]
    assert payload["FrontOrderType"] == 20
    assert payload["Price"] == 69000
    assert payload["Price"] > 0


def test_force_close_1520_remains_market_order():
    cfg = base_config()
    pos = base_position(exchange=27, qty=2)
    status = Status()
    status.open_position = pos
    client = FakeClient(pos.managed_close_positions)
    client.send_order = lambda payload: (client.sent_payloads.append(payload) or {"OrderId": "ok"})
    storage = FakeStorage()
    result = execute_live_exit(
        client,
        cfg,
        "LONG",
        storage,
        pos,
        None,
        status,
        force_marketable_limit=False,
        force_market_order=True,
        signal_price=69005.0,
        pnl_ticks=10.0,
        ma5_exit_context={"exit_reason": "FORCE_CLOSE_1520"},
    )
    assert result.order_id == "ok"
    payload = client.sent_payloads[0]
    assert payload["FrontOrderType"] == 10
    assert payload["Price"] == 0.0


def test_long_entry_marketable_best_uses_best_ask_limit():
    snap = TickSnapshot(datetime(2026, 6, 12, 9, 5), 69005, 1000, 69000, 69010, 10, 69000, 10)
    assert entry_limit_price("LONG", snap, "marketable_best") == 69010
    assert base_config()["exit_front_order_type"] == 10
