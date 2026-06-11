from datetime import datetime

from monitor_1570_kabusapi0513_2lot_ready import (
    ApiHttpError,
    PositionState,
    build_exit_order_payload,
    close_position_groups_from_managed_state,
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
