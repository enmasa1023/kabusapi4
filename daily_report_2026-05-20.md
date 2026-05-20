# 1570 自動監視 日次レポート

対象日: 2026-05-20

## 1. 総括
- スナップショット数: 3535
- 予測回数: 3535
- LONG候補: 912
- SHORT候補: 987
- NO_ACTION: 1636

## 2. 仮想売買
- 完了トレード数: 111
- 勝率: 40.5%
- 平均損益(ティック): -8.13
- 総損益(ティック): -902.13
- 平均保有秒数: 73.8

## 3. 戦略別件数
- STRAT_1M: 34
- SCALP_REBOUND_LONG: 33
- STRAT_3M: 20
- SCALP_BREAKOUT_LONG: 11
- SCALP_STRICT_SHORT: 10
- SCALP_VWAP_PULLBACK_LONG: 3

## 4. 出口理由
- STOP_LOSS: 47
- TAKE_PROFIT_LIMIT_FILLED: 28
- TIME_STOP: 28
- EDGE_DECAY: 8

## Volatility-Regime Gate Summary
- Gate enabled: True
- Gate mode: warn_only
- Mode counts: warn_only=4598
- ALLOW count: 3602
- BLOCK count: 701
- WARN_ONLY count: 295
- Applied BLOCK count: 0
- BLOCK reasons: long vwap conflict; long vwap block=69, dead market; low volume_ratio 0.67=27, low volume_ratio 0.64; short board conflict=19, dead market; low volume_ratio 0.43; short board conflict=18, dead market; low volume_ratio 0.67; short board conflict=17, low volume_ratio 0.76; short board conflict=17, dead market; low volume_ratio 0.69=16, dead market; low volume_ratio 0.74; short board conflict=15
- Regime counts: DEAD=654, NORMAL=2751, ACTIVE=1193
- Raw LONG_CANDIDATE count: 918
- Raw SHORT_CANDIDATE count: 1074
- Final LONG_CANDIDATE count: 918
- Final SHORT_CANDIDATE count: 1074
- Gateで除外された候補数: 0

## 5. システム
- WARN/ERROR件数: 33

### 主要エラー/失敗イベント
- LIVE_ENTRY_FAIL: 24
- CANCEL_ORDER_FAIL: 8
- TAKE_PROFIT_CANCEL_FAIL: 1

### 注文・復旧イベント
- SHORT_MA_GUARD_SKIP: 601
- POSITION_SNAPSHOT: 464
- ENTRY_ORDER_REQUEST: 135
- ENTRY_ORDER_RESPONSE: 135
- POSITION_RECONCILE_RESULT: 135
- POSITION_RECONCILE_START: 135
- TAKE_PROFIT_ORDER_REQUEST: 111
- TAKE_PROFIT_ORDER_RESPONSE: 111
- TAKE_PROFIT_CANCEL_REQUEST: 84
- EXIT_ORDER_REQUEST: 83
- EXIT_ORDER_RESPONSE: 83
- TAKE_PROFIT_CANCEL_RESPONSE: 83
- TAKE_PROFIT_CANCEL_VERIFY: 83
- CANCEL_ORDER_REQUEST: 32
- CANCEL_ORDER_RESPONSE: 24
- ENTRY_POSITION_AFTER_CANCEL: 24

### API Code別件数
- API Code 43: 10

### 直近イベント
- 2026-05-20T15:18:42.778265+09:00 [INFO] ADAPTIVE_SKIP_ENTRY: side=LONG strategy=STRAT_1M reason=STRAT_1M_FROZEN until=2026-05-20 15:36:54+09:00
- 2026-05-20T15:18:46.874614+09:00 [INFO] ADAPTIVE_SKIP_ENTRY: side=LONG strategy=STRAT_1M reason=STRAT_1M_FROZEN until=2026-05-20 15:36:54+09:00
- 2026-05-20T15:18:58.097286+09:00 [INFO] LIVE_ENTRY_OK: LONG order_id=20260520A02N58965171
- 2026-05-20T15:19:05.684518+09:00 [INFO] TAKE_PROFIT_LIMIT_OK: LONG order_id=20260520A02N58965195 price=59090.0
- 2026-05-20T15:19:39.058223+09:00 [INFO] LIVE_TAKE_PROFIT_FILLED: LONG order_id=20260520A02N58965195
- 2026-05-20T15:19:39.071189+09:00 [INFO] LIGHT_BRAKE_FREEZE_STRAT_1M: until=2026-05-20T15:39:38+09:00 pnl_1m=-44.9
- 2026-05-20T15:30:02.382157+09:00 [INFO] STOP_AFTER_SESSION: session end reached
- 2026-05-20T15:30:02.402586+09:00 [INFO] STOP: monitor stop requested