"""Strategy simulation engine for Polymarket SOL up/down windows."""

from __future__ import annotations

import csv
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import yaml

TRADES_HEADER = [
    "signal_timestamp",
    "target_window_start",
    "slug",
    "setup_window_start",
    "setup_streak",
    "setup_abs_delta",
    "setup_total_move",
    "limit_cents",
    "capital_before",
    "free_capital_before",
    "locked_capital_before",
    "invested_amount",
    "contracts",
    "up_filled",
    "down_filled",
    "fill_type",
    "entry_up",
    "entry_down",
    "exit_up",
    "exit_down",
    "pnl",
    "capital_after",
    "free_capital_after",
    "mode",
    "notes",
    "bias",
    "move_amount",
    "regime_mode",
    "setup_direction",
]

OPPOSITE_TRADES_HEADER = [
    "signal_timestamp",
    "target_window_start",
    "slug",
    "setup_window_start",
    "setup_streak",
    "setup_direction",
    "setup_abs_delta",
    "setup_total_move",
    "limit_cents",
    "side",
    "capital_before",
    "free_capital_before",
    "locked_capital_before",
    "invested_amount",
    "contracts",
    "filled",
    "entry",
    "exit",
    "fill_type",
    "pnl",
    "capital_after",
    "free_capital_after",
    "mode",
    "notes",
    "bias",
    "move_amount",
    "regime_mode",
]

FLAT_DUAL_TRADES_HEADER = [
    "signal_timestamp",
    "window_start",
    "slug",
    "remaining_at_entry",
    "price_to_beat",
    "price_at_entry",
    "start_gap",
    "max_gap_before_entry",
    "min_gap_before_entry",
    "flips_before_entry",
    "last_flip_before_time_left",
    "flips_after_entry",
    "last_flip_after_time_left",
    "up_lowest_before_decision",
    "up_lowest_time_left",
    "down_lowest_before_decision",
    "down_lowest_time_left",
    "up_lowest_after_decision",
    "up_lowest_after_time_left",
    "down_lowest_after_decision",
    "down_lowest_after_time_left",
    "gap_at_entry",
    "final_gap",
    "binance_gap_at_entry",
    "trend_open",
    "trend_move",
    "trend_bias",
    "max_move",
    "limit_cents",
    "up_ask_at_entry",
    "down_ask_at_entry",
    "up_ask_at_end",
    "down_ask_at_end",
    "up_low_last_interval",
    "down_low_last_interval",
    "entry_mode",
    "capital_before",
    "free_capital_before",
    "locked_capital_before",
    "invested_amount",
    "contracts",
    "up_filled",
    "down_filled",
    "fill_type",
    "entry_up",
    "entry_down",
    "one_side_filled",
    "one_side_fill_time_left",
    "one_side_max_bid",
    "one_side_max_bid_time_left",
    "one_side_bid_at_end",
    "one_side_paired_time_left",
    "outcome",
    "pnl",
    "capital_after",
    "free_capital_after",
    "mode",
    "notes",
]

HISTORY_LIMIT = 20
REGIME_HISTORY_LIMIT = 400
VALID_BIAS_MODES = ("off", "ref", "rolling", "ema", "daily_open")

DUAL_HEDGE_REQUIRED = [
    "enabled",
    "capital",
    "investable_per_trade",
    "capital_mode",
    "limit_cents",
    "min_streak",
    "max_streak",
    "max_last_delta",
    "use_total_move",
    "min_total_move",
    "max_total_move",
    "use_reset_streak",
    "early_inference_threshold",
    "decision_remaining_seconds",
]

OPPOSITE_REQUIRED = [
    "enabled",
    "capital",
    "investable_per_trade",
    "capital_mode",
    "limit_cents",
    "min_streak",
    "max_streak",
    "max_last_delta",
    "use_total_move",
    "min_total_move",
    "max_total_move",
    "use_reset_streak",
    "early_inference_threshold",
    "decision_remaining_seconds",
]

FLAT_DUAL_REQUIRED = [
    "enabled",
    "capital",
    "investable_per_trade",
    "capital_mode",
    "limit_cents",
    "max_move",
    "decision_fraction",
]


def data_file_paths(coin: str, duration_minutes: int) -> tuple[str, str]:
    """Return (market_data_file, dual_hedge_trades_file) from coin + duration."""
    base = f"{coin}-{int(duration_minutes)}"
    return f"{base}-updown.csv", f"{base}-trades.csv"


def opposite_trades_path(coin: str, duration_minutes: int) -> str:
    return f"{coin}-{int(duration_minutes)}-opposite-trades.csv"


def flat_dual_trades_path(coin: str, duration_minutes: int) -> str:
    return f"{coin}-{int(duration_minutes)}-flat-dual-trades.csv"


def apply_duration_paths(config: dict[str, Any], coin: str, duration_minutes: int) -> dict[str, Any]:
    """Set market + per-strategy trades paths for the effective duration."""
    config = dict(config)
    config["duration_minutes"] = int(duration_minutes)
    config["coin"] = coin
    market_file, dh_trades = data_file_paths(coin, duration_minutes)
    config["market_data_file"] = market_file

    strategies = dict(config.get("strategies") or {})
    if "dual_hedge" in strategies and isinstance(strategies["dual_hedge"], dict):
        dh = dict(strategies["dual_hedge"])
        dh["trades_log_file"] = dh_trades
        strategies["dual_hedge"] = dh
    if "opposite_side" in strategies and isinstance(strategies["opposite_side"], dict):
        opp = dict(strategies["opposite_side"])
        opp["trades_log_file"] = opposite_trades_path(coin, duration_minutes)
        strategies["opposite_side"] = opp
    if "flat_dual" in strategies and isinstance(strategies["flat_dual"], dict):
        fd = dict(strategies["flat_dual"])
        fd["trades_log_file"] = flat_dual_trades_path(coin, duration_minutes)
        strategies["flat_dual"] = fd
    config["strategies"] = strategies
    # Back-compat alias used by older dual-hedge wiring
    config["trades_log_file"] = dh_trades
    return config


def load_config(path: str = "config.yaml") -> dict[str, Any]:
    """Load market + nested strategies config from YAML."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must be a mapping: {path}")

    required_top = ["coin", "duration_minutes", "mode", "strategies"]
    missing = [k for k in required_top if k not in data]
    if missing:
        raise ValueError(f"Config missing keys: {missing}")
    if data["mode"] not in ("simulate", "paper", "live"):
        raise ValueError("mode must be 'simulate', 'paper', or 'live'")

    strategies = data["strategies"]
    if not isinstance(strategies, dict):
        raise ValueError("strategies must be a mapping")

    if "dual_hedge" in strategies:
        dh = strategies["dual_hedge"]
        if not isinstance(dh, dict):
            raise ValueError("strategies.dual_hedge must be a mapping")
        miss = [k for k in DUAL_HEDGE_REQUIRED if k not in dh]
        if miss:
            raise ValueError(f"strategies.dual_hedge missing keys: {miss}")
        if dh["capital_mode"] not in ("locked", "unlocked"):
            raise ValueError("strategies.dual_hedge.capital_mode must be 'locked' or 'unlocked'")

    if "opposite_side" in strategies:
        opp = strategies["opposite_side"]
        if not isinstance(opp, dict):
            raise ValueError("strategies.opposite_side must be a mapping")
        miss = [k for k in OPPOSITE_REQUIRED if k not in opp]
        if miss:
            raise ValueError(f"strategies.opposite_side missing keys: {miss}")
        if opp["capital_mode"] not in ("locked", "unlocked"):
            raise ValueError("strategies.opposite_side.capital_mode must be 'locked' or 'unlocked'")

    if "flat_dual" in strategies:
        fd = strategies["flat_dual"]
        if not isinstance(fd, dict):
            raise ValueError("strategies.flat_dual must be a mapping")
        miss = [k for k in FLAT_DUAL_REQUIRED if k not in fd]
        if miss:
            raise ValueError(f"strategies.flat_dual missing keys: {miss}")
        if fd["capital_mode"] not in ("locked", "unlocked"):
            raise ValueError("strategies.flat_dual.capital_mode must be 'locked' or 'unlocked'")
        if not 0.0 < float(fd["decision_fraction"]) < 1.0:
            raise ValueError("strategies.flat_dual.decision_fraction must be between 0 and 1")

    market_bias = data.get("market_bias")
    if market_bias is None:
        data["market_bias"] = {"mode": "off"}
    elif not isinstance(market_bias, dict):
        raise ValueError("market_bias must be a mapping")
    else:
        mode = str(market_bias.get("mode") or "off").strip().lower()
        if mode not in VALID_BIAS_MODES:
            raise ValueError("market_bias.mode must be one of: " + ", ".join(VALID_BIAS_MODES))
        market_bias["mode"] = mode

    data["_config_duration_minutes"] = int(data["duration_minutes"])
    return apply_duration_paths(data, str(data["coin"]), int(data["duration_minutes"]))


def _parse_price(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text or text.upper() == "N/A":
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _format_mmss(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    return f"{total // 60:02d}:{total % 60:02d}"


def _coin_slug_prefix(coin: str) -> str:
    coin = (coin or "").strip().lower()
    if coin in ("sol", "solana"):
        return "sol"
    return coin


class CapitalManager:
    """Tracks free vs locked capital and per-window position locks."""

    def __init__(self, total_capital: float, investable_per_trade: float, capital_mode: str):
        self.total_capital = float(total_capital)
        self.investable_per_trade = float(investable_per_trade)
        self.capital_mode = capital_mode
        self.free_capital = float(total_capital)
        self.locked_capital = 0.0
        self.open_positions: dict[int, float] = {}

    @property
    def equity(self) -> float:
        return self.free_capital + self.locked_capital

    def _max_investment(self) -> float:
        if self.capital_mode == "unlocked":
            return self.investable_per_trade
        return min(self.investable_per_trade, self.free_capital)

    def calculate_contracts(self, limit_cents: int) -> int:
        cost_per_dual = (limit_cents / 100.0) * 2.0
        if cost_per_dual <= 0:
            return 0
        contracts = int(self._max_investment() // cost_per_dual)
        return max(contracts, 0)

    def calculate_contracts_single(self, entry_price: float) -> int:
        if entry_price <= 0:
            return 0
        contracts = int(self._max_investment() // entry_price)
        return max(contracts, 0)

    def required_cost(self, contracts: int, limit_cents: int) -> float:
        return contracts * (limit_cents / 100.0) * 2.0

    def required_cost_single(self, contracts: int, entry_price: float) -> float:
        return contracts * entry_price

    def try_lock(self, window_start: int, amount: float) -> tuple[bool, str]:
        if amount <= 0:
            return False, "Insufficient free capital"
        if window_start in self.open_positions:
            return False, "Position already open for window"

        if self.capital_mode == "unlocked":
            if amount > self.investable_per_trade + 1e-9:
                return False, "Exceeds investable_per_trade"
            self.open_positions[window_start] = amount
            return True, "unlocked"

        max_we_can_use = min(self.investable_per_trade, self.free_capital)
        if amount > max_we_can_use + 1e-9:
            return False, "Insufficient free capital"

        self.free_capital -= amount
        self.locked_capital += amount
        self.open_positions[window_start] = amount
        return True, "locked"

    def release(self, window_start: int, pnl: float) -> tuple[float, float, float]:
        locked_amount = self.open_positions.pop(window_start, 0.0)

        if self.capital_mode == "locked" and locked_amount > 0:
            self.locked_capital -= locked_amount
            self.free_capital += locked_amount + pnl
        else:
            self.free_capital += pnl

        self.total_capital = self.equity
        return locked_amount, self.free_capital, self.equity


@dataclass
class WindowRecord:
    window_start: int
    price_to_beat: float
    final_price: float
    outcome: str


def seed_history_from_csv(market_data_file: str) -> list[WindowRecord]:
    path = market_data_file
    if not os.path.isfile(path):
        return []
    records: list[WindowRecord] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return []
            field_map = {name.strip().lower(): name for name in reader.fieldnames if name}

            def col(*candidates: str) -> str | None:
                for c in candidates:
                    key = c.strip().lower()
                    if key in field_map:
                        return field_map[key]
                return None

            ts_col = col("time stamp", "timestamp")
            ptb_col = col("price_to_beat", "price to beat")
            final_col = col("final_price", "last price", " last price")
            outcome_col = col("outcome")
            if not all([ts_col, ptb_col, final_col, outcome_col]):
                return []

            for row in reader:
                try:
                    ws = int(float(str(row[ts_col]).strip()))
                    ptb = _parse_price(row[ptb_col])
                    final = _parse_price(row[final_col])
                    outcome = str(row[outcome_col]).strip()
                except (TypeError, ValueError, KeyError):
                    continue
                if outcome not in ("Up", "Down"):
                    continue
                if ptb <= 0 or final <= 0:
                    continue
                records.append(
                    WindowRecord(
                        window_start=ws,
                        price_to_beat=ptb,
                        final_price=final,
                        outcome=outcome,
                    )
                )
    except OSError:
        return []

    records.sort(key=lambda r: r.window_start)
    return records[-REGIME_HISTORY_LIMIT:]


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _signed_bias(move: float, threshold: float) -> str:
    if move <= -threshold:
        return "bearish"
    if move >= threshold:
        return "bullish"
    return "neutral"


def _regime_row_fields(
    regime: RegimeEngine | None,
    current_price: float | None = None,
) -> dict[str, Any]:
    if regime is None or regime.mode == "off":
        return {"bias": "", "move_amount": "", "regime_mode": ""}
    state = regime.get_state(current_price)
    return {
        "bias": state.bias,
        "move_amount": round(state.move_amount, 2),
        "regime_mode": state.mode,
    }


@dataclass
class RegimeState:
    bias: str
    move_amount: float
    mode: str
    ema_value: float | None = None
    reference_price: float | None = None
    ref_age_windows: int | None = None


class RegimeEngine:
    def __init__(
        self,
        mode: str = "off",
        duration_seconds: int = 900,
        reference_price: float | None = None,
        ref_set_timestamp: int | None = None,
        max_ref_age_windows: int = 250,
        lookback_windows: int = 96,
        ema_period: int = 96,
        ema_band: float = 0.75,
        strong_move: float = 1.5,
        anchor_hour_utc: float = 0.0,
    ):
        self.mode = mode if mode in VALID_BIAS_MODES else "off"
        self.anchor_hour_utc = float(anchor_hour_utc) % 24
        self.duration_seconds = int(duration_seconds) if duration_seconds else 900
        self.reference_price = reference_price
        self.ref_set_timestamp = ref_set_timestamp
        self.max_ref_age_windows = int(max_ref_age_windows)
        self.lookback_windows = int(lookback_windows)
        self.ema_period = max(1, int(ema_period))
        self.ema_band = float(ema_band)
        self.strong_move = float(strong_move)

        self.prices: list[float] = []
        self.window_starts: list[int] = []
        self.ema_value: float | None = None
        self._runtime_updates = 0
        self._seeding = False
        self._logged_null_ref = False
        self._logged_stale = False

        if self.mode == "ref" and self.reference_price is None:
            self._log_null_ref()

    def _log_null_ref(self) -> None:
        if self._logged_null_ref:
            return
        self._logged_null_ref = True
        print("⚠️  market_bias.mode=ref but reference_price is null; bias always neutral")

    def _log_stale(self, age: int) -> None:
        if self._logged_stale:
            return
        self._logged_stale = True
        print(f"⚠️  REF_STALE age={age} max={self.max_ref_age_windows}")

    @classmethod
    def from_config(cls, market_bias: dict[str, Any], duration_seconds: int) -> RegimeEngine:
        cfg = market_bias or {}
        mode = str(cfg.get("mode") or "off").strip().lower()
        if mode not in VALID_BIAS_MODES:
            mode = "off"
        return cls(
            mode=mode,
            duration_seconds=int(duration_seconds),
            reference_price=_optional_float(cfg.get("reference_price")),
            ref_set_timestamp=_optional_int(cfg.get("ref_set_timestamp")),
            max_ref_age_windows=int(cfg.get("max_ref_age_windows", 250)),
            lookback_windows=int(cfg.get("lookback_windows", 96)),
            ema_period=int(cfg.get("ema_period", 96)),
            ema_band=float(cfg.get("ema_band", 0.75)),
            strong_move=float(cfg.get("strong_move", 1.5)),
            anchor_hour_utc=float(cfg.get("anchor_hour_utc", 0.0)),
        )

    def _apply_ema(self, price: float) -> None:
        if self.ema_value is None:
            self.ema_value = price
            return
        alpha = 2.0 / (self.ema_period + 1)
        self.ema_value = alpha * price + (1.0 - alpha) * self.ema_value

    def _peek_ema(self, price: float) -> float:
        if self.ema_value is None:
            return price
        alpha = 2.0 / (self.ema_period + 1)
        return alpha * price + (1.0 - alpha) * self.ema_value

    def _rebuild_ema(self) -> None:
        self.ema_value = None
        for price in self.prices:
            self._apply_ema(price)

    def _trim_prices(self) -> None:
        overflow = len(self.prices) - REGIME_HISTORY_LIMIT
        if overflow > 0:
            del self.prices[:overflow]
            del self.window_starts[:overflow]
            self._rebuild_ema()

    def seed_from_history(self, records: list[WindowRecord]) -> None:
        self.prices = []
        self.window_starts = []
        self.ema_value = None
        self._runtime_updates = 0
        self._seeding = True
        try:
            for rec in records:
                if rec.final_price > 0:
                    self.update(rec.final_price, rec.window_start)
        finally:
            self._seeding = False

    def update(self, final_price: float, window_start: int) -> None:
        if final_price <= 0:
            return
        price = float(final_price)
        ws = int(window_start)
        if self.window_starts and self.window_starts[-1] == ws:
            self.prices[-1] = price
            self._rebuild_ema()
        else:
            self.prices.append(price)
            self.window_starts.append(ws)
            self._apply_ema(price)
            if not self._seeding:
                self._runtime_updates += 1
        self._trim_prices()

    def _ref_age_windows(self) -> int:
        if self.ref_set_timestamp is not None and self.duration_seconds > 0:
            ref_window = (
                int(self.ref_set_timestamp) // self.duration_seconds
            ) * self.duration_seconds
            if self.window_starts:
                now_window = self.window_starts[-1]
            else:
                now_window = (int(time.time()) // self.duration_seconds) * self.duration_seconds
            return max(0, int((now_window - ref_window) / self.duration_seconds))
        return self._runtime_updates

    def daily_anchor(self, now: float | None = None) -> int:
        """Most recent anchor time (anchor_hour_utc each day) at or before ``now``."""
        ts = time.time() if now is None else float(now)
        offset = self.anchor_hour_utc * 3600
        return int(ts - ((ts - offset) % 86400))

    def daily_open(self, now: float | None = None) -> float | None:
        # A window's open is the previous window's close, so the anchor's open is the
        # close of the window that ended at the anchor.
        target = self.daily_anchor(now) - self.duration_seconds
        for ws, price in zip(reversed(self.window_starts), reversed(self.prices)):
            if ws == target:
                return price
            if ws < target:
                break
        return None

    def get_state(self, current_price: float | None = None, now: float | None = None) -> RegimeState:
        mode = self.mode
        if mode == "off":
            return RegimeState(bias="neutral", move_amount=0.0, mode=mode)

        pending = current_price is not None and current_price > 0
        if pending:
            current = float(current_price)
        elif self.prices:
            current = self.prices[-1]
        else:
            current = None

        if mode == "ref":
            age = self._ref_age_windows()
            if self.reference_price is None:
                self._log_null_ref()
                return RegimeState(
                    bias="neutral",
                    move_amount=0.0,
                    mode=mode,
                    reference_price=None,
                    ref_age_windows=age,
                )
            if current is None:
                return RegimeState(
                    bias="neutral",
                    move_amount=0.0,
                    mode=mode,
                    reference_price=self.reference_price,
                    ref_age_windows=age,
                )
            move = current - self.reference_price
            if age > self.max_ref_age_windows:
                self._log_stale(age)
                return RegimeState(
                    bias="neutral",
                    move_amount=move,
                    mode=mode,
                    reference_price=self.reference_price,
                    ref_age_windows=age,
                )
            return RegimeState(
                bias=_signed_bias(move, self.strong_move),
                move_amount=move,
                mode=mode,
                reference_price=self.reference_price,
                ref_age_windows=age,
            )

        if mode == "rolling":
            lookback = self.lookback_windows
            if current is None:
                return RegimeState(bias="neutral", move_amount=0.0, mode=mode)
            if pending:
                if len(self.prices) < lookback:
                    return RegimeState(bias="neutral", move_amount=0.0, mode=mode)
                older = self.prices[-lookback]
            else:
                if len(self.prices) < lookback + 1:
                    return RegimeState(bias="neutral", move_amount=0.0, mode=mode)
                older = self.prices[-1 - lookback]
            move = current - older
            return RegimeState(
                bias=_signed_bias(move, self.strong_move),
                move_amount=move,
                mode=mode,
            )

        if mode == "ema":
            if current is None:
                return RegimeState(
                    bias="neutral",
                    move_amount=0.0,
                    mode=mode,
                    ema_value=self.ema_value,
                )
            if pending or self.ema_value is None:
                ema_now = self._peek_ema(current)
            else:
                ema_now = self.ema_value
            diff = current - ema_now
            return RegimeState(
                bias=_signed_bias(diff, self.ema_band),
                move_amount=diff,
                mode=mode,
                ema_value=ema_now,
            )

        if mode == "daily_open":
            open_price = self.daily_open(now)
            if current is None or open_price is None:
                return RegimeState(
                    bias="neutral", move_amount=0.0, mode=mode, reference_price=open_price
                )
            move = current - open_price
            return RegimeState(
                bias=_signed_bias(move, self.strong_move),
                move_amount=move,
                mode=mode,
                reference_price=open_price,
            )

        return RegimeState(bias="neutral", move_amount=0.0, mode=mode)

    def summarize_line(self) -> str:
        state = self.get_state()
        extra = ""
        if self.mode == "rolling":
            extra = f" lookback={self.lookback_windows}"
        elif self.mode == "ema":
            if state.ema_value is not None:
                extra = f" ema={state.ema_value:.2f} band={self.ema_band}"
        elif self.mode == "ref":
            extra = f" ref={state.reference_price}" if state.reference_price is not None else " ref=null"
            if state.ref_age_windows is not None:
                extra += f" age={state.ref_age_windows}"
        elif self.mode == "daily_open":
            anchor = time.strftime("%H:%M", time.gmtime(self.anchor_hour_utc * 3600))
            open_text = f"{state.reference_price:.2f}" if state.reference_price is not None else "missing"
            extra = f" open={open_text} anchor={anchor}UTC"
        return f"regime: mode={state.mode} bias={state.bias} move={state.move_amount:.2f}{extra}"


@dataclass
class OpenTrade:
    signal_timestamp: int
    target_window_start: int
    slug: str
    setup_window_start: int
    setup_streak: int
    setup_abs_delta: float
    setup_total_move: float
    limit_cents: int
    capital_before: float
    free_capital_before: float
    locked_capital_before: float
    invested_amount: float
    contracts: int
    up_filled: bool = False
    down_filled: bool = False
    entry_up: float | None = None
    entry_down: float | None = None
    notes: str = "pending"
    setup_direction: str = ""


@dataclass
class OppositeOpenTrade:
    signal_timestamp: int
    target_window_start: int
    slug: str
    setup_window_start: int
    setup_streak: int
    setup_direction: str
    setup_abs_delta: float
    setup_total_move: float
    limit_cents: int
    side: str
    capital_before: float
    free_capital_before: float
    locked_capital_before: float
    invested_amount: float
    contracts: int
    filled: bool = False
    entry: float | None = None
    notes: str = "pending"


class DualHedgeSimulator:
    def __init__(
        self,
        global_config: dict[str, Any],
        strategy_config: dict[str, Any],
        history: list[WindowRecord] | None = None,
        regime: RegimeEngine | None = None,
    ):
        self.duration_minutes = int(global_config["duration_minutes"])
        self.duration_seconds = self.duration_minutes * 60
        self.coin = str(global_config["coin"])
        self.mode = str(global_config["mode"])
        self.market_data_file = str(global_config["market_data_file"])

        cfg = strategy_config
        self.min_streak = int(cfg["min_streak"])
        self.max_streak = int(cfg["max_streak"])
        self.max_last_delta = float(cfg["max_last_delta"])
        self.use_total_move = bool(cfg["use_total_move"])
        self.min_total_move = float(cfg["min_total_move"])
        self.max_total_move = float(cfg["max_total_move"])
        self.use_reset_streak = bool(cfg.get("use_reset_streak", True))
        self.early_inference_threshold = float(cfg.get("early_inference_threshold", 0.90))
        self.decision_remaining_seconds = float(cfg.get("decision_remaining_seconds", 120))
        self.limit_cents = int(cfg["limit_cents"])
        self.limit_price = self.limit_cents / 100.0
        self.trades_log_file = str(cfg.get("trades_log_file") or global_config.get("trades_log_file"))
        self.skip_in_flat = bool(cfg.get("skip_in_flat", False))
        self.flat_short_lookback = int(cfg.get("flat_short_lookback", 20))
        self.flat_short_max = float(cfg.get("flat_short_max", 0.30))
        self.regime = regime

        self.capital = CapitalManager(
            total_capital=float(cfg["capital"]),
            investable_per_trade=float(cfg["investable_per_trade"]),
            capital_mode=str(cfg["capital_mode"]),
        )

        if history is not None:
            self.history = history
        else:
            self.history = seed_history_from_csv(self.market_data_file)

        self.open_trades: dict[int, OpenTrade] = {}
        self._decided_windows: set[int] = set()
        self._active_window_start: int | None = None
        self.status_line = "dh: idle"

        self._ensure_trades_header()

    def _ensure_trades_header(self) -> None:
        path = self.trades_log_file
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            with open(path, newline="", encoding="utf-8") as f:
                existing = next(csv.reader(f), None)
            if existing == TRADES_HEADER:
                return
            archived = f"{path}.{time.strftime('%Y%m%d-%H%M%S')}.bak"
            os.replace(path, archived)
            print(f"⚠️  Trades log schema updated; old file moved to {archived}")
        with open(path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(TRADES_HEADER)

    def _read_trades_rows(self) -> list[dict[str, str]]:
        path = self.trades_log_file
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return []
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return []
            return [dict(row) for row in reader]

    def _write_trades_rows(self, rows: list[dict[str, Any]]) -> None:
        path = self.trades_log_file
        tmp_path = f"{path}.tmp"
        with open(tmp_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADES_HEADER, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in TRADES_HEADER})
        os.replace(tmp_path, path)

    def _with_regime_fields(
        self,
        row: dict[str, Any],
        current_price: float | None = None,
        setup_direction: str | None = None,
    ) -> dict[str, Any]:
        filled = dict(_regime_row_fields(self.regime, current_price))
        filled.update(row)
        if setup_direction is not None:
            filled["setup_direction"] = setup_direction or ""
        elif "setup_direction" not in filled:
            filled["setup_direction"] = ""
        return filled

    def _append_trades_row(self, row: dict[str, Any], current_price: float | None = None) -> None:
        self._ensure_trades_header()
        filled = self._with_regime_fields(row, current_price=current_price)
        with open(self.trades_log_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADES_HEADER, extrasaction="ignore")
            writer.writerow({k: filled.get(k, "") for k in TRADES_HEADER})

    def _update_pending_trade_row(self, target_window_start: int, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        filled = self._with_regime_fields(row)
        rows = self._read_trades_rows()
        target_key = str(target_window_start)
        updated = False
        for i in range(len(rows) - 1, -1, -1):
            existing = rows[i]
            if str(existing.get("target_window_start", "")) != target_key:
                continue
            fill = str(existing.get("fill_type", "")).strip().lower()
            if fill in ("pending", ""):
                rows[i] = {k: filled.get(k, "") for k in TRADES_HEADER}
                updated = True
                break
        if not updated:
            rows.append({k: filled.get(k, "") for k in TRADES_HEADER})
        self._write_trades_rows(rows)

    def _target_slug(self, target_window_start: int) -> str:
        prefix = _coin_slug_prefix(self.coin)
        return f"{prefix}-updown-{self.duration_minutes}m-{target_window_start}"

    def _append_history(self, record: WindowRecord) -> None:
        if self.history and self.history[-1].window_start == record.window_start:
            self.history[-1] = record
        else:
            self.history.append(record)
        overflow = len(self.history) - REGIME_HISTORY_LIMIT
        if overflow > 0:
            del self.history[:overflow]

    def _print_event(self, message: str) -> None:
        sys.stdout.write("\n" + message + "\n")
        sys.stdout.flush()

    def display_status(self, window_start: int | None = None) -> str:
        if window_start is not None and window_start in self.open_trades:
            trade = self.open_trades[window_start]
            fills = []
            if trade.up_filled:
                fills.append("Up")
            if trade.down_filled:
                fills.append("Down")
            fill_txt = "+".join(fills) if fills else "waiting"
            return f"DH {trade.contracts}c @{trade.limit_cents}¢ [{fill_txt}]"
        if self.status_line:
            return self.status_line
        return "dh: idle"

    def _evaluate_setup(self, history: list[WindowRecord]) -> dict[str, Any]:
        empty = {
            "ok": False,
            "streak": 0,
            "last_delta": 0.0,
            "total_move": 0.0,
            "direction": None,
            "reason": "no_history",
        }
        if not history:
            return empty
        direction = history[-1].outcome
        if direction not in ("Up", "Down"):
            return {**empty, "reason": "unknown_outcome"}

        streak: list[WindowRecord] = []
        for rec in reversed(history):
            if rec.outcome != direction:
                break
            streak.append(rec)
        streak.reverse()
        raw_len = len(streak)
        if self.use_reset_streak:
            counted = ((raw_len - 1) % self.max_streak) + 1
            segment = streak[-counted:]
        else:
            counted = raw_len
            segment = streak
        last = segment[-1]
        first = segment[0]
        abs_delta = abs(last.final_price - last.price_to_beat)
        total_move = abs(last.final_price - first.price_to_beat)

        result = {
            "ok": False,
            "streak": counted,
            "last_delta": abs_delta,
            "total_move": total_move,
            "direction": direction,
            "reason": "",
        }
        if counted < self.min_streak or counted > self.max_streak:
            raw_note = f" raw={raw_len}" if raw_len != counted else ""
            result["reason"] = (
                f"streak={counted}{raw_note} not in [{self.min_streak},{self.max_streak}]"
            )
            return result
        if abs_delta > self.max_last_delta:
            result["reason"] = f"last>{self.max_last_delta}"
            return result
        if self.use_total_move and (
            total_move < self.min_total_move or total_move > self.max_total_move
        ):
            result["reason"] = f"move not in [{self.min_total_move},{self.max_total_move}]"
            return result
        result["ok"] = True
        result["reason"] = "pass" if raw_len == counted else f"pass raw={raw_len}"
        return result

    def _log_skip(
        self,
        setup_window_start: int,
        target_window_start: int,
        slug: str,
        streak_len: int,
        abs_delta: float,
        total_move: float,
        reason: str,
        free_before: float,
        locked_before: float,
        equity_before: float,
        setup_direction: str | None = None,
        current_price: float | None = None,
    ) -> None:
        self._append_trades_row(
            {
                "signal_timestamp": int(time.time()),
                "target_window_start": target_window_start,
                "slug": slug,
                "setup_window_start": setup_window_start,
                "setup_streak": streak_len,
                "setup_abs_delta": round(abs_delta, 2),
                "setup_total_move": round(total_move, 2),
                "limit_cents": self.limit_cents,
                "capital_before": round(equity_before, 2),
                "free_capital_before": round(free_before, 2),
                "locked_capital_before": round(locked_before, 2),
                "invested_amount": 0,
                "contracts": 0,
                "up_filled": "",
                "down_filled": "",
                "fill_type": "skipped",
                "entry_up": "",
                "entry_down": "",
                "exit_up": "",
                "exit_down": "",
                "pnl": "",
                "capital_after": round(equity_before, 2),
                "free_capital_after": round(free_before, 2),
                "mode": self.mode,
                "notes": reason,
                "setup_direction": setup_direction or "",
            },
            current_price=current_price,
        )
        self.status_line = f"dh skip last={abs_delta:.2f}"
        self._print_event(
            f"➖ [dual_hedge] no simulation last={abs_delta:.2f} move={total_move:.2f} | {reason} | {slug}"
        )

    def _maybe_emit_setup(
        self,
        setup_window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
        *,
        early: bool = False,
    ) -> dict[str, Any] | None:
        if setup_window_start in self._decided_windows:
            return None
        if final_price <= 0 or outcome not in ("Up", "Down"):
            return None

        eval_history = [r for r in self.history if r.window_start < setup_window_start]
        eval_history.append(
            WindowRecord(
                window_start=setup_window_start,
                price_to_beat=price_to_beat,
                final_price=final_price,
                outcome=outcome,
            )
        )
        evaluation = self._evaluate_setup(eval_history)
        self._decided_windows.add(setup_window_start)

        streak_len = int(evaluation["streak"])
        abs_delta = float(evaluation["last_delta"])
        total_move = float(evaluation["total_move"])
        target_window_start = setup_window_start + self.duration_seconds
        slug = self._target_slug(target_window_start)
        early_tag = " early" if early else ""

        if not evaluation["ok"]:
            self.status_line = f"dh skip last={abs_delta:.2f}"
            self._print_event(
                f"➖ [dual_hedge]{early_tag} no simulation last={abs_delta:.2f} move={total_move:.2f} "
                f"| streak={streak_len} {evaluation['direction'] or '?'} "
                f"| {evaluation['reason']}"
            )
            return None

        setup_direction = str(evaluation.get("direction") or "")
        free_before = self.capital.free_capital
        locked_before = self.capital.locked_capital
        equity_before = self.capital.equity

        if self.skip_in_flat and self.regime is not None:
            state = self.regime.get_state(final_price)
            if state.bias == "neutral":
                lookback = self.flat_short_lookback
                price_n_ago = None
                if len(eval_history) > lookback:
                    price_n_ago = eval_history[-1 - lookback].final_price
                if price_n_ago is not None:
                    short_move = abs(final_price - price_n_ago)
                    if short_move < self.flat_short_max:
                        self._log_skip(
                            setup_window_start,
                            target_window_start,
                            slug,
                            streak_len,
                            abs_delta,
                            total_move,
                            f"flat_skip bias=neutral short={short_move:.2f}",
                            free_before,
                            locked_before,
                            equity_before,
                            setup_direction=setup_direction,
                            current_price=final_price,
                        )
                        return None

        contracts = self.capital.calculate_contracts(self.limit_cents)
        required_cost = self.capital.required_cost(contracts, self.limit_cents)

        if contracts <= 0 or required_cost <= 0:
            self._log_skip(
                setup_window_start,
                target_window_start,
                slug,
                streak_len,
                abs_delta,
                total_move,
                "Insufficient free capital",
                free_before,
                locked_before,
                equity_before,
                setup_direction=setup_direction,
                current_price=final_price,
            )
            return None

        ok, reason = self.capital.try_lock(target_window_start, required_cost)
        if not ok:
            self._log_skip(
                setup_window_start,
                target_window_start,
                slug,
                streak_len,
                abs_delta,
                total_move,
                reason,
                free_before,
                locked_before,
                equity_before,
                setup_direction=setup_direction,
                current_price=final_price,
            )
            return None

        signal_ts = int(time.time())
        note = "pending;early_inference" if early else "pending"
        if reason == "unlocked":
            note = f"{note};capital_unlocked"

        trade = OpenTrade(
            signal_timestamp=signal_ts,
            target_window_start=target_window_start,
            slug=slug,
            setup_window_start=setup_window_start,
            setup_streak=streak_len,
            setup_abs_delta=round(abs_delta, 2),
            setup_total_move=round(total_move, 2),
            limit_cents=self.limit_cents,
            capital_before=round(equity_before, 2),
            free_capital_before=round(free_before, 2),
            locked_capital_before=round(locked_before, 2),
            invested_amount=round(required_cost, 2),
            contracts=contracts,
            notes=note,
            setup_direction=setup_direction,
        )
        self.open_trades[target_window_start] = trade

        self._append_trades_row(
            {
                "signal_timestamp": trade.signal_timestamp,
                "target_window_start": trade.target_window_start,
                "slug": trade.slug,
                "setup_window_start": trade.setup_window_start,
                "setup_streak": trade.setup_streak,
                "setup_abs_delta": trade.setup_abs_delta,
                "setup_total_move": trade.setup_total_move,
                "limit_cents": trade.limit_cents,
                "capital_before": trade.capital_before,
                "free_capital_before": trade.free_capital_before,
                "locked_capital_before": trade.locked_capital_before,
                "invested_amount": trade.invested_amount,
                "contracts": trade.contracts,
                "up_filled": "",
                "down_filled": "",
                "fill_type": "pending",
                "entry_up": "",
                "entry_down": "",
                "exit_up": "",
                "exit_down": "",
                "pnl": "",
                "capital_after": "",
                "free_capital_after": "",
                "mode": self.mode,
                "notes": trade.notes,
                "setup_direction": trade.setup_direction,
            },
            current_price=final_price,
        )

        self.status_line = f"dh open last={abs_delta:.2f}"
        self._print_event(
            f"✅ [dual_hedge]{early_tag} Simulation started | streak={streak_len} {evaluation['direction']} "
            f"| last={abs_delta:.2f} move={total_move:.2f} "
            f"| → {slug} | {contracts}c @{self.limit_cents}¢ (${required_cost:.2f}) "
            f"| free=${self.capital.free_capital:.2f} locked=${self.capital.locked_capital:.2f}"
        )
        if self.mode in ("paper", "live"):
            self._print_event(
                f"⚠️  mode={self.mode}: order placement not implemented (simulation accounting only)."
            )
        return {
            "signal_timestamp": trade.signal_timestamp,
            "target_window_start": trade.target_window_start,
            "slug": trade.slug,
            "mode": self.mode,
        }

    def _update_fills(self, window_start: int, lowest_up: float, lowest_down: float) -> None:
        trade = self.open_trades.get(window_start)
        if trade is None:
            return
        if lowest_up != float("inf") and 0.0 < lowest_up <= self.limit_price:
            trade.up_filled = True
            trade.entry_up = self.limit_price
        if lowest_down != float("inf") and 0.0 < lowest_down <= self.limit_price:
            trade.down_filled = True
            trade.entry_down = self.limit_price

    def _settle_open_trade(self, window_start: int, outcome: str) -> None:
        trade = self.open_trades.get(window_start)
        if trade is None:
            return

        up_filled = trade.up_filled
        down_filled = trade.down_filled
        if up_filled and down_filled:
            fill_type = "both"
        elif up_filled:
            fill_type = "only_up"
        elif down_filled:
            fill_type = "only_down"
        else:
            fill_type = "none"

        exit_up = None
        exit_down = None
        pnl = 0.0
        contracts = trade.contracts

        if outcome in ("Up", "Down"):
            if up_filled:
                exit_up = 1.0 if outcome == "Up" else 0.0
                pnl += contracts * (exit_up - (trade.entry_up or self.limit_price))
            if down_filled:
                exit_down = 1.0 if outcome == "Down" else 0.0
                pnl += contracts * (exit_down - (trade.entry_down or self.limit_price))
        else:
            if up_filled:
                exit_up = ""
            if down_filled:
                exit_down = ""
            pnl = 0.0

        _locked_amount, free_after, equity_after = self.capital.release(window_start, pnl)

        note = "settled"
        if outcome not in ("Up", "Down"):
            note = "settled;outcome_unknown"
        if self.mode != "simulate":
            note = f"{note};mode={self.mode}_stub"

        self._update_pending_trade_row(
            window_start,
            {
                "signal_timestamp": trade.signal_timestamp,
                "target_window_start": trade.target_window_start,
                "slug": trade.slug,
                "setup_window_start": trade.setup_window_start,
                "setup_streak": trade.setup_streak,
                "setup_abs_delta": trade.setup_abs_delta,
                "setup_total_move": trade.setup_total_move,
                "limit_cents": trade.limit_cents,
                "capital_before": trade.capital_before,
                "free_capital_before": trade.free_capital_before,
                "locked_capital_before": trade.locked_capital_before,
                "invested_amount": trade.invested_amount,
                "contracts": trade.contracts,
                "up_filled": up_filled,
                "down_filled": down_filled,
                "fill_type": fill_type,
                "entry_up": trade.entry_up if up_filled else "",
                "entry_down": trade.entry_down if down_filled else "",
                "exit_up": exit_up if exit_up is not None else "",
                "exit_down": exit_down if exit_down is not None else "",
                "pnl": round(pnl, 2),
                "capital_after": round(equity_after, 2),
                "free_capital_after": round(free_after, 2),
                "mode": self.mode,
                "notes": note,
                "setup_direction": trade.setup_direction,
            },
        )

        self.status_line = f"dh settled pnl={pnl:+.2f}"
        self._print_event(
            f"📒 [dual_hedge] TRADE SETTLED {trade.slug} | fill={fill_type} | outcome={outcome} "
            f"| pnl={pnl:+.2f} | equity=${equity_after:.2f} | free=${free_after:.2f}"
        )
        del self.open_trades[window_start]

    def _try_early_inference(
        self,
        window_start: int,
        price_to_beat: float,
        current_price: float,
        remaining_seconds: float,
        up_ask: float,
        down_ask: float,
    ) -> dict[str, Any] | None:
        if window_start in self._decided_windows:
            return None
        if remaining_seconds > self.decision_remaining_seconds:
            return None

        thresh = self.early_inference_threshold
        up_hit = up_ask >= thresh
        down_hit = down_ask >= thresh
        if up_hit == down_hit:
            return None
        if current_price <= 0 or price_to_beat <= 0:
            return None

        inferred = "Up" if up_hit else "Down"
        return self._maybe_emit_setup(
            setup_window_start=window_start,
            price_to_beat=price_to_beat,
            final_price=current_price,
            outcome=inferred,
            early=True,
        )

    def on_window_update(
        self,
        window_start: int,
        price_to_beat: float,
        current_price: float,
        lowest_up: float,
        lowest_down: float,
        remaining_seconds: float,
        up_ask: float,
        down_ask: float,
        inferred_outcome: str | None,
        **_: Any,
    ) -> dict[str, Any] | None:
        if self._active_window_start != window_start:
            self._active_window_start = window_start
        self._update_fills(window_start, lowest_up, lowest_down)
        return self._try_early_inference(
            window_start,
            price_to_beat,
            current_price,
            remaining_seconds,
            up_ask,
            down_ask,
        )

    def on_window_close(
        self,
        window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
        lowest_up: float,
        lowest_down: float,
    ) -> dict[str, Any] | None:
        self._update_fills(window_start, lowest_up, lowest_down)
        self._settle_open_trade(window_start, outcome)

        signal = None
        if window_start not in self._decided_windows and outcome in ("Up", "Down"):
            signal = self._maybe_emit_setup(
                setup_window_start=window_start,
                price_to_beat=price_to_beat,
                final_price=final_price,
                outcome=outcome,
            )
        else:
            self._decided_windows.add(window_start)

        if outcome in ("Up", "Down") and final_price > 0 and price_to_beat > 0:
            self._append_history(
                WindowRecord(
                    window_start=window_start,
                    price_to_beat=price_to_beat,
                    final_price=final_price,
                    outcome=outcome,
                )
            )

        if len(self._decided_windows) > HISTORY_LIMIT * 2:
            cutoff = window_start - self.duration_seconds * HISTORY_LIMIT
            self._decided_windows = {w for w in self._decided_windows if w >= cutoff}

        return signal


class OppositeSideSimulator:
    def __init__(
        self,
        global_config: dict[str, Any],
        strategy_config: dict[str, Any],
        history: list[WindowRecord] | None = None,
        regime: RegimeEngine | None = None,
    ):
        self.duration_minutes = int(global_config["duration_minutes"])
        self.duration_seconds = self.duration_minutes * 60
        self.coin = str(global_config["coin"])
        self.mode = str(global_config["mode"])
        self.market_data_file = str(global_config["market_data_file"])

        cfg = strategy_config
        self.min_streak = int(cfg["min_streak"])
        self.max_streak = int(cfg["max_streak"])
        self.max_last_delta = float(cfg["max_last_delta"])
        self.use_total_move = bool(cfg["use_total_move"])
        self.min_total_move = float(cfg["min_total_move"])
        self.max_total_move = float(cfg["max_total_move"])
        self.use_reset_streak = bool(cfg.get("use_reset_streak", True))
        self.early_inference_threshold = float(cfg.get("early_inference_threshold", 0.90))
        self.decision_remaining_seconds = float(cfg.get("decision_remaining_seconds", 120))
        self.limit_cents = int(cfg["limit_cents"])
        self.limit_price = self.limit_cents / 100.0
        self.trades_log_file = str(
            cfg.get("trades_log_file")
            or opposite_trades_path(self.coin, self.duration_minutes)
        )
        self.skip_buy_up_when_bearish = bool(cfg.get("skip_buy_up_when_bearish", True))
        self.skip_buy_down_when_bullish = bool(cfg.get("skip_buy_down_when_bullish", True))
        self.regime = regime

        self.capital = CapitalManager(
            total_capital=float(cfg["capital"]),
            investable_per_trade=float(cfg["investable_per_trade"]),
            capital_mode=str(cfg["capital_mode"]),
        )

        if history is not None:
            self.history = history
        else:
            self.history = seed_history_from_csv(self.market_data_file)

        self.open_trades: dict[int, OppositeOpenTrade] = {}
        self._decided_windows: set[int] = set()
        self._active_window_start: int | None = None
        self.status_line = "opp: idle"

        self._ensure_trades_header()

    def _ensure_trades_header(self) -> None:
        path = self.trades_log_file
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            with open(path, newline="", encoding="utf-8") as f:
                existing = next(csv.reader(f), None)
            if existing == OPPOSITE_TRADES_HEADER:
                return
            archived = f"{path}.{time.strftime('%Y%m%d-%H%M%S')}.bak"
            os.replace(path, archived)
            print(f"⚠️  Opposite trades log schema updated; old file moved to {archived}")
        with open(path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(OPPOSITE_TRADES_HEADER)

    def _read_trades_rows(self) -> list[dict[str, str]]:
        path = self.trades_log_file
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return []
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return []
            return [dict(row) for row in reader]

    def _write_trades_rows(self, rows: list[dict[str, Any]]) -> None:
        path = self.trades_log_file
        tmp_path = f"{path}.tmp"
        with open(tmp_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OPPOSITE_TRADES_HEADER, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in OPPOSITE_TRADES_HEADER})
        os.replace(tmp_path, path)

    def _with_regime_fields(
        self,
        row: dict[str, Any],
        current_price: float | None = None,
    ) -> dict[str, Any]:
        filled = dict(_regime_row_fields(self.regime, current_price))
        filled.update(row)
        return filled

    def _append_trades_row(self, row: dict[str, Any], current_price: float | None = None) -> None:
        self._ensure_trades_header()
        filled = self._with_regime_fields(row, current_price=current_price)
        with open(self.trades_log_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OPPOSITE_TRADES_HEADER, extrasaction="ignore")
            writer.writerow({k: filled.get(k, "") for k in OPPOSITE_TRADES_HEADER})

    def _update_pending_trade_row(self, target_window_start: int, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        filled = self._with_regime_fields(row)
        rows = self._read_trades_rows()
        target_key = str(target_window_start)
        updated = False
        for i in range(len(rows) - 1, -1, -1):
            existing = rows[i]
            if str(existing.get("target_window_start", "")) != target_key:
                continue
            fill = str(existing.get("fill_type", "")).strip().lower()
            if fill in ("pending", ""):
                rows[i] = {k: filled.get(k, "") for k in OPPOSITE_TRADES_HEADER}
                updated = True
                break
        if not updated:
            rows.append({k: filled.get(k, "") for k in OPPOSITE_TRADES_HEADER})
        self._write_trades_rows(rows)

    def _target_slug(self, target_window_start: int) -> str:
        prefix = _coin_slug_prefix(self.coin)
        return f"{prefix}-updown-{self.duration_minutes}m-{target_window_start}"

    def _append_history(self, record: WindowRecord) -> None:
        if self.history and self.history[-1].window_start == record.window_start:
            self.history[-1] = record
        else:
            self.history.append(record)
        overflow = len(self.history) - REGIME_HISTORY_LIMIT
        if overflow > 0:
            del self.history[:overflow]

    def _print_event(self, message: str) -> None:
        sys.stdout.write("\n" + message + "\n")
        sys.stdout.flush()

    def display_status(self, window_start: int | None = None) -> str:
        if window_start is not None and window_start in self.open_trades:
            trade = self.open_trades[window_start]
            fill_txt = "filled" if trade.filled else "waiting"
            return f"OPP {trade.side} {trade.contracts}c @{trade.limit_cents}¢ [{fill_txt}]"
        if self.status_line:
            return self.status_line
        return "opp: idle"

    def _evaluate_setup(self, history: list[WindowRecord]) -> dict[str, Any]:
        empty = {
            "ok": False,
            "streak": 0,
            "last_delta": 0.0,
            "total_move": 0.0,
            "direction": None,
            "reason": "no_history",
        }
        if not history:
            return empty
        direction = history[-1].outcome
        if direction not in ("Up", "Down"):
            return {**empty, "reason": "unknown_outcome"}

        streak: list[WindowRecord] = []
        for rec in reversed(history):
            if rec.outcome != direction:
                break
            streak.append(rec)
        streak.reverse()
        raw_len = len(streak)
        if self.use_reset_streak:
            counted = ((raw_len - 1) % self.max_streak) + 1
            segment = streak[-counted:]
        else:
            counted = raw_len
            segment = streak
        last = segment[-1]
        first = segment[0]
        abs_delta = abs(last.final_price - last.price_to_beat)
        total_move = abs(last.final_price - first.price_to_beat)

        result = {
            "ok": False,
            "streak": counted,
            "last_delta": abs_delta,
            "total_move": total_move,
            "direction": direction,
            "reason": "",
        }
        if counted < self.min_streak or counted > self.max_streak:
            raw_note = f" raw={raw_len}" if raw_len != counted else ""
            result["reason"] = (
                f"streak={counted}{raw_note} not in [{self.min_streak},{self.max_streak}]"
            )
            return result
        if abs_delta > self.max_last_delta:
            result["reason"] = f"last>{self.max_last_delta}"
            return result
        if self.use_total_move and (
            total_move < self.min_total_move or total_move > self.max_total_move
        ):
            result["reason"] = f"move not in [{self.min_total_move},{self.max_total_move}]"
            return result
        result["ok"] = True
        result["reason"] = "pass" if raw_len == counted else f"pass raw={raw_len}"
        return result

    def _log_skip(
        self,
        setup_window_start: int,
        target_window_start: int,
        slug: str,
        streak_len: int,
        setup_direction: str | None,
        abs_delta: float,
        total_move: float,
        reason: str,
        free_before: float,
        locked_before: float,
        equity_before: float,
        side: str = "",
        current_price: float | None = None,
    ) -> None:
        self._append_trades_row(
            {
                "signal_timestamp": int(time.time()),
                "target_window_start": target_window_start,
                "slug": slug,
                "setup_window_start": setup_window_start,
                "setup_streak": streak_len,
                "setup_direction": setup_direction or "",
                "setup_abs_delta": round(abs_delta, 2),
                "setup_total_move": round(total_move, 2),
                "limit_cents": self.limit_cents,
                "side": side,
                "capital_before": round(equity_before, 2),
                "free_capital_before": round(free_before, 2),
                "locked_capital_before": round(locked_before, 2),
                "invested_amount": 0,
                "contracts": 0,
                "filled": False,
                "entry": "",
                "exit": "",
                "fill_type": "skipped",
                "pnl": "",
                "capital_after": round(equity_before, 2),
                "free_capital_after": round(free_before, 2),
                "mode": self.mode,
                "notes": reason,
            },
            current_price=current_price,
        )
        self.status_line = f"opp skip last={abs_delta:.2f}"
        self._print_event(
            f"➖ [opposite_side] no simulation last={abs_delta:.2f} move={total_move:.2f} | {reason} | {slug}"
        )

    def _maybe_emit_setup(
        self,
        setup_window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
        *,
        early: bool = False,
    ) -> dict[str, Any] | None:
        if setup_window_start in self._decided_windows:
            return None
        if final_price <= 0 or outcome not in ("Up", "Down"):
            return None

        eval_history = [r for r in self.history if r.window_start < setup_window_start]
        eval_history.append(
            WindowRecord(
                window_start=setup_window_start,
                price_to_beat=price_to_beat,
                final_price=final_price,
                outcome=outcome,
            )
        )
        evaluation = self._evaluate_setup(eval_history)
        self._decided_windows.add(setup_window_start)

        streak_len = int(evaluation["streak"])
        abs_delta = float(evaluation["last_delta"])
        total_move = float(evaluation["total_move"])
        setup_direction = evaluation["direction"]
        target_window_start = setup_window_start + self.duration_seconds
        slug = self._target_slug(target_window_start)
        early_tag = " early" if early else ""

        if not evaluation["ok"]:
            self.status_line = f"opp skip last={abs_delta:.2f}"
            self._print_event(
                f"➖ [opposite_side]{early_tag} no simulation last={abs_delta:.2f} move={total_move:.2f} "
                f"| streak={streak_len} {setup_direction or '?'} "
                f"| {evaluation['reason']}"
            )
            return None

        side = "Down" if setup_direction == "Up" else "Up"
        free_before = self.capital.free_capital
        locked_before = self.capital.locked_capital
        equity_before = self.capital.equity

        if self.regime is not None:
            state = self.regime.get_state(final_price)
            if state.bias == "bearish" and side == "Up" and self.skip_buy_up_when_bearish:
                self._log_skip(
                    setup_window_start,
                    target_window_start,
                    slug,
                    streak_len,
                    setup_direction,
                    abs_delta,
                    total_move,
                    f"regime_block bias={state.bias} side={side} move={state.move_amount:.2f}",
                    free_before,
                    locked_before,
                    equity_before,
                    side=side,
                    current_price=final_price,
                )
                return None
            if state.bias == "bullish" and side == "Down" and self.skip_buy_down_when_bullish:
                self._log_skip(
                    setup_window_start,
                    target_window_start,
                    slug,
                    streak_len,
                    setup_direction,
                    abs_delta,
                    total_move,
                    f"regime_block bias={state.bias} side={side} move={state.move_amount:.2f}",
                    free_before,
                    locked_before,
                    equity_before,
                    side=side,
                    current_price=final_price,
                )
                return None

        contracts = self.capital.calculate_contracts_single(self.limit_price)
        required_cost = self.capital.required_cost_single(contracts, self.limit_price)

        if contracts <= 0 or required_cost <= 0:
            self._log_skip(
                setup_window_start,
                target_window_start,
                slug,
                streak_len,
                setup_direction,
                abs_delta,
                total_move,
                "Insufficient free capital",
                free_before,
                locked_before,
                equity_before,
                side=side,
                current_price=final_price,
            )
            return None

        ok, reason = self.capital.try_lock(target_window_start, required_cost)
        if not ok:
            self._log_skip(
                setup_window_start,
                target_window_start,
                slug,
                streak_len,
                setup_direction,
                abs_delta,
                total_move,
                reason,
                free_before,
                locked_before,
                equity_before,
                side=side,
                current_price=final_price,
            )
            return None

        signal_ts = int(time.time())
        note = "pending;early_inference" if early else "pending"
        if reason == "unlocked":
            note = f"{note};capital_unlocked"

        trade = OppositeOpenTrade(
            signal_timestamp=signal_ts,
            target_window_start=target_window_start,
            slug=slug,
            setup_window_start=setup_window_start,
            setup_streak=streak_len,
            setup_direction=str(setup_direction),
            setup_abs_delta=round(abs_delta, 2),
            setup_total_move=round(total_move, 2),
            limit_cents=self.limit_cents,
            side=side,
            capital_before=round(equity_before, 2),
            free_capital_before=round(free_before, 2),
            locked_capital_before=round(locked_before, 2),
            invested_amount=round(required_cost, 2),
            contracts=contracts,
            notes=note,
        )
        self.open_trades[target_window_start] = trade

        self._append_trades_row(
            {
                "signal_timestamp": trade.signal_timestamp,
                "target_window_start": trade.target_window_start,
                "slug": trade.slug,
                "setup_window_start": trade.setup_window_start,
                "setup_streak": trade.setup_streak,
                "setup_direction": trade.setup_direction,
                "setup_abs_delta": trade.setup_abs_delta,
                "setup_total_move": trade.setup_total_move,
                "limit_cents": trade.limit_cents,
                "side": trade.side,
                "capital_before": trade.capital_before,
                "free_capital_before": trade.free_capital_before,
                "locked_capital_before": trade.locked_capital_before,
                "invested_amount": trade.invested_amount,
                "contracts": trade.contracts,
                "filled": False,
                "entry": "",
                "exit": "",
                "fill_type": "pending",
                "pnl": "",
                "capital_after": "",
                "free_capital_after": "",
                "mode": self.mode,
                "notes": trade.notes,
            },
            current_price=final_price,
        )

        self.status_line = f"opp open {side} last={abs_delta:.2f}"
        self._print_event(
            f"✅ [opposite_side]{early_tag} Simulation started | streak={streak_len} {setup_direction} "
            f"| buy {side} | last={abs_delta:.2f} move={total_move:.2f} "
            f"| → {slug} | {contracts}c @{self.limit_cents}¢ (${required_cost:.2f}) "
            f"| free=${self.capital.free_capital:.2f} locked=${self.capital.locked_capital:.2f}"
        )
        if self.mode in ("paper", "live"):
            self._print_event(
                f"⚠️  mode={self.mode}: order placement not implemented (simulation accounting only)."
            )
        return {
            "signal_timestamp": trade.signal_timestamp,
            "target_window_start": trade.target_window_start,
            "slug": trade.slug,
            "mode": self.mode,
        }

    def _update_fills(self, window_start: int, lowest_up: float, lowest_down: float) -> None:
        trade = self.open_trades.get(window_start)
        if trade is None or trade.filled:
            return
        lowest = lowest_up if trade.side == "Up" else lowest_down
        if lowest != float("inf") and 0.0 < lowest <= self.limit_price:
            trade.filled = True
            trade.entry = self.limit_price

    def _settle_open_trade(self, window_start: int, outcome: str) -> None:
        trade = self.open_trades.get(window_start)
        if trade is None:
            return

        if not trade.filled:
            fill_type = "no_fill"
            exit_val = ""
            pnl = 0.0
        elif outcome in ("Up", "Down"):
            fill_type = "filled"
            exit_val = 1.0 if outcome == trade.side else 0.0
            entry = trade.entry if trade.entry is not None else self.limit_price
            pnl = trade.contracts * (exit_val - entry)
        else:
            fill_type = "filled"
            exit_val = ""
            pnl = 0.0

        _locked_amount, free_after, equity_after = self.capital.release(window_start, pnl)

        note = "settled"
        if outcome not in ("Up", "Down"):
            note = "settled;outcome_unknown"
        if self.mode != "simulate":
            note = f"{note};mode={self.mode}_stub"

        self._update_pending_trade_row(
            window_start,
            {
                "signal_timestamp": trade.signal_timestamp,
                "target_window_start": trade.target_window_start,
                "slug": trade.slug,
                "setup_window_start": trade.setup_window_start,
                "setup_streak": trade.setup_streak,
                "setup_direction": trade.setup_direction,
                "setup_abs_delta": trade.setup_abs_delta,
                "setup_total_move": trade.setup_total_move,
                "limit_cents": trade.limit_cents,
                "side": trade.side,
                "capital_before": trade.capital_before,
                "free_capital_before": trade.free_capital_before,
                "locked_capital_before": trade.locked_capital_before,
                "invested_amount": trade.invested_amount,
                "contracts": trade.contracts,
                "filled": trade.filled,
                "entry": trade.entry if trade.filled else "",
                "exit": exit_val,
                "fill_type": fill_type,
                "pnl": round(pnl, 2),
                "capital_after": round(equity_after, 2),
                "free_capital_after": round(free_after, 2),
                "mode": self.mode,
                "notes": note,
            },
        )

        self.status_line = f"opp settled pnl={pnl:+.2f}"
        self._print_event(
            f"📒 [opposite_side] TRADE SETTLED {trade.slug} | side={trade.side} | fill={fill_type} "
            f"| outcome={outcome} | pnl={pnl:+.2f} | equity=${equity_after:.2f} | free=${free_after:.2f}"
        )
        del self.open_trades[window_start]

    def _try_early_inference(
        self,
        window_start: int,
        price_to_beat: float,
        current_price: float,
        remaining_seconds: float,
        up_ask: float,
        down_ask: float,
    ) -> dict[str, Any] | None:
        if window_start in self._decided_windows:
            return None
        if remaining_seconds > self.decision_remaining_seconds:
            return None

        thresh = self.early_inference_threshold
        up_hit = up_ask >= thresh
        down_hit = down_ask >= thresh
        if up_hit == down_hit:
            return None
        if current_price <= 0 or price_to_beat <= 0:
            return None

        inferred = "Up" if up_hit else "Down"
        return self._maybe_emit_setup(
            setup_window_start=window_start,
            price_to_beat=price_to_beat,
            final_price=current_price,
            outcome=inferred,
            early=True,
        )

    def on_window_update(
        self,
        window_start: int,
        price_to_beat: float,
        current_price: float,
        lowest_up: float,
        lowest_down: float,
        remaining_seconds: float,
        up_ask: float,
        down_ask: float,
        inferred_outcome: str | None,
        **_: Any,
    ) -> dict[str, Any] | None:
        if self._active_window_start != window_start:
            self._active_window_start = window_start
        self._update_fills(window_start, lowest_up, lowest_down)
        return self._try_early_inference(
            window_start,
            price_to_beat,
            current_price,
            remaining_seconds,
            up_ask,
            down_ask,
        )

    def on_window_close(
        self,
        window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
        lowest_up: float,
        lowest_down: float,
    ) -> dict[str, Any] | None:
        self._update_fills(window_start, lowest_up, lowest_down)
        self._settle_open_trade(window_start, outcome)

        signal = None
        if window_start not in self._decided_windows and outcome in ("Up", "Down"):
            signal = self._maybe_emit_setup(
                setup_window_start=window_start,
                price_to_beat=price_to_beat,
                final_price=final_price,
                outcome=outcome,
            )
        else:
            self._decided_windows.add(window_start)

        if outcome in ("Up", "Down") and final_price > 0 and price_to_beat > 0:
            self._append_history(
                WindowRecord(
                    window_start=window_start,
                    price_to_beat=price_to_beat,
                    final_price=final_price,
                    outcome=outcome,
                )
            )

        if len(self._decided_windows) > HISTORY_LIMIT * 2:
            cutoff = window_start - self.duration_seconds * HISTORY_LIMIT
            self._decided_windows = {w for w in self._decided_windows if w >= cutoff}

        return signal


@dataclass
class FlatDualOpenTrade:
    signal_timestamp: int
    window_start: int
    slug: str
    remaining_at_entry: str
    price_to_beat: float
    price_at_entry: float
    gap_at_entry: float
    up_ask_at_entry: float
    down_ask_at_entry: float
    entry_mode: str
    capital_before: float
    free_capital_before: float
    locked_capital_before: float
    invested_amount: float
    contracts: int
    binance_gap: float | str = ""
    up_filled: bool = False
    down_filled: bool = False
    entry_up: float | None = None
    entry_down: float | None = None
    up_resting: bool = False
    down_resting: bool = False
    up_cancelled: bool = False
    down_cancelled: bool = False
    one_side: str = ""
    one_side_fill_left: float | None = None
    one_side_max_bid: float = 0.0
    one_side_max_bid_left: float | None = None
    one_side_paired_left: float | None = None
    trend: dict[str, Any] | None = None
    notes: str = ""


class FlatDualSimulator:
    """Mid-window dual: if price is still near price-to-beat at the decision point, hold both sides.

    A side already asking below the limit is bought at its ask; any side not yet bought
    rests a limit at ``limit_cents`` and fills only on asks observed after entry.
    """

    def __init__(
        self,
        global_config: dict[str, Any],
        strategy_config: dict[str, Any],
        regime: RegimeEngine | None = None,
    ):
        self.duration_minutes = int(global_config["duration_minutes"])
        self.duration_seconds = self.duration_minutes * 60
        self.coin = str(global_config["coin"])
        self.mode = str(global_config["mode"])
        self.regime = regime

        cfg = strategy_config
        self.limit_cents = int(cfg["limit_cents"])
        self.limit_price = self.limit_cents / 100.0
        self.max_move = float(cfg["max_move"])
        self.max_binance_move = float(cfg.get("max_binance_move", self.max_move))
        self.decision_fraction = float(cfg["decision_fraction"])
        self.decision_remaining_seconds = self.duration_seconds * (1.0 - self.decision_fraction)
        self.decision_tolerance_seconds = float(cfg.get("decision_tolerance_seconds", 60))
        self.cancel_remaining_seconds = float(cfg.get("cancel_remaining_seconds", 0))
        self.cancel_move = float(cfg.get("cancel_move", 0))
        self.flip_margin = float(cfg.get("flip_margin", 0.02))
        self.early_check_remaining = [
            self.duration_seconds * (1.0 - float(f)) for f in (cfg.get("early_check_fractions") or [])
        ]
        early_columns = [
            f"early{i}_{name}"
            for i in range(1, len(self.early_check_remaining) + 1)
            for name in ("time_left", "gap", "binance_gap", "up_ask", "down_ask", "up_low", "down_low")
        ]
        split = FLAT_DUAL_TRADES_HEADER.index("gap_at_entry")
        self.trades_header = FLAT_DUAL_TRADES_HEADER[:split] + early_columns + FLAT_DUAL_TRADES_HEADER[split:]
        self.trades_log_file = str(
            cfg.get("trades_log_file") or flat_dual_trades_path(self.coin, self.duration_minutes)
        )

        self.capital = CapitalManager(
            total_capital=float(cfg["capital"]),
            investable_per_trade=float(cfg["investable_per_trade"]),
            capital_mode=str(cfg["capital_mode"]),
        )

        self.open_trades: dict[int, FlatDualOpenTrade] = {}
        self._decided_windows: set[int] = set()
        self._gaps: dict[int, dict[str, float]] = {}
        self._flips: dict[int, dict[str, Any]] = {}
        self._first_seen: dict[int, float] = {}
        self._early: dict[int, dict[str, Any]] = {}
        self._ask_lows: dict[int, dict[str, tuple[float, float]]] = {}
        self._ask_lows_after: dict[int, dict[str, tuple[float, float]]] = {}
        self._interval_lows: dict[int, dict[str, float]] = {}
        self._last_asks: dict[int, tuple[float, float]] = {}
        self._pending_skips: dict[int, dict[str, Any]] = {}
        self._binance_price = 0.0
        self._binance_price_to_beat = 0.0
        self.status_line = "fd: idle"

        self._ensure_trades_header()

    def _ensure_trades_header(self) -> None:
        path = self.trades_log_file
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            with open(path, newline="", encoding="utf-8") as f:
                existing = next(csv.reader(f), None)
            if existing == self.trades_header:
                return
            archived = f"{path}.{time.strftime('%Y%m%d-%H%M%S')}.bak"
            os.replace(path, archived)
            print(f"⚠️  Flat-dual trades log schema updated; old file moved to {archived}")
        with open(path, mode="a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(self.trades_header)

    def _append_row(self, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        with open(self.trades_log_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.trades_header, extrasaction="ignore")
            writer.writerow({k: row.get(k, "") for k in self.trades_header})

    def _print_event(self, message: str) -> None:
        sys.stdout.write("\n" + message + "\n")
        sys.stdout.flush()

    def _slug(self, window_start: int) -> str:
        return f"{_coin_slug_prefix(self.coin)}-updown-{self.duration_minutes}m-{window_start}"

    def display_status(self, window_start: int | None = None) -> str:
        if window_start is not None and window_start in self.open_trades:
            trade = self.open_trades[window_start]
            legs = []
            for side, filled, resting, cancelled in (
                ("Up", trade.up_filled, trade.up_resting, trade.up_cancelled),
                ("Down", trade.down_filled, trade.down_resting, trade.down_cancelled),
            ):
                if filled:
                    legs.append(f"{side}✓")
                elif resting:
                    legs.append(f"{side}@{self.limit_cents}¢")
                elif cancelled:
                    legs.append(f"{side}✗")
            return f"FD {trade.contracts}c [{' '.join(legs)}]"
        return self.status_line

    def _trend_fields(self, window_start: int, current_price: float) -> dict[str, Any]:
        if self.regime is None or self.regime.mode == "off" or current_price <= 0:
            return {}
        state = self.regime.get_state(current_price, now=window_start)
        return {
            "trend_open": round(state.reference_price, 2) if state.reference_price is not None else "",
            "trend_move": round(state.move_amount, 2),
            "trend_bias": state.bias,
        }

    def _log_skip(
        self,
        window_start: int,
        remaining: float,
        price_to_beat: float,
        current_price: float,
        up_ask: float,
        down_ask: float,
        reason: str,
    ) -> None:
        gap = current_price - price_to_beat if current_price > 0 and price_to_beat > 0 else ""
        self._pending_skips[window_start] = (
            {
                "signal_timestamp": int(time.time()),
                "window_start": window_start,
                "slug": self._slug(window_start),
                "remaining_at_entry": _format_mmss(remaining),
                "price_to_beat": round(price_to_beat, 2),
                "price_at_entry": round(current_price, 2) if current_price else "",
                "gap_at_entry": round(gap, 2) if gap != "" else "",
                "binance_gap_at_entry": self._binance_gap(price_to_beat),
                **self._trend_fields(window_start, current_price),
                "max_move": self.max_move,
                "limit_cents": self.limit_cents,
                "up_ask_at_entry": round(up_ask, 2),
                "down_ask_at_entry": round(down_ask, 2),
                "capital_before": round(self.capital.equity, 2),
                "free_capital_before": round(self.capital.free_capital, 2),
                "locked_capital_before": round(self.capital.locked_capital, 2),
                "invested_amount": 0,
                "contracts": 0,
                "fill_type": "skipped",
                "capital_after": round(self.capital.equity, 2),
                "free_capital_after": round(self.capital.free_capital, 2),
                "mode": self.mode,
                "notes": reason,
            }
        )
        self.status_line = f"fd skip {reason}"
        self._print_event(f"➖ [flat_dual] no entry | {reason} | {self._slug(window_start)}")

    def _try_enter(
        self,
        window_start: int,
        price_to_beat: float,
        current_price: float,
        remaining: float,
        up_ask: float,
        down_ask: float,
    ) -> None:
        if window_start in self._decided_windows or remaining > self.decision_remaining_seconds:
            return

        first_seen = self._first_seen.get(window_start, remaining)
        if first_seen < self.decision_remaining_seconds - self.decision_tolerance_seconds:
            self._decided_windows.add(window_start)
            self._log_skip(
                window_start, remaining, price_to_beat, current_price, up_ask, down_ask, "late_start"
            )
            return

        asks_ok = 0.0 < up_ask < 1.0 and 0.0 < down_ask < 1.0
        price_ok = current_price > 0 and price_to_beat > 0
        if not (asks_ok and price_ok):
            if remaining < self.decision_remaining_seconds - self.decision_tolerance_seconds:
                self._decided_windows.add(window_start)
                reason = "no_price" if not price_ok else "no_asks"
                self._log_skip(
                    window_start, remaining, price_to_beat, current_price, up_ask, down_ask, reason
                )
            return

        self._decided_windows.add(window_start)
        gap = round(current_price - price_to_beat, 2)
        binance_gap = self._binance_gap_value()
        binance_moved = binance_gap is not None and abs(binance_gap) > self.max_binance_move
        if abs(gap) > self.max_move or binance_moved:
            binance_bit = f" binance={binance_gap:+.2f}" if binance_gap is not None else " pm_only"
            self._log_skip(
                window_start,
                remaining,
                price_to_beat,
                current_price,
                up_ask,
                down_ask,
                f"moved gap={gap:+.2f}{binance_bit}",
            )
            return

        free_before = self.capital.free_capital
        locked_before = self.capital.locked_capital
        equity_before = self.capital.equity
        contracts = self.capital.calculate_contracts(self.limit_cents)
        required_cost = self.capital.required_cost(contracts, self.limit_cents)
        if contracts <= 0:
            self._log_skip(
                window_start, remaining, price_to_beat, current_price, up_ask, down_ask,
                "insufficient_capital",
            )
            return
        ok, lock_reason = self.capital.try_lock(window_start, required_cost)
        if not ok:
            self._log_skip(
                window_start, remaining, price_to_beat, current_price, up_ask, down_ask, lock_reason
            )
            return

        trade = FlatDualOpenTrade(
            signal_timestamp=int(time.time()),
            window_start=window_start,
            slug=self._slug(window_start),
            remaining_at_entry=_format_mmss(remaining),
            price_to_beat=price_to_beat,
            price_at_entry=current_price,
            gap_at_entry=round(gap, 2),
            up_ask_at_entry=up_ask,
            down_ask_at_entry=down_ask,
            entry_mode="",
            capital_before=round(equity_before, 2),
            free_capital_before=round(free_before, 2),
            locked_capital_before=round(locked_before, 2),
            invested_amount=round(required_cost, 2),
            contracts=contracts,
            binance_gap=self._binance_gap(price_to_beat),
            trend=self._trend_fields(window_start, current_price),
            notes="" if binance_gap is not None else "pm_only",
        )

        cheap = [(ask, side) for ask, side in ((up_ask, "Up"), (down_ask, "Down")) if ask < self.limit_price]
        if cheap:
            _, cheap_side = min(cheap)
            trade.entry_mode = f"cheap_{cheap_side.lower()}"
        else:
            trade.entry_mode = "both_limits"
        for side, ask in (("Up", up_ask), ("Down", down_ask)):
            if ask < self.limit_price:
                self._fill(trade, side, ask)
            else:
                self._rest(trade, side)
        self._track_one_side(trade, remaining, up_ask, down_ask)

        self.open_trades[window_start] = trade
        self.status_line = f"fd open {trade.entry_mode}"
        binance_bit = f"binance={binance_gap:+.2f}" if binance_gap is not None else "pm_only"
        self._print_event(
            f"✅ [flat_dual] entered {trade.entry_mode} | gap={gap:+.2f} {binance_bit} | up={up_ask*100:.0f}¢ "
            f"down={down_ask*100:.0f}¢ | {contracts}c limit {self.limit_cents}¢ | {trade.slug}"
        )

    def _fill(self, trade: FlatDualOpenTrade, side: str, price: float) -> None:
        if side == "Up":
            trade.up_filled, trade.up_resting, trade.entry_up = True, False, price
        else:
            trade.down_filled, trade.down_resting, trade.entry_down = True, False, price

    def _rest(self, trade: FlatDualOpenTrade, side: str) -> None:
        if side == "Up":
            trade.up_resting = True
        else:
            trade.down_resting = True

    def _update_fills(
        self,
        trade: FlatDualOpenTrade,
        remaining: float,
        up_ask: float,
        down_ask: float,
        price_to_beat: float,
        current_price: float,
    ) -> None:
        if self.cancel_remaining_seconds > 0 and remaining <= self.cancel_remaining_seconds:
            if trade.up_resting or trade.down_resting:
                trade.up_cancelled = trade.up_resting
                trade.down_cancelled = trade.down_resting
                trade.up_resting = trade.down_resting = False
                trade.notes = ";".join(n for n in (trade.notes, "resting_cancelled") if n)
            return
        # Checked before fills so a big move cancels the limit before its ask crosses it.
        if self._cancel_on_move(trade, remaining, price_to_beat, current_price):
            return
        if trade.up_resting and 0.0 < up_ask <= self.limit_price:
            self._fill(trade, "Up", self.limit_price)
        if trade.down_resting and 0.0 < down_ask <= self.limit_price:
            self._fill(trade, "Down", self.limit_price)
        self._track_one_side(trade, remaining, up_ask, down_ask)

    def _track_one_side(self, trade: FlatDualOpenTrade, remaining: float, up_ask: float, down_ask: float) -> None:
        """Log-only: while exactly one side is held, the best price it could have been sold for."""
        filled = [s for s, f in (("Up", trade.up_filled), ("Down", trade.down_filled)) if f]
        if len(filled) == 2:
            if trade.one_side and trade.one_side_paired_left is None:
                trade.one_side_paired_left = remaining
            return
        if len(filled) != 1:
            return
        side = filled[0]
        if not trade.one_side:
            trade.one_side, trade.one_side_fill_left = side, remaining
        # In a two-outcome market the bid for one side is about 1 minus the other side's ask.
        other_ask = down_ask if side == "Up" else up_ask
        if 0.0 < other_ask <= 1.0:
            bid = round(1.0 - other_ask, 2)
            if bid > trade.one_side_max_bid:
                trade.one_side_max_bid, trade.one_side_max_bid_left = bid, remaining

    @staticmethod
    def _one_side_fields(trade: FlatDualOpenTrade, gap_fields: dict[str, Any]) -> dict[str, Any]:
        if not trade.one_side:
            return {}
        other_end = gap_fields.get("down_ask_at_end" if trade.one_side == "Up" else "up_ask_at_end")
        return {
            "one_side_filled": trade.one_side,
            "one_side_fill_time_left": _format_mmss(trade.one_side_fill_left),
            "one_side_max_bid": trade.one_side_max_bid if trade.one_side_max_bid > 0 else "",
            "one_side_max_bid_time_left": (
                _format_mmss(trade.one_side_max_bid_left) if trade.one_side_max_bid_left is not None else ""
            ),
            "one_side_bid_at_end": round(1.0 - other_end, 2) if other_end not in ("", None) else "",
            "one_side_paired_time_left": (
                _format_mmss(trade.one_side_paired_left) if trade.one_side_paired_left is not None else ""
            ),
        }

    def _cancel_on_move(
        self, trade: FlatDualOpenTrade, remaining: float, price_to_beat: float, current_price: float
    ) -> bool:
        if self.cancel_move <= 0 or price_to_beat <= 0:
            return False
        if trade.up_filled or trade.down_filled or not (trade.up_resting and trade.down_resting):
            return False
        deltas = []
        if current_price > 0:
            deltas.append(("gap", round(current_price - price_to_beat, 2)))
        binance_gap = self._binance_gap_value()
        if binance_gap is not None:
            deltas.append(("binance", binance_gap))
        trigger = next(((name, d) for name, d in deltas if abs(d) > self.cancel_move), None)
        if trigger is None:
            return False
        sides = [s for s, r in (("up", trade.up_resting), ("down", trade.down_resting)) if r]
        trade.up_cancelled = trade.up_resting
        trade.down_cancelled = trade.down_resting
        trade.up_resting = trade.down_resting = False
        detail = " ".join(f"{name}={d:+.2f}" for name, d in deltas)
        note = f"cancelled_{'_'.join(sides)} {detail} at {_format_mmss(remaining)}"
        trade.notes = ";".join(n for n in (trade.notes, note) if n)
        self._print_event(
            f"🛑 [flat_dual] cancelled {'/'.join(s.title() for s in sides)} limit | {trigger[0]} moved | "
            f"{detail} > {self.cancel_move:.2f} | {_format_mmss(remaining)} left | {trade.slug}"
        )
        return True

    def _track_gap(self, window_start: int, price_to_beat: float, current_price: float) -> None:
        if window_start in self._decided_windows or current_price <= 0 or price_to_beat <= 0:
            return
        gap = current_price - price_to_beat
        g = self._gaps.setdefault(window_start, {"start": gap, "max": gap, "min": gap})
        g["max"] = max(g["max"], gap)
        g["min"] = min(g["min"], gap)

    def _track_flips(
        self, window_start: int, price_to_beat: float, current_price: float, remaining: float
    ) -> None:
        if current_price <= 0 or price_to_beat <= 0:
            return
        gap = current_price - price_to_beat
        if gap >= self.flip_margin:
            side = 1
        elif gap <= -self.flip_margin:
            side = -1
        else:
            # Inside the margin the gap keeps its last side, so wobbles around zero don't count.
            return
        f = self._flips.setdefault(window_start, {"side": side, "before": 0, "after": 0})
        if side == f["side"]:
            return
        f["side"] = side
        phase = "after" if window_start in self._decided_windows else "before"
        f[phase] += 1
        f[f"last_{phase}"] = remaining

    def _track_asks(self, window_start: int, up_ask: float, down_ask: float, remaining: float) -> None:
        store = self._ask_lows_after if window_start in self._decided_windows else self._ask_lows
        lows = store.setdefault(window_start, {})
        for side, ask in (("up", up_ask), ("down", down_ask)):
            if 0.0 < ask <= 1.0 and (side not in lows or ask < lows[side][0]):
                lows[side] = (ask, remaining)
        interval = self._interval_lows.setdefault(window_start, {})
        for side, ask in (("up", up_ask), ("down", down_ask)):
            if 0.0 < ask <= 1.0 and ask < interval.get(side, 2.0):
                interval[side] = ask

    def _record_early_checks(
        self,
        window_start: int,
        price_to_beat: float,
        current_price: float,
        remaining: float,
        up_ask: float,
        down_ask: float,
    ) -> None:
        fields = self._early.setdefault(window_start, {})
        for i, check_remaining in enumerate(self.early_check_remaining, start=1):
            key = f"early{i}_time_left"
            if key in fields or remaining > check_remaining:
                continue
            if remaining < check_remaining - self.decision_tolerance_seconds:
                fields[key] = ""
                continue
            has_price = current_price > 0 and price_to_beat > 0
            fields[key] = _format_mmss(remaining)
            fields[f"early{i}_gap"] = round(current_price - price_to_beat, 2) if has_price else ""
            fields[f"early{i}_binance_gap"] = self._binance_gap(price_to_beat)
            fields[f"early{i}_up_ask"] = round(up_ask, 2) if up_ask > 0 else ""
            fields[f"early{i}_down_ask"] = round(down_ask, 2) if down_ask > 0 else ""
            # Lowest asks since the previous snapshot (or the window start for the first one).
            interval = self._interval_lows.pop(window_start, {})
            fields[f"early{i}_up_low"] = round(interval["up"], 2) if "up" in interval else ""
            fields[f"early{i}_down_low"] = round(interval["down"], 2) if "down" in interval else ""

    def _binance_gap_value(self) -> float | None:
        # Binance SOL/USDT trades a few cents off the settlement SOL/USD price, so it is
        # compared with its own price at the window start, not Polymarket's price to beat.
        if self._binance_price > 0 and self._binance_price_to_beat > 0:
            return round(self._binance_price - self._binance_price_to_beat, 2)
        return None

    def _binance_gap(self, price_to_beat: float) -> float | str:
        gap = self._binance_gap_value()
        return gap if gap is not None else ""

    def _gap_fields(self, window_start: int, price_to_beat: float, final_price: float) -> dict[str, Any]:
        g = self._gaps.get(window_start)
        flips = self._flips.get(window_start, {})
        end_up, end_down = self._last_asks.get(window_start, (0.0, 0.0))
        lows = self._ask_lows.get(window_start, {})
        after = self._ask_lows_after.get(window_start, {})
        tail = self._interval_lows.get(window_start, {})
        final_gap = final_price - price_to_beat if final_price > 0 and price_to_beat > 0 else None
        return {
            **self._early.get(window_start, {}),
            "up_lowest_before_decision": round(lows["up"][0], 2) if "up" in lows else "",
            "up_lowest_time_left": _format_mmss(lows["up"][1]) if "up" in lows else "",
            "down_lowest_before_decision": round(lows["down"][0], 2) if "down" in lows else "",
            "down_lowest_time_left": _format_mmss(lows["down"][1]) if "down" in lows else "",
            "up_lowest_after_decision": round(after["up"][0], 2) if "up" in after else "",
            "up_lowest_after_time_left": _format_mmss(after["up"][1]) if "up" in after else "",
            "down_lowest_after_decision": round(after["down"][0], 2) if "down" in after else "",
            "down_lowest_after_time_left": _format_mmss(after["down"][1]) if "down" in after else "",
            "up_ask_at_end": round(end_up, 2) if end_up > 0 else "",
            "down_ask_at_end": round(end_down, 2) if end_down > 0 else "",
            "up_low_last_interval": round(tail["up"], 2) if "up" in tail else "",
            "down_low_last_interval": round(tail["down"], 2) if "down" in tail else "",
            "start_gap": round(g["start"], 2) if g else "",
            "max_gap_before_entry": round(g["max"], 2) if g else "",
            "min_gap_before_entry": round(g["min"], 2) if g else "",
            "flips_before_entry": flips["before"] if flips else "",
            "last_flip_before_time_left": (
                _format_mmss(flips["last_before"]) if "last_before" in flips else ""
            ),
            "flips_after_entry": flips["after"] if flips else "",
            "last_flip_after_time_left": (
                _format_mmss(flips["last_after"]) if "last_after" in flips else ""
            ),
            "final_gap": round(final_gap, 2) if final_gap is not None else "",
        }

    def on_window_update(
        self,
        window_start: int,
        price_to_beat: float,
        current_price: float,
        remaining_seconds: float,
        up_ask: float,
        down_ask: float,
        binance_price: float = 0.0,
        binance_price_to_beat: float = 0.0,
        **_: Any,
    ) -> None:
        self._binance_price = binance_price
        self._binance_price_to_beat = binance_price_to_beat
        self._first_seen.setdefault(window_start, remaining_seconds)
        self._track_gap(window_start, price_to_beat, current_price)
        self._track_flips(window_start, price_to_beat, current_price, remaining_seconds)
        self._track_asks(window_start, up_ask, down_ask, remaining_seconds)
        self._record_early_checks(
            window_start, price_to_beat, current_price, remaining_seconds, up_ask, down_ask
        )
        if up_ask > 0 and down_ask > 0:
            self._last_asks[window_start] = (up_ask, down_ask)
        trade = self.open_trades.get(window_start)
        if trade is not None:
            self._update_fills(trade, remaining_seconds, up_ask, down_ask, price_to_beat, current_price)
            return
        self._try_enter(window_start, price_to_beat, current_price, remaining_seconds, up_ask, down_ask)

    def on_window_close(
        self,
        window_start: int,
        outcome: str,
        price_to_beat: float = 0.0,
        final_price: float = 0.0,
        **_: Any,
    ) -> None:
        trade = self.open_trades.pop(window_start, None)
        skip_row = self._pending_skips.pop(window_start, None)
        gap_fields = self._gap_fields(window_start, price_to_beat, final_price)
        self._decided_windows = {w for w in self._decided_windows if w >= window_start}
        self._gaps = {w: g for w, g in self._gaps.items() if w > window_start}
        self._flips = {w: f for w, f in self._flips.items() if w > window_start}
        self._first_seen = {w: s for w, s in self._first_seen.items() if w > window_start}
        self._early = {w: v for w, v in self._early.items() if w > window_start}
        self._ask_lows = {w: v for w, v in self._ask_lows.items() if w > window_start}
        self._ask_lows_after = {w: v for w, v in self._ask_lows_after.items() if w > window_start}
        self._interval_lows = {w: v for w, v in self._interval_lows.items() if w > window_start}
        self._last_asks = {w: v for w, v in self._last_asks.items() if w > window_start}
        if skip_row is not None:
            skip_row.update(gap_fields)
            skip_row["outcome"] = outcome
            self._append_row(skip_row)
        if trade is None:
            return

        if trade.up_filled and trade.down_filled:
            fill_type = "both"
        elif trade.up_filled:
            fill_type = "only_up"
        elif trade.down_filled:
            fill_type = "only_down"
        else:
            fill_type = "none"

        pnl = 0.0
        notes = [n for n in (trade.notes,) if n]
        if outcome in ("Up", "Down"):
            if trade.up_filled:
                pnl += trade.contracts * ((1.0 if outcome == "Up" else 0.0) - trade.entry_up)
            if trade.down_filled:
                pnl += trade.contracts * ((1.0 if outcome == "Down" else 0.0) - trade.entry_down)
        else:
            notes.append("outcome_unknown")

        _, free_after, equity_after = self.capital.release(window_start, pnl)
        self._append_row(
            {
                **gap_fields,
                "signal_timestamp": trade.signal_timestamp,
                "window_start": trade.window_start,
                "slug": trade.slug,
                "remaining_at_entry": trade.remaining_at_entry,
                "price_to_beat": round(trade.price_to_beat, 2),
                "price_at_entry": round(trade.price_at_entry, 2),
                "gap_at_entry": trade.gap_at_entry,
                "binance_gap_at_entry": trade.binance_gap,
                **(trade.trend or {}),
                "max_move": self.max_move,
                "limit_cents": self.limit_cents,
                "up_ask_at_entry": round(trade.up_ask_at_entry, 2),
                "down_ask_at_entry": round(trade.down_ask_at_entry, 2),
                "entry_mode": trade.entry_mode,
                "capital_before": trade.capital_before,
                "free_capital_before": trade.free_capital_before,
                "locked_capital_before": trade.locked_capital_before,
                "invested_amount": trade.invested_amount,
                "contracts": trade.contracts,
                "up_filled": trade.up_filled,
                "down_filled": trade.down_filled,
                "fill_type": fill_type,
                "entry_up": trade.entry_up if trade.up_filled else "",
                "entry_down": trade.entry_down if trade.down_filled else "",
                **self._one_side_fields(trade, gap_fields),
                "outcome": outcome,
                "pnl": round(pnl, 2),
                "capital_after": round(equity_after, 2),
                "free_capital_after": round(free_after, 2),
                "mode": self.mode,
                "notes": ";".join(notes) or "settled",
            }
        )
        self.status_line = f"fd settled pnl={pnl:+.2f}"
        self._print_event(
            f"📒 [flat_dual] SETTLED {trade.slug} | {trade.entry_mode} fill={fill_type} "
            f"| outcome={outcome} | pnl={pnl:+.2f} | equity=${equity_after:.2f}"
        )


class StrategyRunner:
    """Fan-out window events to enabled independent strategies."""

    def __init__(
        self,
        strategies: list[Any],
        decision_remaining_seconds: float = 0.0,
        regime: RegimeEngine | None = None,
        history: list[WindowRecord] | None = None,
    ):
        self.strategies = strategies
        self.decision_remaining_seconds = float(decision_remaining_seconds)
        self.regime = regime
        self.history: list[WindowRecord] = history if history is not None else []
        if not self.history:
            for s in strategies:
                if hasattr(s, "history"):
                    self.history = s.history
                    break

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> StrategyRunner:
        strategies_cfg = config.get("strategies") or {}
        history = seed_history_from_csv(str(config["market_data_file"]))
        duration_seconds = int(config["duration_minutes"]) * 60
        regime = RegimeEngine.from_config(
            config.get("market_bias") or {},
            duration_seconds=duration_seconds,
        )
        regime.seed_from_history(history)
        enabled: list[Any] = []
        max_decision = 0.0

        dh_cfg = strategies_cfg.get("dual_hedge")
        if isinstance(dh_cfg, dict) and dh_cfg.get("enabled"):
            enabled.append(DualHedgeSimulator(config, dh_cfg, history=history, regime=regime))
            max_decision = max(max_decision, float(dh_cfg.get("decision_remaining_seconds", 0)))

        opp_cfg = strategies_cfg.get("opposite_side")
        if isinstance(opp_cfg, dict) and opp_cfg.get("enabled"):
            enabled.append(OppositeSideSimulator(config, opp_cfg, history=history, regime=regime))
            max_decision = max(max_decision, float(opp_cfg.get("decision_remaining_seconds", 0)))

        fd_cfg = strategies_cfg.get("flat_dual")
        if isinstance(fd_cfg, dict) and fd_cfg.get("enabled"):
            flat_dual = FlatDualSimulator(config, fd_cfg, regime=regime)
            enabled.append(flat_dual)
            max_decision = max(max_decision, flat_dual.decision_remaining_seconds)

        return cls(
            enabled,
            decision_remaining_seconds=max_decision,
            regime=regime,
            history=history,
        )

    def display_status(self, window_start: int | None = None) -> str:
        parts = [s.display_status(window_start) for s in self.strategies]
        return " | ".join(p for p in parts if p)

    def on_window_update(self, **kwargs: Any) -> None:
        for s in self.strategies:
            s.on_window_update(**kwargs)

    def on_window_close(self, **kwargs: Any) -> None:
        for s in self.strategies:
            s.on_window_close(**kwargs)
        final_price = kwargs.get("final_price", 0) or 0
        window_start = kwargs.get("window_start")
        if self.regime and final_price > 0 and window_start is not None:
            self.regime.update(float(final_price), int(window_start))

    def summarize(self) -> str:
        lines = []
        if self.regime is not None:
            lines.append(f"  {self.regime.summarize_line()}")
        for s in self.strategies:
            if isinstance(s, DualHedgeSimulator):
                lines.append(
                    f"  dual_hedge: capital=${s.capital.equity:.2f} limit={s.limit_cents}¢ "
                    f"trades={s.trades_log_file} history={len(s.history)}"
                )
            elif isinstance(s, OppositeSideSimulator):
                lines.append(
                    f"  opposite_side: capital=${s.capital.equity:.2f} limit={s.limit_cents}¢ "
                    f"trades={s.trades_log_file} history={len(s.history)}"
                )
            elif isinstance(s, FlatDualSimulator):
                lines.append(
                    f"  flat_dual: capital=${s.capital.equity:.2f} limit={s.limit_cents}¢ "
                    f"max_move=${s.max_move:.2f} binance_max=${s.max_binance_move:.2f} decide_at={s.decision_remaining_seconds:.0f}s left "
                    f"trades={s.trades_log_file}"
                )
            else:
                lines.append(f"  {type(s).__name__}")
        return "\n".join(lines) if lines else "  (no strategies enabled)"
