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
]

HISTORY_LIMIT = 20

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


def data_file_paths(coin: str, duration_minutes: int) -> tuple[str, str]:
    """Return (market_data_file, dual_hedge_trades_file) from coin + duration."""
    base = f"{coin}-{int(duration_minutes)}"
    return f"{base}-updown.csv", f"{base}-trades.csv"


def opposite_trades_path(coin: str, duration_minutes: int) -> str:
    return f"{coin}-{int(duration_minutes)}-opposite-trades.csv"


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
    return records[-HISTORY_LIMIT:]


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

        self.capital = CapitalManager(
            total_capital=float(cfg["capital"]),
            investable_per_trade=float(cfg["investable_per_trade"]),
            capital_mode=str(cfg["capital_mode"]),
        )

        self.history: list[WindowRecord] = list(history) if history is not None else []
        if history is None:
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
            archived = f"{path}.bak"
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

    def _append_trades_row(self, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        with open(self.trades_log_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADES_HEADER, extrasaction="ignore")
            writer.writerow({k: row.get(k, "") for k in TRADES_HEADER})

    def _update_pending_trade_row(self, target_window_start: int, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        rows = self._read_trades_rows()
        target_key = str(target_window_start)
        updated = False
        for i in range(len(rows) - 1, -1, -1):
            existing = rows[i]
            if str(existing.get("target_window_start", "")) != target_key:
                continue
            fill = str(existing.get("fill_type", "")).strip().lower()
            if fill in ("pending", ""):
                rows[i] = {k: row.get(k, "") for k in TRADES_HEADER}
                updated = True
                break
        if not updated:
            rows.append({k: row.get(k, "") for k in TRADES_HEADER})
        self._write_trades_rows(rows)

    def _target_slug(self, target_window_start: int) -> str:
        prefix = _coin_slug_prefix(self.coin)
        return f"{prefix}-updown-{self.duration_minutes}m-{target_window_start}"

    def _append_history(self, record: WindowRecord) -> None:
        if self.history and self.history[-1].window_start == record.window_start:
            self.history[-1] = record
        else:
            self.history.append(record)
        if len(self.history) > HISTORY_LIMIT:
            self.history = self.history[-HISTORY_LIMIT:]

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
    ) -> None:
        self._append_trades_row(
            {
                "signal_timestamp": int(time.time()),
                "target_window_start": target_window_start,
                "slug": slug,
                "setup_window_start": setup_window_start,
                "setup_streak": streak_len,
                "setup_abs_delta": round(abs_delta, 6),
                "setup_total_move": round(total_move, 6),
                "limit_cents": self.limit_cents,
                "capital_before": round(equity_before, 6),
                "free_capital_before": round(free_before, 6),
                "locked_capital_before": round(locked_before, 6),
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
                "capital_after": round(equity_before, 6),
                "free_capital_after": round(free_before, 6),
                "mode": self.mode,
                "notes": reason,
            }
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

        free_before = self.capital.free_capital
        locked_before = self.capital.locked_capital
        equity_before = self.capital.equity

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
            setup_abs_delta=round(abs_delta, 6),
            setup_total_move=round(total_move, 6),
            limit_cents=self.limit_cents,
            capital_before=round(equity_before, 6),
            free_capital_before=round(free_before, 6),
            locked_capital_before=round(locked_before, 6),
            invested_amount=round(required_cost, 6),
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
            }
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
                "pnl": round(pnl, 6),
                "capital_after": round(equity_after, 6),
                "free_capital_after": round(free_after, 6),
                "mode": self.mode,
                "notes": note,
            },
        )

        self.status_line = f"dh settled pnl={pnl:+.2f}"
        self._print_event(
            f"📒 [dual_hedge] TRADE SETTLED {trade.slug} | fill={fill_type} | outcome={outcome} "
            f"| pnl={pnl:+.4f} | equity=${equity_after:.2f} | free=${free_after:.2f}"
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

        self.capital = CapitalManager(
            total_capital=float(cfg["capital"]),
            investable_per_trade=float(cfg["investable_per_trade"]),
            capital_mode=str(cfg["capital_mode"]),
        )

        self.history: list[WindowRecord] = list(history) if history is not None else []
        if history is None:
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
            archived = f"{path}.bak"
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

    def _append_trades_row(self, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        with open(self.trades_log_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OPPOSITE_TRADES_HEADER, extrasaction="ignore")
            writer.writerow({k: row.get(k, "") for k in OPPOSITE_TRADES_HEADER})

    def _update_pending_trade_row(self, target_window_start: int, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        rows = self._read_trades_rows()
        target_key = str(target_window_start)
        updated = False
        for i in range(len(rows) - 1, -1, -1):
            existing = rows[i]
            if str(existing.get("target_window_start", "")) != target_key:
                continue
            fill = str(existing.get("fill_type", "")).strip().lower()
            if fill in ("pending", ""):
                rows[i] = {k: row.get(k, "") for k in OPPOSITE_TRADES_HEADER}
                updated = True
                break
        if not updated:
            rows.append({k: row.get(k, "") for k in OPPOSITE_TRADES_HEADER})
        self._write_trades_rows(rows)

    def _target_slug(self, target_window_start: int) -> str:
        prefix = _coin_slug_prefix(self.coin)
        return f"{prefix}-updown-{self.duration_minutes}m-{target_window_start}"

    def _append_history(self, record: WindowRecord) -> None:
        if self.history and self.history[-1].window_start == record.window_start:
            self.history[-1] = record
        else:
            self.history.append(record)
        if len(self.history) > HISTORY_LIMIT:
            self.history = self.history[-HISTORY_LIMIT:]

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
    ) -> None:
        self._append_trades_row(
            {
                "signal_timestamp": int(time.time()),
                "target_window_start": target_window_start,
                "slug": slug,
                "setup_window_start": setup_window_start,
                "setup_streak": streak_len,
                "setup_direction": setup_direction or "",
                "setup_abs_delta": round(abs_delta, 6),
                "setup_total_move": round(total_move, 6),
                "limit_cents": self.limit_cents,
                "side": side,
                "capital_before": round(equity_before, 6),
                "free_capital_before": round(free_before, 6),
                "locked_capital_before": round(locked_before, 6),
                "invested_amount": 0,
                "contracts": 0,
                "filled": False,
                "entry": "",
                "exit": "",
                "fill_type": "skipped",
                "pnl": "",
                "capital_after": round(equity_before, 6),
                "free_capital_after": round(free_before, 6),
                "mode": self.mode,
                "notes": reason,
            }
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
            setup_abs_delta=round(abs_delta, 6),
            setup_total_move=round(total_move, 6),
            limit_cents=self.limit_cents,
            side=side,
            capital_before=round(equity_before, 6),
            free_capital_before=round(free_before, 6),
            locked_capital_before=round(locked_before, 6),
            invested_amount=round(required_cost, 6),
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
            }
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
                "pnl": round(pnl, 6),
                "capital_after": round(equity_after, 6),
                "free_capital_after": round(free_after, 6),
                "mode": self.mode,
                "notes": note,
            },
        )

        self.status_line = f"opp settled pnl={pnl:+.2f}"
        self._print_event(
            f"📒 [opposite_side] TRADE SETTLED {trade.slug} | side={trade.side} | fill={fill_type} "
            f"| outcome={outcome} | pnl={pnl:+.4f} | equity=${equity_after:.2f} | free=${free_after:.2f}"
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


class StrategyRunner:
    """Fan-out window events to enabled independent strategies."""

    def __init__(self, strategies: list[Any], decision_remaining_seconds: float = 0.0):
        self.strategies = strategies
        self.decision_remaining_seconds = float(decision_remaining_seconds)
        self.history: list[WindowRecord] = []
        for s in strategies:
            if hasattr(s, "history"):
                self.history = s.history
                break

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> StrategyRunner:
        strategies_cfg = config.get("strategies") or {}
        history = seed_history_from_csv(str(config["market_data_file"]))
        enabled: list[Any] = []
        max_decision = 0.0

        dh_cfg = strategies_cfg.get("dual_hedge")
        if isinstance(dh_cfg, dict) and dh_cfg.get("enabled"):
            enabled.append(DualHedgeSimulator(config, dh_cfg, history=list(history)))
            max_decision = max(max_decision, float(dh_cfg.get("decision_remaining_seconds", 0)))

        opp_cfg = strategies_cfg.get("opposite_side")
        if isinstance(opp_cfg, dict) and opp_cfg.get("enabled"):
            enabled.append(OppositeSideSimulator(config, opp_cfg, history=list(history)))
            max_decision = max(max_decision, float(opp_cfg.get("decision_remaining_seconds", 0)))

        return cls(enabled, decision_remaining_seconds=max_decision)

    def display_status(self, window_start: int | None = None) -> str:
        parts = [s.display_status(window_start) for s in self.strategies]
        return " | ".join(p for p in parts if p)

    def on_window_update(self, **kwargs: Any) -> None:
        for s in self.strategies:
            s.on_window_update(**kwargs)

    def on_window_close(self, **kwargs: Any) -> None:
        for s in self.strategies:
            s.on_window_close(**kwargs)

    def summarize(self) -> str:
        lines = []
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
            else:
                lines.append(f"  {type(s).__name__}")
        return "\n".join(lines) if lines else "  (no strategies enabled)"
