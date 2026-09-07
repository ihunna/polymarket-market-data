"""Dual-hedge signal engine + simulation for Polymarket SOL up/down windows."""

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

HISTORY_LIMIT = 20


def data_file_paths(coin: str, duration_minutes: int) -> tuple[str, str]:
    """Return (market_data_file, trades_log_file) from coin + duration."""
    base = f"{coin}-{int(duration_minutes)}"
    return f"{base}-updown.csv", f"{base}-trades.csv"


def load_config(path: str = "config.yaml") -> dict[str, Any]:
    """Load strategy/market config from YAML. Raises if missing or invalid."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must be a mapping: {path}")
    required = [
        "coin",
        "duration_minutes",
        "total_capital",
        "investable_per_trade",
        "capital_mode",
        "position_sizing",
        "min_streak",
        "max_streak",
        "max_last_delta",
        "use_total_move",
        "min_total_move",
        "max_total_move",
        "limit_cents",
        "mode",
    ]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Config missing keys: {missing}")
    if data["capital_mode"] not in ("locked", "unlocked"):
        raise ValueError("capital_mode must be 'locked' or 'unlocked'")
    if data["position_sizing"] not in ("fixed",):
        raise ValueError("position_sizing must be 'fixed'")
    market_file, trades_file = data_file_paths(data["coin"], data["duration_minutes"])
    data["market_data_file"] = market_file
    data["trades_log_file"] = trades_file
    return data


def _parse_price(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _coin_slug_prefix(coin: str) -> str:
    """Polymarket short slug coin token (solana -> sol)."""
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
        self.open_positions: dict[int, float] = {}  # window_start → locked_amount

    @property
    def equity(self) -> float:
        return self.free_capital + self.locked_capital

    def calculate_contracts(self, limit_cents: int) -> int:
        cost_per_dual = (limit_cents / 100.0) * 2.0
        if cost_per_dual <= 0:
            return 0
        if self.capital_mode == "unlocked":
            max_investment = self.investable_per_trade
        else:
            max_investment = min(self.investable_per_trade, self.free_capital)
        contracts = int(max_investment // cost_per_dual)
        return max(contracts, 0)

    def required_cost(self, contracts: int, limit_cents: int) -> float:
        return contracts * (limit_cents / 100.0) * 2.0

    def try_lock(self, window_start: int, amount: float) -> tuple[bool, str]:
        """
        Attempt to allocate capital for a trade targeting window_start.
        Returns (ok, reason).
        """
        if amount <= 0:
            return False, "Insufficient free capital"
        if window_start in self.open_positions:
            return False, "Position already open for window"

        if self.capital_mode == "unlocked":
            # Theoretical mode: always take, no lock against free capital
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
        """
        Unlock funds for a resolved window and apply PnL.
        Returns (locked_amount, free_after, equity_after).
        """
        locked_amount = self.open_positions.pop(window_start, 0.0)

        if self.capital_mode == "locked" and locked_amount > 0:
            self.locked_capital -= locked_amount
            self.free_capital += locked_amount + pnl
        else:
            # Unlocked: equity tracked via free_capital only
            self.free_capital += pnl
            self.total_capital = self.equity

        # Keep total_capital as running equity for audit
        self.total_capital = self.equity
        return locked_amount, self.free_capital, self.equity


@dataclass
class WindowRecord:
    window_start: int
    price_to_beat: float
    final_price: float
    outcome: str  # "Up" | "Down"


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


class DualHedgeSimulator:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.duration_minutes = int(config["duration_minutes"])
        self.duration_seconds = self.duration_minutes * 60
        self.coin = str(config["coin"])
        self.min_streak = int(config["min_streak"])
        self.max_streak = int(config["max_streak"])
        self.max_last_delta = float(config["max_last_delta"])
        self.use_total_move = bool(config["use_total_move"])
        self.min_total_move = float(config["min_total_move"])
        self.max_total_move = float(config["max_total_move"])
        self.limit_cents = int(config["limit_cents"])
        self.limit_price = self.limit_cents / 100.0
        self.mode = str(config["mode"])
        self.market_data_file = str(config["market_data_file"])
        self.trades_log_file = str(config["trades_log_file"])
        self.position_sizing = str(config["position_sizing"])

        self.capital = CapitalManager(
            total_capital=float(config["total_capital"]),
            investable_per_trade=float(config["investable_per_trade"]),
            capital_mode=str(config["capital_mode"]),
        )

        self.history: list[WindowRecord] = []
        self.open_trades: dict[int, OpenTrade] = {}  # target_window_start → trade
        self._decided_windows: set[int] = set()
        self._active_window_start: int | None = None
        self.status_line = "sim: idle"

        self._seed_history_from_csv()
        self._ensure_trades_header()

    def _seed_history_from_csv(self) -> None:
        path = self.market_data_file
        if not os.path.isfile(path):
            return
        records: list[WindowRecord] = []
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    return
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
                    return

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
            return

        records.sort(key=lambda r: r.window_start)
        self.history = records[-HISTORY_LIMIT:]

    def _ensure_trades_header(self) -> None:
        path = self.trades_log_file
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            with open(path, newline="", encoding="utf-8") as f:
                existing = next(csv.reader(f), None)
            if existing == TRADES_HEADER:
                return
            # Schema changed — archive old file and start fresh header
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
        """Append a new trade row (signal open or capital skip)."""
        self._ensure_trades_header()
        with open(self.trades_log_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADES_HEADER, extrasaction="ignore")
            writer.writerow({k: row.get(k, "") for k in TRADES_HEADER})

    def _update_pending_trade_row(self, target_window_start: int, row: dict[str, Any]) -> None:
        """Replace the pending row for target_window_start in place; append if missing."""
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
        """Print on its own line so it doesn't collide with the \\r progress bar."""
        sys.stdout.write("\n" + message + "\n")
        sys.stdout.flush()

    def display_status(self, window_start: int | None = None) -> str:
        """Short status fragment for the live progress line."""
        if window_start is not None and window_start in self.open_trades:
            trade = self.open_trades[window_start]
            fills = []
            if trade.up_filled:
                fills.append("Up")
            if trade.down_filled:
                fills.append("Down")
            fill_txt = "+".join(fills) if fills else "waiting"
            return f"SIM open {trade.contracts}c @{trade.limit_cents}¢ [{fill_txt}]"
        if self.status_line:
            return self.status_line
        return "sim: idle"

    def _evaluate_setup(self, history: list[WindowRecord]) -> dict[str, Any]:
        """
        Evaluate trailing streak filters with reset-after-max_streak counting.

        A raw run of the same outcome is mapped onto a cycling counter:
        1..max_streak, then resets to 1. Example with max_streak=4:
        raw 1,2,3,4,5,6,7 → counted 1,2,3,4,1,2,3

        last_delta uses the latest candle; total_move uses only the current
        cycle segment (last `counted` candles), not the full raw run.
        """
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
        # Reset counter after max_streak (e.g. 4 → next candle counts as 1)
        counted = ((raw_len - 1) % self.max_streak) + 1
        segment = streak[-counted:]
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
        self.status_line = f"no simulation last={abs_delta:.2f} move={total_move:.2f}"
        self._print_event(
            f"➖ no simulation last={abs_delta:.2f} move={total_move:.2f} | {reason} | {slug}"
        )

    def _maybe_emit_setup(
        self,
        setup_window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
    ) -> dict[str, Any] | None:
        """Evaluate setup once at window close using final TWAP + outcome."""
        if setup_window_start in self._decided_windows:
            return None
        if final_price <= 0 or outcome not in ("Up", "Down"):
            return None

        eval_history = [
            r for r in self.history if r.window_start < setup_window_start
        ]
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

        if not evaluation["ok"]:
            self.status_line = f"no simulation last={abs_delta:.2f} move={total_move:.2f}"
            self._print_event(
                f"➖ no simulation last={abs_delta:.2f} move={total_move:.2f} "
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
        note = "pending"
        if reason == "unlocked":
            note = "pending;capital_unlocked"

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

        signal = {
            "signal_timestamp": trade.signal_timestamp,
            "target_window_start": trade.target_window_start,
            "slug": trade.slug,
            "setup_window_start": trade.setup_window_start,
            "setup_streak": trade.setup_streak,
            "setup_abs_delta": trade.setup_abs_delta,
            "setup_total_move": trade.setup_total_move,
            "limit_cents": trade.limit_cents,
            "contracts": trade.contracts,
            "invested_amount": trade.invested_amount,
            "mode": self.mode,
        }
        self.status_line = (
            f"Simulation started last={abs_delta:.2f} move={total_move:.2f}"
        )
        self._print_event(
            f"✅ Simulation started | streak={streak_len} {evaluation['direction']} "
            f"| last={abs_delta:.2f} move={total_move:.2f} "
            f"| → {slug} | {contracts}c @{self.limit_cents}¢ (${required_cost:.2f}) "
            f"| free=${self.capital.free_capital:.2f} locked=${self.capital.locked_capital:.2f}"
        )
        if self.mode in ("paper", "live"):
            self._print_event(
                f"⚠️  mode={self.mode}: order placement not implemented (simulation accounting only)."
            )
        return signal

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

        self.status_line = f"settled pnl={pnl:+.2f} equity=${equity_after:.2f}"
        self._print_event(
            f"📒 TRADE SETTLED {trade.slug} | fill={fill_type} | outcome={outcome} "
            f"| pnl={pnl:+.4f} | equity=${equity_after:.2f} | free=${free_after:.2f}"
        )
        del self.open_trades[window_start]

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
        """Track simulated fills only. Setup decisions happen at close."""
        if self._active_window_start != window_start:
            self._active_window_start = window_start
        self._update_fills(window_start, lowest_up, lowest_down)
        return None

    def on_window_close(
        self,
        window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
        lowest_up: float,
        lowest_down: float,
    ) -> dict[str, Any] | None:
        """Settle open trades, then evaluate setup with final TWAP + outcome."""
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
