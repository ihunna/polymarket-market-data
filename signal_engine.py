"""Dual-hedge signal engine + simulation for Polymarket SOL up/down windows."""

from __future__ import annotations

import csv
import os
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
        "min_streak",
        "max_streak",
        "max_last_delta",
        "min_total_move",
        "max_total_move",
        "limit_cents",
        "capital",
        "mode",
        "early_inference_threshold",
        "decision_remaining_seconds",
        "early_decision_remaining",
    ]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Config missing keys: {missing}")
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
    contracts: float
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
        self.min_total_move = float(config["min_total_move"])
        self.max_total_move = float(config["max_total_move"])
        self.limit_cents = int(config["limit_cents"])
        self.limit_price = self.limit_cents / 100.0
        self.capital = float(config["capital"])
        self.mode = str(config["mode"])
        self.early_inference_threshold = float(config["early_inference_threshold"])
        self.decision_remaining_seconds = float(config["decision_remaining_seconds"])
        self.early_decision_remaining = float(config["early_decision_remaining"])
        self.market_data_file = str(config["market_data_file"])
        self.trades_log_file = str(config["trades_log_file"])

        self.history: list[WindowRecord] = []
        self.open_trade: OpenTrade | None = None
        self._decided_windows: set[int] = set()
        self._active_window_start: int | None = None

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
                # Normalize header keys (legacy: "Price to beat", " Last price", etc.)
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
            return
        with open(path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(TRADES_HEADER)

    def _append_trades_row(self, row: dict[str, Any]) -> None:
        self._ensure_trades_header()
        with open(self.trades_log_file, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADES_HEADER, extrasaction="ignore")
            writer.writerow({k: row.get(k, "") for k in TRADES_HEADER})

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

    def _streak_metrics(
        self, history: list[WindowRecord]
    ) -> tuple[int, float, float, str] | None:
        """Return (streak_len, abs_delta, total_move, direction) for trailing streak, or None."""
        if not history:
            return None
        direction = history[-1].outcome
        if direction not in ("Up", "Down"):
            return None

        streak: list[WindowRecord] = []
        for rec in reversed(history):
            if rec.outcome != direction:
                break
            streak.append(rec)
        streak.reverse()
        streak_len = len(streak)
        if streak_len < self.min_streak or streak_len > self.max_streak:
            return None

        last = streak[-1]
        first = streak[0]
        abs_delta = abs(last.final_price - last.price_to_beat)
        total_move = abs(last.final_price - first.price_to_beat)
        if abs_delta > self.max_last_delta:
            return None
        if not (self.min_total_move <= total_move <= self.max_total_move):
            return None
        return streak_len, abs_delta, total_move, direction

    def _infer_outcome_from_asks(
        self, up_ask: float, down_ask: float, threshold: float | None = None
    ) -> str | None:
        thr = self.early_inference_threshold if threshold is None else threshold
        if up_ask >= thr and up_ask >= down_ask:
            return "Up"
        if down_ask >= thr and down_ask >= up_ask:
            return "Down"
        return None

    def _maybe_emit_setup(
        self,
        setup_window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
        provisional: bool,
    ) -> dict[str, Any] | None:
        if setup_window_start in self._decided_windows:
            return None
        if final_price <= 0 or outcome not in ("Up", "Down"):
            return None

        # Build evaluation history: prior completed + this setup candle
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
        metrics = self._streak_metrics(eval_history)
        self._decided_windows.add(setup_window_start)
        if metrics is None:
            return None

        streak_len, abs_delta, total_move, _direction = metrics
        target_window_start = setup_window_start + self.duration_seconds
        slug = self._target_slug(target_window_start)
        contracts = self.capital / (2.0 * self.limit_price) if self.limit_price > 0 else 0.0
        signal_ts = int(time.time())
        note = "pending"
        if provisional:
            note = "pending;early_inference"

        trade = OpenTrade(
            signal_timestamp=signal_ts,
            target_window_start=target_window_start,
            slug=slug,
            setup_window_start=setup_window_start,
            setup_streak=streak_len,
            setup_abs_delta=round(abs_delta, 6),
            setup_total_move=round(total_move, 6),
            limit_cents=self.limit_cents,
            capital_before=self.capital,
            contracts=round(contracts, 6),
            notes=note,
        )
        self.open_trade = trade

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
            "mode": self.mode,
            "provisional": provisional,
        }
        print(
            f"\n📡 SIGNAL → next window {slug} | streak={streak_len} "
            f"| Δ={abs_delta:.3f} | move={total_move:.3f} | limit={self.limit_cents}¢ | mode={self.mode}"
        )
        if self.mode in ("paper", "live"):
            print(f"⚠️  mode={self.mode}: order placement not implemented (simulation accounting only).")
        return signal

    def _update_fills(self, window_start: int, lowest_up: float, lowest_down: float) -> None:
        trade = self.open_trade
        if trade is None or trade.target_window_start != window_start:
            return
        if lowest_up != float("inf") and 0.0 < lowest_up <= self.limit_price:
            trade.up_filled = True
            trade.entry_up = self.limit_price
        if lowest_down != float("inf") and 0.0 < lowest_down <= self.limit_price:
            trade.down_filled = True
            trade.entry_down = self.limit_price

    def _settle_open_trade(self, window_start: int, outcome: str) -> None:
        trade = self.open_trade
        if trade is None or trade.target_window_start != window_start:
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
            # Unknown outcome: mark unsettled economics as zero, note it
            if up_filled:
                exit_up = ""
            if down_filled:
                exit_down = ""
            pnl = 0.0

        capital_after = self.capital + pnl
        note = "settled"
        if outcome not in ("Up", "Down"):
            note = "settled;outcome_unknown"
        if self.mode != "simulate":
            note = f"{note};mode={self.mode}_stub"

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
                "contracts": trade.contracts,
                "up_filled": up_filled,
                "down_filled": down_filled,
                "fill_type": fill_type,
                "entry_up": trade.entry_up if up_filled else "",
                "entry_down": trade.entry_down if down_filled else "",
                "exit_up": exit_up if exit_up is not None else "",
                "exit_down": exit_down if exit_down is not None else "",
                "pnl": round(pnl, 6),
                "capital_after": round(capital_after, 6),
                "mode": self.mode,
                "notes": note,
            }
        )

        self.capital = capital_after
        print(
            f"\n📒 TRADE SETTLED {trade.slug} | fill={fill_type} | outcome={outcome} "
            f"| pnl={pnl:+.4f} | capital={capital_after:.4f}"
        )
        self.open_trade = None

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
        """
        Called frequently from the main loop.
        Returns signal dict or None.
        """
        if self._active_window_start != window_start:
            self._active_window_start = window_start

        # Track simulated fills while we are inside the target window
        self._update_fills(window_start, lowest_up, lowest_down)

        if window_start in self._decided_windows:
            return None

        # Need a usable provisional final price for streak math
        if current_price is None or current_price <= 0:
            return None

        signal = None

        # 1) Early inference
        if remaining_seconds <= self.early_decision_remaining:
            early_outcome = inferred_outcome or self._infer_outcome_from_asks(up_ask, down_ask)
            if early_outcome and (
                max(up_ask, down_ask) >= self.early_inference_threshold
            ):
                signal = self._maybe_emit_setup(
                    setup_window_start=window_start,
                    price_to_beat=price_to_beat,
                    final_price=current_price,
                    outcome=early_outcome,
                    provisional=True,
                )
                return signal

        # 2) Hard deadline
        if remaining_seconds <= self.decision_remaining_seconds:
            forced = inferred_outcome or self._infer_outcome_from_asks(
                up_ask, down_ask, threshold=0.5
            )
            # Prefer dominant side when forcing near deadline
            if forced is None and up_ask > 0 and down_ask > 0:
                if up_ask > down_ask and up_ask > 0.5:
                    forced = "Up"
                elif down_ask > up_ask and down_ask > 0.5:
                    forced = "Down"
            if forced:
                signal = self._maybe_emit_setup(
                    setup_window_start=window_start,
                    price_to_beat=price_to_beat,
                    final_price=current_price,
                    outcome=forced,
                    provisional=True,
                )
        return signal

    def on_window_close(
        self,
        window_start: int,
        price_to_beat: float,
        final_price: float,
        outcome: str,
        lowest_up: float,
        lowest_down: float,
    ) -> dict[str, Any] | None:
        """Final accounting for the just-closed window. Returns signal if emitted at close."""
        # Update fills one last time for positions targeting this window
        self._update_fills(window_start, lowest_up, lowest_down)

        # Settle any open trade that targeted this window
        self._settle_open_trade(window_start, outcome)

        signal = None
        # Fallback: decide setup at close if not already decided
        if window_start not in self._decided_windows and outcome in ("Up", "Down"):
            signal = self._maybe_emit_setup(
                setup_window_start=window_start,
                price_to_beat=price_to_beat,
                final_price=final_price,
                outcome=outcome,
                provisional=False,
            )
        else:
            self._decided_windows.add(window_start)

        # Persist completed window into rolling history
        if outcome in ("Up", "Down") and final_price > 0 and price_to_beat > 0:
            self._append_history(
                WindowRecord(
                    window_start=window_start,
                    price_to_beat=price_to_beat,
                    final_price=final_price,
                    outcome=outcome,
                )
            )

        # Bound decided-window set
        if len(self._decided_windows) > HISTORY_LIMIT * 2:
            cutoff = window_start - self.duration_seconds * HISTORY_LIMIT
            self._decided_windows = {w for w in self._decided_windows if w >= cutoff}

        return signal
