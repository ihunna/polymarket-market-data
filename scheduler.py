import argparse
import os
import sys
import time
import csv
import json
import calendar
import socket
import threading
from collections import deque
from datetime import datetime
import zoneinfo
import websocket
from polymarket_poller import (
    fetch_polymarket_data,
    get_market_metadata_for_slug,
)
from signal_engine import StrategyRunner, load_config, apply_duration_paths

# Some cloud hosts stall on IPv6 connects; resolve IPv4 only for this process.
_system_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _system_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


socket.getaddrinfo = _ipv4_only_getaddrinfo

COIN_NAME = "solana"
SUPPORTED_DURATIONS = (5, 15, 60)
CONFIG_PATH = "config.yaml"

# Set in main() from config / CLI / env
DURATION_MINUTES = 15
WINDOW_DURATION_SECONDS = DURATION_MINUTES * 60
APP_CONFIG = {}
SIMULATOR = None

# Global shared state for order book token asks only
ws_state = {
    "up_raw": 0.0,
    "down_raw": 0.0,
    "connected": False
}

def get_current_active_slug(coin="sol"):
    """Computes the exact slug for short-duration or hourly markets matching Polymarket's URL scheme."""
    if DURATION_MINUTES == 60:
        et_zone = zoneinfo.ZoneInfo("America/New_York")
        now_et = datetime.now(et_zone)
        
        month_name = now_et.strftime("%B").lower()
        day = now_et.strftime("%d").lstrip("0")
        year = now_et.strftime("%Y")
        
        hour_12 = now_et.strftime("%I").lstrip("0")
        ampm = now_et.strftime("%p").lower()
        
        full_coin_name = "solana" if coin == "sol" else coin
        return f"{full_coin_name}-up-or-down-{month_name}-{day}-{year}-{hour_12}{ampm}-et"
    
    now_utc = calendar.timegm(time.gmtime())
    window_start = (now_utc // WINDOW_DURATION_SECONDS) * WINDOW_DURATION_SECONDS
    return f"{coin}-updown-{DURATION_MINUTES}m-{window_start}"

def format_time(seconds):
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins:02d}:{secs:02d}"

def format_dollar(price):
    return f"${price:.2f}"

def format_ticker(label, price, price_to_beat):
    if price <= 0:
        return f"{label}: --"
    return f"{label}: {format_dollar(price)} ({price - price_to_beat:+.2f})"

def format_token_cents(raw):
    if raw <= 0 or raw > 1.0:
        return "0¢"
    return f"{round(raw * 100)}¢"

def format_lowest_cents(lowest_seen):
    if lowest_seen != float('inf') and lowest_seen > 0:
        return f"{round(lowest_seen * 100)}¢"
    return "N/A"

MARKET_CSV_HEADER = [
    "Time stamp",
    "price_to_beat",
    "final_price",
    "lowest_up",
    "lowest_down",
    "outcome",
]

def update_window_progress(window_start, window_end, formatted_message):
    """Updates a single persistent line showing window progress and token ask data."""
    now = time.time()
    elapsed = now - window_start
    remaining = window_end - now
    if remaining < 0:
        remaining = 0

    duration = max(1, window_end - window_start)
    progress = max(0.0, min(1.0, elapsed / duration))
    filled_blocks = int(progress * 20)
    bar = "█" * filled_blocks + "-" * (20 - filled_blocks)
    
    start_dt = datetime.fromtimestamp(window_start)
    window_label = f"{DURATION_MINUTES}m {start_dt.strftime('%H:%M')} window"
    time_str = format_time(remaining)
    
    # Pad to clear leftover chars from longer previous lines
    line = f"{window_label} [{bar}] {time_str} remaining | {formatted_message}"
    sys.stdout.write("\r" + line + " " * 12)
    sys.stdout.flush()

def log_to_csv(timestamp, price_to_beat, final_price, lowest_up, lowest_down, outcome):
    """Appends window results; strategy config never affects these columns."""
    filename = APP_CONFIG.get("market_data_file") or f"{COIN_NAME}-{DURATION_MINUTES}-updown.csv"
    file_exists = os.path.isfile(filename)
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(MARKET_CSV_HEADER)
        writer.writerow([
            timestamp,
            format_dollar(price_to_beat),
            format_dollar(final_price),
            lowest_up,
            lowest_down,
            outcome,
        ])

# --- Single Global Permanent WebSocket Manager for Order Book Asks Only ---
ORDER_BOOK_STALE_SECONDS = 10
ORDER_BOOK_CONNECT_TIMEOUT_SECONDS = 60


class PersistentPolymarketWS:
    def __init__(self):
        self.ws = None
        self.active_tokens = []
        self.is_running = True
        self.lock = threading.Lock()
        self.last_update = time.time()
        self.connected = False
        self.ever_connected = False
        self.disconnected_at = time.time()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()

    def update_tokens(self, token_ids):
        """Switch markets on a fresh connection so no old subscriptions linger."""
        with self.lock:
            had_tokens = bool(self.active_tokens)
            self.active_tokens = token_ids
            if not had_tokens:
                # Nothing to replace: subscribe on the live connection, or let _on_open do it.
                if self.connected and self.ws is not None:
                    self.last_update = time.time()
                    self._send_subscription(self.ws, token_ids)
                return
        self._reconnect()

    def _mark_disconnected(self):
        with self.lock:
            if self.connected or self.disconnected_at is None:
                self.disconnected_at = time.time()
            self.connected = False

    def _reconnect(self):
        self._mark_disconnected()
        ws = self.ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _watchdog_loop(self):
        while self.is_running:
            time.sleep(1)
            now = time.time()
            with self.lock:
                has_tokens = bool(self.active_tokens)
                connected = self.connected
                quiet_for = now - self.last_update
                down_for = now - self.disconnected_at if self.disconnected_at else 0.0
            if not has_tokens:
                continue
            if connected and quiet_for > ORDER_BOOK_STALE_SECONDS:
                sys.stdout.write(f"\n⚠️ Order book quiet for {quiet_for:.0f}s while connected, reconnecting...\n")
                sys.stdout.flush()
                self._reconnect()
            elif not connected and down_for > ORDER_BOOK_CONNECT_TIMEOUT_SECONDS:
                sys.stdout.write(f"\n⚠️ Order book not connected after {down_for:.0f}s, retrying...\n")
                sys.stdout.flush()
                with self.lock:
                    self.disconnected_at = now
                ws = self.ws
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

    def _send_subscription(self, ws, token_ids):
        try:
            payload = {
                "assets_ids": token_ids,
                "type": "market"
            }
            ws.send(json.dumps(payload))
        except Exception:
            pass

    def _on_message(self, ws, message):
        try:
            if message == "PONG":
                return
            data = json.loads(message)
            items = data if isinstance(data, list) else [data]
            
            with self.lock:
                up_t = self.active_tokens[0] if len(self.active_tokens) > 0 else None
                down_t = self.active_tokens[1] if len(self.active_tokens) > 1 else None

            for item in items:
                if not isinstance(item, dict):
                    continue
                asset_ids = {item.get("asset_id")} | {
                    c.get("asset_id") for c in item.get("price_changes", []) if isinstance(c, dict)
                }
                if asset_ids & {up_t, down_t} - {None}:
                    with self.lock:
                        self.last_update = time.time()
                if item.get("event_type") == "price_change":
                    for change in item.get("price_changes", []):
                        asset_id = change.get("asset_id")
                        best_ask_str = change.get("best_ask")
                        if asset_id and best_ask_str:
                            val = float(best_ask_str)
                            if asset_id == up_t:
                                ws_state["up_raw"] = val
                            elif asset_id == down_t:
                                ws_state["down_raw"] = val
        except Exception:
            pass

    def _on_open(self, ws):
        ws_state["connected"] = True
        now = time.time()
        with self.lock:
            took = now - self.disconnected_at if self.disconnected_at else 0.0
            verb = "reconnected" if self.ever_connected else "connected"
            self.connected = True
            self.ever_connected = True
            self.disconnected_at = None
            self.last_update = now
            if self.active_tokens:
                self._send_subscription(ws, self.active_tokens)
        sys.stdout.write(f"\n🔌 Order book {verb} in {took:.1f}s\n")
        sys.stdout.flush()

    def _on_close(self, ws, code, msg):
        ws_state["connected"] = False
        self._mark_disconnected()

    def _on_error(self, ws, error):
        ws_state["connected"] = False
        self._mark_disconnected()

    def _run_loop(self):
        ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        while self.is_running:
            try:
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_close=self._on_close,
                    on_error=self._on_error
                )
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                pass
            if self.is_running:
                time.sleep(2)


PRICE_WS_URL = "wss://ws-live-data.polymarket.com/"
PRICE_WS_ORIGIN = "https://polymarket.com"
PRICE_WS_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
)
PRICE_STALE_SECONDS = 15
PRICE_HISTORY_SECONDS = 3900
PRICE_BOUNDARY_TOLERANCE_SECONDS = 10
PRICE_END_WAIT_SECONDS = 15


PRICE_TOPIC = "crypto_prices_twap_sixty"
BINANCE_TOPIC = "crypto_prices"


class PersistentPriceWS:
    """Streams the 60s-TWAP SOL price Polymarket displays, plus Binance SOL, from its real-time data socket."""

    def __init__(self, price_symbol="sol/usd", binance_symbol="solusdt"):
        self.price_symbol = price_symbol
        self.binance_symbol = binance_symbol
        self.ws = None
        self.connected = False
        self.is_running = True
        self.lock = threading.Lock()
        self.prices = deque()
        self.binance_latest = (0, 0.0)
        threading.Thread(target=self._run_loop, daemon=True).start()
        threading.Thread(target=self._ping_loop, daemon=True).start()

    def _subscribe(self, ws):
        # The server matches live updates against the exact filter text; it must be
        # compact JSON (no spaces), as browsers send it, or only the snapshot arrives.
        compact = {"separators": (",", ":")}
        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": PRICE_TOPIC,
                    "type": "update",
                    "filters": json.dumps({"symbol": self.price_symbol}, **compact),
                },
                {
                    "topic": BINANCE_TOPIC,
                    "type": "update",
                    "filters": json.dumps({"symbol": self.binance_symbol}, **compact),
                },
            ],
        }, **compact))

    def _on_open(self, ws):
        self.connected = True
        try:
            self._subscribe(ws)
        except Exception:
            pass

    def _on_message(self, ws, message):
        if not message or message == "PONG":
            return
        try:
            data = json.loads(message)
        except ValueError:
            return
        for item in data if isinstance(data, list) else [data]:
            if not isinstance(item, dict):
                continue
            topic = item.get("topic")
            payload = item.get("payload") or {}
            if not isinstance(payload, dict):
                continue
            symbol = str(payload.get("symbol") or "").lower()
            points = payload.get("data") if isinstance(payload.get("data"), list) else [payload]
            if topic == PRICE_TOPIC and not symbol:
                symbol = self.price_symbol
            elif topic == BINANCE_TOPIC and not symbol:
                symbol = self.binance_symbol
            for point in points:
                try:
                    ts = int(point.get("timestamp") or 0)
                    value = float(point.get("value") or 0)
                except (TypeError, ValueError, AttributeError):
                    continue
                if ts <= 0 or value <= 0:
                    continue
                if topic == PRICE_TOPIC and symbol == self.price_symbol:
                    self._add_price(ts, value)
                elif topic == BINANCE_TOPIC and symbol == self.binance_symbol:
                    with self.lock:
                        if ts >= self.binance_latest[0]:
                            self.binance_latest = (ts, value)

    def _add_price(self, ts, value):
        with self.lock:
            if self.prices and ts <= self.prices[-1][0]:
                return
            self.prices.append((ts, value))
            cutoff = ts - PRICE_HISTORY_SECONDS * 1000
            while self.prices and self.prices[0][0] < cutoff:
                self.prices.popleft()

    def latest_price(self):
        with self.lock:
            if not self.prices:
                return 0.0
            ts, value = self.prices[-1]
        return value if time.time() * 1000 - ts <= PRICE_STALE_SECONDS * 1000 else 0.0

    def latest_binance(self):
        with self.lock:
            ts, value = self.binance_latest
        return value if value > 0 and time.time() * 1000 - ts <= PRICE_STALE_SECONDS * 1000 else 0.0

    def price_at(self, epoch_seconds):
        """Last streamed price at or before ``epoch_seconds``; 0.0 if none close enough."""
        target = epoch_seconds * 1000
        with self.lock:
            for ts, value in reversed(self.prices):
                if ts <= target:
                    return value if target - ts <= PRICE_BOUNDARY_TOLERANCE_SECONDS * 1000 else 0.0
        return 0.0

    def wait_for_price_at(self, epoch_seconds, timeout=PRICE_END_WAIT_SECONDS):
        """Wait until a tick at or after ``epoch_seconds`` has arrived, then return ``price_at``."""
        deadline = time.time() + timeout
        target = epoch_seconds * 1000
        while time.time() < deadline:
            with self.lock:
                arrived = bool(self.prices) and self.prices[-1][0] >= target
            if arrived:
                break
            time.sleep(0.25)
        return self.price_at(epoch_seconds)

    def _on_close(self, ws, code, msg):
        self.connected = False

    def _on_error(self, ws, error):
        self.connected = False

    def _ping_loop(self):
        while self.is_running:
            time.sleep(5)
            if self.connected and self.ws is not None:
                try:
                    self.ws.send("PING")
                except Exception:
                    pass

    def _run_loop(self):
        while self.is_running:
            try:
                self.ws = websocket.WebSocketApp(
                    PRICE_WS_URL,
                    header=[f"User-Agent: {PRICE_WS_USER_AGENT}"],
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_close=self._on_close,
                    on_error=self._on_error,
                )
                self.ws.run_forever(origin=PRICE_WS_ORIGIN)
            except Exception:
                pass
            self.connected = False
            if self.is_running:
                time.sleep(2)


def run_high_frequency_loop(ws_manager, price_ws, price_to_beat, simulator=None):
    """Executes a window loop tracking token asks and syncing window boundary to ET/UTC epoch."""
    now_utc = calendar.timegm(time.gmtime())
    
    if DURATION_MINUTES == 60:
        et_zone = zoneinfo.ZoneInfo("America/New_York")
        now_et = datetime.now(et_zone)
        start_of_hour_et = now_et.replace(minute=0, second=0, microsecond=0)
        window_start = int(start_of_hour_et.timestamp())
        window_end = window_start + 3600
    else:
        window_start = (now_utc // WINDOW_DURATION_SECONDS) * WINDOW_DURATION_SECONDS
        window_end = window_start + WINDOW_DURATION_SECONDS
    
    active_slug = get_current_active_slug(coin="sol")
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 {DURATION_MINUTES}m market window started! (Slug: {active_slug})")
    
    up_token, down_token, question = get_market_metadata_for_slug(active_slug)
    if not up_token or not down_token:
        print("⚠️ Failed to resolve market tokens. Retrying in 2 seconds...")
        time.sleep(2)
        return price_to_beat

    # Point global socket to this window's tokens
    ws_manager.update_tokens([up_token, down_token])

    # Seed initial REST fallback values for token asks
    initial_data = fetch_polymarket_data(active_slug)
    if initial_data.get("status") == "success":
        ws_state["up_raw"] = initial_data.get("up_raw", 0.0)
        ws_state["down_raw"] = initial_data.get("down_raw", 0.0)
    
    lowest_up_seen = float('inf')
    lowest_down_seen = float('inf')
    initialized = False

    print(f"📊 Price to Beat (Baseline): {format_dollar(price_to_beat)}")

    while True:
        current_time = time.time()
        remaining = window_end - current_time
        if remaining < 0:
            remaining = 0
        
        if current_time >= window_end:
            print("\n⏳ Window ended. Reading end price from the price socket...")
            price_to_beat = price_ws.price_at(window_start) or price_to_beat
            final_price = price_ws.wait_for_price_at(window_end)
            if final_price == 0.0:
                print("⚠️ No socket price at window end, falling back to previous price-to-beat.")
                final_price = price_to_beat
            
            up_final = ws_state["up_raw"]
            down_final = ws_state["down_raw"]
            
            outcome = "UNKNOWN"
            if up_final >= 0.95 or (up_final > down_final and up_final > 0.5):
                outcome = "Up"
            elif down_final >= 0.95 or (down_final > up_final and down_final > 0.5):
                outcome = "Down"
                
            log_to_csv(
                window_start,
                price_to_beat,
                final_price,
                format_lowest_cents(lowest_up_seen),
                format_lowest_cents(lowest_down_seen),
                outcome,
            )

            if simulator is not None:
                simulator.on_window_close(
                    window_start=window_start,
                    price_to_beat=price_to_beat,
                    final_price=final_price,
                    outcome=outcome,
                    lowest_up=lowest_up_seen,
                    lowest_down=lowest_down_seen,
                )

            print(
                f"🏁 {DURATION_MINUTES}m window completed! Outcome: {outcome} | "
                f"PTB: {format_dollar(price_to_beat)} | Final Price: {format_dollar(final_price)} | Logged"
            )
            
            return final_price
        
        up_cost = ws_state["up_raw"]
        down_cost = ws_state["down_raw"]

        if not initialized and up_cost > 0 and down_cost > 0:
            lowest_up_seen = up_cost
            lowest_down_seen = down_cost
            initialized = True

        if initialized:
            if 0.0 < up_cost <= 1.0 and up_cost < lowest_up_seen:
                lowest_up_seen = up_cost
            if 0.0 < down_cost <= 1.0 and down_cost < lowest_down_seen:
                lowest_down_seen = down_cost

        current_price = price_ws.latest_price()
        binance_price = price_ws.latest_binance()
        gap_ptb = price_ws.price_at(window_start) or price_to_beat

        if simulator is not None:
            simulator.on_window_update(
                window_start=window_start,
                price_to_beat=gap_ptb,
                current_price=current_price,
                lowest_up=lowest_up_seen,
                lowest_down=lowest_down_seen,
                remaining_seconds=remaining,
                up_ask=up_cost,
                down_ask=down_cost,
                inferred_outcome=None,
                binance_price=binance_price,
            )

        up_cents = format_token_cents(up_cost)
        down_cents = format_token_cents(down_cost)

        sim_bit = ""
        if simulator is not None:
            sim_bit = f" | {simulator.display_status(window_start)}"
        display_str = (
            f"PTB: {format_dollar(gap_ptb)} | {format_ticker('PM', current_price, gap_ptb)} | "
            f"{format_ticker('Binance', binance_price, gap_ptb)} | Up: {up_cents} | Down: {down_cents}{sim_bit}"
        )
        update_window_progress(window_start, window_end, formatted_message=display_str)
        
        time.sleep(0.5)

def start_aligned_runner(simulator=None):
    """Initializes services and prompts for initial manual PTB."""
    print(f"🔌 Initializing services... (window={DURATION_MINUTES}m)")
    global_ws_manager = PersistentPolymarketWS()
    price_ws = PersistentPriceWS()
    
    while True:
        try:
            val = input("\nEnter initial Price to Beat (e.g. 105.28): ").strip()
            current_ptb = float(val)
            break
        except ValueError:
            print("❌ Invalid number format. Please enter a valid decimal price (e.g. 105.28).")

    while True:
        try:
            current_ptb = run_high_frequency_loop(
                global_ws_manager,
                price_to_beat=current_ptb,
                simulator=simulator,
                price_ws=price_ws,
            )
        except Exception as e:
            print(f"\nError in loop execution: {e}. Restarting cycle...")
            time.sleep(2)

def parse_args():
    parser = argparse.ArgumentParser(description="Polymarket SOL up/down window tracker")
    parser.add_argument(
        "-d", "--duration",
        type=int,
        choices=SUPPORTED_DURATIONS,
        default=None,
        help="Window duration in minutes (5, 15, or 60). Default: from config.yaml / 15.",
    )
    parser.add_argument(
        "-c", "--config",
        type=str,
        default=CONFIG_PATH,
        help="Path to config.yaml (default: config.yaml).",
    )
    parser.add_argument(
        "--dh-last",
        type=float,
        default=None,
        metavar="EQUITY",
        help="Restore dual_hedge starting equity (overrides strategies.dual_hedge.capital).",
    )
    parser.add_argument(
        "--opp-last",
        type=float,
        default=None,
        metavar="EQUITY",
        help="Restore opposite_side starting equity (overrides strategies.opposite_side.capital).",
    )
    parser.add_argument(
        "--ref-price",
        type=float,
        default=None,
        metavar="FLOAT",
        help="Set market_bias.mode=ref and reference_price.",
    )
    parser.add_argument(
        "--bias-mode",
        type=str,
        default=None,
        choices=("off", "ref", "rolling", "ema"),
        help="Override market_bias.mode (off|ref|rolling|ema).",
    )
    return parser.parse_args()


def apply_capital_overrides(config: dict, dh_last: float | None, opp_last: float | None) -> list[str]:
    """Apply CLI capital restores onto strategy configs. Returns human-readable notes."""
    notes: list[str] = []
    strategies = config.setdefault("strategies", {})
    if dh_last is not None:
        if dh_last <= 0:
            raise SystemExit("--dh-last must be > 0")
        dh = strategies.get("dual_hedge")
        if not isinstance(dh, dict):
            raise SystemExit("--dh-last provided but strategies.dual_hedge is missing")
        prev = dh.get("capital")
        dh["capital"] = float(dh_last)
        notes.append(f"dual_hedge capital restored ${float(dh_last):.2f} (config was ${float(prev):.2f})")
    if opp_last is not None:
        if opp_last <= 0:
            raise SystemExit("--opp-last must be > 0")
        opp = strategies.get("opposite_side")
        if not isinstance(opp, dict):
            raise SystemExit("--opp-last provided but strategies.opposite_side is missing")
        prev = opp.get("capital")
        opp["capital"] = float(opp_last)
        notes.append(f"opposite_side capital restored ${float(opp_last):.2f} (config was ${float(prev):.2f})")
    return notes


def apply_bias_overrides(config: dict, ref_price: float | None, bias_mode: str | None) -> list[str]:
    """Apply CLI market_bias overrides after load_config. Returns human-readable notes."""
    notes: list[str] = []
    mb = config.get("market_bias")
    if not isinstance(mb, dict):
        mb = {"mode": "off"}
        config["market_bias"] = mb
    if bias_mode is not None:
        mb["mode"] = bias_mode
        notes.append(f"market_bias.mode overridden to {bias_mode}")
    if ref_price is not None:
        mb["mode"] = "ref"
        mb["reference_price"] = float(ref_price)
        notes.append(f"market_bias.mode=ref reference_price={float(ref_price)}")
    return notes

def resolve_duration(cli_duration, config_duration):
    if cli_duration is not None:
        return cli_duration
    env_val = os.environ.get("WINDOW_DURATION_MINUTES", "").strip()
    if env_val:
        try:
            minutes = int(env_val)
        except ValueError:
            raise SystemExit(f"Invalid WINDOW_DURATION_MINUTES={env_val!r}; expected 5, 15, or 60.")
        if minutes not in SUPPORTED_DURATIONS:
            raise SystemExit(f"Unsupported WINDOW_DURATION_MINUTES={minutes}; choose from {SUPPORTED_DURATIONS}.")
        return minutes
    if config_duration is not None:
        minutes = int(config_duration)
        if minutes not in SUPPORTED_DURATIONS:
            raise SystemExit(f"Unsupported duration_minutes={minutes} in config; choose from {SUPPORTED_DURATIONS}.")
        return minutes
    return 15

if __name__ == "__main__":
    args = parse_args()
    APP_CONFIG = load_config(args.config)
    COIN_NAME = str(APP_CONFIG.get("coin", COIN_NAME))
    DURATION_MINUTES = resolve_duration(args.duration, APP_CONFIG.get("duration_minutes"))
    WINDOW_DURATION_SECONDS = DURATION_MINUTES * 60
    APP_CONFIG = apply_duration_paths(APP_CONFIG, COIN_NAME, DURATION_MINUTES)
    restore_notes = apply_capital_overrides(APP_CONFIG, args.dh_last, args.opp_last)
    bias_notes = apply_bias_overrides(APP_CONFIG, args.ref_price, args.bias_mode)

    SIMULATOR = StrategyRunner.from_config(APP_CONFIG)
    print(f"⚙️  Config loaded | mode={APP_CONFIG['mode']} | duration={DURATION_MINUTES}m")
    for note in restore_notes:
        print(f"💰 {note}")
    for note in bias_notes:
        print(f"📐 {note}")
    print(SIMULATOR.summarize())
    if getattr(SIMULATOR, "regime", None) is not None:
        print(SIMULATOR.regime.summarize_line())
    print(f"📁 market={APP_CONFIG['market_data_file']}")
    start_aligned_runner(simulator=SIMULATOR)
