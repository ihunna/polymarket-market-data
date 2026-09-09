import argparse
import os
import sys
import time
import csv
import json
import calendar
import threading
from datetime import datetime
import zoneinfo
import websocket
from polymarket_poller import (
    fetch_polymarket_data,
    fetch_polymarket_current_price,
    fetch_polymarket_end_price,
    get_market_metadata_for_slug,
)
from signal_engine import StrategyRunner, load_config, apply_duration_paths

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
class PersistentPolymarketWS:
    def __init__(self):
        self.ws = None
        self.active_tokens = []
        self.is_running = True
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def update_tokens(self, token_ids):
        with self.lock:
            self.active_tokens = token_ids
            if self.ws and ws_state["connected"]:
                self._send_subscription(self.ws, token_ids)

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
        with self.lock:
            if self.active_tokens:
                self._send_subscription(ws, self.active_tokens)

    def _on_close(self, ws, code, msg):
        ws_state["connected"] = False

    def _on_error(self, ws, error):
        ws_state["connected"] = False

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

def run_high_frequency_loop(ws_manager, price_to_beat, simulator=None):
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
    current_price = 0.0
    last_price_fetch_at = 0.0

    decision_horizon = 0.0
    if simulator is not None:
        decision_horizon = float(getattr(simulator, "decision_remaining_seconds", 0) or 0)

    print(f"📊 Price to Beat (Baseline): {format_dollar(price_to_beat)}")

    while True:
        current_time = time.time()
        remaining = window_end - current_time
        if remaining < 0:
            remaining = 0
        
        if current_time >= window_end:
            print("\n⏳ Window ended. Fetching end price from Polymarket...")
            final_price = fetch_polymarket_end_price(window_start, duration_minutes=DURATION_MINUTES)
            if final_price == 0.0:
                print("⚠️ Polymarket fetch failed, falling back to previous price-to-beat.")
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

        need_price = decision_horizon > 0 and remaining <= decision_horizon
        if need_price and (current_time - last_price_fetch_at) >= 2.0:
            fetched = fetch_polymarket_current_price(window_start, duration_minutes=DURATION_MINUTES)
            last_price_fetch_at = current_time
            if fetched > 0:
                current_price = fetched

        if simulator is not None:
            simulator.on_window_update(
                window_start=window_start,
                price_to_beat=price_to_beat,
                current_price=current_price,
                lowest_up=lowest_up_seen,
                lowest_down=lowest_down_seen,
                remaining_seconds=remaining,
                up_ask=up_cost,
                down_ask=down_cost,
                inferred_outcome=None,
            )

        up_cents = format_token_cents(up_cost)
        down_cents = format_token_cents(down_cost)

        sim_bit = ""
        if simulator is not None:
            sim_bit = f" | {simulator.display_status(window_start)}"
        display_str = (
            f"PTB: {format_dollar(price_to_beat)} | Up: {up_cents} | Down: {down_cents}{sim_bit}"
        )
        update_window_progress(window_start, window_end, formatted_message=display_str)
        
        time.sleep(0.5)

def start_aligned_runner(simulator=None):
    """Initializes services and prompts for initial manual PTB."""
    print(f"🔌 Initializing services... (window={DURATION_MINUTES}m)")
    global_ws_manager = PersistentPolymarketWS()
    
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

    SIMULATOR = StrategyRunner.from_config(APP_CONFIG)
    print(f"⚙️  Config loaded | mode={APP_CONFIG['mode']} | duration={DURATION_MINUTES}m")
    for note in restore_notes:
        print(f"💰 {note}")
    print(SIMULATOR.summarize())
    print(f"📁 market={APP_CONFIG['market_data_file']}")
    start_aligned_runner(simulator=SIMULATOR)
