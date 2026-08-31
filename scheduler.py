# Save this file as: scheduler.py

import os
import sys
import time
import csv
import json
import calendar
import threading
from datetime import datetime
import websocket
from polymarket_poller import fetch_polymarket_data, fetch_polymarket_end_price, get_market_metadata_for_slug

WINDOW_DURATION_SECONDS = 15 * 60  # 15 minutes (900 seconds)
COIN_NAME = "solana"               
DURATION_MINUTES = 15

# Global shared state for order book token asks only
ws_state = {
    "up_raw": 0.0,
    "down_raw": 0.0,
    "connected": False
}

def get_current_active_slug(coin="sol"):
    """Computes the exact slug for the current live 15-minute window based on real-time epoch."""
    now_utc = calendar.timegm(time.gmtime())
    window_start = (now_utc // WINDOW_DURATION_SECONDS) * WINDOW_DURATION_SECONDS
    return f"{coin}-updown-15m-{window_start}"

def format_time(seconds):
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins:02d}:{secs:02d}"

def update_window_progress(window_start, window_end, formatted_message):
    """Updates a single persistent line showing window progress and token ask data."""
    now = time.time()
    elapsed = now - window_start
    remaining = window_end - now
    if remaining < 0:
        remaining = 0

    progress = max(0.0, min(1.0, elapsed / WINDOW_DURATION_SECONDS))
    filled_blocks = int(progress * 20)
    bar = "█" * filled_blocks + "-" * (20 - filled_blocks)
    
    start_dt = datetime.fromtimestamp(window_start)
    window_label = f"{start_dt.strftime('%H:%M')} window"
    time_str = format_time(remaining)
    
    sys.stdout.write(f"\r{window_label} [{bar}] {time_str} remaining | {formatted_message}   ")
    sys.stdout.flush()

def log_to_csv(timestamp, price_to_beat, final_price, lowest_up, lowest_down, outcome):
    """Appends window results including price-to-beat, final Polymarket price, and extremes to CSV."""
    filename = f"{COIN_NAME}-{DURATION_MINUTES}-updown.csv"
    file_exists = os.path.isfile(filename)
    
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Time stamp", "price_to_beat", "final_price", "lowest_up", "lowest_down", "outcome"])
        writer.writerow([timestamp, price_to_beat, final_price, lowest_up, lowest_down, outcome])

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

def run_high_frequency_loop(ws_manager, price_to_beat):
    """Executes a 15-minute window loop tracking token asks and querying Polymarket only at the buzzer."""
    now_utc = calendar.timegm(time.gmtime())
    window_start = (now_utc // WINDOW_DURATION_SECONDS) * WINDOW_DURATION_SECONDS
    window_end = window_start + WINDOW_DURATION_SECONDS
    
    active_slug = get_current_active_slug(coin="sol")
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Market window started! (Slug: {active_slug})")
    
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
    
    print(f"📊 Price to Beat (Baseline): ${price_to_beat:.2f}")

    while True:
        current_time = time.time()
        
        if current_time >= window_end:
            print("\n⏳ Window ended. Fetching end price from Polymarket...")
            final_price = fetch_polymarket_end_price(window_start)
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
                
            l_up_fmt = f"{round(lowest_up_seen * 100)}¢" if lowest_up_seen != float('inf') and lowest_up_seen > 0 else "N/A"
            l_down_fmt = f"{round(lowest_down_seen * 100)}¢" if lowest_down_seen != float('inf') and lowest_down_seen > 0 else "N/A"
            
            log_to_csv(window_start, price_to_beat, final_price, l_up_fmt, l_down_fmt, outcome)
            
            print(f"🏁 Window completed! Outcome: {outcome} | PTB: ${price_to_beat:.2f} | Final Price: ${final_price:.2f} | Logged")
            
            return final_price
        
        up_cost = ws_state["up_raw"]
        down_cost = ws_state["down_raw"]
        
        # Capture the baseline starting prices first once data is populated
        if not initialized and up_cost > 0 and down_cost > 0:
            lowest_up_seen = up_cost
            lowest_down_seen = down_cost
            initialized = True

        # Track lowest extremes moving forward
        if initialized:
            if 0.0 < up_cost <= 1.0 and up_cost < lowest_up_seen:
                lowest_up_seen = up_cost
            if 0.0 < down_cost <= 1.0 and down_cost < lowest_down_seen:
                lowest_down_seen = down_cost
            
        up_cents = round(up_cost * 100) if up_cost <= 1.0 else 0
        down_cents = round(down_cost * 100) if down_cost <= 1.0 else 0
        
        display_str = f"PTB: ${price_to_beat:.2f} | Up: {up_cents}¢ | Down: {down_cents}¢"
        update_window_progress(window_start, window_end, formatted_message=display_str)
        
        time.sleep(0.5)

def start_aligned_runner():
    """Initializes services and prompts for initial manual PTB."""
    print("🔌 Initializing services...")
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
            current_ptb = run_high_frequency_loop(global_ws_manager, price_to_beat=current_ptb)
        except Exception as e:
            print(f"\nError in loop execution: {e}. Restarting cycle...")
            time.sleep(2)

if __name__ == "__main__":
    start_aligned_runner()