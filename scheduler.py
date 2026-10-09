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
from polymarket_poller import get_market_metadata_for_slug
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

def get_current_active_slug(coin="sol", at=None):
    """Computes the exact slug for short-duration or hourly markets matching Polymarket's URL scheme."""
    if DURATION_MINUTES == 60:
        et_zone = zoneinfo.ZoneInfo("America/New_York")
        now_et = datetime.now(et_zone) if at is None else datetime.fromtimestamp(at, et_zone)
        
        month_name = now_et.strftime("%B").lower()
        day = now_et.strftime("%d").lstrip("0")
        year = now_et.strftime("%Y")
        
        hour_12 = now_et.strftime("%I").lstrip("0")
        ampm = now_et.strftime("%p").lower()
        
        full_coin_name = "solana" if coin == "sol" else coin
        return f"{full_coin_name}-up-or-down-{month_name}-{day}-{year}-{hour_12}{ampm}-et"
    
    now_utc = calendar.timegm(time.gmtime()) if at is None else int(at)
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
    if price_to_beat <= 0:
        return f"{label}: {format_dollar(price)} (--)"
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
ORDER_BOOK_CONNECT_TIMEOUT_SECONDS = 15
NEXT_WINDOW_SUBSCRIBE_SECONDS = 60


class PersistentPolymarketWS:
    def __init__(self):
        self.ws = None
        self.active_tokens = []
        self.next_tokens = []
        self.is_running = True
        self.lock = threading.Lock()
        self.last_update = time.time()
        self.connected = False
        self.ever_connected = False
        self.disconnected_at = time.time()
        self.subscribed = False
        self.books = {}
        self.volume = {}
        self.trades_from = {}
        self.tracked_since = {}
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()

    def _watched(self):
        return list(self.active_tokens) + [t for t in self.next_tokens if t not in self.active_tokens]

    def _best_ask(self, token):
        asks = self.books.get(token, {}).get("asks")
        return min(asks) if asks else 0.0

    def prepare_next(self, token_ids, window_start):
        """Subscribe the next window's tokens ahead of the switch so its book is ready at the start."""
        with self.lock:
            if not token_ids or token_ids in (self.next_tokens, self.active_tokens):
                return
            self.next_tokens = token_ids
            for t in token_ids:
                self.trades_from[t] = window_start
                self.tracked_since[t] = time.time()
            if self.connected and self.ws is not None:
                self._send_subscription(self.ws, token_ids)

    def update_tokens(self, token_ids, window_start):
        """Make token_ids the current window's tokens on the open connection, without reconnecting."""
        with self.lock:
            if token_ids == self.active_tokens:
                return
            prepared = token_ids == self.next_tokens
            old = [t for t in self.active_tokens if t not in token_ids]
            self.active_tokens = token_ids
            self.next_tokens = []
            if not prepared:
                for t in token_ids:
                    self.trades_from[t] = window_start
                    self.tracked_since[t] = time.time()
            for t in old:
                for store in (self.books, self.volume, self.trades_from, self.tracked_since):
                    store.pop(t, None)
            ws_state["up_raw"] = self._best_ask(token_ids[0])
            ws_state["down_raw"] = self._best_ask(token_ids[1])
            if self.connected and self.ws is not None:
                self.last_update = time.time()
                if not prepared:
                    self._send_subscription(self.ws, token_ids)
                if old:
                    try:
                        self.ws.send(json.dumps({"assets_ids": old, "operation": "unsubscribe"}))
                    except Exception:
                        pass

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
        """Call with the lock held. The first message on a connection sets it up; later ones add tokens."""
        try:
            if self.subscribed:
                payload = {"assets_ids": token_ids, "operation": "subscribe"}
            else:
                payload = {"assets_ids": token_ids, "type": "market"}
            ws.send(json.dumps(payload))
            self.subscribed = True
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
                event = item.get("event_type")
                if event == "book":
                    self._replace_book(item)
                elif event == "last_trade_price":
                    self._add_trade(item)
                elif event == "price_change":
                    for change in item.get("price_changes", []):
                        asset_id = change.get("asset_id")
                        self._apply_level(change)
                        best_ask_str = change.get("best_ask")
                        if asset_id and best_ask_str:
                            val = float(best_ask_str)
                            if asset_id == up_t:
                                ws_state["up_raw"] = val
                            elif asset_id == down_t:
                                ws_state["down_raw"] = val
        except Exception:
            pass

    def _replace_book(self, item):
        asset_id = item.get("asset_id")
        with self.lock:
            if asset_id not in self._watched():
                return
            self.books[asset_id] = {
                side: {float(lvl["price"]): float(lvl["size"]) for lvl in item.get(key, [])}
                for side, key in (("bids", "bids"), ("asks", "asks"))
            }
            if asset_id in self.active_tokens[:2]:
                key = "up_raw" if asset_id == self.active_tokens[0] else "down_raw"
                ws_state[key] = self._best_ask(asset_id)

    def _apply_level(self, change):
        asset_id = change.get("asset_id")
        side = "bids" if change.get("side") == "BUY" else "asks"
        with self.lock:
            book = self.books.get(asset_id)
            if book is None or change.get("price") is None:
                return
            price, size = float(change["price"]), float(change.get("size") or 0)
            if size > 0:
                book[side][price] = size
            else:
                book[side].pop(price, None)

    def _add_trade(self, item):
        asset_id = item.get("asset_id")
        with self.lock:
            if asset_id not in self._watched():
                return
            if float(item.get("timestamp") or 0) / 1000 < self.trades_from.get(asset_id, 0):
                return
            usd, count = self.volume.get(asset_id, (0.0, 0))
            self.volume[asset_id] = (usd + float(item["price"]) * float(item["size"]), count + 1)

    def volume_totals(self):
        """(dollars traded, trade count, time since when trades have been seen without a gap)."""
        with self.lock:
            usd = sum(self.volume.get(t, (0.0, 0))[0] for t in self.active_tokens)
            count = sum(self.volume.get(t, (0.0, 0))[1] for t in self.active_tokens)
            since = max((self.tracked_since.get(t, time.time()) for t in self.active_tokens), default=time.time())
            return usd, count, since

    def depth_snapshot(self, within=0.05):
        """Best bid/ask sizes and shares within `within` of the best price, for Up and Down."""
        out = {}
        with self.lock:
            for name, token in zip(("up", "down"), self.active_tokens):
                book = self.books.get(token)
                if not book:
                    continue
                bids, asks = book["bids"], book["asks"]
                if bids:
                    best = max(bids)
                    out[f"{name}_bid_size"] = bids[best]
                    out[f"{name}_bid_depth_5c"] = sum(s for p, s in bids.items() if p >= best - within - 1e-9)
                if asks:
                    best = min(asks)
                    out[f"{name}_ask_size"] = asks[best]
                    out[f"{name}_ask_depth_5c"] = sum(s for p, s in asks.items() if p <= best + within + 1e-9)
        return out

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
            self.subscribed = False
            watched = self._watched()
            if watched:
                # Trades sent while disconnected are lost, so coverage restarts now.
                for t in watched:
                    self.tracked_since[t] = now
                self._send_subscription(ws, watched)
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
PRICE_QUIET_SECONDS = 15
PRICE_CONNECT_TIMEOUT_SECONDS = 60


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
        self.binance_prices = deque()
        self.last_tick = time.time()
        self.ever_connected = False
        self.disconnected_at = time.time()
        threading.Thread(target=self._run_loop, daemon=True).start()
        threading.Thread(target=self._ping_loop, daemon=True).start()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()

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
        now = time.time()
        with self.lock:
            took = now - self.disconnected_at if self.disconnected_at else 0.0
            verb = "reconnected" if self.ever_connected else "connected"
            self.connected = True
            self.ever_connected = True
            self.disconnected_at = None
            self.last_tick = now
        sys.stdout.write(f"\n🔌 Price socket {verb} in {took:.1f}s\n")
        sys.stdout.flush()
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
                    self.last_tick = time.time()
                    self._add_price(ts, value)
                elif topic == BINANCE_TOPIC and symbol == self.binance_symbol:
                    with self.lock:
                        if ts >= self.binance_latest[0]:
                            self.binance_latest = (ts, value)
                        self._append_history(self.binance_prices, ts, value)

    def _add_price(self, ts, value):
        with self.lock:
            self._append_history(self.prices, ts, value)

    @staticmethod
    def _append_history(series, ts, value):
        if series and ts <= series[-1][0]:
            return
        series.append((ts, value))
        cutoff = ts - PRICE_HISTORY_SECONDS * 1000
        while series and series[0][0] < cutoff:
            series.popleft()

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
        return self._value_at(self.prices, epoch_seconds)

    def binance_at(self, epoch_seconds):
        """Last Binance price at or before ``epoch_seconds``; 0.0 if none close enough."""
        return self._value_at(self.binance_prices, epoch_seconds)

    def _value_at(self, series, epoch_seconds):
        target = epoch_seconds * 1000
        with self.lock:
            for ts, value in reversed(series):
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

    def _mark_disconnected(self):
        with self.lock:
            if self.connected or self.disconnected_at is None:
                self.disconnected_at = time.time()
            self.connected = False

    def _close_ws(self):
        ws = self.ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _on_close(self, ws, code, msg):
        self._mark_disconnected()

    def _on_error(self, ws, error):
        self._mark_disconnected()

    def _watchdog_loop(self):
        while self.is_running:
            time.sleep(1)
            now = time.time()
            with self.lock:
                connected = self.connected
                quiet_for = now - self.last_tick
                down_for = now - self.disconnected_at if self.disconnected_at else 0.0
            if connected and quiet_for > PRICE_QUIET_SECONDS:
                sys.stdout.write(f"\n⚠️ Price socket quiet for {quiet_for:.0f}s while connected, reconnecting...\n")
                sys.stdout.flush()
                self._mark_disconnected()
                self._close_ws()
            elif not connected and down_for > PRICE_CONNECT_TIMEOUT_SECONDS:
                sys.stdout.write(f"\n⚠️ Price socket not connected after {down_for:.0f}s, retrying...\n")
                sys.stdout.flush()
                with self.lock:
                    self.disconnected_at = now
                self._close_ws()

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
                self.ws.run_forever(origin=PRICE_WS_ORIGIN, ping_interval=20, ping_timeout=10)
            except Exception:
                pass
            self._mark_disconnected()
            if self.is_running:
                time.sleep(2)


def prepare_next_window(ws_manager, next_start):
    try:
        up_token, down_token, _ = get_market_metadata_for_slug(get_current_active_slug(coin="sol", at=next_start))
        if up_token and down_token:
            ws_manager.prepare_next([up_token, down_token], next_start)
    except Exception:
        pass


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

    # Point the socket at this window's tokens; asks come from the book it already holds or receives next.
    ws_manager.update_tokens([up_token, down_token], window_start)
    next_prepared = False

    lowest_up_seen = float('inf')
    lowest_down_seen = float('inf')
    initialized = False

    print(f"📊 Price to Beat (Baseline): {format_dollar(price_to_beat)}")

    while True:
        current_time = time.time()
        remaining = window_end - current_time
        if remaining < 0:
            remaining = 0

        if not next_prepared and remaining <= NEXT_WINDOW_SUBSCRIBE_SECONDS:
            next_prepared = True
            threading.Thread(target=prepare_next_window, args=(ws_manager, window_end), daemon=True).start()
        
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
        binance_ptb = price_ws.binance_at(window_start)
        volume_usd, trade_count, volume_since = ws_manager.volume_totals()

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
                binance_price_to_beat=binance_ptb,
                volume_usd=volume_usd,
                trade_count=trade_count,
                volume_since=volume_since,
                book_depth=ws_manager.depth_snapshot,
            )

        up_cents = format_token_cents(up_cost)
        down_cents = format_token_cents(down_cost)

        sim_bit = ""
        if simulator is not None:
            sim_bit = f" | {simulator.display_status(window_start)}"
        display_str = (
            f"PTB: {format_dollar(gap_ptb)} | {format_ticker('PM', current_price, gap_ptb)} | "
            f"{format_ticker('Binance', binance_price, binance_ptb)} | Up: {up_cents} | Down: {down_cents}{sim_bit}"
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
        "--fd-last",
        type=float,
        default=None,
        metavar="EQUITY",
        help="Restore flat_dual starting equity (overrides strategies.flat_dual.capital).",
    )
    parser.add_argument(
        "--ds-last",
        type=float,
        default=None,
        metavar="EQUITY",
        help="Restore delta-side starting equity (overrides strategies.flat_dual.delta_side_capital).",
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


def apply_capital_overrides(
    config: dict,
    dh_last: float | None,
    opp_last: float | None,
    fd_last: float | None = None,
    ds_last: float | None = None,
) -> list[str]:
    """Apply CLI capital restores onto strategy configs. Returns human-readable notes.

    Without a flag, each strategy starts from the last capital_after in its own trades log (config value if none).
    """
    notes: list[str] = []
    strategies = config.setdefault("strategies", {})
    for flag, name, key, last in (
        ("--dh-last", "dual_hedge", "capital", dh_last),
        ("--opp-last", "opposite_side", "capital", opp_last),
        ("--fd-last", "flat_dual", "capital", fd_last),
        ("--ds-last", "flat_dual", "delta_side_capital", ds_last),
    ):
        if last is None:
            continue
        if last <= 0:
            raise SystemExit(f"{flag} must be > 0")
        strategy = strategies.get(name)
        if not isinstance(strategy, dict):
            raise SystemExit(f"{flag} provided but strategies.{name} is missing")
        prev = strategy.get(key, 100.0)
        strategy[key] = float(last)
        strategy[f"{key}_restored"] = True
        label = "delta_side" if key == "delta_side_capital" else name
        notes.append(f"{label} capital restored ${float(last):.2f} (config was ${float(prev):.2f})")
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
    restore_notes = apply_capital_overrides(APP_CONFIG, args.dh_last, args.opp_last, args.fd_last, args.ds_last)
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
