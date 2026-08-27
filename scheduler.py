# Save this file as: scheduler.py

import os
import sys
import time
import csv
import calendar
from datetime import datetime
from polymarket_poller import fetch_polymarket_data

INTERVAL_SECONDS = 1
WINDOW_DURATION_SECONDS = 15 * 60  # 15 minutes in seconds (900 seconds)
COIN_NAME = "solana"               # Filename prefix: solana-15-updown.csv
DURATION_MINUTES = 15

def get_slug_for_timestamp(epoch_time, coin="sol"):
    """Computes a static 15-minute market slug anchored to a specific epoch timestamp."""
    ts = (int(epoch_time) // WINDOW_DURATION_SECONDS) * WINDOW_DURATION_SECONDS
    return f"{coin}-updown-15m-{ts}"

def format_time(seconds):
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins:02d}:{secs:02d}"

def update_window_progress(window_start, window_end, formatted_message):
    """Updates a single persistent line showing window progress and live data."""
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

def log_to_csv(timestamp, lowest_up, lowest_down, outcome):
    """Appends the window results to the CSV log file, creating it with headers if missing."""
    filename = f"{COIN_NAME}-{DURATION_MINUTES}-updown.csv"
    file_exists = os.path.isfile(filename)
    
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Time stamp", "lowest_up", "lowest_down", "outcome"])
        writer.writerow([timestamp, lowest_up, lowest_down, outcome])

def run_high_frequency_loop():
    """Executes a 15-minute polling loop, tracks valid low extremes, and logs to CSV upon expiration."""
    utc_now = calendar.timegm(time.gmtime())
    window_start = (utc_now // WINDOW_DURATION_SECONDS) * WINDOW_DURATION_SECONDS
    window_end = window_start + WINDOW_DURATION_SECONDS
    
    active_slug = get_slug_for_timestamp(window_start, coin="sol")
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Market window started! (Slug: {active_slug})")
    
    next_tick_time = time.time()
    latest_data = {
        "display": "Connecting to Polymarket...",
        "up_raw": 0.0,
        "down_raw": 0.0
    }
    
    # Track lowest buy prices observed during this window
    lowest_up_seen = float('inf')
    lowest_down_seen = float('inf')
    
    while True:
        current_time = time.time()
        
        if current_time >= window_end:
            up_final = latest_data.get("up_raw", 0.0)
            down_final = latest_data.get("down_raw", 0.0)
            
            # Determine official outcome based on final settlement values ($1.00 / 100¢)
            outcome = "UNKNOWN"
            if up_final >= 0.95 or (up_final > down_final and up_final > 0.5):
                outcome = "Up"
            elif down_final >= 0.95 or (down_final > up_final and down_final > 0.5):
                outcome = "Down"
                
            # Format lowest prices cleanly in cents (displaying "N/A" if it never dropped into a valid trading range)
            l_up_fmt = f"{round(lowest_up_seen * 100)}¢" if lowest_up_seen != float('inf') and lowest_up_seen > 0 else "N/A"
            l_down_fmt = f"{round(lowest_down_seen * 100)}¢" if lowest_down_seen != float('inf') and lowest_down_seen > 0 else "N/A"
            
            # Write to CSV log file: Time stamp, lowest_up, lowest_down, outcome
            log_to_csv(window_start, l_up_fmt, l_down_fmt, outcome)
            
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🏁 Window completed! Outcome: {outcome} | Lowest Up: {l_up_fmt} | Lowest Down: {l_down_fmt} (Logged)")
            break
        
        if current_time >= next_tick_time:
            result = fetch_polymarket_data(slug=active_slug)
            if result.get("status") == "success":
                up_cost = result.get("up_raw", 0.0)
                down_cost = result.get("down_raw", 0.0)
                
                # Ignore zero, stale ticks, or near-resolved states (>= 0.95 or <= 0.02) 
                # so we only record valid liquid trading ranges for hedging analysis.
                if 0.02 < up_cost < 0.95 and up_cost < lowest_up_seen:
                    lowest_up_seen = up_cost
                if 0.02 < down_cost < 0.95 and down_cost < lowest_down_seen:
                    lowest_down_seen = down_cost
                
                up = result.get("up_cost")
                down = result.get("down_cost")
                latest_data = {
                    "display": f"{active_slug}... | Up: {up} | Down: {down}",
                    "up_raw": up_cost,
                    "down_raw": down_cost
                }
            
            next_tick_time += INTERVAL_SECONDS

        update_window_progress(window_start, window_end, formatted_message=latest_data["display"])
        time.sleep(0.05)

def start_aligned_runner():
    """Continuously runs the high-frequency loop back-to-back 24/7."""
    while True:
        run_high_frequency_loop()

if __name__ == "__main__":
    start_aligned_runner()