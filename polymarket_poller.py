# Save this file as: polymarket_poller.py

import json
import time
from datetime import datetime, timezone

import requests

WINDOW_DURATION_SECONDS = 15 * 60
PRICE_HISTORY_URL = "https://polymarket.com/api/crypto/price-history"
PRICE_HISTORY_HEADERS = {
    "accept": "*/*",
    "user-agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Mobile Safari/537.36",
}

_token_cache = {}

def _epoch_to_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _fetch_price_history(window_start_epoch: int, window_end_epoch: int):
    """Returns price-history points for a window, or None on failure."""
    try:
        resp = requests.get(
            PRICE_HISTORY_URL,
            params={
                "symbol": "SOL",
                "eventStartTime": _epoch_to_iso(window_start_epoch),
                "variant": "fifteen",
                "endDate": _epoch_to_iso(window_end_epoch),
                "twapEnabled": "true",
                "twapLookbackSeconds": "60",
            },
            headers=PRICE_HISTORY_HEADERS,
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, list) or not data:
            return None
        return data
    except Exception:
        return None

def fetch_polymarket_end_price(window_start_epoch: int) -> float:
    """Fetches the TWAP end price for a completed 15-minute window."""
    window_end_epoch = window_start_epoch + WINDOW_DURATION_SECONDS
    window_end_ms = window_end_epoch * 1000
    latest_value = 0.0

    for attempt in range(6):
        points = _fetch_price_history(window_start_epoch, window_end_epoch)
        if points:
            last_point = points[-1]
            latest_value = float(last_point.get("value", 0.0))
            if latest_value > 0 and int(last_point.get("timestamp", 0)) >= window_end_ms:
                return latest_value
        if attempt < 5:
            time.sleep(5)

    return latest_value if latest_value > 0 else 0.0

def get_market_metadata_for_slug(slug):
    """Fetches token IDs and question from Gamma API cleanly without regex overhead."""
    if slug in _token_cache:
        return _token_cache[slug]
        
    gamma_url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    try:
        response = requests.get(gamma_url, timeout=3)
        if response.status_code != 200:
            return None, None, None
            
        data = response.json()
        markets = data.get("markets", [])
        if not markets:
            return None, None, None
            
        market = markets[0]
        question = market.get("question", data.get("title", "Market"))
            
        clob_token_ids_raw = market.get("clobTokenIds", "[]")
        if isinstance(clob_token_ids_raw, str):
            token_ids = json.loads(clob_token_ids_raw)
        else:
            token_ids = clob_token_ids_raw
            
        if len(token_ids) < 2:
            return None, None, None
            
        up_token = token_ids[0]
        down_token = token_ids[1]
        
        _token_cache[slug] = (up_token, down_token, question)
        return up_token, down_token, question
        
    except Exception:
        return None, None, None

def fetch_polymarket_data(slug):
    """Initial REST fallback fetch for asks."""
    try:
        up_token_id, down_token_id, question = get_market_metadata_for_slug(slug)
        if not up_token_id or not down_token_id:
            return {"status": "ignored"}
            
        clob_url = "https://clob.polymarket.com/price"
        up_resp = requests.get(clob_url, params={"token_id": up_token_id, "side": "SELL"}, timeout=2)
        down_resp = requests.get(clob_url, params={"token_id": down_token_id, "side": "SELL"}, timeout=2)
        
        up_cost = float(up_resp.json().get("price", 0.0)) if up_resp.status_code == 200 else 0.0
        down_cost = float(down_resp.json().get("price", 0.0)) if down_resp.status_code == 200 else 0.0

        return {
            "status": "success",
            "up_raw": up_cost,
            "down_raw": down_cost
        }
    except Exception:
        return {"status": "ignored"}