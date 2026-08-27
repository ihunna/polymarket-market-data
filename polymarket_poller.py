# Save this file as: polymarket_poller.py

import json
import requests

_token_cache = {}

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