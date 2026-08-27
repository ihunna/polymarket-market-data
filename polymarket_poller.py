# Save this file as: polymarket_poller.py

import json
import requests

_token_cache = {}

def get_token_ids_for_slug(slug):
    """Fetches token IDs, question, and market metadata from Gamma API once and caches them."""
    if slug in _token_cache:
        return _token_cache[slug]
        
    gamma_url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    try:
        response = requests.get(gamma_url, timeout=3)
        if response.status_code != 200:
            return None, None, None, None
            
        data = response.json()
        markets = data.get("markets", [])
        if not markets:
            return None, None, None, None
            
        market = markets[0]
        question = market.get("question", data.get("title", "Market"))
        description = market.get("description", "")
        
        clob_token_ids_raw = market.get("clobTokenIds", "[]")
        if isinstance(clob_token_ids_raw, str):
            token_ids = json.loads(clob_token_ids_raw)
        else:
            token_ids = clob_token_ids_raw
            
        if len(token_ids) < 2:
            return None, None, None, None
            
        up_token = token_ids[0]
        down_token = token_ids[1]
        
        _token_cache[slug] = (up_token, down_token, question, description)
        return up_token, down_token, question, description
        
    except Exception:
        return None, None, None, None

def fetch_polymarket_data(slug):
    """Fetches token IDs once via cache, queries the CLOB buy-side price endpoint, and returns raw/formatted data."""
    try:
        up_token_id, down_token_id, question, description = get_token_ids_for_slug(slug)
        if not up_token_id or not down_token_id:
            return {"status": "ignored"}
            
        clob_url = "https://clob.polymarket.com/price"
        
        up_resp = requests.get(clob_url, params={"token_id": up_token_id, "side": "BUY"}, timeout=2)
        down_resp = requests.get(clob_url, params={"token_id": down_token_id, "side": "BUY"}, timeout=2)
        
        up_cost = 0.0
        down_cost = 0.0
        
        if up_resp.status_code == 200:
            up_data = up_resp.json()
            if isinstance(up_data, dict):
                up_cost = float(up_data.get("price", 0.0))
            elif isinstance(up_data, (int, float)):
                up_cost = float(up_data)
                
        if down_resp.status_code == 200:
            down_data = down_resp.json()
            if isinstance(down_data, dict):
                down_cost = float(down_data.get("price", 0.0))
            elif isinstance(down_data, (int, float)):
                down_cost = float(down_data)
            
        up_cents = round(up_cost * 100)
        down_cents = round(down_cost * 100)

        up_formatted = f"{up_cents}¢" if up_cost <= 1.0 else f"${up_cost:.2f}"
        down_formatted = f"{down_cents}¢" if down_cost <= 1.0 else f"${down_cost:.2f}"

        return {
            "status": "success",
            "question": question,
            "description": description,
            "up_cost": up_formatted,
            "down_cost": down_formatted,
            "up_raw": up_cost,
            "down_raw": down_cost
        }
        
    except Exception:
        return {"status": "ignored"}

if __name__ == "__main__":
    test_slug = "sol-updown-15m-1787814000"
    result = fetch_polymarket_data(test_slug)
    print(result)