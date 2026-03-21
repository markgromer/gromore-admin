"""
Local-rank heatmap scanner.

Generates a grid of geographic points around a business location and queries
the Google Places API (New) Text Search to determine the business's ranking
for a keyword at each point.
"""

import math
import logging
import re
import requests

log = logging.getLogger(__name__)

MILES_TO_KM = 1.60934
KM_PER_DEG_LAT = 111.32


def generate_grid(center_lat, center_lng, radius_miles, grid_size=6):
    """Return a list of dicts with row, col, lat, lng for an NxN grid."""
    radius_km = radius_miles * MILES_TO_KM
    half = (grid_size - 1) / 2.0
    km_per_deg_lng = KM_PER_DEG_LAT * math.cos(math.radians(center_lat))
    step_km = (2 * radius_km) / (grid_size - 1) if grid_size > 1 else 0

    points = []
    for r in range(grid_size):
        for c in range(grid_size):
            dlat = (half - r) * step_km / KM_PER_DEG_LAT
            dlng = (c - half) * step_km / km_per_deg_lng if km_per_deg_lng else 0
            points.append({
                "row": r,
                "col": c,
                "lat": round(center_lat + dlat, 6),
                "lng": round(center_lng + dlng, 6),
            })
    return points


def calc_search_radius_m(radius_miles, grid_size):
    """Calculate the per-point search radius based on grid spacing."""
    step_km = (2 * radius_miles * MILES_TO_KM) / max(grid_size - 1, 1)
    # Use half the grid spacing as search radius, with a floor of 2km
    return max(step_km * 1000 / 2, 2000)


def geocode_address(api_key, address):
    """Convert an address string to lat/lng using Google Geocoding API."""
    resp = requests.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"address": address, "key": api_key},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    status = data.get("status", "")
    if status not in ("OK",) or not data.get("results"):
        msg = data.get("error_message") or status
        log.warning("Geocoding failed for '%s': %s", address, msg)
        if msg and msg != "ZERO_RESULTS":
            raise RuntimeError(msg)
        return None
    loc = data["results"][0]["geometry"]["location"]
    return {"lat": loc["lat"], "lng": loc["lng"],
            "formatted": data["results"][0].get("formatted_address", address)}


def verify_place_id(api_key, place_id):
    """Look up a Place ID via Place Details to verify what it resolves to.
    Returns dict with name, address, location or error string."""
    if not place_id:
        return None
    # Try New API first
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "displayName,formattedAddress,location",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            d = resp.json()
            return {
                "name": d.get("displayName", {}).get("text", ""),
                "address": d.get("formattedAddress", ""),
                "lat": d.get("location", {}).get("latitude"),
                "lng": d.get("location", {}).get("longitude"),
            }
        # Fallback to legacy
    except Exception:
        pass
    # Legacy Place Details
    url2 = "https://maps.googleapis.com/maps/api/place/details/json"
    try:
        resp = requests.get(url2, params={
            "place_id": place_id, "fields": "name,formatted_address,geometry",
            "key": api_key,
        }, timeout=10)
        data = resp.json()
        if data.get("status") == "OK" and data.get("result"):
            r = data["result"]
            loc = r.get("geometry", {}).get("location", {})
            return {
                "name": r.get("name", ""),
                "address": r.get("formatted_address", ""),
                "lat": loc.get("lat"),
                "lng": loc.get("lng"),
            }
        return {"error": data.get("status", "UNKNOWN"), "message": data.get("error_message", "")}
    except Exception as exc:
        return {"error": str(exc)}

def _search_places(api_key, keyword, lat, lng, radius_m=2000):
    """Query Google Places APIs for a keyword near a point.
    Tries: New Text Search -> Legacy Text Search -> Legacy Nearby Search.
    Returns (places_list, diag_dict)."""
    diag = {"new_api": None, "legacy_api": None, "nearby_api": None}

    # --- Try Places API (New) Text Search ---
    url_new = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.id,places.formattedAddress",
        "Content-Type": "application/json",
    }
    body = {
        "textQuery": keyword,
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": float(min(radius_m, 50000)),
            }
        },
        "maxResultCount": 20,
    }
    try:
        resp = requests.post(url_new, json=body, headers=headers, timeout=15)
        resp_body = resp.text[:500]
        diag["new_api"] = {"status": resp.status_code, "body": resp_body}
        if resp.status_code == 200:
            places = resp.json().get("places", [])
            if places:
                return places, diag
        else:
            log.info("Places API (New) returned %s - trying legacy. Body: %s",
                     resp.status_code, resp_body)
    except Exception as exc:
        diag["new_api"] = {"error": str(exc)}
        log.warning("Places API (New) error at (%.4f, %.4f): %s", lat, lng, exc)

    # --- Fallback 1: Legacy Text Search ---
    url_legacy = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": keyword,
        "location": f"{lat},{lng}",
        "radius": int(min(radius_m, 50000)),
        "key": api_key,
    }
    try:
        resp = requests.get(url_legacy, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        diag["legacy_api"] = {"status": resp.status_code, "gstatus": data.get("status"),
                              "error_message": data.get("error_message", "")}
        raw = data.get("results", [])
        if raw:
            places = []
            for r in raw:
                places.append({
                    "displayName": {"text": r.get("name", "")},
                    "id": r.get("place_id", ""),
                    "formattedAddress": r.get("formatted_address", ""),
                })
            return places, diag
        log.debug("Legacy Text Search empty at (%.4f, %.4f) for '%s' - status: %s",
                  lat, lng, keyword, data.get("status"))
    except Exception as exc:
        diag["legacy_api"] = {"error": str(exc)}
        log.warning("Legacy Text Search error at (%.4f, %.4f): %s", lat, lng, exc)

    # --- Fallback 2: Legacy Nearby Search (keyword-based, better for SABs) ---
    url_nearby = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params_nearby = {
        "keyword": keyword,
        "location": f"{lat},{lng}",
        "radius": int(min(radius_m, 50000)),
        "key": api_key,
    }
    try:
        resp = requests.get(url_nearby, params=params_nearby, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        diag["nearby_api"] = {"status": resp.status_code, "gstatus": data.get("status"),
                              "error_message": data.get("error_message", "")}
        raw = data.get("results", [])
        if not raw:
            log.debug("Nearby Search also empty at (%.4f, %.4f) for '%s' - status: %s",
                      lat, lng, keyword, data.get("status"))
            return [], diag
        places = []
        for r in raw:
            places.append({
                "displayName": {"text": r.get("name", "")},
                "id": r.get("place_id", ""),
                "formattedAddress": r.get("formatted_address", ""),
            })
        return places, diag
    except Exception as exc:
        diag["nearby_api"] = {"error": str(exc)}
        log.warning("Nearby Search error at (%.4f, %.4f): %s", lat, lng, exc)
        return [], diag


def _tokenize(name):
    """Split a name into lowercase word tokens."""
    return set(re.findall(r'[a-z0-9]+', (name or "").lower()))


def _match_business(places, business_name, place_id=None):
    """Find the rank (1-based) of a business in Places results.
    Matches by place_id first, then exact substring, then word overlap."""
    bname = (business_name or "").lower().strip()
    btokens = _tokenize(business_name)
    stop_words = {'the', 'and', 'of', 'in', 'at', 'for', 'a', 'an', 'llc', 'inc', 'co'}
    btokens_clean = btokens - stop_words

    for i, p in enumerate(places, 1):
        pid = p.get("id", "")
        pname = (p.get("displayName", {}).get("text", "") or "").lower().strip()

        # Exact place_id match
        if place_id and pid == place_id:
            return i

        # Substring match (either direction)
        if bname and (bname in pname or pname in bname):
            return i

        # Word overlap match: if 2+ significant words match, count it
        if len(btokens_clean) >= 2:
            ptokens = _tokenize(pname) - stop_words
            overlap = btokens_clean & ptokens
            if len(overlap) >= 2 and len(overlap) >= len(btokens_clean) * 0.5:
                return i

    return 0  # not found


def scan_grid(api_key, keyword, business_name, grid_points,
              place_id=None, search_radius_m=2000):
    """Run a full grid scan. Returns list of point dicts with 'rank' added."""
    results = []
    debug_sample = None
    for pt in grid_points:
        places, api_diag = _search_places(api_key, keyword, pt["lat"], pt["lng"],
                                          radius_m=search_radius_m)
        rank = _match_business(places, business_name, place_id)
        # Capture first point's raw results for diagnostics
        if debug_sample is None:
            debug_sample = {
                "business_name_used": business_name,
                "place_id_used": place_id,
                "search_radius_m": search_radius_m,
                "places_returned": len(places),
                "top_5_names": [
                    (p.get("displayName", {}).get("text", "") or "")
                    for p in places[:5]
                ],
                "api_diagnostics": api_diag,
            }
            log.info("Heatmap debug - matching '%s' (place_id=%s) | "
                     "top results: %s | api_diag: %s",
                     business_name, place_id,
                     debug_sample["top_5_names"], api_diag)
        results.append({
            "row": pt["row"],
            "col": pt["col"],
            "lat": pt["lat"],
            "lng": pt["lng"],
            "rank": rank,
        })
    return results, debug_sample
