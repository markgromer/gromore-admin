"""
Local-rank heatmap scanner.

Generates a grid of geographic points around a business location and queries
the Google Places API (New) Text Search to determine the business's ranking
for a keyword at each point.
"""

import math
import logging
import re
import time
import requests

log = logging.getLogger(__name__)

MILES_TO_KM = 1.60934
KM_PER_DEG_LAT = 111.32

# Phrases that are redundant when we already pass lat/lng + radius to the API
_LOCATION_NOISE = re.compile(
    r'\b(near\s+me|nearby|close\s+to\s+me|around\s+me|in\s+my\s+area)\b',
    re.IGNORECASE,
)


def clean_keyword(raw):
    """Strip location-relative phrases that confuse the API (we already send lat/lng).
    Returns (cleaned_keyword, was_modified)."""
    cleaned = _LOCATION_NOISE.sub('', raw).strip()
    # Collapse extra whitespace
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
    return cleaned, cleaned.lower() != raw.lower().strip()


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
    # Use full grid spacing as search radius so adjacent points overlap, floor of 5km
    return max(step_km * 1000, 5000)


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
        "X-Goog-FieldMask": "displayName,formattedAddress,location,types,businessStatus",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            d = resp.json()
            addr = d.get("formattedAddress", "")
            lat = d.get("location", {}).get("latitude")
            lng = d.get("location", {}).get("longitude")
            types = d.get("types", [])
            status = d.get("businessStatus", "")
            is_sab = not addr and lat is not None  # SABs have location but no public address
            return {
                "name": d.get("displayName", {}).get("text", ""),
                "address": addr if addr else ("Service-area business (no public address)" if is_sab else ""),
                "lat": lat,
                "lng": lng,
                "types": types[:3] if types else [],
                "business_status": status,
                "source": "new_api",
            }
        # Fallback to legacy
    except Exception:
        pass
    # Legacy Place Details
    url2 = "https://maps.googleapis.com/maps/api/place/details/json"
    try:
        resp = requests.get(url2, params={
            "place_id": place_id, "fields": "name,formatted_address,geometry,type,business_status",
            "key": api_key,
        }, timeout=10)
        data = resp.json()
        if data.get("status") == "OK" and data.get("result"):
            r = data["result"]
            loc = r.get("geometry", {}).get("location", {})
            addr = r.get("formatted_address", "")
            lat = loc.get("lat")
            lng = loc.get("lng")
            is_sab = not addr and lat is not None
            return {
                "name": r.get("name", ""),
                "address": addr if addr else ("Service-area business (no public address)" if is_sab else ""),
                "lat": lat,
                "lng": lng,
                "types": r.get("types", [])[:3],
                "business_status": r.get("business_status", ""),
                "source": "legacy_api",
            }
        return {"error": data.get("status", "UNKNOWN"), "message": data.get("error_message", "")}
    except Exception as exc:
        return {"error": str(exc)}

def _search_places(api_key, keyword, lat, lng, radius_m=2000):
    """Query Google Places APIs for a keyword near a point.
    Tries all available APIs and merges results (de-duplicated by place_id).
    This is critical because SABs often appear in Legacy/Nearby but not New API.
    Returns (places_list, diag_dict)."""
    diag = {"new_api": None, "legacy_api": None, "nearby_api": None}
    all_places = []
    seen_ids = set()

    def _add_places(new_places):
        """Merge new places into all_places, skipping duplicates by ID."""
        for p in new_places:
            pid = p.get("id", "")
            if pid and pid in seen_ids:
                continue
            if pid:
                seen_ids.add(pid)
            all_places.append(p)

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
            _add_places(places)
            diag["new_api"]["count"] = len(places)
        else:
            log.info("Places API (New) returned %s. Body: %s",
                     resp.status_code, resp_body)
    except Exception as exc:
        diag["new_api"] = {"error": str(exc)}
        log.warning("Places API (New) error at (%.4f, %.4f): %s", lat, lng, exc)

    # --- Always try Legacy Text Search (different ranking, better SAB coverage) ---
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
        legacy_places = []
        for r in raw:
            legacy_places.append({
                "displayName": {"text": r.get("name", "")},
                "id": r.get("place_id", ""),
                "formattedAddress": r.get("formatted_address", ""),
            })
        _add_places(legacy_places)
        diag["legacy_api"]["count"] = len(legacy_places)
    except Exception as exc:
        diag["legacy_api"] = {"error": str(exc)}
        log.warning("Legacy Text Search error at (%.4f, %.4f): %s", lat, lng, exc)

    # --- Always try Nearby Search (keyword-based, often best for SABs) ---
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
        nearby_places = []
        for r in raw:
            nearby_places.append({
                "displayName": {"text": r.get("name", "")},
                "id": r.get("place_id", ""),
                "formattedAddress": r.get("formatted_address", ""),
            })
        _add_places(nearby_places)
        diag["nearby_api"]["count"] = len(nearby_places)
    except Exception as exc:
        diag["nearby_api"] = {"error": str(exc)}
        log.warning("Nearby Search error at (%.4f, %.4f): %s", lat, lng, exc)

    return all_places, diag


def _tokenize(name):
    """Split a name into lowercase word tokens."""
    return set(re.findall(r'[a-z0-9]+', (name or "").lower()))


def _match_business(places, business_name, place_id=None):
    """Find the rank (1-based) of a business in Places results.
    Matches by place_id first, then exact substring, then word overlap,
    then fuzzy single-word match."""
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
        ptokens = _tokenize(pname) - stop_words
        if len(btokens_clean) >= 2:
            overlap = btokens_clean & ptokens
            if len(overlap) >= 2 and len(overlap) >= len(btokens_clean) * 0.5:
                return i

        # Single-word or short name: check if the core word appears in the result
        if len(btokens_clean) == 1:
            core = list(btokens_clean)[0]
            if len(core) >= 3 and any(core in pt for pt in ptokens):
                return i

    return 0  # not found


def scan_grid(api_key, keyword, business_name, grid_points,
              place_id=None, search_radius_m=2000):
    """Run a full grid scan. Returns list of point dicts with 'rank' added.
    Queries the center point first for reliable diagnostics."""
    grid_size = int(math.sqrt(len(grid_points))) if grid_points else 0
    center_r = grid_size // 2 if grid_size else 0
    center_idx = center_r * grid_size + center_r if grid_size else 0

    # Re-order to process center point first (before any throttling kicks in)
    ordered = []
    if grid_points:
        ordered.append((center_idx, grid_points[center_idx]))
        for idx, pt in enumerate(grid_points):
            if idx != center_idx:
                ordered.append((idx, pt))

    results = [None] * len(grid_points)
    debug_sample = None
    errors = 0
    for seq, (idx, pt) in enumerate(ordered):
        # Rate limit: delay between grid points (3 API calls per point)
        if seq > 0:
            time.sleep(0.5)
        try:
            places, api_diag = _search_places(api_key, keyword, pt["lat"], pt["lng"],
                                              radius_m=search_radius_m)
        except Exception as exc:
            log.warning("scan_grid: API error at point %d (%.4f, %.4f): %s",
                        idx, pt["lat"], pt["lng"], exc)
            places, api_diag = [], {"error": str(exc)}
            errors += 1
        rank = _match_business(places, business_name, place_id)
        # Capture center point's results for diagnostics (first in our ordering)
        if seq == 0:
            debug_sample = {
                "business_name_used": business_name,
                "place_id_used": place_id,
                "search_radius_m": search_radius_m,
                "places_returned": len(places),
                "debug_point": f"center ({pt['lat']:.4f}, {pt['lng']:.4f})",
                "top_results": [
                    {
                        "name": (p.get("displayName", {}).get("text", "") or ""),
                        "id": p.get("id", ""),
                    }
                    for p in places[:10]
                ],
                "api_diagnostics": api_diag,
            }
            log.info("Heatmap debug (center) - matching '%s' (place_id=%s) | "
                     "top results: %s | api_diag: %s",
                     business_name, place_id,
                     [r["name"] for r in debug_sample["top_results"]], api_diag)
        results[idx] = {
            "row": pt["row"],
            "col": pt["col"],
            "lat": pt["lat"],
            "lng": pt["lng"],
            "rank": rank,
        }
    if errors:
        if debug_sample is None:
            debug_sample = {}
        debug_sample["api_errors"] = errors
        debug_sample["total_points"] = len(grid_points)
    return results, debug_sample
