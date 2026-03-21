"""
Local-rank heatmap scanner.

Generates a grid of geographic points around a business location and queries
the Google Places API (New) Text Search to determine the business's ranking
for a keyword at each point.
"""

import math
import logging
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


def _search_places(api_key, keyword, lat, lng, radius_m=500):
    """Query Google Places Text Search (New) for a keyword near a point."""
    url = "https://places.googleapis.com/v1/places:searchText"
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
                "radius": float(radius_m),
            }
        },
        "maxResultCount": 20,
    }
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json().get("places", [])
    except Exception as exc:
        log.warning("Places API error at (%.4f, %.4f): %s", lat, lng, exc)
        return []


def _match_business(places, business_name, place_id=None):
    """Find the rank (1-based) of a business in Places results."""
    bname = (business_name or "").lower().strip()
    for i, p in enumerate(places, 1):
        pid = p.get("id", "")
        pname = (p.get("displayName", {}).get("text", "") or "").lower().strip()
        if place_id and pid == place_id:
            return i
        if bname and bname in pname:
            return i
        if bname and pname in bname:
            return i
    return 0  # not found


def scan_grid(api_key, keyword, business_name, grid_points,
              place_id=None, search_radius_m=500):
    """Run a full grid scan. Returns list of point dicts with 'rank' added."""
    results = []
    for pt in grid_points:
        places = _search_places(api_key, keyword, pt["lat"], pt["lng"],
                                radius_m=search_radius_m)
        rank = _match_business(places, business_name, place_id)
        results.append({
            "row": pt["row"],
            "col": pt["col"],
            "lat": pt["lat"],
            "lng": pt["lng"],
            "rank": rank,
        })
    return results
