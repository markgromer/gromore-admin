"""
Local-rank heatmap scanner.

Generates a grid of geographic points around a business location and checks
live Google Maps results first, with Places API fallbacks, to determine the
business's ranking for a keyword at each point.
"""

import math
import logging
import re
import time
import requests
from urllib.parse import quote

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


def _find_place_candidates(api_key, query, lat=None, lng=None, use_bias=True):
    """Use Find Place From Text for exact listing lookup fallbacks."""
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id,name,formatted_address",
        "key": api_key,
    }
    if use_bias and lat is not None and lng is not None and (lat != 0 or lng != 0):
        params["locationbias"] = f"circle:50000@{lat},{lng}"

    resp = requests.get(
        "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
        params=params,
        timeout=15,
    )
    data = resp.json()
    candidates = []
    if data.get("status") == "OK":
        for candidate in data.get("candidates", []):
            candidates.append({
                "displayName": {"text": candidate.get("name", "")},
                "id": candidate.get("place_id", ""),
                "formattedAddress": candidate.get("formatted_address", ""),
            })
    return candidates, {
        "status": resp.status_code,
        "gstatus": data.get("status", ""),
        "error_message": data.get("error_message", ""),
        "count": len(candidates),
        "query": query,
        "biased": bool(use_bias and lat is not None and lng is not None and (lat != 0 or lng != 0)),
    }


def _estimate_google_maps_zoom(radius_miles):
    """Translate scan radius into a coarse Google Maps zoom level."""
    if radius_miles <= 1:
        return 14
    if radius_miles <= 3:
        return 13
    if radius_miles <= 5:
        return 12
    if radius_miles <= 10:
        return 11
    return 10


def _extract_place_id_from_maps_href(href):
    """Extract a Place ID from a Google Maps place href when available."""
    href = (href or "").strip()
    if not href:
        return ""
    match = re.search(r'!19s([A-Za-z0-9_-]+)', href)
    if match:
        return match.group(1)
    match = re.search(r'place_id=([A-Za-z0-9_-]+)', href)
    if match:
        return match.group(1)
    return ""


def _extract_browser_result_address(result_text, business_name=""):
    """Best-effort address extraction from a Google Maps result row."""
    text = " ".join((result_text or "").split())
    name = " ".join((business_name or "").split())
    if not text:
        return ""
    if name and text.lower().startswith(name.lower()):
        text = text[len(name):].strip()

    text = text.replace("·", " ").replace("•", " ").replace("", " ").replace("", " ")
    street_match = re.search(
        r'(\d{2,6}\s+[A-Za-z0-9.#\-\s]+?(?:Ave|Avenue|Rd|Road|St|Street|Dr|Drive|Blvd|Boulevard|Ln|Lane|Way|Ct|Court|Cir|Circle|Pl|Place|Pkwy|Parkway|Hwy|Highway)\b[^()]*)',
        text,
        re.IGNORECASE,
    )
    if street_match:
        return street_match.group(1).strip(" -,")

    city_match = re.search(r'([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,\s*[A-Z]{2}(?:\s+\d{5})?)', text)
    if city_match:
        return city_match.group(1).strip(" -,")
    return ""


def _normalize_browser_maps_result(raw_result):
    """Map a scraped Google Maps result row into the heatmap place shape."""
    name = " ".join((raw_result.get("name") or raw_result.get("aria") or "").split())
    href = raw_result.get("href", "")
    place_id = _extract_place_id_from_maps_href(href)
    text = raw_result.get("text", "")
    return {
        "displayName": {"text": name},
        "id": place_id,
        "formattedAddress": _extract_browser_result_address(text, business_name=name),
        "googleMapsUri": href,
        "source": "browser_maps",
    }


def _search_google_maps_page(page, keyword, lat, lng, radius_m=2000, max_results=20):
    """Search Google Maps in a headless browser and return the visible ranked pack."""
    radius_miles = max(float(radius_m or 0) / 1609.34, 0.1)
    zoom = _estimate_google_maps_zoom(radius_miles)
    url = f"https://www.google.com/maps/search/{quote(keyword, safe='')}/@{lat:.6f},{lng:.6f},{zoom}z?hl=en"
    diag = {"browser_maps": {"provider": "browser_maps", "url": url, "zoom": zoom}}

    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass

    try:
        page.locator('[role="article"]').first.wait_for(timeout=12000)
    except Exception:
        diag["browser_maps"]["title"] = page.title()
        diag["browser_maps"]["count"] = 0
        return [], diag

    feed = page.locator('[role="feed"]')
    last_count = 0
    for _ in range(4):
        current_count = page.locator('[role="article"]').count()
        if current_count >= max_results or current_count == last_count:
            break
        last_count = current_count
        if feed.count() > 0:
            try:
                feed.evaluate('(node) => { node.scrollTop = node.scrollHeight; }')
            except Exception:
                pass
        page.wait_for_timeout(700)

    raw_results = page.evaluate(
        r"""
() => Array.from(document.querySelectorAll('[role="article"]')).slice(0, 20).map((node, index) => {
  const anchor = node.querySelector('a[href*="/place/"]');
  return {
    rank: index + 1,
    name: (anchor?.getAttribute('aria-label') || anchor?.innerText || node.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim(),
    href: anchor?.href || '',
    aria: node.getAttribute('aria-label') || '',
    text: (node.innerText || '').replace(/\s+/g, ' ').trim(),
  };
}).filter((item) => item.name);
        """
    )

    places = [_normalize_browser_maps_result(row) for row in raw_results[:max_results]]
    diag["browser_maps"]["count"] = len(places)
    diag["browser_maps"]["title"] = page.title()
    diag["browser_maps"]["top_names"] = [place.get("displayName", {}).get("text", "") for place in places[:5]]
    return places, diag

def _search_places(api_key, keyword, lat, lng, radius_m=2000, brand_query=False, fallback_queries=None,
                   match_business_name=None, match_place_id=None, match_alternate_names=None,
                   target_place_ids=None):
    """Query Google Places APIs for a keyword near a point.
    Tries all available APIs and merges results (de-duplicated by place_id).
    This is critical because SABs often appear in Legacy/Nearby but not New API.
    Returns (places_list, diag_dict)."""
    diag = {"new_api": None, "legacy_api": None, "nearby_api": None, "find_place": None}
    all_places = []
    provider_places = []
    seen_ids = set()

    def _add_places(new_places, provider=None):
        """Merge new places into all_places, skipping duplicates by ID."""
        if provider:
            provider_places.append((provider, list(new_places or [])))
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
            _add_places(places, "new_api")
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
        _add_places(legacy_places, "legacy_api")
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
        _add_places(nearby_places, "nearby_api")
        diag["nearby_api"]["count"] = len(nearby_places)
    except Exception as exc:
        diag["nearby_api"] = {"error": str(exc)}
        log.warning("Nearby Search error at (%.4f, %.4f): %s", lat, lng, exc)

    if not all_places and brand_query:
        attempted = []
        queries = []
        for query in [keyword, *(fallback_queries or [])]:
            normalized = " ".join((query or "").strip().split())
            if normalized and normalized.lower() not in {item.lower() for item in queries}:
                queries.append(normalized)

        for fallback_query in queries:
            try:
                candidates, attempt = _find_place_candidates(api_key, fallback_query, lat=lat, lng=lng, use_bias=True)
                attempt["mode"] = "biased"
                attempted.append(attempt)
                _add_places(candidates, "find_place")
                if candidates:
                    break
                candidates, attempt = _find_place_candidates(api_key, fallback_query, lat=lat, lng=lng, use_bias=False)
                attempt["mode"] = "global"
                attempted.append(attempt)
                _add_places(candidates, "find_place")
                if candidates:
                    break
            except Exception as exc:
                attempted.append({"query": fallback_query, "error": str(exc)})

        diag["find_place"] = {
            "attempts": attempted,
            "count": len(all_places),
        }

    if match_business_name or match_place_id or target_place_ids:
        best_provider = None
        best_rank = 0
        best_places = None
        for provider, places in provider_places:
            rank = _match_business(
                places,
                match_business_name or "",
                match_place_id,
                alternate_names=match_alternate_names,
                target_place_ids=target_place_ids,
            )
            if rank and (not best_rank or rank < best_rank):
                best_provider = provider
                best_rank = rank
                best_places = places

        if best_places is not None:
            diag["selected_provider"] = {
                "provider": best_provider,
                "target_rank": best_rank,
                "reason": "target_match",
            }
            return best_places, diag

    return all_places, diag


def _tokenize(name):
    """Split a name into lowercase word tokens."""
    return set(re.findall(r'[a-z0-9]+', (name or "").lower()))


def _name_match_variants(name):
    """Generate stable name variants for franchise and compact-brand matching."""
    normalized = " ".join((name or "").strip().lower().split())
    if not normalized:
        return set()

    variants = set()
    raw_variants = {normalized}
    for separator in (" - ", " | ", " – ", " — ", ":"):
        if separator in normalized:
            raw_variants.add(normalized.split(separator, 1)[0].strip())
    if " of " in normalized:
        raw_variants.add(normalized.split(" of ", 1)[0].strip())

    for value in raw_variants:
        cleaned = re.sub(r"[^a-z0-9]+", " ", value).strip()
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        if not cleaned:
            continue
        variants.add(cleaned)
        compact = cleaned.replace(" ", "")
        if compact:
            variants.add(compact)
            if compact.endswith("s") and len(compact) >= 5:
                variants.add(compact[:-1])
    return variants


def _names_loosely_match(left, right):
    """Return True when two business names differ only by market suffixes or spacing."""
    left_variants = _name_match_variants(left)
    right_variants = _name_match_variants(right)
    if not left_variants or not right_variants:
        return False
    if left_variants & right_variants:
        return True
    for left_value in left_variants:
        for right_value in right_variants:
            if len(left_value) >= 5 and len(right_value) >= 5 and (
                left_value in right_value or right_value in left_value
            ):
                return True
    return False


def _normalize_place_id(value):
    """Normalize Google Place IDs so resource paths and raw IDs compare cleanly."""
    normalized = (value or "").strip()
    if not normalized:
        return ""
    if "/" in normalized:
        normalized = normalized.rsplit("/", 1)[-1]
    return normalized


def _matches_candidate_name(place_name, candidate_names):
    """Return True when a place name looks like one of the target listing names."""
    pname = (place_name or "").lower().strip()
    if not pname:
        return False

    stop_words = {'the', 'and', 'of', 'in', 'at', 'for', 'a', 'an', 'llc', 'inc', 'co'}
    ptokens = _tokenize(pname) - stop_words
    for name in candidate_names or []:
        cleaned_name = " ".join((name or "").strip().split()).lower()
        if not cleaned_name:
            continue
        if _names_loosely_match(cleaned_name, pname):
            return True
        if cleaned_name in pname or pname in cleaned_name:
            return True
        ntokens = _tokenize(cleaned_name) - stop_words
        if len(ntokens) >= 2:
            overlap = ntokens & ptokens
            if len(overlap) >= 2 and len(overlap) >= len(ntokens) * 0.5:
                return True
        elif len(ntokens) == 1:
            core = list(ntokens)[0]
            if len(core) >= 3 and any(core in token for token in ptokens):
                return True
    return False


def _extract_competitors(places, place_id=None, candidate_names=None, target_place_ids=None, limit=10):
    """Build a Local Falcon-style ranked pack for one grid point."""
    normalized_target_ids = {
        _normalize_place_id(value)
        for value in [place_id, *(target_place_ids or [])]
        if _normalize_place_id(value)
    }
    competitors = []
    for index, place in enumerate(places[:max(limit, 1)], 1):
        place_name = (place.get("displayName", {}).get("text", "") or "").strip()
        normalized_place_id = _normalize_place_id(place.get("id", ""))
        competitors.append({
            "rank": index,
            "name": place_name,
            "place_id": normalized_place_id,
            "address": (place.get("formattedAddress", "") or "").strip(),
            "is_target": bool(
                (normalized_place_id and normalized_place_id in normalized_target_ids) or
                _matches_candidate_name(place_name, candidate_names)
            ),
        })
    return competitors


def summarize_competitor_landscape(results):
    """Aggregate competitors across the full grid for leaderboard views."""
    summary = {}
    for cell in results or []:
        for competitor in cell.get("competitors") or []:
            if competitor.get("is_target"):
                continue
            key = competitor.get("place_id") or (competitor.get("name") or "").lower().strip()
            if not key:
                continue
            entry = summary.setdefault(key, {
                "place_id": competitor.get("place_id", ""),
                "name": competitor.get("name", "Unknown business"),
                "address": competitor.get("address", ""),
                "appearances": 0,
                "best_rank": 999,
                "rank_total": 0,
            })
            entry["appearances"] += 1
            entry["rank_total"] += int(competitor.get("rank") or 0)
            entry["best_rank"] = min(entry["best_rank"], int(competitor.get("rank") or 999))
            if not entry.get("address") and competitor.get("address"):
                entry["address"] = competitor["address"]

    leaderboard = []
    for entry in summary.values():
        appearances = max(entry.pop("appearances"), 1)
        rank_total = entry.pop("rank_total")
        entry["avg_rank"] = round(rank_total / appearances, 1)
        entry["grid_share"] = appearances
        leaderboard.append(entry)

    leaderboard.sort(key=lambda item: (-item["grid_share"], item["avg_rank"], item["best_rank"], item["name"].lower()))
    return leaderboard[:12]


def _match_business(places, business_name, place_id=None, alternate_names=None, target_place_ids=None):
    """Find the rank (1-based) of a business in Places results.
    Matches by place_id first, then exact substring, then word overlap,
    then fuzzy single-word match."""
    stop_words = {'the', 'and', 'of', 'in', 'at', 'for', 'a', 'an', 'llc', 'inc', 'co'}
    target_place_ids = {
        _normalize_place_id(value)
        for value in [place_id, *(target_place_ids or [])]
        if _normalize_place_id(value)
    }

    candidate_names = []
    seen_names = set()
    for name in [business_name, *(alternate_names or [])]:
        cleaned_name = " ".join((name or "").strip().split())
        if not cleaned_name:
            continue
        key = cleaned_name.lower()
        if key in seen_names:
            continue
        seen_names.add(key)
        candidate_names.append({
            "name": key,
            "tokens": _tokenize(cleaned_name) - stop_words,
        })

    for i, p in enumerate(places, 1):
        pid = _normalize_place_id(p.get("id", ""))
        pname = (p.get("displayName", {}).get("text", "") or "").lower().strip()
        ptokens = _tokenize(pname) - stop_words

        # Exact place_id match
        if pid and pid in target_place_ids:
            return i

        for candidate in candidate_names:
            bname = candidate["name"]
            btokens_clean = candidate["tokens"]

            if _names_loosely_match(bname, pname):
                return i

            # Substring match (either direction)
            if bname and (bname in pname or pname in bname):
                return i

            # Word overlap match: if 2+ significant words match, count it
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
              place_id=None, search_radius_m=2000, alternate_names=None, brand_query=False,
              target_place_ids=None):
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
    candidate_names = [business_name, *(alternate_names or [])]
    normalized_target_ids = {
        _normalize_place_id(value)
        for value in [place_id, *(target_place_ids or [])]
        if _normalize_place_id(value)
    }

    def _empty_result(pt):
        return {
            "row": pt["row"],
            "col": pt["col"],
            "lat": pt["lat"],
            "lng": pt["lng"],
            "rank": 0,
            "competitors": [],
        }

    def _search_point(page_obj, context_obj, pt):
        """Return ranked places for one grid point, switching providers when needed."""
        api_diag = {}
        rank_provider = "browser_maps"
        places = []

        if page_obj is not None and context_obj is not None:
            context_obj.set_geolocation({"latitude": pt["lat"], "longitude": pt["lng"]})
            places, api_diag = _search_google_maps_page(
                page_obj,
                keyword,
                pt["lat"],
                pt["lng"],
                radius_m=search_radius_m,
            )

        browser_rank = _match_business(
            places,
            business_name,
            place_id,
            alternate_names=alternate_names,
            target_place_ids=normalized_target_ids,
        )
        should_try_places = bool(api_key) and (not places or not browser_rank)

        if should_try_places:
            fallback_places, fallback_diag = _search_places(
                api_key,
                keyword,
                pt["lat"],
                pt["lng"],
                radius_m=search_radius_m,
                brand_query=brand_query,
                fallback_queries=candidate_names,
                match_business_name=business_name,
                match_place_id=place_id,
                match_alternate_names=alternate_names,
                target_place_ids=normalized_target_ids,
            )
            api_diag.update(fallback_diag)
            fallback_rank = _match_business(
                fallback_places,
                business_name,
                place_id,
                alternate_names=alternate_names,
                target_place_ids=normalized_target_ids,
            )
            if fallback_places and (not places or (not browser_rank and fallback_rank)):
                places = fallback_places
                rank_provider = api_diag.get("selected_provider", {}).get("provider") or "places_fallback"

        api_diag["rank_provider"] = rank_provider
        return places, api_diag
    browser = None
    context = None
    page = None
    browser_error = None
    try:
        import importlib
        sync_playwright = importlib.import_module("playwright.sync_api").sync_playwright
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            center_point = ordered[0][1] if ordered else {"lat": 0, "lng": 0}
            context = browser.new_context(
                viewport={"width": 1440, "height": 1200},
                locale="en-US",
                geolocation={"latitude": center_point["lat"], "longitude": center_point["lng"]},
                permissions=["geolocation"],
            )
            page = context.new_page()

            for seq, (idx, pt) in enumerate(ordered):
                if seq > 0:
                    time.sleep(0.35)
                try:
                    places, api_diag = _search_point(page, context, pt)
                except Exception as exc:
                    log.warning("scan_grid: browser search error at point %d (%.4f, %.4f): %s",
                                idx, pt["lat"], pt["lng"], exc)
                    browser_error = str(exc)
                    if api_key:
                        try:
                            places, api_diag = _search_places(
                                api_key,
                                keyword,
                                pt["lat"],
                                pt["lng"],
                                radius_m=search_radius_m,
                                brand_query=brand_query,
                                fallback_queries=candidate_names,
                                match_business_name=business_name,
                                match_place_id=place_id,
                                match_alternate_names=alternate_names,
                                target_place_ids=normalized_target_ids,
                            )
                            api_diag.setdefault("browser_maps", {"error": str(exc)})
                            api_diag["rank_provider"] = api_diag.get("selected_provider", {}).get("provider") or "places_fallback"
                        except Exception as fallback_exc:
                            log.warning("scan_grid: fallback API error at point %d (%.4f, %.4f): %s",
                                        idx, pt["lat"], pt["lng"], fallback_exc)
                            places, api_diag = [], {"browser_maps": {"error": str(exc)}, "error": str(fallback_exc)}
                            errors += 1
                    else:
                        places, api_diag = [], {"browser_maps": {"error": str(exc)}}
                        errors += 1

                rank = _match_business(
                    places,
                    business_name,
                    place_id,
                    alternate_names=alternate_names,
                    target_place_ids=normalized_target_ids,
                )
                competitors = _extract_competitors(
                    places,
                    place_id=place_id,
                    candidate_names=candidate_names,
                    target_place_ids=normalized_target_ids,
                )
                if seq == 0:
                    debug_sample = {
                        "business_name_used": business_name,
                        "place_id_used": place_id,
                        "target_place_ids_used": sorted(normalized_target_ids),
                        "alternate_names_used": alternate_names or [],
                        "brand_query": brand_query,
                        "search_radius_m": search_radius_m,
                        "rank_provider": api_diag.get("rank_provider"),
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
                    log.info("Heatmap debug (center) - matching '%s' (place_id=%s) | top results: %s | api_diag: %s",
                             business_name, place_id,
                             [r["name"] for r in debug_sample["top_results"]], api_diag)
                results[idx] = {
                    "row": pt["row"],
                    "col": pt["col"],
                    "lat": pt["lat"],
                    "lng": pt["lng"],
                    "rank": rank,
                    "competitors": competitors,
                }
    except Exception as exc:
        browser_error = str(exc)

    if browser is None and ordered and browser_error:
        raise RuntimeError(f"Google Maps browser engine unavailable: {browser_error}")

    if browser is None and ordered:
        for seq, (idx, pt) in enumerate(ordered):
            if seq > 0:
                time.sleep(0.35)
            try:
                places, api_diag = _search_places(
                    api_key,
                    keyword,
                    pt["lat"],
                    pt["lng"],
                    radius_m=search_radius_m,
                    brand_query=brand_query,
                    fallback_queries=candidate_names,
                    match_business_name=business_name,
                    match_place_id=place_id,
                    match_alternate_names=alternate_names,
                    target_place_ids=normalized_target_ids,
                )
                api_diag.setdefault("browser_maps", {"error": browser_error or "Playwright unavailable"})
                api_diag["rank_provider"] = api_diag.get("selected_provider", {}).get("provider") or "places_fallback"
            except Exception as exc:
                log.warning("scan_grid: API error at point %d (%.4f, %.4f): %s",
                            idx, pt["lat"], pt["lng"], exc)
                places, api_diag = [], {"browser_maps": {"error": browser_error or "Playwright unavailable"}, "error": str(exc)}
                errors += 1
            rank = _match_business(
                places,
                business_name,
                place_id,
                alternate_names=alternate_names,
                target_place_ids=normalized_target_ids,
            )
            competitors = _extract_competitors(
                places,
                place_id=place_id,
                candidate_names=candidate_names,
                target_place_ids=normalized_target_ids,
            )
            if seq == 0:
                debug_sample = {
                    "business_name_used": business_name,
                    "place_id_used": place_id,
                    "target_place_ids_used": sorted(normalized_target_ids),
                    "alternate_names_used": alternate_names or [],
                    "brand_query": brand_query,
                    "search_radius_m": search_radius_m,
                    "rank_provider": api_diag.get("rank_provider"),
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
            results[idx] = {
                "row": pt["row"],
                "col": pt["col"],
                "lat": pt["lat"],
                "lng": pt["lng"],
                "rank": rank,
                "competitors": competitors,
            }
    for idx, result in enumerate(results):
        if result is None and idx < len(grid_points):
            results[idx] = _empty_result(grid_points[idx])

    if errors:
        if debug_sample is None:
            debug_sample = {}
        debug_sample["api_errors"] = errors
        debug_sample["total_points"] = len(grid_points)
    return results, debug_sample
