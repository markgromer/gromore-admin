"""
Competitor intelligence scraping: Google Places, Meta Ad Library, website basics.
All results are cached in the competitor_intel table with a 7-day refresh window.
"""

import json
import logging
from datetime import datetime, timedelta

import requests

log = logging.getLogger(__name__)

_STALE_DAYS = 7


def _is_stale(fetched_at_str):
    if not fetched_at_str:
        return True
    try:
        fetched = datetime.fromisoformat(fetched_at_str)
        return datetime.utcnow() - fetched > timedelta(days=_STALE_DAYS)
    except (ValueError, TypeError):
        return True


# ── Google Places ────────────────────────────────────────────────

def _scrape_google_places(competitor, api_key):
    """Search Google Places for the competitor, return ratings/reviews/category."""
    name = competitor.get("name", "")
    if not name or not api_key:
        return None

    # Try the New Places API first
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": (
            "places.displayName,places.id,places.rating,"
            "places.userRatingCount,places.types,"
            "places.formattedAddress,places.websiteUri,"
            "places.currentOpeningHours,places.priceLevel,"
            "places.googleMapsUri"
        ),
        "Content-Type": "application/json",
    }
    body = {"textQuery": name, "maxResultCount": 1}

    try:
        resp = requests.post(url, json=body, headers=headers, timeout=15)
        if resp.status_code == 200:
            places = resp.json().get("places", [])
            if places:
                p = places[0]
                return {
                    "name": (p.get("displayName") or {}).get("text", ""),
                    "place_id": p.get("id", ""),
                    "rating": p.get("rating"),
                    "review_count": p.get("userRatingCount"),
                    "types": p.get("types", []),
                    "address": p.get("formattedAddress", ""),
                    "website": p.get("websiteUri", ""),
                    "maps_url": p.get("googleMapsUri", ""),
                    "price_level": p.get("priceLevel", ""),
                }
        else:
            log.warning("Places API returned %s for %s", resp.status_code, name)
    except Exception as exc:
        log.warning("Places API error for %s: %s", name, exc)

    return None


# ── Meta Ad Library ──────────────────────────────────────────────

def _scrape_meta_ads(competitor, meta_token):
    """Query the Meta Ad Library for active ads by this competitor's page."""
    page_name = competitor.get("name", "")
    if not page_name or not meta_token:
        return None

    url = "https://graph.facebook.com/v21.0/ads_archive"
    params = {
        "access_token": meta_token,
        "search_terms": page_name,
        "ad_reached_countries": '["US"]',
        "ad_active_status": "ACTIVE",
        "fields": "ad_creative_bodies,ad_creative_link_titles,ad_delivery_start_time,page_name,publisher_platforms",
        "limit": 25,
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            ads = []
            for ad in data[:25]:
                ads.append({
                    "bodies": ad.get("ad_creative_bodies", []),
                    "titles": ad.get("ad_creative_link_titles", []),
                    "start_date": ad.get("ad_delivery_start_time", ""),
                    "page_name": ad.get("page_name", ""),
                    "platforms": ad.get("publisher_platforms", []),
                })
            return {
                "active_ad_count": len(data),
                "sample_ads": ads[:10],
            }
        else:
            log.warning("Meta Ad Library returned %s for %s", resp.status_code, page_name)
    except Exception as exc:
        log.warning("Meta Ad Library error for %s: %s", page_name, exc)

    return None


# ── Website basics ───────────────────────────────────────────────

def _scrape_website(competitor):
    """Fetch competitor's website and extract basic meta info."""
    website = (competitor.get("website") or "").strip()
    if not website:
        return None
    if not website.startswith(("http://", "https://")):
        website = "https://" + website

    try:
        resp = requests.get(website, timeout=15, allow_redirects=True,
                            headers={"User-Agent": "Mozilla/5.0 (compatible; GroMoreBot/1.0)"})
        if resp.status_code != 200:
            return {"url": website, "status": resp.status_code, "error": "Non-200 response"}

        html = resp.text[:50000]
        title = ""
        description = ""

        # Extract <title>
        import re
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        if title_match:
            title = title_match.group(1).strip()[:200]

        # Extract meta description
        desc_match = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
            html, re.IGNORECASE | re.DOTALL,
        )
        if not desc_match:
            desc_match = re.search(
                r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
                html, re.IGNORECASE | re.DOTALL,
            )
        if desc_match:
            description = desc_match.group(1).strip()[:500]

        return {
            "url": resp.url,
            "status": resp.status_code,
            "title": title,
            "description": description,
        }
    except Exception as exc:
        return {"url": website, "error": str(exc)[:200]}


# ── Public API: refresh competitor intel ─────────────────────────

def refresh_competitor_intel(db, brand, competitor):
    """Refresh all intel for a single competitor. Returns dict of results."""
    brand_id = brand["id"]
    comp_id = competitor["id"]
    results = {}

    # Google Places
    existing = db.get_competitor_intel(comp_id, "google_places")
    if not existing or _is_stale(existing.get("fetched_at")):
        api_key = (brand.get("google_maps_api_key") or "").strip()
        places_data = _scrape_google_places(competitor, api_key)
        if places_data:
            db.upsert_competitor_intel(comp_id, brand_id, "google_places", json.dumps(places_data))
            results["google_places"] = places_data
        elif existing:
            results["google_places"] = json.loads(existing.get("data_json", "{}"))
    elif existing:
        results["google_places"] = json.loads(existing.get("data_json", "{}"))

    # Meta Ad Library
    existing = db.get_competitor_intel(comp_id, "meta_ads")
    if not existing or _is_stale(existing.get("fetched_at")):
        from webapp.api_bridge import _get_meta_token
        connections = db.get_brand_connections(brand_id)
        meta_conn = connections.get("meta")
        meta_token = None
        if meta_conn and meta_conn.get("status") == "connected":
            meta_token = _get_meta_token(db, brand_id, meta_conn)
        ads_data = _scrape_meta_ads(competitor, meta_token)
        if ads_data:
            db.upsert_competitor_intel(comp_id, brand_id, "meta_ads", json.dumps(ads_data))
            results["meta_ads"] = ads_data
        elif existing:
            results["meta_ads"] = json.loads(existing.get("data_json", "{}"))
    elif existing:
        results["meta_ads"] = json.loads(existing.get("data_json", "{}"))

    # Website
    existing = db.get_competitor_intel(comp_id, "website")
    if not existing or _is_stale(existing.get("fetched_at")):
        site_data = _scrape_website(competitor)
        if site_data:
            db.upsert_competitor_intel(comp_id, brand_id, "website", json.dumps(site_data))
            results["website"] = site_data
        elif existing:
            results["website"] = json.loads(existing.get("data_json", "{}"))
    elif existing:
        results["website"] = json.loads(existing.get("data_json", "{}"))

    return results


def get_competitor_report(db, brand, competitor):
    """Get cached intel for a competitor without refreshing."""
    comp_id = competitor["id"]
    all_intel = db.get_competitor_intel(comp_id)
    report = {"competitor": competitor}
    for row in all_intel:
        try:
            report[row["intel_type"]] = json.loads(row.get("data_json", "{}"))
        except (json.JSONDecodeError, TypeError):
            report[row["intel_type"]] = {}
        report[row["intel_type"] + "_fetched"] = row.get("fetched_at", "")
    return report
