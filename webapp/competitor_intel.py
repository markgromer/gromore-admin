"""
Competitor intelligence scraping: Google Places, Meta Ad Library, website basics.
All results are cached in the competitor_intel table with a 7-day refresh window.
"""

import json
import logging
import os
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

        def _clean_text(value: str, max_len: int) -> str:
            import re
            import html as _html

            value = value or ""
            # Some sites double-escape entities (e.g. &amp;#x200f;). Unescape a few times.
            for _ in range(3):
                unescaped = _html.unescape(value)
                if unescaped == value:
                    break
                value = unescaped
            # Strip common invisible directional marks that sometimes appear in SEO meta.
            value = value.replace("\u200e", "").replace("\u200f", "")
            value = re.sub(r"\s+", " ", value).strip()
            return value[:max_len]

        # Extract <title>
        import re
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        if title_match:
            title = _clean_text(title_match.group(1), 200)

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
            description = _clean_text(desc_match.group(1), 500)

        # Extract a few headings for higher-signal positioning clues.
        h1s = []
        for m in re.finditer(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL):
            txt = _clean_text(re.sub(r"<[^>]+>", " ", m.group(1)), 120)
            if txt:
                h1s.append(txt)
            if len(h1s) >= 3:
                break

        h2s = []
        for m in re.finditer(r"<h2[^>]*>(.*?)</h2>", html, re.IGNORECASE | re.DOTALL):
            txt = _clean_text(re.sub(r"<[^>]+>", " ", m.group(1)), 120)
            if txt:
                h2s.append(txt)
            if len(h2s) >= 5:
                break

        return {
            "url": resp.url,
            "status": resp.status_code,
            "title": title,
            "description": description,
            "h1": h1s,
            "h2": h2s,
        }
    except Exception as exc:
        return {"url": website, "error": str(exc)[:200]}


def _generate_competitor_research(*, api_key: str, model: str, brand: dict, competitor: dict, intel: dict) -> dict:
    """Generate a structured research + counter-moves brief from already-fetched intel.

    This must not invent competitor claims. It should base observations on the provided intel.
    """
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    # Keep payload compact.
    payload = {
        "brand": {
            "name": brand.get("name"),
            "industry": brand.get("industry"),
            "website": brand.get("website"),
            "service_area": brand.get("service_area"),
            "primary_services": brand.get("primary_services"),
            "active_offers": brand.get("active_offers"),
            "brand_voice": brand.get("brand_voice"),
        },
        "competitor": {
            "name": competitor.get("name"),
            "website": competitor.get("website"),
            "google_maps_url": competitor.get("google_maps_url"),
            "facebook_url": competitor.get("facebook_url"),
            "instagram_url": competitor.get("instagram_url"),
            "yelp_url": competitor.get("yelp_url"),
            "notes": competitor.get("notes"),
        },
        "intel": {
            "google_places": intel.get("google_places") or {},
            "meta_ads": intel.get("meta_ads") or {},
            "website": intel.get("website") or {},
        },
        "output_schema": {
            "positioning_summary": "string, 2-4 sentences. Must cite only provided intel.",
            "observed_offers": ["string"],
            "observed_services": ["string"],
            "messaging_angles": ["string"],
            "proof_points": ["string"],
            "counter_moves": [
                {
                    "move": "string",
                    "why": "string",
                    "how": "string",
                }
            ],
            "conquest_campaign_notes": ["string"],
            "landing_page_opportunities": ["string"],
            "data_gaps": ["string"],
        },
    }

    system = (
        "You are a senior paid media + conversion strategist. "
        "Your job is to produce competitor research and counter-moves that can be acted on. "
        "CRITICAL RULE: Do not invent facts about the competitor. Only use what is explicitly in the input. "
        "If the input does not contain enough evidence, leave arrays empty and add a note to data_gaps. "
        "Return ONLY valid JSON matching the output_schema. No markdown. No extra keys."
    )

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(payload)},
            ],
            "response_format": {"type": "json_object"},
        },
        timeout=60,
    )

    if resp.status_code != 200:
        raise ValueError(f"OpenAI request failed ({resp.status_code}): {resp.text}")

    content = (((resp.json().get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
    content = (content or "").strip()
    if not content:
        raise ValueError("Empty AI response")

    try:
        out = json.loads(content)
    except Exception:
        # Fallback: try to find the first JSON object.
        import re
        m = re.search(r"\{[\s\S]*\}", content)
        if not m:
            raise
        out = json.loads(m.group(0))

    # Normalize keys we expect.
    out.setdefault("observed_offers", [])
    out.setdefault("observed_services", [])
    out.setdefault("messaging_angles", [])
    out.setdefault("proof_points", [])
    out.setdefault("counter_moves", [])
    out.setdefault("conquest_campaign_notes", [])
    out.setdefault("landing_page_opportunities", [])
    out.setdefault("data_gaps", [])
    return out


# ── Public API: refresh competitor intel ─────────────────────────

def refresh_competitor_intel(db, brand, competitor, *, force: bool = False):
    """Refresh all intel for a single competitor.

    When force=True, bypass the stale window (used for manual "Scan").
    Returns dict of results.
    """
    brand_id = brand["id"]
    comp_id = competitor["id"]
    results = {}

    # Google Places
    existing = db.get_competitor_intel(comp_id, "google_places")
    if force or (not existing) or _is_stale(existing.get("fetched_at")):
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
    if force or (not existing) or _is_stale(existing.get("fetched_at")):
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
    if force or (not existing) or _is_stale(existing.get("fetched_at")):
        site_data = _scrape_website(competitor)
        if site_data:
            db.upsert_competitor_intel(comp_id, brand_id, "website", json.dumps(site_data))
            results["website"] = site_data
        elif existing:
            results["website"] = json.loads(existing.get("data_json", "{}"))
    elif existing:
        results["website"] = json.loads(existing.get("data_json", "{}"))

    # AI research + counter moves (optional)
    existing = db.get_competitor_intel(comp_id, "research")
    should_generate = force or (not existing) or _is_stale(existing.get("fetched_at"))
    if should_generate:
        # Brand-level key/model first, then app-level settings/env.
        api_key = ((brand.get("openai_api_key") or "").strip() or db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip())
        if api_key:
            model = (
                (brand.get("openai_model_analysis") or "").strip()
                or (brand.get("openai_model") or "").strip()
                or db.get_setting("openai_model", "").strip()
                or os.environ.get("OPENAI_MODEL", "").strip()
                or "gpt-4o-mini"
            )
            try:
                research_data = _generate_competitor_research(
                    api_key=api_key,
                    model=model,
                    brand=brand,
                    competitor=competitor,
                    intel=results,
                )
                db.upsert_competitor_intel(comp_id, brand_id, "research", json.dumps(research_data))
                results["research"] = research_data
            except Exception as exc:
                log.warning("Competitor research generation failed for %s: %s", competitor.get("name"), exc)
                if existing:
                    try:
                        results["research"] = json.loads(existing.get("data_json", "{}"))
                    except Exception:
                        pass
        elif existing:
            try:
                results["research"] = json.loads(existing.get("data_json", "{}"))
            except Exception:
                pass
    elif existing:
        try:
            results["research"] = json.loads(existing.get("data_json", "{}"))
        except Exception:
            pass

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

    # Defensive cleanup for older cached website blobs that may contain HTML entities.
    website = report.get("website")
    if isinstance(website, dict):
        import re
        import html as _html

        def _clean_cached_text(value: str, max_len: int) -> str:
            value = value or ""
            for _ in range(3):
                unescaped = _html.unescape(value)
                if unescaped == value:
                    break
                value = unescaped
            value = value.replace("\u200e", "").replace("\u200f", "")
            value = re.sub(r"\s+", " ", value).strip()
            return value[:max_len]

        if website.get("title"):
            website["title"] = _clean_cached_text(str(website.get("title")), 200)
        if website.get("description"):
            website["description"] = _clean_cached_text(str(website.get("description")), 500)
        if isinstance(website.get("h1"), list):
            website["h1"] = [_clean_cached_text(str(x), 120) for x in website.get("h1") if x]
        if isinstance(website.get("h2"), list):
            website["h2"] = [_clean_cached_text(str(x), 120) for x in website.get("h2") if x][:10]

    return report
