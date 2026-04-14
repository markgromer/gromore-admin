import html
import logging
import re
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import requests


log = logging.getLogger(__name__)


COMMERCIAL_PROSPECT_TYPES = [
    {
        "key": "hoa",
        "label": "HOAs",
        "query": "HOA management companies in {location}",
    },
    {
        "key": "apartment",
        "label": "Apartment Complexes",
        "query": "apartment complexes in {location}",
    },
    {
        "key": "property_manager",
        "label": "Property Managers",
        "query": "property management companies in {location}",
    },
    {
        "key": "condo",
        "label": "Condo Associations",
        "query": "condo associations in {location}",
    },
    {
        "key": "commercial_real_estate",
        "label": "Commercial Real Estate",
        "query": "commercial property management in {location}",
    },
]


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


def _normalize_website(value):
    website = (value or "").strip()
    if not website:
        return ""
    parsed = urlparse(website if website.startswith(("http://", "https://")) else f"https://{website}")
    host = (parsed.netloc or parsed.path or "").lower().strip()
    path = parsed.path if parsed.netloc else ""
    return (host.rstrip("/") + path.rstrip("/")).strip()


def _clean_html_text(value):
    text = html.unescape(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_email_candidates(emails):
    cleaned = []
    seen = set()
    blocked_prefixes = ("noreply@", "no-reply@", "donotreply@")
    for raw in emails:
        email_value = (raw or "").strip().lower().strip(".,;:()[]{}<>")
        if not email_value or email_value in seen or any(email_value.startswith(prefix) for prefix in blocked_prefixes):
            continue
        seen.add(email_value)
        cleaned.append(email_value)
    return cleaned[:5]


def _extract_public_emails(website):
    normalized = _normalize_website(website)
    if not normalized:
        return []

    root_url = website if website.startswith(("http://", "https://")) else f"https://{website}"
    session = requests.Session()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GroMoreBot/1.0)"}
    urls_to_visit = [root_url]
    emails = []
    visited = set()
    candidate_paths = ["/contact", "/contact-us", "/about", "/management", "/leasing", "/team"]

    while urls_to_visit and len(visited) < 5 and len(emails) < 3:
        current = urls_to_visit.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            resp = session.get(current, timeout=10, allow_redirects=True, headers=headers)
        except Exception:
            continue
        if resp.status_code != 200 or "text/html" not in (resp.headers.get("Content-Type") or "text/html"):
            continue

        body = resp.text[:120000]
        emails.extend(EMAIL_RE.findall(body))

        for href in re.findall(r'href=["\']([^"\']+)["\']', body, flags=re.IGNORECASE):
            if href.startswith("mailto:"):
                emails.append(href.split(":", 1)[1].split("?", 1)[0])
                continue
            absolute = urljoin(resp.url, href)
            parsed = urlparse(absolute)
            if not parsed.netloc or _normalize_website(parsed.netloc) != _normalize_website(urlparse(resp.url).netloc):
                continue
            lowered_path = parsed.path.lower()
            if any(token in lowered_path for token in ("contact", "about", "management", "leasing", "team")) and absolute not in visited:
                urls_to_visit.append(absolute)

        if len(visited) == 1:
            for path in candidate_paths:
                next_url = urljoin(resp.url, path)
                if next_url not in visited:
                    urls_to_visit.append(next_url)

    return _clean_email_candidates(emails)


def _search_google_places(query, api_key, max_results):
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.formattedAddress,places.websiteUri,"
            "places.primaryType,places.types,places.nationalPhoneNumber,places.rating,"
            "places.userRatingCount,places.googleMapsUri"
        ),
        "Content-Type": "application/json",
    }
    payload = {"textQuery": query, "maxResultCount": max(1, min(int(max_results or 10), 20))}
    resp = requests.post(url, json=payload, headers=headers, timeout=20)
    if resp.status_code != 200:
        log.warning("Commercial places search failed for %s: %s", query, resp.status_code)
        return []
    return resp.json().get("places", [])


def _search_duckduckgo(query, max_results):
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    except Exception:
        return []
    if resp.status_code != 200:
        return []

    body = resp.text[:160000]
    matches = re.findall(
        r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>.*?(?:<a[^>]+class="result__snippet"[^>]*>(.*?)</a>|<div[^>]+class="result__snippet"[^>]*>(.*?)</div>)?',
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    results = []
    for href, title, snippet_a, snippet_b in matches[: max(1, min(int(max_results or 10), 15))]:
        resolved = href
        if "duckduckgo.com/l/?" in href:
            parsed = urlparse(href)
            uddg = parse_qs(parsed.query).get("uddg", [""])[0]
            if uddg:
                resolved = unquote(uddg)
        results.append(
            {
                "websiteUri": resolved,
                "displayName": {"text": _clean_html_text(title)},
                "formattedAddress": _clean_html_text(snippet_a or snippet_b or ""),
            }
        )
    return results


def _score_result(candidate):
    score = 20
    if candidate.get("emails"):
        score += 30
    if candidate.get("website"):
        score += 15
    if candidate.get("phone"):
        score += 10
    if candidate.get("address"):
        score += 10
    if candidate.get("rating"):
        try:
            score += min(int(float(candidate.get("rating")) * 2), 10)
        except Exception:
            pass
    return min(score, 100)


def _extract_site_snapshot(website):
    normalized = _normalize_website(website)
    if not normalized:
        return {}
    try:
        from webapp.competitor_intel import _scrape_website
    except Exception:
        return {}
    try:
        snapshot = _scrape_website({"website": website}) or {}
    except Exception:
        return {}
    return snapshot if isinstance(snapshot, dict) else {}


def search_commercial_prospects(location, prospect_types, *, api_key="", max_results_per_type=8):
    location = (location or "").strip()
    selected_keys = {key for key in (prospect_types or []) if key}
    if not location:
        return []

    selected_types = [item for item in COMMERCIAL_PROSPECT_TYPES if item["key"] in selected_keys] or COMMERCIAL_PROSPECT_TYPES[:3]
    lookup = {}

    for item in selected_types:
        query = item["query"].format(location=location)
        raw_results = _search_google_places(query, api_key, max_results_per_type) if api_key else _search_duckduckgo(query, max_results_per_type)
        for raw in raw_results:
            business_name = ((raw.get("displayName") or {}).get("text") or "").strip()
            website = (raw.get("websiteUri") or "").strip()
            key = raw.get("id") or _normalize_website(website) or business_name.lower()
            if not key:
                continue
            existing = lookup.get(key, {})
            emails = existing.get("emails") or []
            if website and not emails:
                emails = _extract_public_emails(website)
            audit_snapshot = existing.get("audit_snapshot") or {}
            if website and not audit_snapshot:
                audit_snapshot = _extract_site_snapshot(website)
            merged = {
                "business_name": business_name or existing.get("business_name") or "Unknown Prospect",
                "contact_name": existing.get("contact_name") or business_name,
                "website": website or existing.get("website") or "",
                "address": (raw.get("formattedAddress") or existing.get("address") or "").strip(),
                "phone": (raw.get("nationalPhoneNumber") or existing.get("phone") or "").strip(),
                "rating": raw.get("rating") if raw.get("rating") is not None else existing.get("rating"),
                "review_count": raw.get("userRatingCount") if raw.get("userRatingCount") is not None else existing.get("review_count"),
                "maps_url": (raw.get("googleMapsUri") or existing.get("maps_url") or "").strip(),
                "emails": emails,
                "prospect_type": item["key"],
                "prospect_type_label": item["label"],
                "service_area": location,
                "source_query": query,
                "audit_snapshot": audit_snapshot,
            }
            merged["score"] = _score_result(merged)
            lookup[key] = merged

    return sorted(lookup.values(), key=lambda row: (-int(row.get("score") or 0), row.get("business_name") or ""))