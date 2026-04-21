"""
Competitor intelligence scraping: Google Places, Meta Ad Library, website basics.
All results are cached in the competitor_intel table with a 7-day refresh window.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

_STALE_DAYS = 7
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
_PRICE_PAGE_HINTS = (
    "price", "pricing", "cost", "special", "coupon", "offer",
    "financing", "membership", "service", "services",
)
_PRICE_CUE_WORDS = (
    "price", "pricing", "starting at", "from $", "only $", "special", "coupon",
    "offer", "financing", "membership", "per month", "/mo", "free estimate",
    "free inspection", "discount", "off", "flat rate", "upfront",
)
_COMMON_SERVICE_KEYWORDS = (
    "repair", "replacement", "installation", "cleaning", "drain", "camera inspection",
    "inspection", "maintenance", "service", "emergency", "tune-up", "tune up",
    "water heater", "toilet", "sewer", "pipe", "leak", "faucet", "hvac", "ac",
    "air conditioning", "heating", "roof", "termite", "lawn", "mow", "garage door",
)
_MONEY_RANGE_RE = re.compile(
    r"\$(\d{1,4}(?:,\d{3})?(?:\.\d{2})?)\s*(?:-|to)\s*\$?(\d{1,4}(?:,\d{3})?(?:\.\d{2})?)",
    re.IGNORECASE,
)
_MONEY_RE = re.compile(r"\$(\d{1,4}(?:,\d{3})?(?:\.\d{2})?)")
_PERCENT_OFF_RE = re.compile(r"\b(\d{1,2})\s*%\s*off\b", re.IGNORECASE)
_PRICE_UNIT_RE = re.compile(r"(?:/|per )\s*(hour|hr|month|mo|visit|service|year|yr)\b", re.IGNORECASE)
_FREE_OFFER_RE = re.compile(r"\bfree\s+(estimate|inspection|quote|consultation)\b", re.IGNORECASE)


def _is_stale(fetched_at_str):
    if not fetched_at_str:
        return True
    try:
        fetched = datetime.fromisoformat(fetched_at_str)
        return datetime.utcnow() - fetched > timedelta(days=_STALE_DAYS)
    except (ValueError, TypeError):
        return True


def _normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _normalize_host(url: str) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    host = urlparse(normalized).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _extract_gbp_cid(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    match = re.search(r"[?&]cid=(\d{8,32})", raw)
    if match:
        return match.group(1)
    lowered = raw.lower()
    if lowered.startswith("cid"):
        digits = re.sub(r"\D", "", raw)
        return digits[:32]
    if re.fullmatch(r"\d{8,32}", raw):
        return raw
    return ""


def _build_google_places_queries(competitor: dict) -> List[str]:
    queries: List[str] = []
    name = (competitor.get("name") or "").strip()
    website_host = _normalize_host(competitor.get("website") or "")
    maps_url = _normalize_url(competitor.get("google_maps_url") or "")
    gbp_cid = _extract_gbp_cid(competitor.get("gbp_cid") or maps_url)

    for query in (
        maps_url if gbp_cid else "",
        f"https://www.google.com/maps?cid={gbp_cid}" if gbp_cid else "",
        f"{name} {website_host}" if name and website_host else "",
        name,
    ):
        cleaned = (query or "").strip()
        if cleaned and cleaned not in queries:
            queries.append(cleaned)
    return queries[:4]


def _score_google_places_candidate(competitor: dict, place: dict) -> tuple[float, List[str]]:
    score = 0.0
    reasons: List[str] = []
    competitor_name = _normalize_name(competitor.get("name") or "")
    place_name = _normalize_name(((place.get("displayName") or {}).get("text") or ""))
    competitor_host = _normalize_host(competitor.get("website") or "")
    place_host = _normalize_host(place.get("websiteUri") or "")
    competitor_maps_url = _normalize_url(competitor.get("google_maps_url") or "")
    place_maps_url = _normalize_url(place.get("googleMapsUri") or "")
    competitor_cid = _extract_gbp_cid(competitor.get("gbp_cid") or competitor_maps_url)
    place_cid = _extract_gbp_cid(place_maps_url)

    if competitor_cid and place_cid == competitor_cid:
        score += 12.0
        reasons.append("Exact GBP CID match")
    if competitor_maps_url and place_maps_url and competitor_maps_url.rstrip("/") == place_maps_url.rstrip("/"):
        score += 9.0
        reasons.append("Exact Google Maps URL match")
    if competitor_host and place_host and competitor_host == place_host:
        score += 4.0
        reasons.append("Website host match")
    if competitor_name and place_name:
        if competitor_name == place_name:
            score += 6.0
            reasons.append("Exact business name match")
        elif competitor_name in place_name or place_name in competitor_name:
            score += 3.0
            reasons.append("Close business name match")

    review_count = float(place.get("userRatingCount") or 0)
    rating = float(place.get("rating") or 0)
    score += min(review_count / 500.0, 1.0)
    score += min(rating / 10.0, 0.5)

    if not reasons:
        reasons.append("Best available Places candidate")
    return round(score, 3), reasons


def _fetch_page(url: str) -> Dict[str, Any]:
    """Fetch a public page with an SSL fallback for weak cert chains."""
    page_url = _normalize_url(url)
    if not page_url:
        return {"url": "", "error": "Missing URL"}

    session = requests.Session()
    session.headers.update(_BROWSER_HEADERS)
    try:
        resp = session.get(page_url, timeout=15, allow_redirects=True)
    except requests.exceptions.SSLError:
        try:
            requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
            resp = session.get(page_url, timeout=15, allow_redirects=True, verify=False)
        except Exception as exc:
            return {"url": page_url, "error": f"SSL fallback failed: {exc}"}
    except Exception as exc:
        return {"url": page_url, "error": str(exc)[:200]}

    content_type = (resp.headers.get("Content-Type") or "").lower()
    if resp.status_code >= 400:
        return {"url": resp.url or page_url, "status": resp.status_code, "error": f"HTTP {resp.status_code}"}
    if "html" not in content_type and "xml" not in content_type and resp.text[:50].lstrip()[:1] != "<":
        return {
            "url": resp.url or page_url,
            "status": resp.status_code,
            "error": f"Unsupported content type: {content_type or 'unknown'}",
        }

    return {
        "url": resp.url or page_url,
        "status": resp.status_code,
        "html": resp.text[:250000],
    }


def _clean_text(value: str, max_len: int = 300) -> str:
    value = value or ""
    value = re.sub(r"\s+", " ", value).strip()
    return value[:max_len]


def _search_public_results(query: str, *, max_results: int = 6) -> List[Dict[str, str]]:
    """Use DDG HTML search for public discovery URLs."""
    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers=_BROWSER_HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        results: List[Dict[str, str]] = []
        for anchor in soup.select("a.result__a"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            if "uddg=" in href:
                from urllib.parse import parse_qs, unquote
                parsed = urlparse(href)
                href = unquote(parse_qs(parsed.query).get("uddg", [href])[0])
            snippet_el = anchor.find_parent(class_="result")
            snippet = ""
            if snippet_el:
                snippet_node = snippet_el.select_one(".result__snippet")
                if snippet_node:
                    snippet = _clean_text(snippet_node.get_text(" ", strip=True), 220)
            results.append({
                "url": href,
                "title": _clean_text(anchor.get_text(" ", strip=True), 120),
                "snippet": snippet,
            })
            if len(results) >= max_results:
                break
        return results
    except Exception as exc:
        log.warning("Public pricing search failed for %s: %s", query, exc)
        return []


def _service_terms_for_brand(brand: dict) -> List[str]:
    raw = (brand.get("primary_services") or "").strip()
    parts: List[str] = []
    for chunk in re.split(r"[\n,;/|]+", raw):
        cleaned = _clean_text(chunk, 60)
        if cleaned:
            parts.append(cleaned)
    return parts[:8]


def _extract_candidate_links(page_url: str, html: str, service_terms: List[str]) -> List[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    base_host = urlparse(page_url).netloc.lower()
    scored: List[tuple[int, str]] = []
    service_terms_lower = [s.lower() for s in service_terms]

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        target = urljoin(page_url, href)
        parsed = urlparse(target)
        if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() != base_host:
            continue
        text = _clean_text(anchor.get_text(" ", strip=True), 120)
        blob = f"{parsed.path.lower()} {text.lower()}"
        score = 0
        if any(hint in blob for hint in _PRICE_PAGE_HINTS):
            score += 3
        if any(term in blob for term in service_terms_lower):
            score += 2
        if any(term in blob for term in _COMMON_SERVICE_KEYWORDS):
            score += 1
        if score <= 0:
            continue
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path or '/'}"
        if parsed.query:
            clean_url += f"?{parsed.query}"
        scored.append((score, clean_url))

    ordered: List[str] = []
    for _, link in sorted(scored, key=lambda item: (-item[0], item[1])):
        if link not in ordered:
            ordered.append(link)
        if len(ordered) >= 8:
            break
    return ordered


def _discover_pricing_targets(brand: dict, competitor: dict) -> List[Dict[str, str]]:
    competitor_name = competitor.get("name", "")
    service_area = (brand.get("service_area") or "").strip()
    service_terms = _service_terms_for_brand(brand)
    website = _normalize_url(competitor.get("website", ""))
    targets: List[Dict[str, str]] = []

    def add_target(url: str, source: str, query: str = ""):
        normalized = _normalize_url(url)
        if not normalized:
            return
        if normalized not in {t["url"] for t in targets}:
            targets.append({"url": normalized, "source": source, "query": query})

    if website:
        homepage = _fetch_page(website)
        if homepage.get("html"):
            add_target(homepage.get("url", website), "website_home")
            for link in _extract_candidate_links(homepage.get("url", website), homepage.get("html", ""), service_terms):
                add_target(link, "website_link")
        else:
            add_target(website, "website_home")

    queries = [
        f'"{competitor_name}" pricing',
        f'"{competitor_name}" coupons',
        f'"{competitor_name}" "starting at"',
    ]
    if service_area:
        queries.append(f'"{competitor_name}" {service_area} pricing')
    for term in service_terms[:4]:
        queries.append(f'"{competitor_name}" "{term}" price')

    website_host = urlparse(website).netloc.lower() if website else ""
    for query in queries[:8]:
        for result in _search_public_results(query, max_results=5):
            candidate_url = result.get("url", "")
            if not candidate_url.startswith(("http://", "https://")):
                continue
            candidate_host = urlparse(candidate_url).netloc.lower()
            if website_host and candidate_host and website_host not in candidate_host and candidate_host not in website_host:
                title_blob = f"{result.get('title', '')} {result.get('snippet', '')}".lower()
                if competitor_name.lower() not in title_blob:
                    continue
            add_target(candidate_url, "search_result", query=query)
            if len(targets) >= 10:
                return targets[:10]
    return targets[:10]


def _extract_service_label(text: str, heading: str, service_terms: List[str]) -> str:
    combined = f"{heading} {text}".lower()
    for term in service_terms:
        if term.lower() in combined:
            return term
    for term in _COMMON_SERVICE_KEYWORDS:
        if term in combined:
            return term.title()
    if heading and heading.lower() not in {"pricing", "specials", "offers"}:
        return heading[:80]
    return "General Service"


def _score_price_hit(*, source_url: str, text: str, heading: str, service_label: str, price_type: str) -> float:
    score = 0.45
    blob = f"{text} {heading}".lower()
    if "$" in text:
        score += 0.15
    if any(word in blob for word in _PRICE_CUE_WORDS):
        score += 0.12
    if heading:
        score += 0.06
    if service_label and service_label != "General Service":
        score += 0.10
    if any(hint in (source_url or "").lower() for hint in _PRICE_PAGE_HINTS):
        score += 0.15
    if price_type != "flat_rate":
        score += 0.05
    return round(max(0.25, min(score, 0.98)), 2)


def _detect_price_type(text: str, amount_min: Optional[float], amount_max: Optional[float]) -> str:
    lower = (text or "").lower()
    if _FREE_OFFER_RE.search(lower):
        return "free_offer"
    if _PERCENT_OFF_RE.search(lower):
        return "discount_percent"
    if "membership" in lower or "/month" in lower or "per month" in lower or "/mo" in lower:
        return "membership"
    if "/hour" in lower or "/hr" in lower or "per hour" in lower:
        return "hourly"
    if amount_max is not None and amount_max > (amount_min or 0):
        return "range"
    if "starting at" in lower or "from $" in lower or "as low as" in lower:
        return "starting_at"
    if "inspection" in lower and amount_min:
        return "inspection_fee"
    if "discount" in lower or "coupon" in lower or "off" in lower:
        return "discount"
    if "estimate" in lower and amount_min == 0:
        return "free_estimate"
    return "flat_rate"


def _dedupe_price_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for item in sorted(items, key=lambda x: (-float(x.get("confidence", 0)), x.get("source_url", ""), x.get("service", ""), float(x.get("amount_min") or 0))):
        key = (
            item.get("source_url", ""),
            item.get("service", ""),
            item.get("price_type", ""),
            item.get("amount_min"),
            item.get("amount_max"),
            item.get("snippet", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _extract_structured_price_mentions(page_url: str, html: str, service_terms: List[str]) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    container = soup.find("main") or soup.find("article") or soup.body or soup
    for tag in container(["script", "style", "noscript", "svg"]):
        tag.decompose()

    items: List[Dict[str, Any]] = []
    heading = ""
    seen_blocks = set()

    for node in container.find_all(["h1", "h2", "h3", "h4", "p", "li", "td", "div"]):
        if node.name == "div" and node.find(["p", "li", "h1", "h2", "h3", "h4", "td"], recursive=False):
            continue
        text = _clean_text(node.get_text(" ", strip=True), 260)
        if len(text) < 6 or text in seen_blocks:
            continue
        seen_blocks.add(text)

        if node.name in {"h1", "h2", "h3", "h4"}:
            heading = text
            continue

        lower_text = text.lower()
        if "$" not in text and not _FREE_OFFER_RE.search(lower_text) and not _PERCENT_OFF_RE.search(lower_text):
            continue
        if len(re.findall(r"\d", text)) > 18 and "$" not in text:
            continue

        service_label = _extract_service_label(text, heading, service_terms)

        for match in _MONEY_RANGE_RE.finditer(text):
            amount_min = float(match.group(1).replace(",", ""))
            amount_max = float(match.group(2).replace(",", ""))
            price_type = _detect_price_type(text, amount_min, amount_max)
            snippet = _clean_text(f"{heading} | {text}" if heading else text, 220)
            items.append({
                "service": service_label,
                "price_type": price_type,
                "amount_min": amount_min,
                "amount_max": amount_max,
                "currency": "USD",
                "source_url": page_url,
                "source_title": heading[:120] if heading else "",
                "snippet": snippet,
                "confidence": _score_price_hit(
                    source_url=page_url,
                    text=text,
                    heading=heading,
                    service_label=service_label,
                    price_type=price_type,
                ),
            })

        range_spans = {(m.start(), m.end()) for m in _MONEY_RANGE_RE.finditer(text)}
        for match in _MONEY_RE.finditer(text):
            if any(start <= match.start() and match.end() <= end for start, end in range_spans):
                continue
            amount = float(match.group(1).replace(",", ""))
            unit_match = _PRICE_UNIT_RE.search(lower_text)
            price_type = _detect_price_type(text, amount, None)
            snippet = _clean_text(f"{heading} | {text}" if heading else text, 220)
            item = {
                "service": service_label,
                "price_type": price_type,
                "amount_min": amount,
                "amount_max": None,
                "currency": "USD",
                "source_url": page_url,
                "source_title": heading[:120] if heading else "",
                "snippet": snippet,
                "confidence": _score_price_hit(
                    source_url=page_url,
                    text=text,
                    heading=heading,
                    service_label=service_label,
                    price_type=price_type,
                ),
            }
            if unit_match:
                item["unit"] = unit_match.group(1).lower()
            items.append(item)

        free_match = _FREE_OFFER_RE.search(lower_text)
        if free_match:
            price_type = "free_offer"
            snippet = _clean_text(f"{heading} | {text}" if heading else text, 220)
            items.append({
                "service": service_label,
                "price_type": price_type,
                "amount_min": 0.0,
                "amount_max": None,
                "currency": "USD",
                "source_url": page_url,
                "source_title": heading[:120] if heading else "",
                "snippet": snippet,
                "confidence": _score_price_hit(
                    source_url=page_url,
                    text=text,
                    heading=heading,
                    service_label=service_label,
                    price_type=price_type,
                ),
            })

    return _dedupe_price_items(items)


def _summarize_pricing_snapshot(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    billable = [
        float(item.get("amount_min"))
        for item in items
        if item.get("amount_min") is not None
        and float(item.get("amount_min") or 0) > 0
        and item.get("price_type") != "discount_percent"
    ]
    confidences = [float(item.get("confidence") or 0) for item in items]
    avg_conf = (sum(confidences) / len(confidences)) if confidences else 0
    confidence_band = "high" if avg_conf >= 0.78 else "medium" if avg_conf >= 0.6 else "low"

    return {
        "sample_count": len(items),
        "billable_sample_count": len(billable),
        "price_min": min(billable) if billable else None,
        "price_max": max(billable) if billable else None,
        "price_avg": round(sum(billable) / len(billable), 2) if billable else None,
        "services": sorted({item.get("service", "") for item in items if item.get("service")}),
        "confidence_band": confidence_band,
        "average_confidence": round(avg_conf, 2),
    }


def summarize_market_pricing(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    billable: List[float] = []
    competitor_count = 0
    source_count = 0
    for report in reports or []:
        pricing = (report or {}).get("pricing") or {}
        items = pricing.get("items") or []
        if items:
            competitor_count += 1
        for item in items:
            amount = item.get("amount_min")
            if amount is None:
                continue
            amount = float(amount or 0)
            if amount <= 0:
                continue
            if item.get("price_type") == "discount_percent":
                continue
            billable.append(amount)
            source_count += 1

    if not billable:
        return {
            "competitors_with_pricing": competitor_count,
            "billable_source_count": source_count,
            "average_price": None,
            "lowest_price": None,
            "highest_price": None,
        }

    return {
        "competitors_with_pricing": competitor_count,
        "billable_source_count": source_count,
        "average_price": round(sum(billable) / len(billable), 2),
        "lowest_price": min(billable),
        "highest_price": max(billable),
    }


def _scrape_pricing_intel(brand: dict, competitor: dict) -> Optional[Dict[str, Any]]:
    service_terms = _service_terms_for_brand(brand)
    targets = _discover_pricing_targets(brand, competitor)
    if not targets:
        return None

    pages: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    for target in targets[:10]:
        fetched = _fetch_page(target["url"])
        if fetched.get("html"):
            page_url = fetched.get("url", target["url"])
            page_items = _extract_structured_price_mentions(page_url, fetched.get("html", ""), service_terms)
            if page_items:
                items.extend(page_items)
            pages.append({
                "url": page_url,
                "source": target.get("source", ""),
                "query": target.get("query", ""),
                "match_count": len(page_items),
            })
        else:
            errors.append(f"{target['url']}: {fetched.get('error', 'fetch failed')}")

    items = _dedupe_price_items(items)
    items = sorted(items, key=lambda x: (-float(x.get("confidence") or 0), x.get("service", ""), float(x.get("amount_min") or 0)))[:30]

    return {
        "summary": _summarize_pricing_snapshot(items),
        "items": items,
        "pages_scanned": pages,
        "service_terms_used": service_terms,
        "errors": errors[:8],
    }


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
    queries = _build_google_places_queries(competitor)
    candidates = []
    seen_ids = set()

    for query in queries:
        body = {"textQuery": query, "maxResultCount": 5}
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=15)
            if resp.status_code != 200:
                log.warning("Places API returned %s for %s query %s", resp.status_code, name, query)
                continue
            places = resp.json().get("places", [])
            for p in places:
                place_id = p.get("id", "")
                dedupe_key = place_id or (p.get("googleMapsUri") or "")
                if dedupe_key in seen_ids:
                    continue
                seen_ids.add(dedupe_key)
                score, reasons = _score_google_places_candidate(competitor, p)
                candidates.append(
                    {
                        "name": (p.get("displayName") or {}).get("text", ""),
                        "place_id": place_id,
                        "rating": p.get("rating"),
                        "review_count": p.get("userRatingCount"),
                        "types": p.get("types", []),
                        "address": p.get("formattedAddress", ""),
                        "website": p.get("websiteUri", ""),
                        "maps_url": p.get("googleMapsUri", ""),
                        "price_level": p.get("priceLevel", ""),
                        "match_score": score,
                        "match_reasons": reasons,
                        "query_used": query,
                    }
                )
            if any("Exact GBP CID match" in candidate.get("match_reasons", []) for candidate in candidates):
                break
        except Exception as exc:
            log.warning("Places API error for %s: %s", name, exc)

    if candidates:
        candidates.sort(
            key=lambda item: (
                -float(item.get("match_score") or 0),
                -float(item.get("review_count") or 0),
                -float(item.get("rating") or 0),
                item.get("name") or "",
            )
        )
        best = dict(candidates[0])
        best["candidate_count"] = len(candidates)
        best["search_queries"] = queries
        return best

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
            "name": brand.get("display_name") or brand.get("name"),
            "industry": brand.get("industry"),
            "website": brand.get("website"),
            "service_area": brand.get("service_area"),
            "primary_services": brand.get("primary_services"),
            "active_offers": brand.get("active_offers"),
            "brand_voice": brand.get("brand_voice"),
            "target_audience": brand.get("target_audience"),
            "reporting_notes": brand.get("reporting_notes"),
        },
        "competitor": {
            "name": competitor.get("name"),
            "website": competitor.get("website"),
            "google_maps_url": competitor.get("google_maps_url"),
            "gbp_cid": competitor.get("gbp_cid"),
            "facebook_url": competitor.get("facebook_url"),
            "instagram_url": competitor.get("instagram_url"),
            "yelp_url": competitor.get("yelp_url"),
            "notes": competitor.get("notes"),
        },
        "intel": {
            "google_places": intel.get("google_places") or {},
            "meta_ads": intel.get("meta_ads") or {},
            "website": intel.get("website") or {},
            "pricing": intel.get("pricing") or {},
        },
        "output_schema": {
            "positioning_summary": "string, 2-4 sentences. Must cite only provided intel.",
            "observed_offers": ["string"],
            "observed_services": ["string"],
            "observed_pricing": ["string"],
            "pricing_position": "string, short note describing whether pricing appears budget, market, premium, or unclear.",
            "pricing_strategy": "string, one direct recommendation for how the brand should position pricing against this competitor.",
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
        "Your job is to produce competitor research and counter-moves that can be acted on immediately. "
        "CRITICAL RULE: Do not invent facts about the competitor. Only use what is explicitly in the input intel. "
        "If public pricing evidence is present, summarize the actual pricing signals and keep them tied to the cited evidence. "
        "If a field is unknown, do not guess; add a short note to data_gaps. "
        "You MAY still propose counter-moves that are generally effective in this industry/service area, but you must phrase them as proactive moves for the brand (not as claims about the competitor). "
        "Make the output specific and concrete: prefer steps, hooks, and landing page sections over generic advice. "
        "Aim for 6-8 counter_moves when possible. "
        "Return ONLY valid JSON matching the output_schema. No markdown. No extra keys."
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(payload)},
        ],
        "response_format": {"type": "json_object"},
    }

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=body,
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
    out.setdefault("observed_pricing", [])
    out.setdefault("pricing_position", "")
    out.setdefault("pricing_strategy", "")
    out.setdefault("messaging_angles", [])
    out.setdefault("proof_points", [])
    out.setdefault("counter_moves", [])
    out.setdefault("conquest_campaign_notes", [])
    out.setdefault("landing_page_opportunities", [])
    out.setdefault("data_gaps", [])
    return out


# ── Public API: refresh competitor intel ─────────────────────────

def refresh_competitor_intel(db, brand, competitor, *, force: bool = False, only_types: Optional[List[str]] = None):
    """Refresh all intel for a single competitor.

    When force=True, bypass the stale window (used for manual "Scan").
    Returns dict of results.  The special key '_errors' is a list of
    human-readable error strings so the caller can surface them in the UI.
    """
    brand_id = brand["id"]
    comp_id = competitor["id"]
    results = {}
    errors = []
    requested_types = set(only_types or [])

    def wants(intel_type: str) -> bool:
        return not requested_types or intel_type in requested_types

    # Google Places
    if wants("google_places"):
        existing = db.get_competitor_intel(comp_id, "google_places")
        if force or (not existing) or _is_stale(existing.get("fetched_at")):
            api_key = (brand.get("google_maps_api_key") or "").strip()
            if not api_key:
                errors.append("Google Places skipped: no Google Maps API key on this brand.")
            places_data = _scrape_google_places(competitor, api_key)
            if places_data:
                db.upsert_competitor_intel(comp_id, brand_id, "google_places", json.dumps(places_data))
                results["google_places"] = places_data
            elif existing:
                results["google_places"] = json.loads(existing.get("data_json", "{}"))
        elif existing:
            results["google_places"] = json.loads(existing.get("data_json", "{}"))

    # Meta Ad Library
    if wants("meta_ads"):
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
    if wants("website"):
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

    # Pricing
    if wants("pricing"):
        existing = db.get_competitor_intel(comp_id, "pricing")
        if force or (not existing) or _is_stale(existing.get("fetched_at")):
            pricing_data = _scrape_pricing_intel(brand, competitor)
            if pricing_data:
                db.upsert_competitor_intel(comp_id, brand_id, "pricing", json.dumps(pricing_data))
                results["pricing"] = pricing_data
            elif existing:
                results["pricing"] = json.loads(existing.get("data_json", "{}"))
        elif existing:
            results["pricing"] = json.loads(existing.get("data_json", "{}"))

    # AI research + counter moves (optional)
    if wants("research"):
        existing = db.get_competitor_intel(comp_id, "research")
        should_generate = force or (not existing) or _is_stale(existing.get("fetched_at"))
        if should_generate:
            # Brand-level key/model first, then app-level settings/env.
            api_key = ((brand.get("openai_api_key") or "").strip() or db.get_setting("openai_api_key", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip())
            if api_key:
                model = (
                    (brand.get("openai_model_analysis") or "").strip()
                    or (brand.get("openai_model") or "").strip()
                    or db.get_setting("openai_model_competitor", "").strip()
                    or db.get_setting("openai_model", "").strip()
                    or os.environ.get("OPENAI_MODEL", "").strip()
                    or "gpt-4o-mini"
                )
                try:
                    research_input = {
                        "google_places": results.get("google_places"),
                        "meta_ads": results.get("meta_ads"),
                        "website": results.get("website"),
                        "pricing": results.get("pricing"),
                    }
                    research_data = _generate_competitor_research(
                        api_key=api_key,
                        model=model,
                        brand=brand,
                        competitor=competitor,
                        intel=research_input,
                    )
                    db.upsert_competitor_intel(comp_id, brand_id, "research", json.dumps(research_data))
                    results["research"] = research_data
                except Exception as exc:
                    log.warning("Competitor research generation failed for %s: %s", competitor.get("name"), exc)
                    errors.append(f"AI research failed: {exc}")
                    if existing:
                        try:
                            results["research"] = json.loads(existing.get("data_json", "{}"))
                        except Exception:
                            pass
            else:
                errors.append("AI research skipped: no OpenAI API key configured.")
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

    results["_errors"] = errors
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
