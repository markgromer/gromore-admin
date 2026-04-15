import html
import logging
import re
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


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
UNIT_COUNT_RE = re.compile(r"\b(\d{2,4})\s*(?:\+)?\s*(?:units?|apartments?|apartment homes?|residences?|homes?|doors?)\b", re.IGNORECASE)
BUILDING_COUNT_RE = re.compile(r"\b(\d{1,3})\s*(?:\+)?\s*buildings?\b", re.IGNORECASE)
COMMUNITY_COUNT_RE = re.compile(r"\b(\d{1,4})\s*(?:\+)?\s*(?:properties|communities|locations|sites|associations)\b", re.IGNORECASE)
RELIEF_AREA_COUNT_RE = re.compile(r"\b(\d{1,2})\s*(?:\+)?\s*(?:dog parks?|pet parks?|bark parks?|dog runs?|pet relief areas?|relief areas?)\b", re.IGNORECASE)
COMMON_AREA_COUNT_RE = re.compile(r"\b(\d{1,2})\s*(?:\+)?\s*(?:courtyards?|common areas?|green spaces?|dog areas?|pet areas?)\b", re.IGNORECASE)
WASTE_STATION_COUNT_RE = re.compile(r"\b(\d{1,2})\s*(?:\+)?\s*(?:pet waste stations?|waste stations?|dog waste stations?|bag stations?)\b", re.IGNORECASE)
PET_FRIENDLY_RE = re.compile(r"\bpet[-\s]?friendly\b", re.IGNORECASE)
ONSITE_DISPOSAL_RE = re.compile(r"\b(?:onsite|on-site|on site)\s+(?:dumpster|trash|waste|disposal)\b|\bdumpster enclosure\b|\bonsite disposal\b", re.IGNORECASE)
OFFSITE_DISPOSAL_RE = re.compile(r"\b(?:haul|remove|taken?)\s+(?:off[-\s]?site|from the property)\b|\boff[-\s]?site disposal\b", re.IGNORECASE)
CADENCE_HINTS = (
    (re.compile(r"\b(?:bi[-\s]?weekly|every other week|once every two weeks|every two weeks)\b", re.IGNORECASE), "every_2_weeks", "Every other week"),
    (re.compile(r"\b(?:weekly|once a week|1x per week|one visit per week)\b", re.IGNORECASE), "1x_week", "Weekly"),
    (re.compile(r"\b(?:twice weekly|2x per week|two times per week|two visits per week)\b", re.IGNORECASE), "2x_week", "Twice weekly"),
    (re.compile(r"\b(?:three times per week|3x per week|three visits per week)\b", re.IGNORECASE), "3x_week", "Three times weekly"),
    (re.compile(r"\b(?:daily|seven days a week|7x per week)\b", re.IGNORECASE), "7x_week", "Daily"),
)
PET_SIGNAL_PATTERNS = (
    (re.compile(r"\bdog parks?\b", re.IGNORECASE), "Dog park"),
    (re.compile(r"\bbark parks?\b", re.IGNORECASE), "Bark park"),
    (re.compile(r"\bdog runs?\b", re.IGNORECASE), "Dog run"),
    (re.compile(r"\bpet (?:relief|play) areas?\b", re.IGNORECASE), "Pet relief area"),
    (re.compile(r"\bpet washing station\b", re.IGNORECASE), "Pet wash station"),
    (re.compile(r"\bpet spa\b", re.IGNORECASE), "Pet spa"),
)
SITE_INTEL_PATH_HINTS = ("/amenities", "/community", "/property", "/properties", "/pet-friendly", "/leasing", "/about", "/faq")
SITE_INTEL_LINK_HINTS = ("amenit", "community", "property", "pet", "dog", "faq", "about", "leasing")
CONTACT_PAGE_PATH_HINTS = ("/contact", "/contact-us", "/team", "/staff", "/management", "/leasing", "/about")
CONTACT_PAGE_LINK_HINTS = ("contact", "team", "staff", "management", "leasing", "office")
PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})")
ROLE_LABELS = (
    "regional manager",
    "community manager",
    "property manager",
    "assistant manager",
    "leasing manager",
    "leasing office",
    "leasing consultant",
    "office manager",
    "management office",
)
ROLE_PRIORITY = {label: index for index, label in enumerate(ROLE_LABELS)}
MANAGEMENT_COMPANY_RE = re.compile(
    r"(?:professionally managed by|managed by|property management by)\s+([^.;|]{3,90})",
    re.IGNORECASE,
)
COMPLAINT_PATTERNS = (
    ("pet_waste", "Pet waste complaints", re.compile(r"\b(?:pet waste|dog waste|dog poop|poop|waste station|bag station)\b", re.IGNORECASE)),
    ("dog_area", "Dog area complaints", re.compile(r"\b(?:dog park|bark park|dog run|pet area|relief area)\b", re.IGNORECASE)),
    ("cleanliness", "Cleanliness or odor complaints", re.compile(r"\b(?:odor|odour|smell|dirty|filthy|cleanliness|unsanitary|waste smell|trash)\b", re.IGNORECASE)),
)


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


def _clean_signal_candidates(values, *, limit=8):
    cleaned = []
    seen = set()
    for raw in values or []:
        value = _clean_page_text(raw, 120)
        if not value:
            continue
        dedupe_key = value.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        cleaned.append(value)
    return cleaned[:limit]


def _clean_page_text(value, max_len=160000):
    text = html.unescape(value or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()[:max_len]


def _clean_phone_candidates(values, *, limit=4):
    cleaned = []
    seen = set()
    for raw in values or []:
        match = PHONE_RE.search(raw or "")
        if not match:
            continue
        value = re.sub(r"\s+", " ", match.group(0)).strip()
        digits = re.sub(r"\D", "", value)
        if len(digits) == 10:
            value = f"+1{digits}"
        elif len(digits) == 11 and digits.startswith("1"):
            value = f"+{digits}"
        if value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    return cleaned[:limit]


def _normalize_public_url(value):
    url = (value or "").strip()
    if not url:
        return ""
    parsed = urlparse(url if url.startswith(("http://", "https://")) else f"https://{url}")
    if not (parsed.netloc or parsed.path):
        return ""
    path = parsed.path or "/"
    return f"{parsed.scheme or 'https'}://{parsed.netloc or parsed.path}{path}".rstrip("/")


def _same_domain(url, website):
    left = _normalize_website(urlparse(url or "").netloc or url or "")
    right = _normalize_website(urlparse(website or "").netloc or website or "")
    return bool(left and right and left == right)


def _extract_contact_pages(website, *, max_pages=4, request_timeout=10):
    normalized = _normalize_website(website)
    if not normalized:
        return []

    root_url = website if website.startswith(("http://", "https://")) else f"https://{website}"
    session = requests.Session()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GroMoreBot/1.0)"}
    queue = [root_url]
    visited = set()
    pages = []

    while queue and len(visited) < max_pages:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            resp = session.get(current, timeout=request_timeout, allow_redirects=True, headers=headers)
        except Exception:
            continue
        if resp.status_code != 200 or "html" not in (resp.headers.get("Content-Type") or "text/html").lower():
            continue

        soup = BeautifulSoup(resp.text[:180000], "html.parser")
        raw_lines = [_clean_page_text(line, 180) for line in soup.get_text("\n", strip=True).splitlines() if _clean_page_text(line, 180)]
        page_text = _clean_page_text(" ".join(raw_lines))
        title = _clean_page_text((soup.title.string if soup.title and soup.title.string else ""), 160)
        emails = _clean_email_candidates(EMAIL_RE.findall(resp.text))
        emails.extend(
            value.split(":", 1)[1].split("?", 1)[0]
            for value in re.findall(r'href=["\'](mailto:[^"\']+)["\']', resp.text, flags=re.IGNORECASE)
        )
        phones = _clean_phone_candidates(PHONE_RE.findall(resp.text))
        pages.append({
            "url": resp.url,
            "title": title,
            "text": page_text,
            "lines": raw_lines,
            "emails": _clean_email_candidates(emails),
            "phones": _clean_phone_candidates(phones),
        })

        if len(visited) == 1:
            parsed_root = urlparse(resp.url)
            base_host = _normalize_website(parsed_root.netloc)
            for path in CONTACT_PAGE_PATH_HINTS:
                candidate = urljoin(resp.url, path)
                if candidate not in visited and candidate not in queue:
                    queue.append(candidate)
            for anchor in soup.find_all("a", href=True):
                href = (anchor.get("href") or "").strip()
                if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
                    continue
                absolute = urljoin(resp.url, href)
                parsed = urlparse(absolute)
                if _normalize_website(parsed.netloc) != base_host:
                    continue
                blob = f"{parsed.path.lower()} {_clean_page_text(anchor.get_text(' ', strip=True), 120).lower()}"
                if not any(token in blob for token in CONTACT_PAGE_LINK_HINTS):
                    continue
                if absolute not in visited and absolute not in queue:
                    queue.append(absolute)
                if len(queue) >= max_pages + 3:
                    break
    return pages


def _extract_name_from_contact_line(line):
    if not line:
        return ""
    match = re.search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b", line)
    return _clean_page_text(match.group(1), 80) if match else ""


def _extract_contact_candidates(website, *, max_pages=4, request_timeout=10):
    pages = _extract_contact_pages(website, max_pages=max_pages, request_timeout=request_timeout)
    contacts = []
    all_emails = []
    all_phones = []
    contact_urls = []
    seen = set()

    for page in pages:
        contact_urls.append(page.get("url") or "")
        all_emails.extend(page.get("emails") or [])
        all_phones.extend(page.get("phones") or [])
        lines = page.get("lines") or [segment.strip() for segment in (page.get("text") or "").split("\n") if segment.strip()]
        for line in lines[:120]:
            line_lower = line.lower()
            for role in ROLE_LABELS:
                if role not in line_lower:
                    continue
                name = _extract_name_from_contact_line(line)
                contact = {
                    "name": name,
                    "role": role.title(),
                    "email": (page.get("emails") or [""])[0],
                    "phone": (page.get("phones") or [""])[0],
                    "source_url": page.get("url") or "",
                    "evidence": _clean_page_text(line, 180),
                }
                dedupe_key = (contact["name"].lower(), contact["role"].lower(), contact["source_url"])
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                contacts.append(contact)
                break

    contacts = contacts[:6]
    contact_urls = [url for url in _clean_signal_candidates(contact_urls, limit=6) if url]
    return {
        "decision_maker_contacts": contacts,
        "emails": _clean_email_candidates(all_emails),
        "phones": _clean_phone_candidates(all_phones),
        "contact_urls": contact_urls,
    }


def _score_contact_role(role):
    return ROLE_PRIORITY.get((role or "").strip().lower(), len(ROLE_PRIORITY) + 10)


def _score_contact_candidate(contact, website=""):
    role_rank = _score_contact_role(contact.get("role"))
    score = max(10, 70 - (role_rank * 6))
    if contact.get("email"):
        score += 12
    if contact.get("phone"):
        score += 8
    if contact.get("name"):
        score += 5
    if contact.get("evidence"):
        score += 5
    if contact.get("source_url") and _same_domain(contact.get("source_url"), website):
        score += 8
    return min(score, 100)


def _contact_priority_label(score):
    if score >= 85:
        return "Best"
    if score >= 70:
        return "Strong"
    if score >= 55:
        return "Fallback"
    return "Weak"


def _extract_public_complaint_signals(mentions):
    signals = []
    seen = set()
    for item in mentions or []:
        if not isinstance(item, dict):
            continue
        evidence = _clean_page_text(f"{item.get('title') or ''} {item.get('snippet') or ''}", 280)
        if not evidence:
            continue
        for key, label, pattern in COMPLAINT_PATTERNS:
            if not pattern.search(evidence):
                continue
            dedupe_key = (key, item.get("url") or "", evidence.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            signals.append(
                {
                    "category": key,
                    "label": label,
                    "title": _clean_page_text(item.get("title"), 160),
                    "url": _normalize_public_url(item.get("url")),
                    "snippet": _clean_page_text(item.get("snippet"), 220),
                }
            )
            break
    return signals[:8]


def extract_commercial_public_intel(
    business_name,
    *,
    website="",
    address="",
    service_area="",
    prospect_type="",
    phone="",
    max_queries=5,
    max_results_per_query=5,
    contact_page_limit=4,
    request_timeout=10,
):
    business_name = _clean_page_text(business_name, 160)
    if not business_name:
        return {}

    queries = [
        f'"{business_name}" "leasing office"',
        f'"{business_name}" "community manager"',
        f'"{business_name}" reviews',
        f'"{business_name}" "pet friendly"',
    ]
    if address:
        queries.insert(0, f'"{business_name}" "{address}"')
    elif service_area:
        queries.insert(0, f'"{business_name}" "{service_area}"')

    mentions = []
    management_company = ""
    management_url = ""
    role_hint = ""
    seen_urls = set()

    for query in queries[:max(1, int(max_queries or 1))]:
        for result in _search_duckduckgo(query, max_results_per_query, request_timeout=request_timeout):
            url = _normalize_public_url(result.get("websiteUri"))
            title = _clean_page_text(((result.get("displayName") or {}).get("text") or ""), 160)
            snippet = _clean_page_text(result.get("formattedAddress") or "", 220)
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            evidence = f"{title} {snippet}".strip()
            mention = {
                "title": title or urlparse(url).netloc,
                "url": url,
                "snippet": snippet,
                "query": query,
                "same_domain": _same_domain(url, website),
            }
            mentions.append(mention)

            management_match = MANAGEMENT_COMPANY_RE.search(evidence)
            if management_match and not management_company:
                management_company = _clean_page_text(management_match.group(1), 120)
                management_url = url

            evidence_lower = evidence.lower()
            for role in ROLE_LABELS:
                if role in evidence_lower and not role_hint:
                    role_hint = role.title()
                    break
            if len(mentions) >= 8:
                break
        if len(mentions) >= 8:
            break

    contact_intel = _extract_contact_candidates(
        website,
        max_pages=max(1, int(contact_page_limit or 1)),
        request_timeout=request_timeout,
    ) if website else {"decision_maker_contacts": [], "emails": [], "phones": [], "contact_urls": []}
    contacts = contact_intel.get("decision_maker_contacts") or []
    if contacts:
        for item in contacts:
            item["priority_score"] = _score_contact_candidate(item, website=website)
            item["priority_label"] = _contact_priority_label(item["priority_score"])
        contacts = sorted(contacts, key=lambda item: (-int(item.get("priority_score") or 0), _score_contact_role(item.get("role"))))[:6]
        role_hint = contacts[0].get("role") or role_hint

    primary_contact_name = contacts[0].get("name") if contacts else ""
    complaint_signals = _extract_public_complaint_signals(mentions)
    return {
        "public_mentions": mentions[:6],
        "complaint_signals": complaint_signals,
        "decision_maker_contacts": contacts,
        "decision_maker_role_hint": role_hint,
        "primary_contact_name": primary_contact_name,
        "management_company": management_company,
        "management_url": management_url,
        "contact_urls": contact_intel.get("contact_urls") or [],
        "emails": _clean_email_candidates(contact_intel.get("emails") or []),
        "phones": _clean_phone_candidates((contact_intel.get("phones") or []) + ([phone] if phone else [])),
    }


def _fetch_site_intel_pages(website, *, max_pages=3, request_timeout=10):
    normalized = _normalize_website(website)
    if not normalized:
        return []

    root_url = website if website.startswith(("http://", "https://")) else f"https://{website}"
    session = requests.Session()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GroMoreBot/1.0)"}
    queue = [root_url]
    visited = set()
    pages = []

    while queue and len(visited) < max_pages:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            resp = session.get(current, timeout=request_timeout, allow_redirects=True, headers=headers)
        except Exception:
            continue
        if resp.status_code != 200 or "html" not in (resp.headers.get("Content-Type") or "text/html").lower():
            continue

        soup = BeautifulSoup(resp.text[:180000], "html.parser")
        page_text = _clean_page_text(soup.get_text(" ", strip=True))
        title = _clean_page_text((soup.title.string if soup.title and soup.title.string else ""), 160)
        pages.append({
            "url": resp.url,
            "title": title,
            "text": page_text,
        })

        if len(visited) == 1:
            parsed_root = urlparse(resp.url)
            base_host = _normalize_website(parsed_root.netloc)
            for path in SITE_INTEL_PATH_HINTS:
                candidate = urljoin(resp.url, path)
                if candidate not in visited and candidate not in queue:
                    queue.append(candidate)
            for anchor in soup.find_all("a", href=True):
                href = (anchor.get("href") or "").strip()
                if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
                    continue
                anchor_text = _clean_page_text(anchor.get_text(" ", strip=True), 120).lower()
                absolute = urljoin(resp.url, href)
                parsed = urlparse(absolute)
                if _normalize_website(parsed.netloc) != base_host:
                    continue
                blob = f"{parsed.path.lower()} {anchor_text}"
                if not any(token in blob for token in SITE_INTEL_LINK_HINTS):
                    continue
                if absolute not in visited and absolute not in queue:
                    queue.append(absolute)
                if len(queue) >= max_pages + 2:
                    break
    return pages


def _pick_best_count(text, pattern, *, minimum=1, maximum=5000):
    best = None
    for match in pattern.finditer(text or ""):
        try:
            value = int(match.group(1))
        except Exception:
            continue
        if value < minimum or value > maximum:
            continue
        phrase = _clean_page_text(match.group(0), 80)
        candidate = (value, phrase)
        if best is None or candidate[0] > best[0]:
            best = candidate
    return best


def _infer_pet_traffic(unit_count=0, *, pet_friendly=False, relief_area_count=0, common_area_count=0, account_type=""):
    if unit_count >= 180 or relief_area_count >= 2:
        return "High around dog-friendly common areas"
    if unit_count >= 80 or relief_area_count >= 1 or common_area_count >= 2:
        return "Moderate to high around shared pet areas"
    if pet_friendly:
        return "Moderate if pet policies are actively used"
    if any(token in (account_type or "") for token in ("hoa", "property_manager", "property_manager", "condo")):
        return "Shared-area pet traffic likely needs confirmation"
    return ""


def _infer_site_condition(*, pet_friendly=False, relief_area_count=0, common_area_count=0, property_count="", account_type=""):
    if pet_friendly or relief_area_count or common_area_count:
        base = "Pet-friendly common areas likely need recurring cleanup and visible service proof"
        if property_count:
            return f"{property_count} footprint with {base.lower()}"
        return base
    if any(token in (account_type or "") for token in ("hoa", "property_manager", "condo")):
        return "Shared green space coverage likely matters, but site layout still needs confirmation"
    return ""


def _infer_disposal_notes(text):
    if OFFSITE_DISPOSAL_RE.search(text or ""):
        return "Waste appears to require removal from the property"
    if ONSITE_DISPOSAL_RE.search(text or ""):
        return "Onsite dumpster or disposal area appears available"
    if re.search(r"\bdumpster\b", text or "", re.IGNORECASE):
        return "Dumpster access likely available, but disposal procedure still needs confirmation"
    return ""


def _infer_service_cadence(text):
    for pattern, key, label in CADENCE_HINTS:
        if pattern.search(text or ""):
            return {
                "service_frequency_hint": key,
                "service_days_hint": label,
            }
    return {}


def extract_commercial_site_intel(website, *, business_name="", prospect_type="", max_pages=3, request_timeout=10):
    pages = _fetch_site_intel_pages(website, max_pages=max_pages, request_timeout=request_timeout)
    if not pages:
        return {}

    combined_text = " ".join(page.get("text") or "" for page in pages)
    unit_match = _pick_best_count(combined_text, UNIT_COUNT_RE, minimum=10, maximum=2500)
    building_match = _pick_best_count(combined_text, BUILDING_COUNT_RE, minimum=1, maximum=200)
    community_match = _pick_best_count(combined_text, COMMUNITY_COUNT_RE, minimum=2, maximum=2500)
    relief_match = _pick_best_count(combined_text, RELIEF_AREA_COUNT_RE, minimum=1, maximum=20)
    common_match = _pick_best_count(combined_text, COMMON_AREA_COUNT_RE, minimum=1, maximum=50)
    waste_station_match = _pick_best_count(combined_text, WASTE_STATION_COUNT_RE, minimum=1, maximum=50)

    property_count = ""
    site_signals = []
    if unit_match:
        property_count = f"{unit_match[0]} units"
        site_signals.append(property_count)
        if building_match:
            property_count = f"{property_count} across {building_match[0]} buildings"
            site_signals.append(f"{building_match[0]} buildings")
    elif community_match:
        label = "properties" if "property_manager" in (prospect_type or "") else "communities"
        property_count = f"{community_match[0]} {label}"
        site_signals.append(property_count)

    pet_friendly = bool(PET_FRIENDLY_RE.search(combined_text))
    if pet_friendly:
        site_signals.append("Pet-friendly")

    relief_area_count = relief_match[0] if relief_match else 0
    common_area_count = common_match[0] if common_match else 0
    waste_station_count = waste_station_match[0] if waste_station_match else 0
    pet_features = []
    for pattern, label in PET_SIGNAL_PATTERNS:
        if pattern.search(combined_text):
            pet_features.append(label)
            if label in ("Dog park", "Bark park", "Dog run", "Pet relief area") and relief_area_count == 0:
                relief_area_count = 1
    if pet_features:
        site_signals.extend(feature for feature in pet_features if feature not in site_signals)

    if common_area_count == 0 and re.search(r"\bcourtyard\b|\bgreen space\b|\bcommon area\b", combined_text, re.IGNORECASE):
        common_area_count = 1

    cadence_hint = _infer_service_cadence(combined_text)
    disposal_notes = _infer_disposal_notes(combined_text)
    if waste_station_count:
        site_signals.append(f"{waste_station_count} waste stations")
    if cadence_hint.get("service_days_hint"):
        site_signals.append(cadence_hint["service_days_hint"])
    if disposal_notes:
        site_signals.append(disposal_notes)

    pet_traffic_estimate = _infer_pet_traffic(
        unit_match[0] if unit_match else 0,
        pet_friendly=pet_friendly,
        relief_area_count=relief_area_count,
        common_area_count=common_area_count,
        account_type=prospect_type,
    )
    site_condition = _infer_site_condition(
        pet_friendly=pet_friendly,
        relief_area_count=relief_area_count,
        common_area_count=common_area_count,
        property_count=property_count,
        account_type=prospect_type,
    )

    title_source = next((page.get("title") for page in pages if page.get("title")), "")
    property_label = _clean_page_text(business_name or title_source, 160)
    site_signals = _clean_signal_candidates(site_signals)
    if property_label and property_label not in site_signals:
        site_signals = site_signals[:6]

    return {
        "property_count": property_count,
        "walkthrough_property_label": property_label,
        "walkthrough_common_area_count": common_area_count,
        "walkthrough_relief_area_count": relief_area_count,
        "walkthrough_waste_station_count": waste_station_count,
        "pet_traffic_estimate": pet_traffic_estimate,
        "site_condition": site_condition,
        "disposal_notes": disposal_notes,
        "service_frequency_hint": cadence_hint.get("service_frequency_hint") or "",
        "service_days_hint": cadence_hint.get("service_days_hint") or "",
        "site_signals": site_signals,
        "scraped_page_count": len(pages),
    }


def _extract_public_emails(website, *, max_pages=5, request_timeout=10):
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

    while urls_to_visit and len(visited) < max(1, int(max_pages or 1)) and len(emails) < 3:
        current = urls_to_visit.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            resp = session.get(current, timeout=request_timeout, allow_redirects=True, headers=headers)
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


def _search_duckduckgo(query, max_results, request_timeout=20):
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        resp = requests.get(url, timeout=request_timeout, headers={"User-Agent": "Mozilla/5.0"})
    except Exception:
        return []
    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text[:160000], "html.parser")
    results = []
    for anchor in soup.select("a.result__a")[: max(1, min(int(max_results or 10), 15))]:
        href = (anchor.get("href") or "").strip()
        resolved = href
        if "duckduckgo.com/l/?" in href:
            parsed = urlparse(href)
            uddg = parse_qs(parsed.query).get("uddg", [""])[0]
            if uddg:
                resolved = unquote(uddg)
        snippet = ""
        result_node = anchor.find_parent(class_="result")
        if result_node:
            snippet_node = result_node.select_one(".result__snippet")
            if snippet_node:
                snippet = _clean_html_text(snippet_node.get_text(" ", strip=True))
        results.append(
            {
                "websiteUri": resolved,
                "displayName": {"text": _clean_html_text(anchor.get_text(" ", strip=True))},
                "formattedAddress": snippet,
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
                emails = _extract_public_emails(website, max_pages=1, request_timeout=4)
            audit_snapshot = existing.get("audit_snapshot") or {}
            site_intel = existing.get("site_intel") or {}
            public_intel = existing.get("public_intel") or {}
            if public_intel.get("emails"):
                emails = _clean_email_candidates((public_intel.get("emails") or []) + emails)
            merged = {
                "business_name": business_name or existing.get("business_name") or "Unknown Prospect",
                "contact_name": public_intel.get("primary_contact_name") or existing.get("contact_name") or business_name,
                "website": website or existing.get("website") or "",
                "address": (raw.get("formattedAddress") or existing.get("address") or "").strip(),
                "phone": (raw.get("nationalPhoneNumber") or existing.get("phone") or (public_intel.get("phones") or [""])[0]).strip(),
                "rating": raw.get("rating") if raw.get("rating") is not None else existing.get("rating"),
                "review_count": raw.get("userRatingCount") if raw.get("userRatingCount") is not None else existing.get("review_count"),
                "maps_url": (raw.get("googleMapsUri") or existing.get("maps_url") or "").strip(),
                "emails": emails,
                "prospect_type": item["key"],
                "prospect_type_label": item["label"],
                "service_area": location,
                "source_query": query,
                "audit_snapshot": audit_snapshot or {},
                "site_intel": site_intel,
                "public_intel": public_intel,
                "property_count": site_intel.get("property_count") or existing.get("property_count") or "",
                "walkthrough_property_label": site_intel.get("walkthrough_property_label") or existing.get("walkthrough_property_label") or business_name,
                "walkthrough_waste_station_count": site_intel.get("walkthrough_waste_station_count") if site_intel.get("walkthrough_waste_station_count") is not None else existing.get("walkthrough_waste_station_count") or 0,
                "walkthrough_common_area_count": site_intel.get("walkthrough_common_area_count") if site_intel.get("walkthrough_common_area_count") is not None else existing.get("walkthrough_common_area_count") or 0,
                "walkthrough_relief_area_count": site_intel.get("walkthrough_relief_area_count") if site_intel.get("walkthrough_relief_area_count") is not None else existing.get("walkthrough_relief_area_count") or 0,
                "pet_traffic_estimate": site_intel.get("pet_traffic_estimate") or existing.get("pet_traffic_estimate") or "",
                "site_condition": site_intel.get("site_condition") or existing.get("site_condition") or "",
                "disposal_notes": site_intel.get("disposal_notes") or existing.get("disposal_notes") or "",
                "service_frequency_hint": site_intel.get("service_frequency_hint") or existing.get("service_frequency_hint") or "",
                "service_days_hint": site_intel.get("service_days_hint") or existing.get("service_days_hint") or "",
                "site_signals": site_intel.get("site_signals") or existing.get("site_signals") or [],
                "decision_maker_role": public_intel.get("decision_maker_role_hint") or existing.get("decision_maker_role") or "",
                "decision_maker_contacts": public_intel.get("decision_maker_contacts") or existing.get("decision_maker_contacts") or [],
                "public_mentions": public_intel.get("public_mentions") or existing.get("public_mentions") or [],
                "complaint_signals": public_intel.get("complaint_signals") or existing.get("complaint_signals") or [],
                "management_company": public_intel.get("management_company") or existing.get("management_company") or "",
                "management_url": public_intel.get("management_url") or existing.get("management_url") or "",
                "contact_urls": public_intel.get("contact_urls") or existing.get("contact_urls") or [],
            }
            merged["score"] = _score_result(merged)
            lookup[key] = merged

    return sorted(lookup.values(), key=lambda row: (-int(row.get("score") or 0), row.get("business_name") or ""))