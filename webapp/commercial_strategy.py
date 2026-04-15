import json
import re


COMMERCIAL_QUALIFICATION_CORE_FIELDS = [
    {
        "key": "property_count",
        "label": "Property Count Or Units",
        "prompt": "How many properties or units are under management right now?",
        "placeholder": "4 sites / 312 units",
        "multiline": False,
    },
    {
        "key": "decision_maker_role",
        "label": "Buyer Role",
        "prompt": "Who owns leasing and marketing decisions for those properties?",
        "placeholder": "Regional manager, board president, operations lead",
        "multiline": False,
    },
    {
        "key": "current_vendor_status",
        "label": "Current Vendor Status",
        "prompt": "Are you replacing an existing vendor or trying to improve current performance?",
        "placeholder": "Incumbent agency, in-house, evaluating options",
        "multiline": False,
    },
]


COMMERCIAL_QUALIFICATION_FIELDS = [
    {
        "key": "service_scope",
        "label": "Services In Scope",
        "prompt": "Which services are actually in scope for this account right now?",
        "placeholder": "Paid ads, landing pages, review generation, reporting",
        "multiline": True,
    },
    {
        "key": "buying_timeline",
        "label": "Buying Timeline",
        "prompt": "What timeline are they working against for a decision or vendor change?",
        "placeholder": "Need options before the next board meeting in 3 weeks",
        "multiline": True,
    },
    {
        "key": "decision_process",
        "label": "Decision Process",
        "prompt": "Who signs off, and what does the approval process look like?",
        "placeholder": "Regional manager shortlists, ownership approves, board signs contract",
        "multiline": True,
    },
    {
        "key": "commercial_goal",
        "label": "Primary Commercial Goal",
        "prompt": "What commercial outcome matters most right now: occupancy, reputation, or operational consistency?",
        "placeholder": "Increase occupancy across two underperforming properties",
        "multiline": True,
    },
    {
        "key": "budget_range",
        "label": "Budget Or Contract Range",
        "prompt": "Is there a working monthly budget or contract range?",
        "placeholder": "$3k-$5k per month approved if the rollout is phased",
        "multiline": True,
    },
]


def _json_object(raw_value, fallback=None):
    if fallback is None:
        fallback = {}
    if isinstance(raw_value, dict):
        return raw_value
    try:
        parsed = json.loads(raw_value or "{}")
    except Exception:
        return fallback
    return parsed if isinstance(parsed, dict) else fallback


def _json_list(raw_value):
    if isinstance(raw_value, list):
        return raw_value
    try:
        parsed = json.loads(raw_value or "[]")
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _clip(value, max_len=220):
    text = (value or "").strip()
    return text[:max_len]


def _text(value):
    return str(value or "").strip()


def _split_service_lines(*values, max_items=8):
    items = []
    seen = set()
    for raw_value in values:
        for part in re.split(r"[\n,;|]+", _text(raw_value)):
            cleaned = part.strip(" -\t")
            if not cleaned:
                continue
            dedupe_key = cleaned.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            items.append(cleaned)
            if len(items) >= max_items:
                return items
    return items


def _brand_service_lines(brand):
    brand = brand or {}
    return _split_service_lines(
        brand.get("primary_services"),
        brand.get("active_offers"),
        brand.get("sales_bot_service_menu"),
    )


def _is_digital_marketing_brand(brand, service_lines):
    haystack = " ".join([
        _text((brand or {}).get("industry")),
        _text((brand or {}).get("display_name")),
        " ".join(service_lines),
    ]).lower()
    digital_tokens = (
        "marketing", "seo", "google ads", "facebook ads", "meta ads", "lead gen",
        "lead generation", "website", "web design", "landing page", "reviews",
        "review management", "social media", "agency", "ppc", "ads",
    )
    return any(token in haystack for token in digital_tokens)


def _service_pitch_label(brand, prospect):
    service_lines = _brand_service_lines(brand)
    if service_lines:
        if len(service_lines) == 1:
            return service_lines[0]
        if len(service_lines) == 2:
            return f"{service_lines[0]} and {service_lines[1]}"
        return f"{service_lines[0]}, {service_lines[1]}, and {service_lines[2]}"

    account_type = _text(prospect.get("account_type") or prospect.get("industry")).lower()
    if any(token in account_type for token in ("apartment", "hoa", "property", "commercial")):
        return "commercial service coverage"
    return "recurring service support"


def _service_goal_examples(brand, prospect):
    service_phrase = _service_pitch_label(brand, prospect)
    if "pet waste" in service_phrase.lower() or "waste station" in service_phrase.lower():
        return "reduce resident complaints, keep stations stocked, and make service proof easy to share"
    if any(token in service_phrase.lower() for token in ("clean", "janitorial", "porter")):
        return "keep common areas cleaner, tighten visit consistency, and reduce manager follow-up"
    if any(token in service_phrase.lower() for token in ("landscap", "grounds", "lawn")):
        return "keep the property looking sharper, reduce complaints, and lock in reliable recurring service"
    if any(token in service_phrase.lower() for token in ("hvac", "plumb", "electric", "pest", "maintenance", "repair")):
        return "improve response consistency, tighten vendor accountability, and reduce tenant or manager issues"
    return "improve service consistency, reduce complaints, and make the vendor handoff easier to manage"


def _service_scope_placeholder(brand, prospect):
    service_lines = _brand_service_lines(brand)
    if service_lines:
        return ", ".join(service_lines[:4])
    return "Recurring service coverage, site visits, add-ons, and manager reporting"


def _prospect_label(prospect):
    return (prospect.get("business_name") or prospect.get("name") or "this property group").strip()


def _role_guess(prospect):
    explicit = (prospect.get("decision_maker_role") or "").strip()
    if explicit:
        return explicit
    industry = (prospect.get("industry") or "").lower()
    if "hoa" in industry or "association" in industry:
        return "board president or community manager"
    if "apartment" in industry or "leasing" in industry:
        return "property manager or regional manager"
    return "property manager or operations lead"


def _qualification_answers(prospect):
    raw = _json_object(prospect.get("qualification_answers_json"))
    return {field["key"]: _text(raw.get(field["key"])) for field in COMMERCIAL_QUALIFICATION_FIELDS}


def _build_qualification_items(prospect, answers):
    items = []
    direct_values = {
        "property_count": _text(prospect.get("property_count")),
        "decision_maker_role": _text(prospect.get("decision_maker_role")),
        "current_vendor_status": _text(prospect.get("current_vendor_status")),
        "confirmed_contact": _text(prospect.get("email")) or _text(prospect.get("phone")),
    }
    for field in COMMERCIAL_QUALIFICATION_CORE_FIELDS:
        value = direct_values.get(field["key"], "")
        items.append({
            "key": field["key"],
            "label": field["label"],
            "prompt": field["prompt"],
            "value": value,
            "required": True,
            "complete": bool(value),
        })
    items.append({
        "key": "confirmed_contact",
        "label": "Confirmed Decision-Maker Contact",
        "prompt": "What is the best direct email or phone for the buying contact?",
        "value": direct_values["confirmed_contact"],
        "required": True,
        "complete": bool(direct_values["confirmed_contact"]),
    })
    for field in COMMERCIAL_QUALIFICATION_FIELDS:
        value = answers.get(field["key"], "")
        items.append({
            "key": field["key"],
            "label": field["label"],
            "prompt": field["prompt"],
            "value": value,
            "required": True,
            "complete": bool(value),
        })
    return items


def _derive_pain_points(prospect, audit_snapshot, source_details):
    return _derive_pain_points_with_brand(prospect, audit_snapshot, source_details, brand=None)


def _derive_pain_points_with_brand(prospect, audit_snapshot, source_details, brand=None):
    pains = []
    industry = (prospect.get("industry") or "").lower()
    emails = source_details.get("emails") or []
    review_count = source_details.get("review_count") or audit_snapshot.get("review_count") or 0
    rating = source_details.get("rating") or audit_snapshot.get("rating")
    site_title = (audit_snapshot.get("title") or "").lower()
    h1s = [str(item).lower() for item in (audit_snapshot.get("h1") or [])]
    service_lines = _brand_service_lines(brand)
    is_digital_marketing = _is_digital_marketing_brand(brand, service_lines)

    if not is_digital_marketing:
        if "hoa" in industry or "association" in industry:
            pains.append("board confidence usually drops when service follow-through and resident communication feel inconsistent")
        if "apartment" in industry or "leasing" in industry:
            pains.append("resident complaints rise fast when common-area service and onsite presentation slip")
        if "property" in industry or "commercial" in industry:
            pains.append("regional operators usually care most about consistent vendor coverage, access coordination, and proof the work was done")
        if not emails:
            pains.append("the right onsite or regional contact is not obvious yet, so qualification and handoff need to be tighter")
        if rating and float(rating) < 4.2:
            pains.append("public reputation looks soft enough that trust and contract confidence may take longer to earn")
        if review_count and int(review_count) < 20:
            pains.append("outside credibility signals look thin, so direct proof of service and clear scope matter more")
        if not site_title and not h1s:
            pains.append("public information on this account is thin, so the pitch needs to lean on service fit, not assumptions")
        if not pains:
            pains.append("commercial buyers usually want fewer complaints, cleaner execution, and a vendor they do not have to chase")
        return pains[:4]

    if "hoa" in industry or "association" in industry:
        pains.append("board communication and resident trust are usually fragmented across vendors")
    if "apartment" in industry or "leasing" in industry:
        pains.append("leasing demand often drops when search visibility and page clarity are weak")
    if "property" in industry or "commercial" in industry:
        pains.append("regional operators need predictable lead flow and a cleaner vendor story across locations")
    if not emails:
        pains.append("public contact paths are weak, which usually means web conversion paths are weak too")
    if rating and float(rating) < 4.2:
        pains.append("review sentiment looks soft enough to hurt trust with residents and owners")
    if review_count and int(review_count) < 20:
        pains.append("review volume appears thin for a commercial operator that needs authority")
    if not site_title and not h1s:
        pains.append("the website does not clearly state a strong positioning message above the fold")
    if not pains:
        pains.append("commercial operators usually lose opportunities when digital visibility and vendor responsiveness are not packaged clearly")
    return pains[:4]


def _derive_audit_findings(prospect, audit_snapshot, source_details):
    return _derive_audit_findings_with_brand(prospect, audit_snapshot, source_details, brand=None)


def _derive_audit_findings_with_brand(prospect, audit_snapshot, source_details, brand=None):
    findings = []
    website = (prospect.get("website") or "").strip()
    emails = source_details.get("emails") or []
    address = source_details.get("address") or ""
    rating = source_details.get("rating") or audit_snapshot.get("rating")
    review_count = source_details.get("review_count") or audit_snapshot.get("review_count") or 0
    title = audit_snapshot.get("title") or ""
    description = audit_snapshot.get("description") or ""
    h1s = audit_snapshot.get("h1") or []
    service_lines = _brand_service_lines(brand)
    is_digital_marketing = _is_digital_marketing_brand(brand, service_lines)

    if website:
        findings.append({
            "severity": "info",
            "title": "Website found",
            "detail": website,
        })
    else:
        findings.append({
            "severity": "warning",
            "title": "No verified website",
            "detail": "Public online details are thin, so qualification will likely need phone-first or direct contact outreach.",
        })

    if emails:
        findings.append({
            "severity": "positive",
            "title": "Public contact path found",
            "detail": ", ".join(emails[:3]),
        })
    else:
        findings.append({
            "severity": "warning",
            "title": "No public email found",
            "detail": "This account likely needs phone-first outreach or manual contact enrichment before heavy nurture.",
        })

    if title or description:
        findings.append({
            "severity": "info",
            "title": "Site positioning snapshot",
            "detail": _clip(title or description, 160),
        })
    else:
        findings.append({
            "severity": "warning",
            "title": "Weak site messaging signal",
            "detail": "The public site did not expose much useful positioning, so rely on direct qualification instead of site assumptions.",
        })

    if is_digital_marketing and not h1s:
        findings.append({
            "severity": "warning",
            "title": "No clear headline found",
            "detail": "That usually means the site is not explaining the offer fast enough for commercial buyers.",
        })

    if rating:
        detail = f"{rating} rating"
        if review_count:
            detail += f" from {review_count} reviews"
        findings.append({
            "severity": "positive" if float(rating) >= 4.5 else "info" if float(rating) >= 4.2 else "warning",
            "title": "Reputation signal",
            "detail": detail,
        })

    if address:
        findings.append({
            "severity": "info",
            "title": "Location captured",
            "detail": address,
        })
    return findings[:6]


def _choose_outreach_angle(prospect, pain_points, audit_findings):
    return _choose_outreach_angle_with_brand(prospect, pain_points, audit_findings, brand=None)


def _choose_outreach_angle_with_brand(prospect, pain_points, audit_findings, brand=None):
    industry = (prospect.get("industry") or "").lower()
    service_lines = _brand_service_lines(brand)
    is_digital_marketing = _is_digital_marketing_brand(brand, service_lines)

    if not is_digital_marketing:
        service_phrase = _service_pitch_label(brand, prospect).lower()
        if "pet waste" in service_phrase or "waste station" in service_phrase:
            return "site cleanliness, station upkeep, and resident experience"
        if any(token in service_phrase for token in ("clean", "janitorial", "porter")):
            return "property cleanliness and vendor consistency"
        if any(token in service_phrase for token in ("landscap", "grounds", "lawn")):
            return "grounds presentation and recurring service consistency"
        if any(token in service_phrase for token in ("hvac", "plumb", "electric", "pest", "maintenance", "repair")):
            return "response speed, recurring coverage, and on-site reliability"
        return "commercial service consistency and property presentation"

    if any("review" in point for point in pain_points):
        return "reputation and trust"
    if "apartment" in industry or "leasing" in industry:
        return "occupancy and leasing demand"
    if "hoa" in industry or "association" in industry:
        return "resident trust and board confidence"
    if any(item.get("severity") == "warning" and "website" in item.get("title", "").lower() for item in audit_findings):
        return "website conversion and credibility"
    return "commercial lead flow and vendor positioning"


def build_commercial_outreach_brief(prospect, brand=None):
    source_details = _json_object(prospect.get("source_details_json"))
    audit_snapshot = _json_object(prospect.get("audit_snapshot_json"))
    qualification_answers = _qualification_answers(prospect)
    qualification_items = _build_qualification_items(prospect, qualification_answers)
    pain_points = _json_list(prospect.get("pain_points_json")) or _derive_pain_points_with_brand(prospect, audit_snapshot, source_details, brand=brand)
    audit_findings = _derive_audit_findings_with_brand(prospect, audit_snapshot, source_details, brand=brand)
    outreach_angle = (prospect.get("outreach_angle") or "").strip() or _choose_outreach_angle_with_brand(prospect, pain_points, audit_findings, brand=brand)

    role_guess = _role_guess(prospect)
    stage = (prospect.get("stage") or "new").strip().lower()
    business_name = _prospect_label(prospect)
    service_area = (prospect.get("service_area") or source_details.get("service_area") or "their market").strip() or "their market"
    service_phrase = _service_pitch_label(brand, prospect)
    service_goal_examples = _service_goal_examples(brand, prospect)
    is_digital_marketing = _is_digital_marketing_brand(brand, _brand_service_lines(brand))

    missing_for_proposal = [item["label"] for item in qualification_items if item["required"] and not item["complete"]]

    readiness_status = "ready" if not missing_for_proposal else "needs_qualification"
    if stage in {"won", "lost"}:
        readiness_status = stage

    next_actions = [
        f"Confirm whether {business_name} manages one site or multiple properties.",
        f"Validate the real buyer: {role_guess}.",
        f"Lead the first outreach with {outreach_angle}, not a generic services pitch.",
    ]
    if missing_for_proposal:
        for missing in missing_for_proposal[:2]:
            next_actions.append(f"Lock down {missing.lower()} before building a proposal.")
        next_actions.append("Do not build a full proposal until the missing qualification data is confirmed.")
    else:
        next_actions.append("Prepare a scoped proposal tied to the confirmed portfolio size and current vendor gaps.")

    qualification_questions = [item["prompt"] for item in qualification_items if not item["complete"]][:5]
    if not qualification_questions:
        qualification_questions = [field["prompt"] for field in COMMERCIAL_QUALIFICATION_FIELDS[:3]]

    qualification_form = []
    for field in COMMERCIAL_QUALIFICATION_CORE_FIELDS:
        value = _text(prospect.get(field["key"]))
        qualification_form.append({
            "key": field["key"],
            "label": field["label"],
            "placeholder": field["placeholder"],
            "multiline": field["multiline"],
            "value": value,
            "complete": bool(value),
        })
    for field in COMMERCIAL_QUALIFICATION_FIELDS:
        value = qualification_answers.get(field["key"], "")
        placeholder = field["placeholder"]
        prompt = field["prompt"]
        if field["key"] == "service_scope":
            placeholder = _service_scope_placeholder(brand, prospect)
            prompt = "Which services are actually in scope for this account right now?"
        elif field["key"] == "commercial_goal":
            prompt = "What outcome matters most for this account if the right vendor is in place?"
            placeholder = service_goal_examples
        qualification_form.append({
            "key": field["key"],
            "label": field["label"],
            "placeholder": placeholder,
            "multiline": field["multiline"],
            "value": value,
            "complete": bool(value),
            "prompt": prompt,
        })

    subject = f"Quick idea for {business_name} in {service_area}"
    if is_digital_marketing:
        first_email = (
            f"Hi {{name}},\n\n"
            f"I took a quick look at {business_name} and saw an opening around {outreach_angle}. "
            f"From the outside, it looks like one of the main issues may be {pain_points[0]}.\n\n"
            f"We help commercial property teams tighten lead flow, credibility, and follow-through without adding another messy vendor layer. "
            f"If useful, I can send a short 3-point audit for {business_name} and outline what I would test first.\n\n"
            f"Would it be useful if I sent that over?"
        )
        call_opener = (
            f"I was looking at {business_name} because we work with service businesses trying to win more commercial accounts. "
            f"I noticed a possible gap around {outreach_angle}, and I wanted to see who handles that on your side."
        )
    else:
        first_email = (
            f"Hi {{name}},\n\n"
            f"I took a quick look at {business_name}, and it seems like {outreach_angle} may be a real lever there. "
            f"From the outside, one likely friction point is {pain_points[0]}.\n\n"
            f"We help commercial properties with {service_phrase} without creating another vendor headache for the manager or board. "
            f"If useful, I can outline a simple service approach for {business_name} based on property size, access needs, and coverage expectations.\n\n"
            f"Would it help if I sent that over?"
        )
        call_opener = (
            f"I was looking at {business_name} because we help commercial properties with {service_phrase}. "
            f"I noticed a possible gap around {outreach_angle}, and I wanted to see who handles that on your side."
        )

    return {
        "account_name": business_name,
        "role_guess": role_guess,
        "outreach_angle": outreach_angle,
        "service_pitch": service_phrase,
        "pain_points": pain_points,
        "audit_findings": audit_findings,
        "qualification_questions": qualification_questions,
        "next_actions": next_actions,
        "qualification_form": qualification_form,
        "qualification_summary": {
            "items": qualification_items,
            "required_count": len([item for item in qualification_items if item["required"]]),
            "complete_count": len([item for item in qualification_items if item["required"] and item["complete"]]),
        },
        "proposal_readiness": {
            "status": readiness_status,
            "label": "Proposal-ready" if readiness_status == "ready" else "Needs qualification" if readiness_status == "needs_qualification" else readiness_status.title(),
            "missing": missing_for_proposal,
        },
        "subject": subject,
        "email_body": first_email,
        "call_opener": call_opener,
    }