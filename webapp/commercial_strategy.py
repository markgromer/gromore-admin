import json


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


def _derive_pain_points(prospect, audit_snapshot, source_details):
    pains = []
    industry = (prospect.get("industry") or "").lower()
    emails = source_details.get("emails") or []
    review_count = source_details.get("review_count") or audit_snapshot.get("review_count") or 0
    rating = source_details.get("rating") or audit_snapshot.get("rating")
    site_title = (audit_snapshot.get("title") or "").lower()
    h1s = [str(item).lower() for item in (audit_snapshot.get("h1") or [])]

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
    findings = []
    website = (prospect.get("website") or "").strip()
    emails = source_details.get("emails") or []
    address = source_details.get("address") or ""
    rating = source_details.get("rating") or audit_snapshot.get("rating")
    review_count = source_details.get("review_count") or audit_snapshot.get("review_count") or 0
    title = audit_snapshot.get("title") or ""
    description = audit_snapshot.get("description") or ""
    h1s = audit_snapshot.get("h1") or []

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
            "detail": "Without a stable site, it is harder to prove credibility and harder for prospects to convert after outreach.",
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
            "detail": "The site did not expose a clear title or meta description worth using in outreach.",
        })

    if not h1s:
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
    industry = (prospect.get("industry") or "").lower()
    if any("review" in point for point in pain_points):
        return "reputation and trust"
    if "apartment" in industry or "leasing" in industry:
        return "occupancy and leasing demand"
    if "hoa" in industry or "association" in industry:
        return "resident trust and board confidence"
    if any(item.get("severity") == "warning" and "website" in item.get("title", "").lower() for item in audit_findings):
        return "website conversion and credibility"
    return "commercial lead flow and vendor positioning"


def build_commercial_outreach_brief(prospect):
    source_details = _json_object(prospect.get("source_details_json"))
    audit_snapshot = _json_object(prospect.get("audit_snapshot_json"))
    pain_points = _json_list(prospect.get("pain_points_json")) or _derive_pain_points(prospect, audit_snapshot, source_details)
    audit_findings = _derive_audit_findings(prospect, audit_snapshot, source_details)
    outreach_angle = (prospect.get("outreach_angle") or "").strip() or _choose_outreach_angle(prospect, pain_points, audit_findings)

    property_count = (prospect.get("property_count") or "").strip()
    current_vendor_status = (prospect.get("current_vendor_status") or "").strip()
    role_guess = _role_guess(prospect)
    stage = (prospect.get("stage") or "new").strip().lower()
    business_name = _prospect_label(prospect)
    service_area = (prospect.get("service_area") or source_details.get("service_area") or "their market").strip() or "their market"

    missing_for_proposal = []
    if not property_count:
        missing_for_proposal.append("property count or unit count")
    if not current_vendor_status:
        missing_for_proposal.append("current vendor status")
    if not (prospect.get("email") or prospect.get("phone")):
        missing_for_proposal.append("confirmed decision-maker contact")
    if not (prospect.get("decision_maker_role") or ""):
        missing_for_proposal.append("buyer role confirmation")

    readiness_status = "ready" if stage in {"proposal", "negotiation"} and not missing_for_proposal else "needs_qualification"
    if stage in {"won", "lost"}:
        readiness_status = stage

    next_actions = [
        f"Confirm whether {business_name} manages one site or multiple properties.",
        f"Validate the real buyer: {role_guess}.",
        f"Lead the first outreach with {outreach_angle}, not a generic services pitch.",
    ]
    if missing_for_proposal:
        next_actions.append("Do not build a full proposal until the missing qualification data is confirmed.")
    else:
        next_actions.append("Prepare a scoped proposal tied to the confirmed portfolio size and current vendor gaps.")

    qualification_questions = [
        "How many properties or units are under management right now?",
        "Who owns leasing and marketing decisions for those properties?",
        "Are you replacing an existing vendor or trying to improve current performance?",
        "What is the most important commercial outcome right now: occupancy, reputation, or operational consistency?",
    ]

    subject = f"Quick idea for {business_name} in {service_area}"
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

    return {
        "account_name": business_name,
        "role_guess": role_guess,
        "outreach_angle": outreach_angle,
        "pain_points": pain_points,
        "audit_findings": audit_findings,
        "qualification_questions": qualification_questions,
        "next_actions": next_actions,
        "proposal_readiness": {
            "status": readiness_status,
            "label": "Proposal-ready" if readiness_status == "ready" else "Needs qualification" if readiness_status == "needs_qualification" else readiness_status.title(),
            "missing": missing_for_proposal,
        },
        "subject": subject,
        "email_body": first_email,
        "call_opener": call_opener,
    }