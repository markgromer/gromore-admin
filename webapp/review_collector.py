"""
Review Collector: compliant review requests, private feedback, and tracking.
"""
from __future__ import annotations

import html
import re
import secrets
from datetime import UTC, datetime, timedelta
from urllib.parse import quote_plus

from flask import url_for

from webapp.email_sender import send_simple_email
from webapp.warren_bulk_messages import collect_recipients, group_catalog
from webapp.warren_sender import send_transactional_sms


MAX_REVIEW_REQUESTS = 250
DEFAULT_REVIEW_AUTOMATION_GROUPS = ["warren_won_clients", "jobber_clients", "sng_active_clients"]

DEFAULT_SMS_TEMPLATE = (
    "Hi {{ first_name }}, thanks for choosing {{ brand_name }}. "
    "Would you mind sharing a quick review? {{ review_link }}"
)
DEFAULT_EMAIL_SUBJECT = "Quick favor from {{ brand_name }}"
DEFAULT_EMAIL_TEMPLATE = """Hi {{ first_name }},

Thanks for choosing {{ brand_name }}. Your feedback helps local customers know who to trust.

Would you take a minute to share a review?

{{ review_link }}

Thank you,
{{ brand_name }}"""


def _clean(value):
    return str(value or "").strip()


def _first_name(name):
    parts = _clean(name).split()
    return parts[0] if parts else "there"


def _normalize_phone(value):
    raw = _clean(value)
    if not raw:
        return ""
    digits = re.sub(r"\D+", "", raw)
    if raw.startswith("+") and digits:
        return f"+{digits}"
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return raw


def _safe_json_list(value):
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        import json
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _coerce_int(value, default, minimum=0, maximum=9999):
    try:
        numeric = int(float(value))
    except (TypeError, ValueError):
        numeric = int(default)
    return max(minimum, min(maximum, numeric))


def automation_settings(brand):
    groups = _safe_json_list((brand or {}).get("review_automation_group_keys"))
    channels_raw = (brand or {}).get("review_automation_channels")
    channels = _safe_json_list(channels_raw)
    if not channels and channels_raw:
        channels = [part.strip() for part in str(channels_raw).split(",")]
    channels = [c for c in dict.fromkeys(channels or ["sms"]) if c in {"sms", "email"}]
    return {
        "enabled": int((brand or {}).get("review_automation_enabled") or 0) == 1,
        "groups": groups or DEFAULT_REVIEW_AUTOMATION_GROUPS,
        "channels": channels or ["sms"],
        "delay_days": _coerce_int((brand or {}).get("review_automation_delay_days"), 1, 0, 90),
        "cooldown_days": _coerce_int((brand or {}).get("review_automation_cooldown_days"), 90, 1, 1000),
        "max_attempts": _coerce_int((brand or {}).get("review_automation_max_attempts"), 2, 1, 10),
        "repeat_after_days": _coerce_int((brand or {}).get("review_automation_repeat_after_days"), 365, 30, 5000),
        "service_window_days": _coerce_int((brand or {}).get("review_automation_service_window_days"), 180, 1, 5000),
        "min_private_rating": _coerce_int((brand or {}).get("review_automation_min_private_rating"), 4, 1, 5),
    }


def review_destination_url(brand):
    manual = _clean((brand or {}).get("review_destination_url"))
    if manual:
        return manual
    place_id = _clean((brand or {}).get("google_place_id"))
    if place_id:
        return f"https://search.google.com/local/writereview?placeid={quote_plus(place_id)}"
    return ""


def review_setup_status(brand):
    destination = review_destination_url(brand)
    return {
        "ready": bool(destination),
        "review_url": destination,
        "source": "manual" if _clean((brand or {}).get("review_destination_url")) else ("google_place_id" if destination else ""),
        "sms_ready": bool(_clean((brand or {}).get("quo_api_key")) and _clean((brand or {}).get("quo_phone_number"))),
    }


def get_templates(brand):
    return {
        "sms": _clean((brand or {}).get("review_request_sms_template")) or DEFAULT_SMS_TEMPLATE,
        "email_subject": _clean((brand or {}).get("review_request_email_subject")) or DEFAULT_EMAIL_SUBJECT,
        "email": _clean((brand or {}).get("review_request_email_template")) or DEFAULT_EMAIL_TEMPLATE,
    }


def _parse_dt(value):
    raw = _clean(value)
    if not raw:
        return None
    if raw.isdigit():
        try:
            ts = int(raw)
            if ts > 10**12:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, UTC).replace(tzinfo=None)
        except Exception:
            return None
    for candidate in (raw, raw[:19], raw[:10]):
        try:
            return datetime.fromisoformat(candidate.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(raw[:10], fmt)
        except Exception:
            continue
    return None


def _format_date(dt):
    return dt.strftime("%Y-%m-%d") if dt else ""


def _extract_service_date(recipient):
    keys = (
        "last_service_date", "last_service_at", "last_job_completed_at", "job_completed_at",
        "completed_at", "service_date", "last_visit_at", "visit_date", "last_invoice_at",
        "last_payment_at", "paid_at", "updated_at", "updatedAt", "created_at", "createdAt",
    )
    for key in keys:
        dt = _parse_dt((recipient or {}).get(key))
        if dt:
            return dt
    raw = (recipient or {}).get("raw") if isinstance(recipient, dict) else None
    if isinstance(raw, dict):
        for key in keys:
            dt = _parse_dt(raw.get(key))
            if dt:
                return dt
    return None


def _norm_name(value):
    return re.sub(r"[^a-z0-9]+", " ", _clean(value).lower()).strip()


def _recent_google_review_names(db, brand):
    if not (brand or {}).get("google_place_id") or not (brand or {}).get("google_maps_api_key"):
        return set(), ""
    try:
        from webapp.google_business import build_gbp_context

        gbp = build_gbp_context(db, brand["id"])
    except Exception:
        return set(), "Google review lookup failed"
    names = set()
    for review in (gbp or {}).get("reviews") or []:
        normalized = _norm_name(review.get("author"))
        if normalized:
            names.add(normalized)
    return names, ""


def evaluate_review_candidate(db, brand, recipient, *, google_review_names=None, now=None):
    now = now or datetime.utcnow()
    settings = automation_settings(brand)
    email = _clean((recipient or {}).get("email")).lower()
    phone = _normalize_phone((recipient or {}).get("phone"))
    name = _clean((recipient or {}).get("name")) or "Customer"
    source_id = _clean((recipient or {}).get("source_id"))
    service_dt = _extract_service_date(recipient)
    service_date = _format_date(service_dt)
    history = db.get_review_request_history(
        brand["id"],
        email=email,
        phone=phone,
        source_id=source_id,
        customer_name=name,
        limit=20,
    )

    reasons = []
    status = "eligible"
    next_eligible_at = ""

    if not email and not phone:
        status = "missing_contact"
        reasons.append("No email or phone is available.")
    if phone and db.is_opted_out(brand["id"], phone):
        status = "sms_opted_out"
        reasons.append("Customer opted out of SMS.")

    normalized_name = _norm_name(name)
    if normalized_name and normalized_name in (google_review_names or set()):
        status = "already_reviewed"
        reasons.append("Customer name appears in visible recent Google reviews.")

    confirmed = next((r for r in history if r.get("public_review_confirmed_at")), None)
    if confirmed:
        status = "already_reviewed"
        reasons.append("A public review was already confirmed in WARREN.")

    clicked = next((r for r in history if r.get("clicked_review_at")), None)
    if clicked and status == "eligible":
        clicked_at = _parse_dt(clicked.get("clicked_review_at"))
        if clicked_at and (now - clicked_at).days < settings["repeat_after_days"]:
            status = "likely_reviewed"
            next_eligible_at = _format_date(clicked_at + timedelta(days=settings["repeat_after_days"]))
            reasons.append("Customer already clicked through to the public review page.")

    feedback = next((r for r in history if int(r.get("rating") or 0) > 0), None)
    if feedback and int(feedback.get("rating") or 0) < settings["min_private_rating"]:
        status = "service_recovery"
        reasons.append(f"Private feedback was below {settings['min_private_rating']} stars; handle recovery before asking again.")

    sent_history = [r for r in history if int(r.get("sent_sms") or 0) or int(r.get("sent_email") or 0)]
    if len(sent_history) >= settings["max_attempts"] and status == "eligible":
        status = "max_attempts"
        reasons.append(f"{len(sent_history)} request attempts already sent.")
    if sent_history and status == "eligible":
        last_sent_at = _parse_dt(sent_history[0].get("last_sent_at") or sent_history[0].get("created_at"))
        if last_sent_at and (now - last_sent_at).days < settings["cooldown_days"]:
            status = "cooldown"
            next_eligible_at = _format_date(last_sent_at + timedelta(days=settings["cooldown_days"]))
            reasons.append(f"Last review request was {(now - last_sent_at).days} day(s) ago.")

    if service_dt and status == "eligible":
        age_days = (now - service_dt).days
        if age_days < settings["delay_days"]:
            status = "too_soon"
            next_eligible_at = _format_date(service_dt + timedelta(days=settings["delay_days"]))
            reasons.append(f"Service was {age_days} day(s) ago; wait until the review ask window.")
        elif age_days > settings["service_window_days"]:
            status = "too_old"
            reasons.append(f"Last service appears {age_days} day(s) old; use reactivation instead of post-service automation.")
    elif not service_dt and status == "eligible":
        status = "needs_service_date"
        reasons.append("No service completion date was found from the CRM record.")

    return {
        "eligible": status == "eligible",
        "status": status,
        "reasons": reasons or ["Ready for review request."],
        "service_date": service_date,
        "next_eligible_at": next_eligible_at,
        "history_count": len(history),
        "sent_count": len(sent_history),
        "name": name,
        "email": email,
        "phone": phone,
        "source": _clean((recipient or {}).get("source")),
        "source_id": source_id,
    }


def build_review_intelligence(db, brand, recipients):
    google_review_names, google_lookup_warning = _recent_google_review_names(db, brand)
    candidates = []
    eligible = 0
    suppressed = 0
    for recipient in recipients or []:
        candidate = evaluate_review_candidate(db, brand, recipient, google_review_names=google_review_names)
        candidate["recipient"] = recipient
        candidates.append(candidate)
        if candidate["eligible"]:
            eligible += 1
        else:
            suppressed += 1
    return {
        "candidates": candidates,
        "eligible": eligible,
        "suppressed": suppressed,
        "google_review_names_checked": len(google_review_names),
        "google_lookup_warning": google_lookup_warning,
    }


def render_template_text(template, *, brand, recipient, review_link):
    brand_name = _clean((brand or {}).get("display_name")) or "our team"
    name = _clean((recipient or {}).get("name")) or "Customer"
    values = {
        "brand_name": brand_name,
        "customer_name": name,
        "first_name": _first_name(name),
        "review_link": review_link,
    }
    text = template or ""
    for key, value in values.items():
        text = text.replace("{{ " + key + " }}", value).replace("{{" + key + "}}", value)
    return text.strip()


def preview_review_audience(db, brand, group_keys, channels):
    recipients, errors = collect_recipients(db, brand, group_keys)
    channels = set(channels or [])
    intelligence = build_review_intelligence(db, brand, recipients[:MAX_REVIEW_REQUESTS])
    return {
        "recipients": recipients[:12],
        "candidates": intelligence["candidates"][:25],
        "eligible": intelligence["eligible"],
        "suppressed": intelligence["suppressed"],
        "google_review_names_checked": intelligence["google_review_names_checked"],
        "google_lookup_warning": intelligence["google_lookup_warning"],
        "errors": errors,
        "total": len(recipients),
        "sms_count": sum(1 for r in recipients if r.get("phone")),
        "email_count": sum(1 for r in recipients if r.get("email")),
        "sendable_sms": sum(1 for r in recipients if r.get("phone") and not db.is_opted_out(brand["id"], r.get("phone"))),
        "sms_selected": "sms" in channels,
        "email_selected": "email" in channels,
    }


def build_landing_url(token, external_base_url=None):
    path = url_for("client_public.client_review_request", token=token)
    if external_base_url:
        return external_base_url.rstrip("/") + path
    return url_for("client_public.client_review_request", token=token, _external=True)


def _email_html(text, review_link):
    escaped = html.escape(text).replace("\n", "<br>")
    button = (
        f'<p style="margin:22px 0;"><a href="{html.escape(review_link)}" '
        'style="background:#2563eb;color:#fff;text-decoration:none;padding:12px 18px;'
        'border-radius:8px;font-weight:700;display:inline-block;">Share your review</a></p>'
    )
    return f'<div style="font-family:Arial,sans-serif;line-height:1.6;color:#0f172a;">{escaped}{button}</div>'


def send_review_requests(
    db,
    app_config,
    brand,
    *,
    group_keys=None,
    channels=None,
    manual_recipient=None,
    created_by="",
):
    channels = [c for c in dict.fromkeys(channels or []) if c in {"sms", "email"}]
    if not channels:
        raise ValueError("Select SMS, email, or both.")

    setup = review_setup_status(brand)
    if not setup["ready"]:
        raise ValueError("Add a Google Place ID or manual review link before sending requests.")

    recipients = []
    errors = {}
    if manual_recipient:
        manual = {
            "name": _clean(manual_recipient.get("name")) or "Customer",
            "email": _clean(manual_recipient.get("email")).lower(),
            "phone": _normalize_phone(manual_recipient.get("phone")),
            "source": "manual",
            "source_id": "",
            "groups": ["manual"],
        }
        if manual["email"] or manual["phone"]:
            recipients.append(manual)
    if group_keys:
        recipients_from_groups, errors = collect_recipients(db, brand, group_keys)
        recipients.extend(recipients_from_groups)

    deduped = {}
    for recipient in recipients:
        key = recipient.get("email") or recipient.get("phone") or f"{recipient.get('source')}:{recipient.get('source_id')}"
        if key:
            deduped[key] = recipient
    recipients = list(deduped.values())[:MAX_REVIEW_REQUESTS]
    if not recipients:
        raise ValueError("No reachable recipients matched that request.")

    intelligence = build_review_intelligence(db, brand, recipients)
    eligible_candidates = [candidate for candidate in intelligence["candidates"] if candidate["eligible"]]
    if not eligible_candidates:
        suppressed_detail = "; ".join(
            f"{candidate['name']}: {candidate['status']}" for candidate in intelligence["candidates"][:6]
        )
        raise ValueError(f"No recipients are eligible for a review request yet. {suppressed_detail}")

    sms_ready = setup["sms_ready"]
    smtp_ready = bool(app_config.get("SMTP_USER") and app_config.get("SMTP_PASSWORD"))
    if "sms" in channels and not sms_ready:
        raise ValueError("SMS is not configured for this brand.")
    if "email" in channels and not smtp_ready:
        raise ValueError("Email SMTP is not configured.")

    templates = get_templates(brand)
    external_base_url = app_config.get("APP_URL", "")
    sent_sms = sent_email = failed = skipped = 0
    request_ids = []
    details = []

    for candidate in eligible_candidates:
        recipient = candidate["recipient"]
        token = secrets.token_urlsafe(24)
        landing_url = build_landing_url(token, external_base_url=external_base_url)
        subject = render_template_text(templates["email_subject"], brand=brand, recipient=recipient, review_link=landing_url)
        sms_text = render_template_text(templates["sms"], brand=brand, recipient=recipient, review_link=landing_url)
        email_text = render_template_text(templates["email"], brand=brand, recipient=recipient, review_link=landing_url)
        request_id = db.create_review_request(
            brand["id"],
            token=token,
            customer_name=recipient.get("name", ""),
            customer_email=recipient.get("email", ""),
            customer_phone=recipient.get("phone", ""),
            source=recipient.get("source", ""),
            source_id=recipient.get("source_id", ""),
            groups=recipient.get("groups") or group_keys or [],
            channels=channels,
            review_url=setup["review_url"],
            message_text=sms_text if "sms" in channels else email_text,
            email_subject=subject,
            service_date=candidate.get("service_date", ""),
            eligibility_status=candidate.get("status", ""),
            eligibility_reasons=candidate.get("reasons") or [],
            next_eligible_at=candidate.get("next_eligible_at", ""),
            candidate={k: v for k, v in candidate.items() if k != "recipient"},
            created_by=created_by,
        )
        request_ids.append(request_id)

        request_sent_sms = 0
        request_sent_email = 0
        request_failed = 0
        request_detail = ""

        if "sms" in channels:
            phone = recipient.get("phone")
            if not phone:
                skipped += 1
            elif db.is_opted_out(brand["id"], phone):
                skipped += 1
                request_detail = "SMS opted out"
            else:
                ok, detail = send_transactional_sms(db, brand, phone, sms_text, append_opt_out_footer=True)
                if ok:
                    sent_sms += 1
                    request_sent_sms = 1
                else:
                    failed += 1
                    request_failed = 1
                    request_detail = str(detail)[:500]

        if "email" in channels:
            email_address = recipient.get("email")
            if not email_address:
                skipped += 1
            else:
                try:
                    send_simple_email(
                        app_config,
                        email_address,
                        subject,
                        email_text,
                        html=_email_html(email_text, landing_url),
                        brand=brand,
                    )
                    sent_email += 1
                    request_sent_email = 1
                except Exception as exc:
                    failed += 1
                    request_failed = 1
                    request_detail = str(exc)[:500]

        if request_failed and request_detail:
            details.append(f"{recipient.get('name')}: {request_detail}")
        db.update_review_request_delivery(
            request_id,
            sent_sms=request_sent_sms,
            sent_email=request_sent_email,
            failed=request_failed,
            failure_detail=request_detail,
        )

    return {
        "request_ids": request_ids,
        "recipient_count": len(eligible_candidates),
        "sent_sms": sent_sms,
        "sent_email": sent_email,
        "failed": failed,
        "skipped": skipped,
        "suppressed": intelligence["suppressed"],
        "errors": errors,
        "detail": "\n".join(details[:10]),
        "sent_at": datetime.now(UTC).isoformat(),
    }


def available_review_groups(brand):
    return group_catalog(brand)


def process_review_automation(db, app_config, brand, *, created_by="automation"):
    settings = automation_settings(brand)
    if not settings["enabled"]:
        return {"ran": False, "reason": "Review automation is disabled.", "sent_sms": 0, "sent_email": 0, "recipient_count": 0}
    return {
        "ran": True,
        **send_review_requests(
            db,
            app_config,
            brand,
            group_keys=settings["groups"],
            channels=settings["channels"],
            created_by=created_by,
        ),
    }
