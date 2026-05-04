"""
Review Collector: compliant review requests, private feedback, and tracking.
"""
from __future__ import annotations

import html
import re
import secrets
from datetime import UTC, datetime
from urllib.parse import quote_plus

from flask import url_for

from webapp.email_sender import send_simple_email
from webapp.warren_bulk_messages import collect_recipients, group_catalog
from webapp.warren_sender import send_transactional_sms


MAX_REVIEW_REQUESTS = 250

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
    return {
        "recipients": recipients[:12],
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

    for recipient in recipients:
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
        "recipient_count": len(recipients),
        "sent_sms": sent_sms,
        "sent_email": sent_email,
        "failed": failed,
        "skipped": skipped,
        "errors": errors,
        "detail": "\n".join(details[:10]),
        "sent_at": datetime.now(UTC).isoformat(),
    }


def available_review_groups(brand):
    return group_catalog(brand)
