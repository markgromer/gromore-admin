"""
Warren billing reminders for active SNG clients on a shared billing day.

For the MVP, brands choose one billing day for all active clients, then Warren
sends email and optional SMS reminders a configurable number of days beforehand.
"""
import json
import logging
from datetime import datetime

from webapp.crm_bridge import sng_get_payment_reminder_candidates
from webapp.email_sender import send_simple_email
from webapp.warren_sender import send_transactional_sms

log = logging.getLogger(__name__)


def _parse_channels(value):
    if isinstance(value, list):
        raw_channels = value
    else:
        text = (value or "email").strip()
        if not text:
            text = "email"
        if text.startswith("["):
            try:
                raw_channels = json.loads(text)
            except Exception:
                raw_channels = [part.strip() for part in text.split(",")]
        else:
            raw_channels = [part.strip() for part in text.split(",")]
    cleaned = []
    for channel in raw_channels:
        normalized = str(channel or "").strip().lower()
        if normalized in {"email", "sms"} and normalized not in cleaned:
            cleaned.append(normalized)
    return cleaned or ["email"]


def _friendly_due_date(due_date):
    if hasattr(due_date, "strftime"):
        return f"{due_date.strftime('%b')} {due_date.day}"
    return str(due_date or "")


def _build_default_message(brand, candidate, channel):
    brand_name = (brand.get("display_name") or brand.get("name") or "our team").strip()
    client_name = (candidate.get("client_name") or "there").strip()
    due_date_label = _friendly_due_date(candidate.get("due_date_obj"))

    if channel == "sms":
        return (
            f"Heads up from {brand_name} - your billing date is {due_date_label}. "
            f"If you need to update billing or have any questions, just reply here."
        )

    return (
        f"Hi {client_name},\n\n"
        f"This is a quick reminder from {brand_name} that your billing date is {due_date_label}.\n\n"
        f"If you need to update your billing details or have any questions before then, just reply to this email and we will help.\n\n"
        f"Thanks,\n{brand_name}"
    )


def _render_message(brand, candidate, channel):
    template = (brand.get("sales_bot_payment_reminder_template") or "").strip()
    fallback = _build_default_message(brand, candidate, channel)
    if not template:
        return fallback

    replacements = {
        "{brand_name}": (brand.get("display_name") or brand.get("name") or "our team").strip(),
        "{client_name}": (candidate.get("client_name") or "there").strip(),
        "{due_date}": _friendly_due_date(candidate.get("due_date_obj")),
        "{days_before}": str(candidate.get("days_before") or ""),
    }
    rendered = template
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered.strip() or fallback


def _send_email_reminder(app_config, brand, candidate):
    subject = f"Payment reminder for {_friendly_due_date(candidate.get('due_date_obj'))}"
    text = _render_message(brand, candidate, "email")
    html = "<div style=\"font-family:Arial,sans-serif;white-space:pre-wrap;line-height:1.6;\">%s</div>" % text.replace("\n", "<br>")
    send_simple_email(app_config, candidate["client_email"], subject, text, html)


def process_payment_reminders(db, app_config, today=None):
    today = today or datetime.utcnow().date()
    stats = {
        "brands": 0,
        "candidates": 0,
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "errors": [],
    }

    for brand in db.get_all_brands():
        if int(brand.get("sales_bot_payment_reminders_enabled") or 0) != 1:
            continue
        if (brand.get("crm_type") or "").strip().lower() != "sweepandgo" or not (brand.get("crm_api_key") or "").strip():
            continue

        stats["brands"] += 1
        days_before = brand.get("sales_bot_payment_reminder_days_before") or 3
        billing_day = brand.get("sales_bot_payment_reminder_billing_day") or 1
        candidates, error = sng_get_payment_reminder_candidates(
            brand,
            billing_day=billing_day,
            days_before=days_before,
            today=today,
        )
        if error:
            stats["errors"].append(str(error)[:200])
            continue

        channels = _parse_channels(brand.get("sales_bot_payment_reminder_channels"))
        for candidate in candidates:
            stats["candidates"] += 1
            external_client_id = candidate.get("external_client_id") or ""
            due_date = candidate.get("due_date") or ""
            if not external_client_id or not due_date:
                stats["skipped"] += 1
                continue

            for channel in channels:
                recipient = candidate.get("client_email") if channel == "email" else candidate.get("client_phone")
                if not recipient:
                    stats["skipped"] += 1
                    continue
                if db.has_sent_client_billing_reminder(brand["id"], external_client_id, due_date, channel):
                    stats["skipped"] += 1
                    continue

                try:
                    if channel == "email":
                        _send_email_reminder(app_config, brand, candidate)
                        ok, detail = True, "sent"
                    else:
                        ok, detail = send_transactional_sms(db, brand, candidate["client_phone"], _render_message(brand, candidate, "sms"))

                    db.record_client_billing_reminder(
                        brand["id"],
                        external_client_id,
                        due_date,
                        channel,
                        recipient=recipient,
                        status="sent" if ok else "failed",
                        detail=str(detail)[:500],
                    )
                    if ok:
                        stats["sent"] += 1
                    else:
                        stats["failed"] += 1
                except Exception as exc:
                    log.warning("Payment reminder failed: brand=%s client=%s channel=%s err=%s", brand.get("id"), external_client_id, channel, exc)
                    db.record_client_billing_reminder(
                        brand["id"],
                        external_client_id,
                        due_date,
                        channel,
                        recipient=recipient,
                        status="failed",
                        detail=str(exc)[:500],
                    )
                    stats["failed"] += 1

    stats["errors"] = stats["errors"][:10]
    return stats