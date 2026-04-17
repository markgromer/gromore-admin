"""Day-ahead Sweep and Go appointment reminders with local-time scheduling guardrails."""

import json
import logging
from datetime import datetime, timedelta, timezone

from webapp.crm_bridge import sng_get_day_ahead_appointment_candidates
from webapp.email_sender import send_simple_email
from webapp.warren_sender import send_transactional_sms

log = logging.getLogger(__name__)


def _get_zoneinfo(name):
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    try:
        return ZoneInfo(name or "America/New_York")
    except Exception:
        return ZoneInfo("America/New_York")


def _parse_channels(value):
    if isinstance(value, list):
        raw_channels = value
    else:
        text = (value or "sms").strip()
        if not text:
            text = "sms"
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
    return cleaned or ["sms"]


def _parse_send_minutes(value):
    text = str(value or "17:00").strip()
    try:
        hour, minute = [int(part) for part in text.split(":", 1)]
    except Exception:
        return 17 * 60
    hour = max(0, min(23, hour))
    minute = max(0, min(59, minute))
    return hour * 60 + minute


def _format_minutes(total_minutes):
    total_minutes = max(0, int(total_minutes or 0))
    hour = total_minutes // 60
    minute = total_minutes % 60
    return f"{hour:02d}:{minute:02d}"


def _brand_local_now(brand, now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    tz_name = (brand.get("sales_bot_appointment_reminder_timezone") or "America/New_York").strip()
    return now.astimezone(_get_zoneinfo(tz_name))


def _friendly_date(value):
    if hasattr(value, "strftime"):
        return f"{value.strftime('%A, %b')} {value.day}"
    return str(value or "")


def _build_default_message(brand, candidate, channel):
    brand_name = (brand.get("display_name") or brand.get("name") or "our team").strip()
    client_name = (candidate.get("client_name") or "there").strip()
    appointment_date = _friendly_date(candidate.get("appointment_date_obj"))
    assigned_to_name = (candidate.get("assigned_to_name") or "").strip()
    address = (candidate.get("address") or "").strip()

    if channel == "sms":
        base = f"Heads up from {brand_name} - you're on the schedule for {appointment_date}."
        if assigned_to_name:
            base += f" Your tech is {assigned_to_name}."
        if address:
            base += f" Service address: {address}."
        base += " Reply here if anything changed."
        return base

    lines = [
        f"Hi {client_name},",
        "",
        f"This is a reminder from {brand_name} that we have you scheduled for {appointment_date}.",
    ]
    if assigned_to_name:
        lines.append(f"Your technician is currently assigned as {assigned_to_name}.")
    if address:
        lines.append(f"Service address: {address}")
    lines.extend([
        "",
        "If anything changed or you need to update the appointment, just reply to this message.",
        "",
        f"Thanks,\n{brand_name}",
    ])
    return "\n".join(lines)


def _render_message(brand, candidate, channel):
    template = (brand.get("sales_bot_appointment_reminder_template") or "").strip()
    fallback = _build_default_message(brand, candidate, channel)
    if not template:
        return fallback

    replacements = {
        "{brand_name}": (brand.get("display_name") or brand.get("name") or "our team").strip(),
        "{client_name}": (candidate.get("client_name") or "there").strip(),
        "{appointment_date}": _friendly_date(candidate.get("appointment_date_obj")),
        "{assigned_to_name}": (candidate.get("assigned_to_name") or "").strip(),
        "{address}": (candidate.get("address") or "").strip(),
    }
    rendered = template
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered.strip() or fallback


def _send_email_reminder(app_config, brand, candidate):
    subject = f"Appointment reminder for {_friendly_date(candidate.get('appointment_date_obj'))}"
    text = _render_message(brand, candidate, "email")
    html = "<div style=\"font-family:Arial,sans-serif;white-space:pre-wrap;line-height:1.6;\">%s</div>" % text.replace("\n", "<br>")
    send_simple_email(app_config, candidate["client_email"], subject, text, html)


def _candidate_channels(brand, candidate, configured_channels):
    available = []
    if candidate.get("client_phone") and "sms" in configured_channels:
        available.append("sms")
    if candidate.get("client_email") and "email" in configured_channels:
        available.append("email")
    if not available:
        return []

    respect_preference = int(brand.get("sales_bot_appointment_reminder_respect_client_channel") or 1) == 1
    if not respect_preference:
        return available

    preferred_channel = (candidate.get("preferred_channel") or "").strip().lower()
    if preferred_channel in available:
        return [preferred_channel]
    if candidate.get("prefers_sms") and "sms" in available:
        return ["sms"]
    if candidate.get("prefers_email") and "email" in available:
        return ["email"]
    return available


def process_appointment_reminders(db, app_config, now=None, max_per_brand=None):
    stats = {
        "brands": 0,
        "candidates": 0,
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "errors": [],
    }
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    for brand in db.get_all_brands():
        if int(brand.get("sales_bot_appointment_reminders_enabled") or 0) != 1:
            continue

        local_now = _brand_local_now(brand, now=now)
        target_date = local_now.date() + timedelta(days=1)
        send_after_minutes = _parse_send_minutes(brand.get("sales_bot_appointment_reminder_send_time"))
        base_summary = {
            "local_time": local_now.strftime("%Y-%m-%d %H:%M"),
            "timezone": (brand.get("sales_bot_appointment_reminder_timezone") or "America/New_York").strip() or "America/New_York",
            "send_after": _format_minutes(send_after_minutes),
        }

        if (brand.get("crm_type") or "").strip().lower() != "sweepandgo" or not (brand.get("crm_api_key") or "").strip():
            db.record_appointment_reminder_run(
                brand["id"],
                target_date,
                status="config_error",
                reason="Sweep and Go CRM is not fully configured for appointment reminders.",
                summary=base_summary,
            )
            continue

        send_after_minutes = _parse_send_minutes(brand.get("sales_bot_appointment_reminder_send_time"))
        if (local_now.hour * 60 + local_now.minute) < send_after_minutes:
            db.record_appointment_reminder_run(
                brand["id"],
                target_date,
                status="waiting",
                reason=f"Current local time {local_now.strftime('%H:%M')} is before the send time {_format_minutes(send_after_minutes)}.",
                summary=base_summary,
            )
            continue

        stats["brands"] += 1
        candidates, error = sng_get_day_ahead_appointment_candidates(
            brand,
            target_date=target_date,
            max_jobs=max_per_brand,
        )
        if error:
            stats["errors"].append(str(error)[:200])
            db.record_appointment_reminder_run(
                brand["id"],
                target_date,
                status="failed",
                reason=str(error)[:500],
                summary=base_summary,
            )
            continue

        configured_channels = _parse_channels(brand.get("sales_bot_appointment_reminder_channels"))
        reminder_type = "appointment_day_ahead"
        brand_stats = {
            "candidates": 0,
            "sent": 0,
            "failed": 0,
            "skipped": 0,
        }
        for candidate in candidates:
            stats["candidates"] += 1
            brand_stats["candidates"] += 1
            appointment_key = candidate.get("appointment_key") or ""
            appointment_date = candidate.get("appointment_date") or target_date.isoformat()
            if not appointment_key:
                stats["skipped"] += 1
                brand_stats["skipped"] += 1
                continue

            for channel in _candidate_channels(brand, candidate, configured_channels):
                recipient = candidate.get("client_phone") if channel == "sms" else candidate.get("client_email")
                if not recipient:
                    stats["skipped"] += 1
                    brand_stats["skipped"] += 1
                    continue
                if db.has_sent_client_billing_reminder(brand["id"], appointment_key, appointment_date, channel, reminder_type=reminder_type):
                    stats["skipped"] += 1
                    brand_stats["skipped"] += 1
                    continue

                detail_payload = {
                    "job_id": candidate.get("job_id") or "",
                    "status_name": candidate.get("status_name") or "",
                    "assigned_to_name": candidate.get("assigned_to_name") or "",
                    "address": candidate.get("address") or "",
                    "preferred_channel": candidate.get("preferred_channel") or "",
                }

                try:
                    if channel == "email":
                        _send_email_reminder(app_config, brand, candidate)
                        ok, detail = True, "sent"
                    else:
                        ok, detail = send_transactional_sms(db, brand, candidate["client_phone"], _render_message(brand, candidate, "sms"))

                    db.record_client_billing_reminder(
                        brand["id"],
                        appointment_key,
                        appointment_date,
                        channel,
                        recipient=recipient,
                        status="sent" if ok else "failed",
                        detail=json.dumps({"result": detail, **detail_payload}, separators=(",", ":"))[:500],
                        reminder_type=reminder_type,
                    )
                    if ok:
                        stats["sent"] += 1
                        brand_stats["sent"] += 1
                    else:
                        stats["failed"] += 1
                        brand_stats["failed"] += 1
                except Exception as exc:
                    log.warning(
                        "Appointment reminder failed: brand=%s appointment=%s channel=%s err=%s",
                        brand.get("id"),
                        appointment_key,
                        channel,
                        exc,
                    )
                    db.record_client_billing_reminder(
                        brand["id"],
                        appointment_key,
                        appointment_date,
                        channel,
                        recipient=recipient,
                        status="failed",
                        detail=json.dumps({"error": str(exc), **detail_payload}, separators=(",", ":"))[:500],
                        reminder_type=reminder_type,
                    )
                    stats["failed"] += 1
                    brand_stats["failed"] += 1

        if brand_stats["failed"]:
            run_status = "partial" if brand_stats["sent"] else "failed"
        else:
            run_status = "completed"
        if not brand_stats["candidates"]:
            run_reason = "No eligible Sweep and Go jobs were found for tomorrow."
        else:
            run_reason = (
                f"Processed {brand_stats['candidates']} appointment candidate(s): "
                f"{brand_stats['sent']} sent, {brand_stats['failed']} failed, {brand_stats['skipped']} skipped."
            )
        db.record_appointment_reminder_run(
            brand["id"],
            target_date,
            status=run_status,
            reason=run_reason,
            candidates=brand_stats["candidates"],
            sent=brand_stats["sent"],
            failed=brand_stats["failed"],
            skipped=brand_stats["skipped"],
            summary={
                **base_summary,
                "channels": configured_channels,
            },
        )

    stats["errors"] = stats["errors"][:10]
    return stats