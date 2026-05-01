"""Day-ahead Sweep and Go appointment reminders with local-time scheduling guardrails."""

import json
import logging
import re
from datetime import datetime, timedelta, timezone

from webapp.crm_bridge import sng_get_day_ahead_appointment_candidates
from webapp.email_sender import send_simple_email
from webapp.warren_sender import send_transactional_sms

log = logging.getLogger(__name__)


_TIMEZONE_ALIASES = {
    "america/new_york": "America/New_York",
    "us/eastern": "America/New_York",
    "eastern": "America/New_York",
    "eastern time": "America/New_York",
    "eastern time (us & canada)": "America/New_York",
    "est": "America/New_York",
    "edt": "America/New_York",
    "america/chicago": "America/Chicago",
    "us/central": "America/Chicago",
    "central": "America/Chicago",
    "central time": "America/Chicago",
    "central time (us & canada)": "America/Chicago",
    "cst": "America/Chicago",
    "cdt": "America/Chicago",
    "america/denver": "America/Denver",
    "us/mountain": "America/Denver",
    "mountain": "America/Denver",
    "mountain time": "America/Denver",
    "mountain time (us & canada)": "America/Denver",
    "mst": "America/Denver",
    "mdt": "America/Denver",
    "america/los_angeles": "America/Los_Angeles",
    "us/pacific": "America/Los_Angeles",
    "pacific": "America/Los_Angeles",
    "pacific time": "America/Los_Angeles",
    "pacific time (us & canada)": "America/Los_Angeles",
    "pst": "America/Los_Angeles",
    "pdt": "America/Los_Angeles",
    "america/anchorage": "America/Anchorage",
    "alaska": "America/Anchorage",
    "alaska time": "America/Anchorage",
    "akst": "America/Anchorage",
    "akdt": "America/Anchorage",
    "pacific/honolulu": "Pacific/Honolulu",
    "hawaii": "Pacific/Honolulu",
    "hawaii time": "Pacific/Honolulu",
    "hst": "Pacific/Honolulu",
}


def _normalize_timezone_name(value):
    text = str(value or "").strip()
    if not text:
        return "America/New_York"
    return _TIMEZONE_ALIASES.get(text.lower(), text)


def _get_zoneinfo(name):
    normalized_name = _normalize_timezone_name(name)
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    try:
        return ZoneInfo(normalized_name)
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
    if not text:
        return 17 * 60

    # Accept values like 17:00, 17:00:00, 5 PM, and 5:30 PM from legacy forms.
    ampm_match = re.match(r"^\s*(\d{1,2})(?::(\d{2}))?(?::\d{2})?\s*([AaPp][Mm])\s*$", text)
    if ampm_match:
        hour = int(ampm_match.group(1))
        minute = int(ampm_match.group(2) or 0)
        marker = ampm_match.group(3).lower()
        hour = hour % 12
        if marker == "pm":
            hour += 12
        return max(0, min(23, hour)) * 60 + max(0, min(59, minute))

    hhmm_match = re.match(r"^\s*(\d{1,2}):(\d{2})(?::\d{2})?\s*$", text)
    if hhmm_match:
        hour = int(hhmm_match.group(1))
        minute = int(hhmm_match.group(2))
        return max(0, min(23, hour)) * 60 + max(0, min(59, minute))

    compact_match = re.match(r"^\s*(\d{3,4})\s*$", text)
    if compact_match:
        raw = compact_match.group(1)
        hour = int(raw[:-2])
        minute = int(raw[-2:])
        return max(0, min(23, hour)) * 60 + max(0, min(59, minute))

    return 17 * 60


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


def _normalize_phone(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("+"):
        digits = re.sub(r"\D", "", raw[1:])
        return f"+{digits}" if digits else ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if len(digits) > 11:
        return f"+{digits}"
    return digits


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
    send_simple_email(app_config, candidate["client_email"], subject, text, html, brand=brand)


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


def process_appointment_reminders(
    db,
    app_config,
    now=None,
    max_per_brand=None,
    brand_ids=None,
    ignore_send_time=False,
    include_disabled=False,
):
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

    scoped_brand_ids = {int(value) for value in (brand_ids or []) if str(value).strip().isdigit()}
    for brand in db.get_all_brands():
        if scoped_brand_ids and int(brand.get("id") or 0) not in scoped_brand_ids:
            continue
        if not include_disabled and int(brand.get("sales_bot_appointment_reminders_enabled") or 0) != 1:
            continue
        target_date = None
        base_summary = {}
        try:
            local_now = _brand_local_now(brand, now=now)
            try:
                db.update_brand_text_field(
                    brand["id"],
                    "sales_bot_appointment_reminder_last_attempt_at",
                    now.astimezone(timezone.utc).isoformat(),
                )
            except Exception:
                log.exception("Failed to update appointment reminder heartbeat for brand=%s", brand.get("id"))
            # Calculate tomorrow's date in the brand's local timezone
            target_date = local_now.date() + timedelta(days=1)
            send_after_minutes = _parse_send_minutes(brand.get("sales_bot_appointment_reminder_send_time"))
            base_summary = {
                "local_time": local_now.strftime("%Y-%m-%d %H:%M"),
                "timezone": (brand.get("sales_bot_appointment_reminder_timezone") or "America/New_York").strip() or "America/New_York",
                "send_after": _format_minutes(send_after_minutes),
            }

            sng_ready = (brand.get("crm_type") or "").strip().lower() == "sweepandgo" and bool((brand.get("crm_api_key") or "").strip())
            try:
                from webapp.teamup_calendar import teamup_config, teamup_missing_fields

                teamup_cfg = teamup_config(db, brand["id"])
                teamup_ready = bool(teamup_cfg) and not teamup_missing_fields(teamup_cfg)
            except Exception:
                teamup_ready = False

            if not sng_ready and not teamup_ready:
                db.record_appointment_reminder_run(
                    brand["id"],
                    target_date,
                    status="config_error",
                    reason="No appointment source is fully configured. Connect Sweep and Go or Teamup Calendar for appointment reminders.",
                    summary=base_summary,
                )
                continue

            send_after_minutes = _parse_send_minutes(brand.get("sales_bot_appointment_reminder_send_time"))
            if (not ignore_send_time) and (local_now.hour * 60 + local_now.minute) < send_after_minutes:
                current_mins = (local_now.hour * 60 + local_now.minute)
                log.debug(f"[Appointment] Brand {brand['id']}: Current time {current_mins} min < send time {send_after_minutes} min, waiting")
                continue

            stats["brands"] += 1
            log.info(f"[Appointment] Brand {brand['id']}: Proceeding (current time {local_now.strftime('%H:%M')} >= send time {_format_minutes(send_after_minutes)})")
            candidates = []
            source_errors = []
            source_names = []
            if sng_ready:
                # Query SNG for day-ahead appointments using target_date (tomorrow in the brand's local timezone)
                # NOTE: This assumes SNG's jobs_for_date endpoint interprets dates in the account's local context.
                # If SNG always interprets dates as UTC regardless of account settings, we need to adjust this.
                sng_candidates, error = sng_get_day_ahead_appointment_candidates(
                    brand,
                    target_date=target_date,
                    max_jobs=max_per_brand,
                )
                if error:
                    source_errors.append(str(error)[:200])
                else:
                    candidates.extend(sng_candidates)
                    source_names.append("sweepandgo")
            if teamup_ready:
                from webapp.teamup_calendar import teamup_day_ahead_appointment_candidates

                teamup_candidates, error = teamup_day_ahead_appointment_candidates(
                    db,
                    brand,
                    target_date=target_date,
                    max_events=max_per_brand,
                )
                if error:
                    source_errors.append(str(error)[:200])
                else:
                    candidates.extend(teamup_candidates)
                    source_names.append("teamup_calendar")
            if source_errors and not candidates:
                stats["errors"].extend(source_errors)
                db.record_appointment_reminder_run(
                    brand["id"],
                    target_date,
                    status="failed",
                    reason="; ".join(source_errors)[:500],
                    summary={**base_summary, "sources": source_names},
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
                    if channel == "sms":
                        recipient = _normalize_phone(recipient)
                    if not recipient:
                        stats["skipped"] += 1
                        brand_stats["skipped"] += 1
                        continue

                    detail_payload = {
                        "job_id": candidate.get("job_id") or "",
                        "source": candidate.get("source") or "sweepandgo",
                        "event_title": candidate.get("event_title") or "",
                        "status_name": candidate.get("status_name") or "",
                        "assigned_to_name": candidate.get("assigned_to_name") or "",
                        "address": candidate.get("address") or "",
                        "preferred_channel": candidate.get("preferred_channel") or "",
                        "recipient_dedupe_key": recipient if channel == "sms" else "",
                    }
                    pending_detail = json.dumps({"state": "pending", **detail_payload}, separators=(",", ":"))[:500]
                    claimed = db.try_claim_client_billing_reminder(
                        brand["id"],
                        appointment_key,
                        appointment_date,
                        channel,
                        recipient=recipient,
                        detail=pending_detail,
                        reminder_type=reminder_type,
                        dedupe_recipient=channel == "sms",
                    )
                    if not claimed:
                        stats["skipped"] += 1
                        brand_stats["skipped"] += 1
                        continue

                    try:
                        if channel == "email":
                            _send_email_reminder(app_config, brand, candidate)
                            ok, detail = True, "sent"
                        else:
                            ok, detail = send_transactional_sms(db, brand, recipient, _render_message(brand, candidate, "sms"))

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

            if brand_stats["sent"] or brand_stats["failed"]:
                if brand_stats["failed"]:
                    run_status = "partial" if brand_stats["sent"] else "failed"
                else:
                    run_status = "completed"
                if not brand_stats["candidates"]:
                    run_reason = "No eligible appointment candidates were found for tomorrow."
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
                        "sources": source_names,
                        "source_errors": source_errors,
                    },
                )
        except Exception as exc:
            brand_id = brand.get("id")
            log.exception("Unhandled appointment reminder error for brand=%s", brand_id)
            stats["errors"].append(f"brand {brand_id}: {str(exc)[:180]}")
            fallback_target_date = target_date or (now.astimezone(_get_zoneinfo(brand.get("sales_bot_appointment_reminder_timezone"))).date() + timedelta(days=1))
            fallback_summary = base_summary or {
                "timezone": (brand.get("sales_bot_appointment_reminder_timezone") or "America/New_York").strip() or "America/New_York",
                "send_after": _format_minutes(_parse_send_minutes(brand.get("sales_bot_appointment_reminder_send_time"))),
            }
            try:
                db.record_appointment_reminder_run(
                    brand_id,
                    fallback_target_date,
                    status="failed",
                    reason=f"Unhandled appointment reminder error: {str(exc)[:500]}",
                    summary=fallback_summary,
                )
            except Exception:
                log.exception("Failed to record appointment reminder run failure for brand=%s", brand_id)
            continue

    stats["errors"] = stats["errors"][:10]
    return stats
