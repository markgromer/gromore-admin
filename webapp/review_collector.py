"""
Review Collector: compliant review requests, private feedback, and tracking.
"""
from __future__ import annotations

import html
import json
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
DEFAULT_REVIEW_AUDIENCE_GROUPS = [
    "warren_won_clients",
    "jobber_clients",
    "sng_active_clients",
    "sng_inactive_clients",
]
REVIEW_BLOCKED_GROUPS = {"warren_active_leads"}

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

JOB_COMPLETION_EVENT_TOKENS = (
    "job_completed", "job.complete", "job:completed", "job completed",
    "visit_completed", "visit.complete", "appointment_completed",
    "appointment.complete", "work_order_completed", "service_completed",
    "service.complete", "client:job_completed", "job_closed", "job.close",
)


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
        "recent_service_days": _coerce_int((brand or {}).get("review_automation_recent_service_days"), 7, 1, 30),
        "min_private_rating": _coerce_int((brand or {}).get("review_automation_min_private_rating"), 4, 1, 5),
    }


def default_review_audience_groups(brand, groups):
    available = {group.get("key") for group in groups or []}
    saved = [key for key in _safe_json_list((brand or {}).get("review_default_group_keys")) if key in available]
    if saved:
        return saved

    automation_saved = [
        key for key in _safe_json_list((brand or {}).get("review_automation_group_keys"))
        if key in available and key != "warren_active_leads"
    ]
    if automation_saved:
        return automation_saved

    preferred = [key for key in DEFAULT_REVIEW_AUDIENCE_GROUPS if key in available]
    if preferred:
        return preferred

    return []


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


def is_job_completion_event(event_type, summary=None):
    text = " ".join(
        str(part or "").strip().lower()
        for part in (
            event_type,
            (summary or {}).get("event_type"),
            (summary or {}).get("status"),
            (summary or {}).get("job_status"),
            (summary or {}).get("appointment_status"),
            (summary or {}).get("service_status"),
        )
        if str(part or "").strip()
    )
    if not text:
        return False
    if any(token in text for token in JOB_COMPLETION_EVENT_TOKENS):
        return True
    return bool(
        ("job" in text or "visit" in text or "appointment" in text or "service" in text)
        and any(done in text for done in ("complete", "completed", "closed", "finished", "done"))
    )


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
        "service_date_source": _clean((recipient or {}).get("service_date_source")),
        "service_date_detail": _clean((recipient or {}).get("service_date_detail")),
    }


def enrich_review_recipients_with_recent_service(db, brand, recipients, *, now=None):
    settings = automation_settings(brand)
    enriched = []
    warnings = []
    crm_lookups = 0
    crm_matches = 0
    today = (now or datetime.utcnow()).date()
    max_lookups = 75
    for recipient in recipients or []:
        item = dict(recipient or {})
        if _extract_service_date(item) or crm_lookups >= max_lookups:
            enriched.append(item)
            continue
        crm_lookups += 1
        try:
            from webapp.crm_bridge import crm_find_recent_service_date_for_review

            match, error = crm_find_recent_service_date_for_review(
                brand,
                item,
                lookback_days=settings["recent_service_days"],
                today=today,
            )
        except Exception as exc:
            match, error = None, str(exc)
        if error and len(warnings) < 3:
            warnings.append(str(error)[:160])
        if match and match.get("service_date"):
            item["last_service_date"] = match["service_date"]
            item["service_date_source"] = match.get("source") or "crm_recent_service"
            item["service_date_detail"] = match.get("detail") or ""
            crm_matches += 1
        enriched.append(item)
    return enriched, {
        "crm_lookups": crm_lookups,
        "crm_matches": crm_matches,
        "warnings": warnings,
    }


def build_review_intelligence(db, brand, recipients, *, enrich_missing_dates=False):
    enrichment = {"crm_lookups": 0, "crm_matches": 0, "warnings": []}
    if enrich_missing_dates:
        recipients, enrichment = enrich_review_recipients_with_recent_service(db, brand, recipients)
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
        "enrichment": enrichment,
    }


def _parse_action_payload(action):
    try:
        payload = json.loads((action or {}).get("message_text") or "{}")
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _action_recipient(action):
    payload = _parse_action_payload(action)
    recipient = payload.get("recipient") if isinstance(payload.get("recipient"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    if not recipient:
        recipient = _summary_recipient(summary, event_id=(action or {}).get("source_event_id"), event_type=(action or {}).get("source_event_type"))
    return recipient or {}


def _count_by(items, key):
    counts = {}
    for item in items or []:
        value = _clean(item.get(key)) or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return counts


def build_review_autopilot_dashboard(db, brand, app_config=None):
    settings = automation_settings(brand)
    setup = review_setup_status(brand)
    app_config = app_config or {}
    smtp_ready = bool(app_config.get("SMTP_USER") and app_config.get("SMTP_PASSWORD"))
    sms_ready = setup["sms_ready"]
    now = datetime.utcnow()

    actions = db.get_crm_event_actions(brand["id"], limit=300, action_kind="review_request")
    requests = db.get_review_requests(brand["id"], limit=300)
    queued_actions = [a for a in actions if _clean(a.get("status")) == "queued"]
    sent_actions = [a for a in actions if _clean(a.get("status")) == "sent"]
    resolved_actions = [a for a in actions if _clean(a.get("status")) == "resolved"]
    failed_actions = [a for a in actions if _clean(a.get("status")) == "failed"]
    due_actions = [
        a for a in queued_actions
        if (_parse_dt(a.get("scheduled_for")) or now) <= now
    ]

    queue = []
    for action in sorted(queued_actions, key=lambda a: a.get("scheduled_for") or "")[:12]:
        recipient = _action_recipient(action)
        scheduled_dt = _parse_dt(action.get("scheduled_for"))
        queue.append({
            "id": action.get("id"),
            "customer": _clean(recipient.get("name")) or "Customer",
            "recipient": _clean(recipient.get("phone")) or _clean(recipient.get("email")) or _clean(action.get("recipient")),
            "source": _clean(recipient.get("source")) or "crm_event",
            "source_event_type": _clean(action.get("source_event_type")),
            "service_date": _clean(recipient.get("last_service_date")),
            "scheduled_for": action.get("scheduled_for") or "",
            "scheduled_label": _format_date(scheduled_dt) if scheduled_dt else "",
            "due": scheduled_dt <= now if scheduled_dt else True,
            "detail": _clean(action.get("detail")),
        })

    suppressed = [r for r in requests if _clean(r.get("eligibility_status")) not in ("", "eligible")]
    suppressed_counts = _count_by(suppressed, "eligibility_status")
    recovery_items = []
    min_rating = settings["min_private_rating"]
    for request in requests:
        try:
            rating = int(request.get("rating") or 0)
        except (TypeError, ValueError):
            rating = 0
        if rating and rating < min_rating:
            recovery_items.append({
                "id": request.get("id"),
                "customer": request.get("customer_name") or "Customer",
                "rating": rating,
                "feedback": request.get("feedback_text") or "",
                "submitted_at": request.get("feedback_submitted_at") or request.get("updated_at") or "",
                "status": request.get("status") or "",
            })
    recovery_items = recovery_items[:8]

    blockers = []
    if not settings["enabled"]:
        blockers.append({
            "level": "warning",
            "title": "Review Autopilot is paused",
            "detail": "WARREN can still send manual requests, but CRM job completions will not trigger automatic asks.",
            "fix": "Turn on automatic review requests after the destination and channels are ready.",
        })
    if not setup["ready"]:
        blockers.append({
            "level": "danger",
            "title": "No public review destination",
            "detail": "WARREN needs a Google Place ID or review URL before it can send a compliant request.",
            "fix": "Add the Google Place ID or paste the direct review link in Request Setup.",
        })
    if "sms" in settings["channels"] and not sms_ready:
        blockers.append({
            "level": "danger",
            "title": "SMS channel is selected but not connected",
            "detail": "Review requests will fail over SMS until this brand has a sending number and API key.",
            "fix": "Connect OpenPhone/Quo or remove SMS from automation channels.",
        })
    if "email" in settings["channels"] and not smtp_ready:
        blockers.append({
            "level": "danger",
            "title": "Email channel is selected but SMTP is not ready",
            "detail": "Email review requests cannot send without server SMTP credentials.",
            "fix": "Configure SMTP or leave email off for this brand.",
        })
    if settings["enabled"] and not queued_actions and not sent_actions and not resolved_actions:
        blockers.append({
            "level": "warning",
            "title": "No CRM completion events have queued review asks yet",
            "detail": "WARREN is waiting for completed-job webhooks with customer contact details.",
            "fix": "Verify Jobber/SNG completion webhooks are enabled and sending customer name plus phone or email.",
        })
    if suppressed_counts.get("needs_service_date"):
        blockers.append({
            "level": "warning",
            "title": "Some customers are missing service dates",
            "detail": f"{suppressed_counts['needs_service_date']} recipient(s) could not be timed against the post-service window.",
            "fix": "Use CRM completion events or map the CRM field that carries completed/service date.",
        })
    if recovery_items:
        blockers.append({
            "level": "danger",
            "title": "Service recovery is needed before more asks",
            "detail": f"{len(recovery_items)} customer(s) gave low private feedback.",
            "fix": "Handle recovery, then mark the task done or mark the customer reviewed/suppressed.",
        })

    if blockers and any(b["level"] == "danger" for b in blockers):
        health = "blocked"
        health_label = "Blocked"
    elif blockers:
        health = "attention"
        health_label = "Needs attention"
    elif settings["enabled"]:
        health = "healthy"
        health_label = "Autopilot ready"
    else:
        health = "paused"
        health_label = "Paused"

    return {
        "health": health,
        "health_label": health_label,
        "blockers": blockers,
        "queue": queue,
        "recovery_items": recovery_items,
        "suppressed_counts": suppressed_counts,
        "source_counts": _count_by(actions, "source_event_type"),
        "action_status_counts": _count_by(actions, "status"),
        "metrics": {
            "crm_queued": len(queued_actions),
            "crm_due": len(due_actions),
            "crm_sent": len(sent_actions),
            "crm_resolved": len(resolved_actions),
            "crm_failed": len(failed_actions),
            "suppressed": len(suppressed),
            "recovery": len(recovery_items),
        },
        "playbook": [
            "Completed job or visit arrives from CRM.",
            f"WARREN waits {settings['delay_days']} day(s), and if the customer is missing a service date, it checks the CRM for completed/paid work in the last {settings['recent_service_days']} day(s).",
            "WARREN checks review history, opt-outs, service age, and feedback before sending.",
            "Eligible customers get the same public review destination by selected channel.",
            "Low private feedback creates a service-recovery task instead of another public ask.",
        ],
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
    group_keys = [key for key in group_keys or [] if key not in REVIEW_BLOCKED_GROUPS]
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


def _candidate_status_counts(candidates):
    counts = {}
    for candidate in candidates or []:
        if candidate.get("eligible"):
            continue
        status = _clean(candidate.get("status")) or "suppressed"
        counts[status] = counts.get(status, 0) + 1
    return counts


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
            "source": _clean(manual_recipient.get("source")) or "manual",
            "source_id": _clean(manual_recipient.get("source_id")),
            "groups": ["manual"],
            "last_service_date": _clean(manual_recipient.get("last_service_date") or manual_recipient.get("service_date")),
        }
        if manual["email"] or manual["phone"]:
            recipients.append(manual)
    if group_keys:
        group_keys = [key for key in group_keys or [] if key not in REVIEW_BLOCKED_GROUPS]
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

    intelligence = build_review_intelligence(db, brand, recipients, enrich_missing_dates=True)
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
    return [group for group in group_catalog(brand) if group.get("key") not in REVIEW_BLOCKED_GROUPS]


def _summary_recipient(summary, event_id="", event_type=""):
    summary = summary or {}
    return {
        "name": _clean(summary.get("client_name") or summary.get("customer_name") or summary.get("name")) or "Customer",
        "email": _clean(summary.get("client_email") or summary.get("customer_email") or summary.get("email")).lower(),
        "phone": _normalize_phone(summary.get("client_phone") or summary.get("customer_phone") or summary.get("phone")),
        "source": _clean(summary.get("source")) or "crm_event",
        "source_id": _clean(summary.get("client_id") or summary.get("customer_id") or summary.get("job_id") or summary.get("item_id") or event_id),
        "last_service_date": _clean(
            summary.get("service_date")
            or summary.get("completed_at")
            or summary.get("occurred_at")
            or summary.get("job_completed_at")
            or summary.get("event_time")
        ),
        "event_type": event_type,
    }


def queue_review_request_from_crm_event(db, brand, event_id, event_type, summary, *, now=None):
    settings = automation_settings(brand)
    if not settings["enabled"]:
        return 0
    if not is_job_completion_event(event_type, summary):
        return 0
    if not review_setup_status(brand)["ready"]:
        return 0

    recipient = _summary_recipient(summary, event_id=event_id, event_type=event_type)
    if not (recipient["email"] or recipient["phone"]):
        return 0

    now = now or datetime.now(UTC)
    service_dt = _parse_dt(recipient.get("last_service_date")) or now.replace(tzinfo=None)
    scheduled_for = service_dt + timedelta(days=settings["delay_days"])
    now_naive = now.replace(tzinfo=None)
    if scheduled_for < now_naive:
        scheduled_for = now_naive

    action_id = db.queue_crm_event_action(
        brand["id"],
        source_event_id=event_id,
        source_event_type=event_type,
        rule_key="review_request",
        action_kind="review_request",
        channel="review",
        recipient=recipient["phone"] or recipient["email"] or recipient["source_id"],
        client_id=recipient["source_id"],
        subject=f"Review request - {recipient['name']}",
        message_text=json.dumps({"summary": summary or {}, "recipient": recipient}, separators=(",", ":"))[:2000],
        attempt_number=1,
        max_attempts=1,
        scheduled_for=scheduled_for.replace(tzinfo=UTC).isoformat(),
        detail=f"Queued review request from {event_type}",
    )
    return 1 if action_id else 0


def process_review_request_action(db, app_config, brand, action):
    payload = _parse_action_payload(action)
    recipient = payload.get("recipient") if isinstance(payload.get("recipient"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    if not recipient:
        recipient = _summary_recipient(summary, event_id=action.get("source_event_id"), event_type=action.get("source_event_type"))
    result = send_review_requests(
        db,
        app_config,
        brand,
        channels=automation_settings(brand)["channels"],
        manual_recipient=recipient,
        created_by=f"crm_event:{action.get('source_event_type') or ''}",
    )
    return result


def maybe_create_review_recovery_task(db, brand, review_request):
    try:
        rating = int((review_request or {}).get("rating") or 0)
    except (TypeError, ValueError):
        rating = 0
    settings = automation_settings(brand)
    if not rating or rating >= settings["min_private_rating"]:
        return 0

    request_id = (review_request or {}).get("id")
    source_ref = f"review_request:{request_id}"
    existing = db.get_brand_task_by_source(brand["id"], "review_recovery", source_ref)
    if existing and (existing.get("status") or "open") != "done":
        return existing.get("id") or 0

    customer = _clean((review_request or {}).get("customer_name")) or "Customer"
    feedback = _clean((review_request or {}).get("feedback_text")) or "No written feedback was provided."
    steps = [
        {"label": "Review the private feedback and customer history.", "done": False},
        {"label": "Contact the customer personally before asking for any public review.", "done": False},
        {"label": "Record the recovery outcome in WARREN.", "done": False},
    ]
    description = (
        f"{customer} left {rating}/5 private feedback from a review request.\n\n"
        f"Feedback: {feedback}\n\n"
        "Do not send another public review ask until this is handled."
    )
    return db.create_brand_task(
        brand["id"],
        f"Service recovery: {customer}",
        description=description,
        steps_json=json.dumps(steps),
        priority="high",
        source="review_recovery",
        source_ref=source_ref,
        created_by=None,
    )


def process_review_automation(db, app_config, brand, *, created_by="automation"):
    settings = automation_settings(brand)
    if not settings["enabled"]:
        return {"ran": False, "reason": "Review automation is disabled.", "sent_sms": 0, "sent_email": 0, "recipient_count": 0}
    recipients, errors = collect_recipients(db, brand, settings["groups"])
    recipients = recipients[:MAX_REVIEW_REQUESTS]
    intelligence = build_review_intelligence(db, brand, recipients, enrich_missing_dates=True)
    sweep = {
        "checked": len(recipients),
        "eligible": intelligence["eligible"],
        "suppressed": intelligence["suppressed"],
        "suppressed_counts": _candidate_status_counts(intelligence["candidates"]),
        "crm_service_lookups": (intelligence.get("enrichment") or {}).get("crm_lookups", 0),
        "crm_service_matches": (intelligence.get("enrichment") or {}).get("crm_matches", 0),
        "crm_service_warnings": (intelligence.get("enrichment") or {}).get("warnings", []),
        "errors": errors,
        "sent_sms": 0,
        "sent_email": 0,
        "failed": 0,
        "skipped": 0,
        "recipient_count": 0,
    }
    if not recipients:
        return {
            "ran": True,
            "sent": False,
            "reason": "No reachable recipients matched the automation audiences.",
            **sweep,
        }
    if not intelligence["eligible"]:
        return {
            "ran": True,
            "sent": False,
            "reason": "No recipients are eligible for a review request yet.",
            **sweep,
        }
    setup = review_setup_status(brand)
    if not setup["ready"]:
        return {
            "ran": True,
            "sent": False,
            "reason": "Add a Google Place ID or manual review link before WARREN can send the eligible requests.",
            **sweep,
        }
    smtp_ready = bool(app_config.get("SMTP_USER") and app_config.get("SMTP_PASSWORD"))
    if "sms" in settings["channels"] and not setup["sms_ready"]:
        return {
            "ran": True,
            "sent": False,
            "reason": "SMS is selected for review automation, but SMS is not configured for this brand.",
            **sweep,
        }
    if "email" in settings["channels"] and not smtp_ready:
        return {
            "ran": True,
            "sent": False,
            "reason": "Email is selected for review automation, but SMTP is not configured.",
            **sweep,
        }
    delivery = send_review_requests(
        db,
        app_config,
        brand,
        group_keys=settings["groups"],
        channels=settings["channels"],
        created_by=created_by,
    )
    return {
        "ran": True,
        "sent": True,
        **sweep,
        **delivery,
    }
