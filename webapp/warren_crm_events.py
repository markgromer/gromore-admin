import json
import logging
from datetime import UTC, datetime, timedelta

from webapp.email_sender import send_simple_email
from webapp.warren_contact_policy import lookup_contact_policy
from webapp.warren_sender import send_reply, send_transactional_sms

log = logging.getLogger(__name__)


RULE_DEFINITIONS = {
    "failed_payment": {
        "label": "Failed Payments",
        "event_types": {"client:client_payment_declined"},
        "resolution_event_types": {"client:client_payment_accepted"},
        "default_channels": ["sms", "email"],
        "default_delay_minutes": 5,
        "default_retry_days": 2,
        "default_max_attempts": 3,
        "default_template": (
            "Hi {client_name}, this is {brand_name}. We had a problem processing your payment. "
            "If you want, reply here and we can help you update billing right away."
        ),
        "default_owner_subject": "Failed payment alert - {client_name}",
        "default_owner_template": (
            "{brand_name} received a failed payment event for {client_name}.\n\n"
            "Client ID: {client_id}\nPayment ID: {payment_id}\nStatus: {status}\n"
            "Phone: {client_phone}\nEmail: {client_email}\n\n"
            "Source event: {event_type}"
        ),
    },
    "invoice_finalized": {
        "label": "Invoice Finalized",
        "event_types": {"client:invoice_finalized"},
        "resolution_event_types": set(),
        "default_channels": ["email"],
        "default_delay_minutes": 15,
        "default_retry_days": 0,
        "default_max_attempts": 1,
        "default_template": (
            "Hi {client_name}, your invoice from {brand_name} is finalized. If you need a copy or want help before it is due, reply here and we will take care of it."
        ),
        "default_owner_subject": "Invoice finalized - {client_name}",
        "default_owner_template": (
            "{brand_name} finalized an invoice for {client_name}.\n\n"
            "Client ID: {client_id}\nInvoice ID: {invoice_id}\nStatus: {status}\nSource event: {event_type}"
        ),
    },
    "subscription_canceled": {
        "label": "Subscription Canceled",
        "event_types": {"client:subscription_canceled"},
        "resolution_event_types": set(),
        "default_channels": ["email"],
        "default_delay_minutes": 10,
        "default_retry_days": 1,
        "default_max_attempts": 2,
        "default_template": (
            "Hi {client_name}, we saw that your {brand_name} subscription was canceled. If that was not intentional or you want help getting service back on track, reply here and we will help."
        ),
        "default_owner_subject": "Subscription canceled - {client_name}",
        "default_owner_template": (
            "{brand_name} received a subscription cancellation event for {client_name}.\n\n"
            "Client ID: {client_id}\nSubscription ID: {subscription_id}\nStatus: {status}\nSource event: {event_type}"
        ),
    },
    "subscription_paused": {
        "label": "Subscription Paused",
        "event_types": {"client:subscription_paused"},
        "resolution_event_types": {"client:subscription_unpaused"},
        "default_channels": ["email"],
        "default_delay_minutes": 10,
        "default_retry_days": 2,
        "default_max_attempts": 2,
        "default_template": (
            "Hi {client_name}, we noticed your {brand_name} subscription is paused. If you need help resuming service or want to talk through timing, reply here and we will help."
        ),
        "default_owner_subject": "Subscription paused - {client_name}",
        "default_owner_template": (
            "{brand_name} received a subscription paused event for {client_name}.\n\n"
            "Client ID: {client_id}\nSubscription ID: {subscription_id}\nStatus: {status}\nSource event: {event_type}"
        ),
    },
    "quote_not_signed_up": {
        "label": "Quote Not Signed Up",
        "event_types": {
            "client:free_quote_created",
            "client:free_quote_requested",
            "client:quote_created",
            "client:quote_generated",
            "free_quote:created",
            "free_quote.created",
            "partial_quote",
            "quote:created",
            "quote.created",
            "quote_not_signed_up",
            "quote_started",
            "quote:sent",
        },
        "resolution_event_types": {
            "client:client_created",
            "client:client_activated",
            "client:client_payment_accepted",
            "client:subscription_created",
            "client:subscription_started",
        },
        "default_channels": ["sms"],
        "default_delay_minutes": 1440,
        "default_retry_days": 0,
        "default_max_attempts": 1,
        "default_owner_alert": False,
        "default_template": (
            "Hi {client_name}, this is {brand_name}. Just checking in on the quote we sent. "
            "Did you want help getting service started?"
        ),
        "default_owner_subject": "Quote follow-up queued - {client_name}",
        "default_owner_template": (
            "{brand_name} queued a quote follow-up check for {client_name}.\n\n"
            "Client ID: {client_id}\nQuote ID: {quote_id}\nPhone: {client_phone}\nEmail: {client_email}\n\n"
            "Warren will only text if the contact is still not active in Sweep and Go when the delay expires."
        ),
    },
}

EVENT_TYPE_TO_RULE_KEY = {
    event_type: rule_key
    for rule_key, definition in RULE_DEFINITIONS.items()
    for event_type in definition["event_types"]
}

RESOLUTION_EVENT_TO_RULE_KEYS = {}
for rule_key, definition in RULE_DEFINITIONS.items():
    for event_type in definition["resolution_event_types"]:
        RESOLUTION_EVENT_TO_RULE_KEYS.setdefault(event_type, []).append(rule_key)


def _safe_json_object(raw_value):
    if isinstance(raw_value, dict):
        return dict(raw_value)
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _parse_channel_list(value, fallback=None):
    fallback = list(fallback or ["email"])
    raw_channels = []
    if isinstance(value, list):
        raw_channels = value
    else:
        text = (value or "").strip()
        if text.startswith("["):
            try:
                raw_channels = json.loads(text)
            except Exception:
                raw_channels = [part.strip() for part in text.split(",")]
        elif text:
            raw_channels = [part.strip() for part in text.split(",")]

    cleaned = []
    for channel in raw_channels:
        normalized = str(channel or "").strip().lower()
        if normalized in {"sms", "email"} and normalized not in cleaned:
            cleaned.append(normalized)
    return cleaned or fallback


def _parse_email_list(raw_value):
    emails = []
    seen = set()
    for part in str(raw_value or "").replace("\n", ",").split(","):
        email = part.strip().lower()
        if not email or "@" not in email or email in seen:
            continue
        seen.add(email)
        emails.append(email)
    return emails


def _coerce_bounded_int(value, default, minimum, maximum):
    try:
        numeric = int(float(value))
    except (TypeError, ValueError):
        numeric = int(default)
    return max(minimum, min(maximum, numeric))


def get_default_crm_event_rules():
    rules = {}
    for rule_key, definition in RULE_DEFINITIONS.items():
        rules[rule_key] = {
            "enabled": False,
            "channels": list(definition["default_channels"]),
            "delay_minutes": int(definition["default_delay_minutes"]),
            "retry_days": int(definition["default_retry_days"]),
            "max_attempts": int(definition["default_max_attempts"]),
            "respect_dnd": True,
            "owner_alert": bool(definition.get("default_owner_alert", True)),
            "template": definition["default_template"],
        }
    return {
        "alert_emails": [],
        "rules": rules,
    }


def load_crm_event_rules(brand):
    defaults = get_default_crm_event_rules()
    raw_rules = _safe_json_object((brand or {}).get("sales_bot_crm_event_rules"))
    raw_global_emails = _parse_email_list((brand or {}).get("sales_bot_crm_event_alert_emails"))

    merged = {
        "alert_emails": raw_global_emails,
        "rules": defaults["rules"],
    }
    for rule_key, definition in defaults["rules"].items():
        raw_rule = raw_rules.get(rule_key)
        if not isinstance(raw_rule, dict):
            continue
        merged["rules"][rule_key] = {
            "enabled": bool(raw_rule.get("enabled")),
            "channels": _parse_channel_list(raw_rule.get("channels"), definition["channels"]),
            "delay_minutes": _coerce_bounded_int(raw_rule.get("delay_minutes", definition["delay_minutes"]), definition["delay_minutes"], 0, 10080),
            "retry_days": _coerce_bounded_int(raw_rule.get("retry_days", definition["retry_days"]), definition["retry_days"], 0, 30),
            "max_attempts": _coerce_bounded_int(raw_rule.get("max_attempts", definition["max_attempts"]), definition["max_attempts"], 1, 10),
            "respect_dnd": bool(raw_rule.get("respect_dnd", True)),
            "owner_alert": bool(raw_rule.get("owner_alert", True)),
            "template": str(raw_rule.get("template") or definition["template"]).strip() or definition["template"],
        }
    return merged


def serialize_crm_event_rules(form_data):
    payload = {"alert_emails": _parse_email_list(form_data.get("sales_bot_crm_event_alert_emails", "")), "rules": {}}
    for rule_key, definition in RULE_DEFINITIONS.items():
        payload["rules"][rule_key] = {
            "enabled": bool(form_data.get(f"crm_rule_{rule_key}_enabled")),
            "channels": _parse_channel_list(form_data.getlist(f"crm_rule_{rule_key}_channels"), definition["default_channels"]),
            "delay_minutes": _coerce_bounded_int(form_data.get(f"crm_rule_{rule_key}_delay_minutes"), definition["default_delay_minutes"], 0, 10080),
            "retry_days": _coerce_bounded_int(form_data.get(f"crm_rule_{rule_key}_retry_days"), definition["default_retry_days"], 0, 30),
            "max_attempts": _coerce_bounded_int(form_data.get(f"crm_rule_{rule_key}_max_attempts"), definition["default_max_attempts"], 1, 10),
            "respect_dnd": bool(form_data.get(f"crm_rule_{rule_key}_respect_dnd")),
            "owner_alert": bool(form_data.get(f"crm_rule_{rule_key}_owner_alert")),
            "template": str(form_data.get(f"crm_rule_{rule_key}_template") or definition["default_template"]).strip() or definition["default_template"],
        }
    return payload


def build_crm_event_template_context(brand, summary, event_type, *, attempt_number=1):
    brand_name = (brand.get("display_name") or brand.get("name") or "our team").strip()
    context = {
        "brand_name": brand_name,
        "client_name": (summary.get("client_name") or "there").strip(),
        "client_id": summary.get("client_id") or "",
        "client_email": summary.get("client_email") or "",
        "client_phone": summary.get("client_phone") or "",
        "payment_id": summary.get("payment_id") or "",
        "invoice_id": summary.get("invoice_id") or "",
        "subscription_id": summary.get("subscription_id") or "",
        "quote_id": summary.get("quote_id") or "",
        "quote_amount": summary.get("quote_amount") or "",
        "quote_service": summary.get("quote_service") or "",
        "quote_address": summary.get("quote_address") or "",
        "status": summary.get("status") or "",
        "event_type": event_type or "",
        "attempt_number": str(attempt_number),
    }
    context["event_label"] = RULE_DEFINITIONS.get(EVENT_TYPE_TO_RULE_KEY.get(event_type, ""), {}).get("label", event_type or "CRM event")
    for key, value in (summary or {}).items():
        normalized_key = str(key or "").strip()
        if not normalized_key or normalized_key in context:
            continue
        if isinstance(value, (dict, list, tuple, set)):
            continue
        context[normalized_key] = str(value or "")
    return context


def render_template_string(template, context):
    rendered = str(template or "")
    for key, value in (context or {}).items():
        rendered = rendered.replace("{" + key + "}", str(value or ""))
    return rendered.strip()


def get_internal_alert_recipients(db, brand, rules_config=None):
    recipients = []
    seen = set()

    def _append(email):
        normalized = (email or "").strip().lower()
        if not normalized or "@" not in normalized or normalized in seen:
            return
        seen.add(normalized)
        recipients.append(normalized)

    config = rules_config or load_crm_event_rules(brand)
    for email in config.get("alert_emails", []):
        _append(email)

    for contact in db.get_brand_contacts(brand["id"]):
        role = (contact.get("role") or "").strip().lower()
        if role in {"owner", "manager", "staff", "admin"} or int(contact.get("auto_send") or 0) == 1:
            _append(contact.get("email"))

    if not recipients:
        for contact in db.get_brand_upgrade_contacts(brand["id"]):
            _append(contact.get("email"))
    return recipients


def get_rule_key_for_event_type(event_type):
    return EVENT_TYPE_TO_RULE_KEY.get((event_type or "").strip(), "")


def _queue_rule_actions(db, brand, event_id, event_type, summary, rules_config, now=None):
    now = now or datetime.now(UTC)
    rule_key = get_rule_key_for_event_type(event_type)
    if not rule_key:
        return 0
    rule = rules_config["rules"].get(rule_key) or {}
    if not rule.get("enabled"):
        return 0

    queued = 0
    rule_def = RULE_DEFINITIONS[rule_key]
    delay_minutes = max(0, int(rule.get("delay_minutes") or 0))
    retry_days = max(0, int(rule.get("retry_days") or 0))
    max_attempts = max(1, int(rule.get("max_attempts") or 1))
    context = build_crm_event_template_context(brand, summary, event_type)

    if rule_key == "quote_not_signed_up":
        recipient = (
            (summary.get("client_phone") or "").strip()
            or (summary.get("client_email") or "").strip().lower()
            or (summary.get("client_id") or "").strip()
            or str(event_id or "").strip()
        )
        action_id = db.queue_crm_event_action(
            brand["id"],
            source_event_id=event_id,
            source_event_type=event_type,
            rule_key=rule_key,
            action_kind="sng_quote_nurture",
            channel="sms",
            recipient=recipient,
            client_id=summary.get("client_id", ""),
            subject=f"{rule_def['label']} - {context['client_name']}",
            message_text=render_template_string(rule.get("template"), context),
            attempt_number=1,
            max_attempts=1,
            scheduled_for=(now + timedelta(minutes=delay_minutes)).isoformat(),
            detail=f"Queued quote follow-up eligibility check from {event_type}",
        )
        if action_id:
            queued += 1
    else:
        recipients_by_channel = {
            "sms": (summary.get("client_phone") or "").strip(),
            "email": (summary.get("client_email") or "").strip().lower(),
        }
        for channel in _parse_channel_list(rule.get("channels"), rule_def["default_channels"]):
            recipient = recipients_by_channel.get(channel, "")
            if not recipient:
                continue
            for attempt_number in range(1, max_attempts + 1):
                scheduled_for = now + timedelta(minutes=delay_minutes) + timedelta(days=retry_days * (attempt_number - 1))
                action_id = db.queue_crm_event_action(
                    brand["id"],
                    source_event_id=event_id,
                    source_event_type=event_type,
                    rule_key=rule_key,
                    action_kind="client_message",
                    channel=channel,
                    recipient=recipient,
                    client_id=summary.get("client_id", ""),
                    payment_id=summary.get("payment_id", ""),
                    invoice_id=summary.get("invoice_id", ""),
                    subscription_id=summary.get("subscription_id", ""),
                    subject=f"{rule_def['label']} - {context['client_name']}",
                    message_text=render_template_string(rule.get("template"), build_crm_event_template_context(brand, summary, event_type, attempt_number=attempt_number)),
                    attempt_number=attempt_number,
                    max_attempts=max_attempts,
                    scheduled_for=scheduled_for.isoformat(),
                    detail=f"Queued from {event_type}",
                )
                if action_id:
                    queued += 1

    if rule.get("owner_alert"):
        owner_subject = render_template_string(rule_def["default_owner_subject"], context)
        owner_body = render_template_string(rule_def["default_owner_template"], context)
        for email in get_internal_alert_recipients(db, brand, rules_config):
            action_id = db.queue_crm_event_action(
                brand["id"],
                source_event_id=event_id,
                source_event_type=event_type,
                rule_key=rule_key,
                action_kind="owner_alert",
                channel="email",
                recipient=email,
                client_id=summary.get("client_id", ""),
                payment_id=summary.get("payment_id", ""),
                invoice_id=summary.get("invoice_id", ""),
                subscription_id=summary.get("subscription_id", ""),
                subject=owner_subject,
                message_text=owner_body,
                attempt_number=1,
                max_attempts=1,
                scheduled_for=(now + timedelta(minutes=delay_minutes)).isoformat(),
                detail=f"Owner alert from {event_type}",
            )
            if action_id:
                queued += 1

    return queued


def _load_sng_action_summary(db, action):
    summary = {}
    event_row = db.get_sng_webhook_event_by_external_id(action["brand_id"], action.get("source_event_id", ""))
    if event_row:
        try:
            parsed = json.loads(event_row.get("summary_json") or "{}")
            if isinstance(parsed, dict):
                summary.update(parsed)
        except Exception:
            pass

    if action.get("client_id") and not summary.get("client_id"):
        summary["client_id"] = action.get("client_id")
    if action.get("recipient") and not summary.get("client_phone"):
        recipient = str(action.get("recipient") or "").strip()
        if "@" in recipient:
            summary.setdefault("client_email", recipient)
        else:
            summary.setdefault("client_phone", recipient)
    summary.setdefault("event_type", action.get("source_event_type", ""))
    return summary


def _build_sng_quote_lead_summary(summary):
    parts = ["Sweep and Go quote follow-up candidate"]
    if summary.get("client_name"):
        parts.append(f"Name: {summary['client_name']}")
    if summary.get("client_phone"):
        parts.append(f"Phone: {summary['client_phone']}")
    if summary.get("client_email"):
        parts.append(f"Email: {summary['client_email']}")
    if summary.get("quote_id"):
        parts.append(f"Quote ID: {summary['quote_id']}")
    if summary.get("quote_amount"):
        parts.append(f"Quote Amount: {summary['quote_amount']}")
    if summary.get("quote_service"):
        parts.append(f"Service: {summary['quote_service']}")
    if summary.get("quote_address"):
        parts.append(f"Address: {summary['quote_address']}")
    return "\n".join(parts)


def _ensure_sng_quote_lead_thread(db, brand, action, summary):
    external_id = (
        f"sng_quote:{summary.get('quote_id')}"
        if summary.get("quote_id")
        else f"sng_quote_event:{action.get('source_event_id')}"
    )
    summary_text = _build_sng_quote_lead_summary(summary)
    thread_id = db.upsert_lead_thread(
        brand["id"],
        "lead_form",
        external_id,
        data={
            "lead_name": summary.get("client_name", ""),
            "lead_email": summary.get("client_email", ""),
            "lead_phone": summary.get("client_phone", ""),
            "source": "sweepandgo_quote",
            "status": "quoted",
            "quote_status": "sent",
            "summary": summary_text,
        },
    )
    existing_events = db.get_lead_events(brand["id"], thread_id, event_type="sng_quote_followup_imported", limit=1)
    if not existing_events:
        db.add_lead_message(
            thread_id,
            direction="inbound",
            role="lead",
            content=f"[Sweep and Go Quote]\n{summary_text}",
            channel="lead_form",
            external_message_id=action.get("source_event_id", ""),
            metadata={"source": "sweepandgo_quote", "summary": summary},
        )
        db.add_lead_event(
            brand["id"],
            thread_id,
            "sng_quote_followup_imported",
            event_value=str(action.get("source_event_id") or "")[:200],
            metadata={"action_id": action.get("id"), "summary": summary},
        )
    return thread_id


def _process_sng_quote_nurture_action(db, brand, action, now_iso, *, skip_dnd=False):
    summary = _load_sng_action_summary(db, action)
    contact_probe = {
        "lead_phone": summary.get("client_phone") or action.get("recipient", ""),
        "lead_email": summary.get("client_email", ""),
    }
    contact_policy = lookup_contact_policy(db, brand, contact_probe)
    if contact_policy.get("is_active_client"):
        db.update_crm_event_action(
            action["id"],
            status="resolved",
            resolved_at=now_iso,
            resolution_reason="Resolved at send time: contact is active in Sweep and Go",
            detail="Skipped quote nurture because the contact is already active.",
        )
        return "resolved", "active_client"

    if contact_policy.get("suppress_marketing"):
        reason = contact_policy.get("reason") or "contact_policy"
        db.update_crm_event_action(
            action["id"],
            status="resolved",
            resolved_at=now_iso,
            resolution_reason=f"Resolved by contact policy: {reason}",
            detail=f"Skipped quote nurture because contact policy returned {reason}.",
        )
        return "resolved", reason

    thread_id = _ensure_sng_quote_lead_thread(db, brand, action, summary)
    thread = db.get_lead_thread(thread_id, brand_id=brand["id"])

    phone = (summary.get("client_phone") or thread.get("lead_phone") or "").strip()
    if not phone:
        db.update_crm_event_action(action["id"], status="failed", detail="Lead imported, but SNG did not provide an SMS phone number.")
        db.add_lead_event(brand["id"], thread_id, "sng_quote_nurture_failed", event_value="missing_phone")
        return "failed", "missing_phone"

    message_text = (action.get("message_text") or "").strip()
    if not message_text:
        message_text = render_template_string(
            RULE_DEFINITIONS["quote_not_signed_up"]["default_template"],
            build_crm_event_template_context(brand, summary, action.get("source_event_type", "")),
        )

    message_id = db.add_lead_message(
        thread_id,
        direction="outbound",
        role="assistant",
        content=message_text,
        channel="sms",
        metadata={
            "source": "sng_quote_nurture",
            "action_id": action.get("id"),
            "delivery_status": "pending",
            "auto_send_requested": True,
            "auto_sent": False,
        },
    )
    ok, detail = send_reply(db, brand, thread_id, message_text, channel="sms", skip_dnd=skip_dnd, logged_message_id=message_id)
    if ok:
        db.add_lead_event(
            brand["id"],
            thread_id,
            "sng_quote_nurture_sms_sent",
            event_value=message_text[:200],
            metadata={"to": phone, "detail": str(detail)[:500]},
        )
    else:
        db.add_lead_event(
            brand["id"],
            thread_id,
            "sng_quote_nurture_sms_failed",
            event_value=str(detail)[:200],
            metadata={"to": phone},
        )

    db.update_crm_event_action(
        action["id"],
        status="sent" if ok else "failed",
        sent_at=now_iso if ok else "",
        detail=str(detail or "")[:1000],
    )
    return ("sent" if ok else "failed"), str(detail or "")


def _resolve_actions_for_event(db, brand, event_type, summary):
    resolved = 0
    for rule_key in RESOLUTION_EVENT_TO_RULE_KEYS.get((event_type or "").strip(), []):
        resolved += db.resolve_crm_event_actions(
            brand["id"],
            rule_key,
            client_id=summary.get("client_id", ""),
            payment_id=summary.get("payment_id", ""),
            invoice_id=summary.get("invoice_id", ""),
            subscription_id=summary.get("subscription_id", ""),
            reason=f"Resolved by {event_type}",
        )
    return resolved


def process_pending_crm_event_actions(db, app_config, brand_id=None, now=None, limit=100):
    now = now or datetime.now(UTC)
    now_iso = now.isoformat()
    stats = {"processed": 0, "sent": 0, "failed": 0, "deferred": 0, "resolved": 0}
    brands = {}
    due_actions = db.get_due_crm_event_actions(now_iso=now_iso, brand_id=brand_id, limit=limit)
    for action in due_actions:
        brand = brands.get(action["brand_id"])
        if not brand:
            brand = db.get_brand(action["brand_id"])
            if brand:
                brands[action["brand_id"]] = brand
        if not brand:
            db.update_crm_event_action(action["id"], status="failed", detail="brand_not_found")
            stats["failed"] += 1
            stats["processed"] += 1
            continue

        rules_config = load_crm_event_rules(brand)
        rule = rules_config["rules"].get(action.get("rule_key"), {})
        if action.get("channel") == "sms" and rule.get("respect_dnd"):
            from webapp.warren_nurture import _is_dnd

            if _is_dnd(brand):
                deferred_for = (now + timedelta(minutes=30)).isoformat()
                db.update_crm_event_action(action["id"], scheduled_for=deferred_for, detail="Held by quiet hours")
                stats["deferred"] += 1
                continue

        try:
            if action.get("rule_key") == "quote_not_signed_up" or action.get("action_kind") == "sng_quote_nurture":
                status, _detail = _process_sng_quote_nurture_action(
                    db,
                    brand,
                    action,
                    now_iso,
                    skip_dnd=not bool(rule.get("respect_dnd", True)),
                )
                stats[status if status in stats else "processed"] += 1
            elif action.get("channel") == "sms":
                ok, detail = send_transactional_sms(db, brand, action.get("recipient"), action.get("message_text"), append_opt_out_footer=True)
                db.update_crm_event_action(
                    action["id"],
                    status="sent" if ok else "failed",
                    sent_at=now_iso if ok else "",
                    detail=str(detail or "")[:1000],
                )
                stats["sent" if ok else "failed"] += 1
            else:
                html = "<div style=\"font-family:Arial,sans-serif;white-space:pre-wrap;line-height:1.6;\">%s</div>" % str(action.get("message_text") or "").replace("\n", "<br>")
                send_simple_email(app_config, action.get("recipient"), action.get("subject") or "CRM Event Alert", action.get("message_text") or "", html, brand=brand)
                ok, detail = True, "sent"
                db.update_crm_event_action(
                    action["id"],
                    status="sent",
                    sent_at=now_iso,
                    detail=str(detail or "")[:1000],
                )
                stats["sent"] += 1
        except Exception as exc:
            log.warning("CRM event action failed: brand=%s action=%s err=%s", brand.get("id"), action.get("id"), exc)
            db.update_crm_event_action(action["id"], status="failed", detail=str(exc)[:1000])
            stats["failed"] += 1
        stats["processed"] += 1

    return stats


def process_incoming_sng_event(db, app_config, brand, event_id, event_type, summary, base_detail=""):
    rules_config = load_crm_event_rules(brand)
    resolved = _resolve_actions_for_event(db, brand, event_type, summary)
    queued = _queue_rule_actions(db, brand, event_id, event_type, summary, rules_config)
    delivery_stats = process_pending_crm_event_actions(db, app_config, brand_id=brand["id"], limit=100)

    if queued or resolved:
        status = "processed"
        detail = f"{base_detail or event_type} - queued {queued} CRM action(s); resolved {resolved}; sent {delivery_stats.get('sent', 0)}; failed {delivery_stats.get('failed', 0)}."
    elif get_rule_key_for_event_type(event_type):
        status = "ignored"
        detail = f"{base_detail or event_type} - rule exists, but it is disabled or missing recipients."
    else:
        status = "ignored"
        detail = base_detail or f"No CRM automation is configured for {event_type}."

    db.update_sng_webhook_event(brand["id"], event_id, status=status, detail=detail)
    result = {
        "status": status,
        "queued": queued,
        **delivery_stats,
    }
    result["resolved"] = resolved + int(delivery_stats.get("resolved") or 0)
    return result
