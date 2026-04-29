"""
Audience resolution and delivery for Warren bulk messages.
"""
from __future__ import annotations

import html
import json
import logging
import re
from datetime import UTC, datetime

from webapp.email_sender import send_bulk_email
from webapp.warren_sender import send_transactional_sms

log = logging.getLogger(__name__)


MAX_GROUP_PAGES = 10
MAX_SEND_RECIPIENTS = 500


def _clean(value):
    return str(value or "").strip()


def _digits(value):
    return re.sub(r"\D+", "", _clean(value))


def _normalize_email(value):
    return _clean(value).lower()


def _normalize_phone(value):
    raw = _clean(value)
    if not raw:
        return ""
    if raw.startswith("+"):
        return "+" + _digits(raw)
    digits = _digits(raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return raw


def _first(data, *keys):
    if not isinstance(data, dict):
        return ""
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return ""


def _name_from_record(record):
    name = _first(record, "name", "full_name", "fullName", "client_name", "display_name", "companyName", "company")
    if name:
        return _clean(name)
    first = _first(record, "first_name", "firstName", "firstname")
    last = _first(record, "last_name", "lastName", "lastname")
    return " ".join(part for part in (_clean(first), _clean(last)) if part).strip()


def _email_from_record(record):
    email = _first(record, "email", "email_address", "emailAddress")
    if email:
        return _normalize_email(email)
    emails = record.get("emails") if isinstance(record, dict) else None
    if isinstance(emails, list):
        primary = next((item for item in emails if isinstance(item, dict) and item.get("primary")), None)
        candidate = primary or next((item for item in emails if isinstance(item, dict)), None)
        if candidate:
            return _normalize_email(_first(candidate, "address", "email", "value"))
    return ""


def _phone_from_record(record):
    phone = _first(record, "phone", "mobile", "mobile_phone", "phone_number", "phoneNumber", "cell_phone")
    if phone:
        return _normalize_phone(phone)
    phones = record.get("phones") if isinstance(record, dict) else None
    if isinstance(phones, list):
        primary = next((item for item in phones if isinstance(item, dict) and item.get("primary")), None)
        candidate = primary or next((item for item in phones if isinstance(item, dict)), None)
        if candidate:
            return _normalize_phone(_first(candidate, "number", "phone", "value"))
    return ""


def _external_id(record):
    return _clean(_first(record, "id", "client_id", "clientId", "uuid", "external_id", "externalId"))


def _recipient_key(recipient):
    email = _normalize_email(recipient.get("email"))
    phone = _normalize_phone(recipient.get("phone"))
    if email:
        return f"email:{email}"
    if phone:
        return f"phone:{phone}"
    return f"{recipient.get('source', 'unknown')}:{recipient.get('source_id', '')}:{recipient.get('name', '')}".lower()


def _dedupe(recipients):
    deduped = {}
    for recipient in recipients:
        key = _recipient_key(recipient)
        if not key:
            continue
        existing = deduped.get(key)
        if existing:
            existing_groups = set(existing.get("groups", []))
            existing_groups.update(recipient.get("groups", []))
            existing["groups"] = sorted(existing_groups)
            if not existing.get("email") and recipient.get("email"):
                existing["email"] = recipient["email"]
            if not existing.get("phone") and recipient.get("phone"):
                existing["phone"] = recipient["phone"]
            continue
        recipient["key"] = key
        deduped[key] = recipient
    return list(deduped.values())


def _recipient(name="", email="", phone="", source="", source_id="", group=""):
    return {
        "name": _clean(name) or "Customer",
        "email": _normalize_email(email),
        "phone": _normalize_phone(phone),
        "source": _clean(source),
        "source_id": _clean(source_id),
        "groups": [group] if group else [],
    }


def _thread_recipient(thread, group):
    return _recipient(
        name=thread.get("lead_name"),
        email=thread.get("lead_email"),
        phone=thread.get("lead_phone"),
        source=f"warren:{thread.get('status') or 'lead'}",
        source_id=thread.get("id"),
        group=group,
    )


def _sng_recipient(record, group, source):
    return _recipient(
        name=_name_from_record(record),
        email=_email_from_record(record),
        phone=_phone_from_record(record),
        source=source,
        source_id=_external_id(record),
        group=group,
    )


def _collect_sng_pages(fetcher):
    records = []
    errors = []
    page = 1
    while page <= MAX_GROUP_PAGES:
        result, error = fetcher(page)
        if error:
            errors.append(str(error))
            break
        if not isinstance(result, dict):
            break
        data = result.get("data")
        if isinstance(data, list):
            records.extend(data)
        paginate = result.get("paginate") or {}
        try:
            total_pages = int(paginate.get("total_pages") or paginate.get("pages") or 1)
        except (TypeError, ValueError):
            total_pages = 1
        if page >= total_pages:
            break
        page += 1
    return records, errors


def _collect_free_quotes(brand):
    from webapp.crm_bridge import sng_get_free_quotes

    result, error = sng_get_free_quotes(brand)
    if error:
        return [], [str(error)]
    if not isinstance(result, dict):
        return [], []
    quotes = result.get("free_quotes")
    if not isinstance(quotes, list):
        quotes = result.get("data") if isinstance(result.get("data"), list) else []
    return quotes, []


def _collect_jobber_clients(brand):
    from webapp.crm_bridge import _jobber_graphql

    query = """
    query BulkMessageClients($first: Int!, $after: String) {
      clients(first: $first, after: $after) {
        nodes {
          id
          firstName
          lastName
          companyName
          emails { address primary }
          phones { number primary }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
    """
    records = []
    errors = []
    after = None
    for _page in range(MAX_GROUP_PAGES):
        variables = {"first": 100, "after": after}
        result, error = _jobber_graphql(brand, query, variables)
        if error:
            errors.append(str(error))
            break
        clients = (result or {}).get("clients") if isinstance(result, dict) else {}
        records.extend((clients or {}).get("nodes") or [])
        page_info = (clients or {}).get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if not after:
            break
    return records, errors


def group_catalog(brand):
    crm_type = _clean((brand or {}).get("crm_type")).lower()
    connected = bool((brand or {}).get("crm_api_key"))
    groups = [
        {"key": "warren_active_leads", "label": "Warren Active Leads", "source": "Warren"},
        {"key": "warren_won_clients", "label": "Warren Won Clients", "source": "Warren"},
        {"key": "warren_past_leads", "label": "Warren Lost/Past Leads", "source": "Warren"},
    ]
    if crm_type == "sweepandgo" and connected:
        groups.extend([
            {"key": "sng_active_clients", "label": "SNG Active Clients", "source": "Sweep and Go"},
            {"key": "sng_inactive_clients", "label": "SNG Inactive/Past Clients", "source": "Sweep and Go"},
            {"key": "sng_no_subscription", "label": "SNG Active, No Subscription", "source": "Sweep and Go"},
            {"key": "sng_open_leads", "label": "SNG Open Leads", "source": "Sweep and Go"},
            {"key": "sng_free_quotes", "label": "SNG Free Quotes", "source": "Sweep and Go"},
        ])
    if crm_type == "jobber" and connected:
        groups.append({"key": "jobber_clients", "label": "Jobber Clients", "source": "Jobber"})
    return groups


def collect_recipients(db, brand, group_keys):
    brand_id = brand["id"]
    selected = set(group_keys or [])
    recipients = []
    errors = {}

    if "warren_active_leads" in selected:
        recipients.extend(_thread_recipient(t, "warren_active_leads") for t in db.get_active_lead_contacts(brand_id, limit=1000))
    if "warren_won_clients" in selected:
        recipients.extend(_thread_recipient(t, "warren_won_clients") for t in db.get_lead_threads(brand_id, status="won", limit=1000))
    if "warren_past_leads" in selected:
        recipients.extend(_thread_recipient(t, "warren_past_leads") for t in db.get_lead_threads(brand_id, status="lost", limit=1000))

    crm_type = _clean(brand.get("crm_type")).lower()
    if crm_type == "sweepandgo" and brand.get("crm_api_key"):
        from webapp.crm_bridge import (
            sng_get_active_clients,
            sng_get_active_no_subscription,
            sng_get_inactive_clients,
            sng_get_leads,
        )

        sng_groups = {
            "sng_active_clients": (lambda page: sng_get_active_clients(brand, page=page), "sng:active_client"),
            "sng_inactive_clients": (lambda page: sng_get_inactive_clients(brand, page=page), "sng:inactive_client"),
            "sng_no_subscription": (lambda page: sng_get_active_no_subscription(brand, page=page), "sng:no_subscription"),
            "sng_open_leads": (lambda page: sng_get_leads(brand, page=page), "sng:lead"),
        }
        for key, (fetcher, source) in sng_groups.items():
            if key not in selected:
                continue
            rows, group_errors = _collect_sng_pages(fetcher)
            if group_errors:
                errors[key] = group_errors[0]
            recipients.extend(_sng_recipient(row, key, source) for row in rows)
        if "sng_free_quotes" in selected:
            rows, group_errors = _collect_free_quotes(brand)
            if group_errors:
                errors["sng_free_quotes"] = group_errors[0]
            recipients.extend(_sng_recipient(row, "sng_free_quotes", "sng:free_quote") for row in rows)

    if crm_type == "jobber" and brand.get("crm_api_key") and "jobber_clients" in selected:
        rows, group_errors = _collect_jobber_clients(brand)
        if group_errors:
            errors["jobber_clients"] = group_errors[0]
        recipients.extend(_sng_recipient(row, "jobber_clients", "jobber:client") for row in rows)

    recipients = [r for r in recipients if r.get("email") or r.get("phone")]
    recipients = _dedupe(recipients)
    return recipients, errors


def preview_bulk_message(db, brand, group_keys, channels):
    recipients, errors = collect_recipients(db, brand, group_keys)
    channels = set(channels or [])
    sms_ready = bool(_clean(brand.get("quo_api_key")) and _clean(brand.get("quo_phone_number")))

    return {
        "recipients": recipients,
        "errors": errors,
        "total": len(recipients),
        "sms_count": sum(1 for r in recipients if r.get("phone")),
        "email_count": sum(1 for r in recipients if r.get("email")),
        "sendable_sms": sum(1 for r in recipients if r.get("phone") and not db.is_opted_out(brand["id"], r.get("phone"))),
        "opted_out_sms": sum(1 for r in recipients if r.get("phone") and db.is_opted_out(brand["id"], r.get("phone"))),
        "sms_ready": sms_ready,
        "email_selected": "email" in channels,
        "sms_selected": "sms" in channels,
    }


def send_bulk_message(db, app_config, brand, group_keys, channels, subject, body, sent_by="client"):
    group_keys = list(dict.fromkeys(group_keys or []))
    channels = [c for c in dict.fromkeys(channels or []) if c in {"email", "sms"}]
    subject = _clean(subject)[:200]
    body = _clean(body)

    if not group_keys:
        raise ValueError("Select at least one group.")
    if not channels:
        raise ValueError("Select email, SMS, or both.")
    if not body:
        raise ValueError("Message body is required.")
    if "email" in channels and not subject:
        raise ValueError("Email subject is required when email is selected.")

    recipients, errors = collect_recipients(db, brand, group_keys)
    recipients = recipients[:MAX_SEND_RECIPIENTS]
    if not recipients:
        raise ValueError("No reachable recipients matched those groups.")

    sent_sms = 0
    sent_email = 0
    failed = 0
    skipped = 0
    details = []

    if "sms" in channels:
        for recipient in recipients:
            phone = recipient.get("phone")
            if not phone:
                skipped += 1
                continue
            if db.is_opted_out(brand["id"], phone):
                skipped += 1
                continue
            ok, detail = send_transactional_sms(db, brand, phone, body, append_opt_out_footer=True)
            if ok:
                sent_sms += 1
            elif detail == "opted_out":
                skipped += 1
            else:
                failed += 1
                details.append(f"{recipient.get('name')}: {detail}")

    if "email" in channels:
        email_recipients = [{"email": r["email"], "name": r.get("name", "")} for r in recipients if r.get("email")]
        if email_recipients:
            html_body = (
                "<div style=\"font-family:Arial,sans-serif;white-space:pre-wrap;line-height:1.6;\">"
                f"{html.escape(body).replace(chr(10), '<br>')}"
                "</div>"
            )
            try:
                sent_email = send_bulk_email(app_config, email_recipients, subject, body, html_body=html_body)
            except Exception as exc:
                failed += len(email_recipients)
                details.append(str(exc)[:300])
        else:
            skipped += len(recipients)

    if hasattr(db, "record_warren_bulk_message_run"):
        db.record_warren_bulk_message_run(
            brand["id"],
            subject=subject,
            body_text=body,
            groups=group_keys,
            channels=channels,
            sent_by=sent_by,
            recipient_count=len(recipients),
            sent_sms=sent_sms,
            sent_email=sent_email,
            skipped=skipped,
            failed=failed,
            detail="\n".join(details[:10]),
            errors=errors,
        )

    log.info(
        "Warren bulk message sent brand=%s groups=%s channels=%s sms=%s email=%s failed=%s skipped=%s",
        brand.get("id"), group_keys, channels, sent_sms, sent_email, failed, skipped,
    )

    return {
        "recipient_count": len(recipients),
        "sent_sms": sent_sms,
        "sent_email": sent_email,
        "failed": failed,
        "skipped": skipped,
        "errors": errors,
        "detail": "\n".join(details[:10]),
        "sent_at": datetime.now(UTC).isoformat(),
    }
