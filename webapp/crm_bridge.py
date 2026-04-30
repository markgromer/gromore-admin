"""
CRM integration bridge - pushes lead/conversion events to external CRMs.
Supports GoHighLevel, HubSpot, Sweep and Go, Jobber, RazorSync, generic
webhooks, and standalone payment-provider revenue pulls.
"""

import json
import logging
import re
import base64
from collections import Counter
from datetime import date, datetime, timedelta

import requests
from flask import current_app

log = logging.getLogger(__name__)

TIMEOUT = 15


GHL_LEADCONNECTOR_BASE = "https://services.leadconnectorhq.com"
GHL_LEADCONNECTOR_VERSION = "2021-07-28"


def _ghl_location_id(brand):
    return (brand.get("titan_ghl_location_id") or "").strip() or None


def _ghl_lc_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Version": GHL_LEADCONNECTOR_VERSION,
    }


def _parse_ts_ms(value):
    """Best-effort timestamp parse to epoch-ms."""
    from datetime import datetime

    if value is None:
        return 0

    if isinstance(value, (int, float)):
        ts = int(value)
        # Heuristic: seconds vs ms
        return ts * 1000 if ts < 10**12 else ts

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return 0
        # numeric string
        if s.isdigit():
            ts = int(s)
            return ts * 1000 if ts < 10**12 else ts
        # ISO-ish
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
        except (ValueError, TypeError):
            return 0

    return 0


def _month_bounds(month_prefix=None):
    month_prefix = (month_prefix or "").strip() or datetime.now().strftime("%Y-%m")
    start = datetime.strptime(month_prefix + "-01", "%Y-%m-%d")
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return month_prefix, start, end


def _parse_razorsync_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"/Date\((-?\d+)", text)
    if match:
        try:
            return datetime.fromtimestamp(int(match.group(1)) / 1000)
        except (ValueError, OSError, OverflowError):
            return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _money_to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, dict):
        for key in ("amount", "Amount", "value", "Value"):
            if key in value:
                return _money_to_float(value.get(key))
        return 0.0
    try:
        return float(str(value).replace("$", "").replace(",", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _sng_extract_payments(result_dict):
    result_dict = _sng_normalize_detail_payload(result_dict)
    if not isinstance(result_dict, dict):
        return []
    payments = result_dict.get("payments")
    if isinstance(payments, list):
        return payments
    data = result_dict.get("data")
    if isinstance(data, dict):
        payments = data.get("payments")
        if isinstance(payments, list):
            return payments
        client = data.get("client")
        if isinstance(client, dict):
            payments = client.get("payments")
            if isinstance(payments, list):
                return payments
    client = result_dict.get("client")
    if isinstance(client, dict):
        payments = client.get("payments")
        if isinstance(payments, list):
            return payments
    return []


def _sng_normalize_detail_payload(payload):
    if isinstance(payload, dict):
        return payload
    if not isinstance(payload, list):
        return payload
    if not payload:
        return {}

    dict_items = [item for item in payload if isinstance(item, dict)]
    if not dict_items:
        if len(payload) == 1:
            return _sng_normalize_detail_payload(payload[0])
        return {}

    payment_like_keys = {"amount", "amount_paid", "total", "value", "status", "date", "created_at", "createdAt", "tip_amount", "tip"}
    if all(payment_like_keys.intersection(item.keys()) for item in dict_items):
        return {"payments": dict_items}

    if len(payload) == 1:
        return _sng_normalize_detail_payload(payload[0])

    for item in dict_items:
        if isinstance(item.get("payments"), list):
            return item
        data = item.get("data")
        if isinstance(data, dict):
            if isinstance(data.get("payments"), list):
                return item
            if isinstance(data.get("client"), dict):
                return item
        if isinstance(item.get("client"), dict):
            return item

    return dict_items[0]


def _sng_extract_client_record(result_dict):
    result_dict = _sng_normalize_detail_payload(result_dict)
    if not isinstance(result_dict, dict):
        return {}
    data = result_dict.get("data")
    if isinstance(data, dict):
        client = data.get("client")
        if isinstance(client, dict):
            return client
    client = result_dict.get("client")
    if isinstance(client, dict):
        return client
    if isinstance(data, dict):
        return data
    return result_dict


def _sng_parse_date(value):
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        ts = int(value)
        if ts > 10**12:
            ts = ts / 1000.0
        try:
            return datetime.utcfromtimestamp(ts).date()
        except Exception:
            return None
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(raw[:10], fmt).date()
        except Exception:
            continue
    return None


def _sng_money(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned.startswith("$"):
            cleaned = cleaned[1:].strip()
        try:
            return float(cleaned)
        except Exception:
            return 0.0
    return 0.0


def _sng_json_object(value):
    if isinstance(value, dict):
        return value
    if not value or not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _sng_json_list(value):
    if isinstance(value, list):
        return value
    if not value or not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _sng_payment_amount(pmt):
    if not isinstance(pmt, dict):
        return 0.0
    base = (
        pmt.get("amount")
        or pmt.get("amount_paid")
        or pmt.get("total")
        or pmt.get("value")
        or 0
    )
    tip = pmt.get("tip_amount") or pmt.get("tip") or 0
    return _sng_money(base) + _sng_money(tip)


def _sng_webhook_rows(result_dict):
    if isinstance(result_dict, list):
        return result_dict
    if not isinstance(result_dict, dict):
        return []
    for key in ("data", "items", "webhooks", "events"):
        rows = result_dict.get(key)
        if isinstance(rows, list):
            return rows
        parsed_rows = _sng_json_list(rows)
        if parsed_rows:
            return parsed_rows
    return []


def _sng_webhook_total_pages(result_dict):
    if not isinstance(result_dict, dict):
        return 1
    paginate = result_dict.get("paginate") or result_dict.get("pagination") or {}
    if not isinstance(paginate, dict):
        return 1
    try:
        return max(int(paginate.get("total_pages") or paginate.get("pages") or 1), 1)
    except (TypeError, ValueError):
        return 1


def _sng_find_nested_value(payload, *keys):
    wanted = {str(key or "").strip().lower() for key in keys if str(key or "").strip()}
    if not wanted:
        return ""

    queue = [payload]
    depth = 0
    while queue and depth < 6:
        next_queue = []
        for item in queue:
            if isinstance(item, dict):
                for key, value in item.items():
                    if str(key).strip().lower() in wanted and value not in (None, ""):
                        return value
                    if isinstance(value, (dict, list)):
                        next_queue.append(value)
            elif isinstance(item, list):
                next_queue.extend(value for value in item if isinstance(value, (dict, list)))
        queue = next_queue
        depth += 1
    return ""


def _sng_webhook_payload(row):
    if not isinstance(row, dict):
        return {}

    for key in ("payload", "payload_json", "body"):
        value = row.get(key)
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            return {"items": value}
        parsed = _sng_json_object(value)
        if parsed:
            return parsed

    direct_payload = {}
    for key in ("client", "payment", "invoice", "subscription"):
        value = row.get(key)
        if isinstance(value, dict):
            direct_payload[key] = value
        else:
            parsed = _sng_json_object(value)
            if parsed:
                direct_payload[key] = parsed
    if direct_payload:
        for key in ("event_type", "type", "topic", "name", "created_at", "received_at", "sent_at", "date"):
            if row.get(key) not in (None, ""):
                direct_payload[key] = row.get(key)
        return direct_payload

    return row


def _sng_webhook_event_type(row):
    if not isinstance(row, dict):
        return ""
    for key in ("event_type", "type", "topic", "name"):
        value = row.get(key)
        if isinstance(value, str) and value.strip() and value.strip().lower() != "event":
            return value.strip()

    payload = _sng_webhook_payload(row)
    for key in ("event_type", "type", "topic", "name"):
        value = _sng_find_nested_value(payload, key)
        if isinstance(value, str) and value.strip() and value.strip().lower() != "event":
            return value.strip()
    return ""


def _sng_webhook_payment_row(row):
    payload = _sng_webhook_payload(row)
    payment = payload.get("payment") if isinstance(payload.get("payment"), dict) else {}
    payment_block = payment if payment else payload

    payment_date = _sng_parse_date(
        payment_block.get("date")
        or payment_block.get("created_at")
        or payment_block.get("createdAt")
        or row.get("created_at")
        or row.get("received_at")
        or row.get("sent_at")
    )
    if not payment_date:
        return None

    amount = _sng_payment_amount(payment_block)
    if amount <= 0:
        amount = _sng_payment_amount(payload)
    if amount <= 0:
        return None

    payment_id = (
        payment_block.get("id")
        or payment_block.get("payment_id")
        or row.get("payment_id")
        or row.get("id")
        or ""
    )
    return {
        "date": payment_date,
        "amount": amount,
        "payment_id": str(payment_id or ""),
        "raw": row,
    }


def _sng_sum_webhook_payments_for_month(brand, month_prefix, max_pages=12):
    total_revenue = 0.0
    payment_count = 0
    diag = {
        "pages_fetched": 0,
        "rows_seen": 0,
        "matched_events": 0,
        "event_types_seen": {},
        "sample_event": None,
        "first_error": None,
    }
    seen_payment_keys = set()
    page = 1

    while page <= max_pages:
        result, error = sng_list_webhook_events(brand, page=page)
        if error:
            if not diag["first_error"]:
                diag["first_error"] = error
            break

        rows = _sng_webhook_rows(result)
        diag["pages_fetched"] += 1
        if not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            diag["rows_seen"] += 1
            event_type = _sng_webhook_event_type(row) or "unknown"
            diag["event_types_seen"][event_type] = diag["event_types_seen"].get(event_type, 0) + 1
            if event_type != "client:client_payment_accepted":
                continue

            payment_row = _sng_webhook_payment_row(row)
            if not payment_row:
                continue
            if payment_row["date"].strftime("%Y-%m") != month_prefix:
                continue

            dedupe_key = payment_row["payment_id"] or f"{payment_row['date'].isoformat()}:{payment_row['amount']:.2f}"
            if dedupe_key in seen_payment_keys:
                continue
            seen_payment_keys.add(dedupe_key)

            total_revenue += payment_row["amount"]
            payment_count += 1
            diag["matched_events"] += 1
            if diag["sample_event"] is None:
                diag["sample_event"] = {
                    "event_type": event_type,
                    "payment_id": payment_row["payment_id"],
                    "amount": round(payment_row["amount"], 2),
                    "date": payment_row["date"].isoformat(),
                }

        if page >= _sng_webhook_total_pages(result):
            break
        page += 1

    return round(total_revenue, 2), payment_count, diag


def _sng_successful_payments(result_dict):
    rows = []
    for payment in _sng_extract_payments(result_dict):
        if not isinstance(payment, dict):
            continue
        status = str(payment.get("status") or "").strip().lower()
        if status not in {"succeeded", "success", "successful", "paid", "completed"}:
            continue
        payment_date = _sng_parse_date(payment.get("date") or payment.get("created_at") or payment.get("createdAt"))
        if not payment_date:
            continue
        rows.append({
            "date": payment_date,
            "amount": _sng_payment_amount(payment),
            "raw": payment,
        })
    rows.sort(key=lambda item: item["date"])
    return rows


def _sng_add_months(base_date, months):
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    return date(year, month, min(base_date.day, last_day))


def _infer_next_payment_date(successful_payments, today=None):
    today = today or datetime.utcnow().date()
    payment_dates = []
    for payment in successful_payments:
        payment_date = payment.get("date") if isinstance(payment, dict) else None
        if payment_date and (not payment_dates or payment_date != payment_dates[-1]):
            payment_dates.append(payment_date)
    if len(payment_dates) < 2:
        return None

    intervals = []
    for index in range(1, len(payment_dates)):
        delta_days = (payment_dates[index] - payment_dates[index - 1]).days
        if delta_days > 0:
            intervals.append(delta_days)
    if not intervals:
        return None

    cadence_labels = []
    for interval in intervals:
        if 26 <= interval <= 35:
            cadence_labels.append(("months", 1))
        elif 55 <= interval <= 66:
            cadence_labels.append(("months", 2))
        elif 80 <= interval <= 100:
            cadence_labels.append(("months", 3))
        elif 6 <= interval <= 8:
            cadence_labels.append(("days", 7))
        elif 13 <= interval <= 15:
            cadence_labels.append(("days", 14))
        elif 20 <= interval <= 24:
            cadence_labels.append(("days", 21))
        elif 27 <= interval <= 31:
            cadence_labels.append(("days", 30))

    last_date = payment_dates[-1]
    if cadence_labels:
        cadence_kind, cadence_value = Counter(cadence_labels).most_common(1)[0][0]
        due_date = _sng_add_months(last_date, cadence_value) if cadence_kind == "months" else last_date + timedelta(days=cadence_value)
    else:
        sorted_intervals = sorted(intervals)
        median_interval = sorted_intervals[len(sorted_intervals) // 2]
        if median_interval < 6 or median_interval > 100:
            return None
        due_date = last_date + timedelta(days=median_interval)

    if due_date <= today - timedelta(days=7):
        return None
    if due_date >= today + timedelta(days=120):
        return None
    return due_date


def _billing_date_for_month(year, month, billing_day):
    billing_day = max(1, min(31, int(billing_day or 1)))
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    return date(year, month, min(billing_day, last_day))


def _next_uniform_billing_date(billing_day, today=None):
    today = today or datetime.utcnow().date()
    try:
        billing_day = max(1, min(31, int(billing_day or 1)))
    except (TypeError, ValueError):
        return None

    current_month_due = _billing_date_for_month(today.year, today.month, billing_day)
    if current_month_due < today:
        next_month_year = today.year + (1 if today.month == 12 else 0)
        next_month = 1 if today.month == 12 else today.month + 1
        return _billing_date_for_month(next_month_year, next_month, billing_day)
    return current_month_due


def _sng_extract_contact_info(client_record, fallback_client_id=""):
    client_record = client_record if isinstance(client_record, dict) else {}

    def _first_non_empty(keys):
        for key in keys:
            value = client_record.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""

    first_name = _first_non_empty(["first_name", "firstName"])
    last_name = _first_non_empty(["last_name", "lastName"])
    full_name = _first_non_empty(["name", "full_name", "fullName", "client_name", "customer_name"])
    if not full_name:
        full_name = " ".join(part for part in (first_name, last_name) if part).strip()

    return {
        "external_client_id": _first_non_empty(["client", "id", "client_id", "customer_id"]) or fallback_client_id,
        "client_name": full_name,
        "client_email": _first_non_empty(["email", "email_address", "emailAddress"]),
        "client_phone": _first_non_empty(["phone", "phone_number", "phoneNumber", "mobile", "mobile_phone", "mobilePhone", "cell_phone", "cellPhone"]),
    }


def _sng_truthy(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _sng_job_key(job_row, target_date):
    if not isinstance(job_row, dict):
        return ""
    job_id = str(job_row.get("id") or "").strip()
    if job_id and job_id != "0":
        return f"job:{job_id}"

    location_id = str(job_row.get("client_location_id") or job_row.get("commercial_location_id") or "").strip()
    client_id = str(job_row.get("client_id") or job_row.get("commercial_client_id") or job_row.get("client") or "").strip()
    address = str(job_row.get("address") or "").strip().lower()
    job_type = str(job_row.get("type") or "").strip().lower()
    order = str(job_row.get("order") or "").strip()
    return "undispatched:{target}:{location}:{client}:{job_type}:{order}:{address}".format(
        target=target_date,
        location=location_id or "na",
        client=client_id or "na",
        job_type=job_type or "unknown",
        order=order or "na",
        address=address or "na",
    )


def _sng_is_terminal_dispatch_status(status_name):
    status = str(status_name or "").strip().lower()
    # Keep reminder eligibility broad and exclude only clearly terminal outcomes.
    return status in {
        "completed",
        "cancelled",
        "canceled",
        "failed",
        "closed",
    }


def sng_get_day_ahead_appointment_candidates(brand, target_date, max_jobs=None):
    """Get candidates for day-ahead appointment reminders.
    
    IMPORTANT: This function assumes SNG's jobs_for_date endpoint interprets dates
    in the same timezone context as our caller (i.e., the caller must pass dates
    that are already in the correct timezone). We calculate target_date in the brand's
    local timezone before calling this function. If SNG interprets the date string
    differently (e.g., always as UTC), we need to adjust the datetime conversion at
    the call site in process_appointment_reminders().
    """
    if hasattr(target_date, "isoformat"):
        target_date = target_date.isoformat()
    target_date = str(target_date or "").strip()
    if not target_date:
        return [], "A target date is required"

    result, error = sng_get_dispatch_board(brand, target_date)
    if error or not isinstance(result, dict):
        return [], error or "Sweep and Go dispatch board returned no data"

    candidates = []
    seen_keys = set()
    rows = result.get("data") or []
    for job_row in rows:
        if not isinstance(job_row, dict):
            continue
        status_name = str(job_row.get("status_name") or "").strip().lower()
        if _sng_is_terminal_dispatch_status(status_name):
            continue

        job_key = _sng_job_key(job_row, target_date)
        if not job_key or job_key in seen_keys:
            continue
        seen_keys.add(job_key)

        contact = _sng_extract_contact_info(job_row, fallback_client_id=job_key)
        if not contact["client_email"] and not contact["client_phone"]:
            continue

        preferred_channel = str(job_row.get("channel") or "").strip().lower()
        if preferred_channel not in {"sms", "email", "call"}:
            preferred_channel = ""

        address_bits = [
            str(job_row.get("address") or "").strip(),
            str(job_row.get("city") or "").strip(),
            str(job_row.get("state_name") or "").strip(),
        ]
        address_label = ", ".join(bit for bit in address_bits if bit)

        candidates.append({
            **contact,
            "appointment_key": job_key,
            "appointment_date": target_date,
            "appointment_date_obj": _sng_parse_date(target_date),
            "job_id": str(job_row.get("id") or "").strip(),
            "job_type": str(job_row.get("type") or "").strip().lower(),
            "status_name": status_name,
            "assigned_to_name": str(job_row.get("assigned_to_name") or "").strip(),
            "address": address_label,
            "preferred_channel": preferred_channel,
            "prefers_sms": _sng_truthy(job_row.get("on_the_way")),
            "prefers_email": preferred_channel == "email",
            "raw": job_row,
        })

        if max_jobs and len(candidates) >= max(0, int(max_jobs)):
            break

    return candidates, None


def sng_get_payment_reminder_candidates(brand, billing_day, days_before=3, today=None, max_clients=None, force=False):
    today = today or datetime.utcnow().date()
    due_date = _next_uniform_billing_date(billing_day, today=today)
    if not due_date:
        return [], "A valid billing day is required"

    try:
        days_before = int(max(0, min(21, float(days_before or 0))))
    except (TypeError, ValueError):
        days_before = 3

    target_due_date = today + timedelta(days=days_before)
    if not force and due_date != target_due_date:
        return [], None
    effective_days_before = max(0, (due_date - today).days) if force else days_before

    candidates = []
    page = 1
    while True:
        result, error = sng_get_active_clients(brand, page=page)
        if error or not isinstance(result, dict):
            return candidates, error

        for client_row in (result.get("data") or []):
            client_id = ""
            if isinstance(client_row, dict):
                client_id = str(client_row.get("client") or client_row.get("id") or client_row.get("client_id") or "").strip()
            if not client_id:
                continue

            contact = _sng_extract_contact_info(client_row, fallback_client_id=client_id)
            if not contact["client_email"] and not contact["client_phone"]:
                detail_result, detail_error = sng_get_client_details(brand, client_id)
                if not detail_error and isinstance(detail_result, dict):
                    contact = _sng_extract_contact_info(_sng_extract_client_record(detail_result), fallback_client_id=client_id)
            if not contact["client_email"] and not contact["client_phone"]:
                continue

            candidates.append({
                **contact,
                "due_date": due_date.isoformat(),
                "due_date_obj": due_date,
                "days_before": effective_days_before,
                "billing_day": int(billing_day),
            })
            if max_clients and len(candidates) >= max(0, int(max_clients)):
                return candidates, None

        paginate = result.get("paginate") or {}
        if page >= (paginate.get("total_pages") or 1):
            break
        page += 1

    return candidates, None


def push_lead(brand, lead_data):
    """
    Push a lead event to the brand's configured CRM.

    brand: dict with crm_type, crm_api_key, crm_webhook_url, crm_pipeline_id,
           crm_server_url (for MCP-backed CRMs like Sweep and Go)
    lead_data: dict with name, email, phone, source, notes, etc.

    Returns (success: bool, detail: str)
    """
    crm_type = (brand.get("crm_type") or "").strip().lower()
    if not crm_type or crm_type == "none":
        return False, "No CRM configured for this brand"

    dispatch = {
        "gohighlevel": _push_gohighlevel,
        "hubspot": _push_hubspot,
        "sweepandgo": _push_sweepandgo,
        "jobber": _push_jobber,
        "razorsync": _push_razorsync,
        "webhook": _push_webhook,
    }

    handler = dispatch.get(crm_type)
    if not handler:
        return False, f"Unknown CRM type: {crm_type}"

    try:
        return handler(brand, lead_data)
    except Exception as e:
        log.exception("CRM push failed for %s", crm_type)
        return False, str(e)


def _push_gohighlevel(brand, lead_data):
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return False, "GoHighLevel token not configured"

    pipeline_id = (brand.get("crm_pipeline_id") or "").strip()

    payload = {
        "firstName": lead_data.get("first_name", ""),
        "lastName": lead_data.get("last_name", ""),
        "email": lead_data.get("email", ""),
        "phone": lead_data.get("phone", ""),
        "source": lead_data.get("source", "Ad Platform"),
        "tags": lead_data.get("tags", []),
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Create contact
    resp = requests.post(
        "https://rest.gohighlevel.com/v1/contacts/",
        json=payload,
        headers=headers,
        timeout=TIMEOUT,
    )

    if resp.status_code not in (200, 201):
        return False, f"GHL contact create failed: {resp.status_code} {resp.text[:200]}"

    contact_id = resp.json().get("contact", {}).get("id")

    # If pipeline configured, create an opportunity
    if pipeline_id and contact_id:
        opp_payload = {
            "pipelineId": pipeline_id,
            "name": f"{lead_data.get('first_name', '')} {lead_data.get('last_name', '')}".strip() or "New Lead",
            "contactId": contact_id,
            "status": "open",
            "source": lead_data.get("source", "Ad Platform"),
        }
        opp_resp = requests.post(
            "https://rest.gohighlevel.com/v1/pipelines/opportunities/",
            json=opp_payload,
            headers=headers,
            timeout=TIMEOUT,
        )
        if opp_resp.status_code not in (200, 201):
            log.warning("GHL opportunity create failed: %s", opp_resp.text[:200])

    return True, f"Contact created: {contact_id}"


def _push_hubspot(brand, lead_data):
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return False, "HubSpot API key not configured"

    payload = {
        "properties": {
            "firstname": lead_data.get("first_name", ""),
            "lastname": lead_data.get("last_name", ""),
            "email": lead_data.get("email", ""),
            "phone": lead_data.get("phone", ""),
            "hs_lead_status": "NEW",
            "lead_source": lead_data.get("source", "Ad Platform"),
        }
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.hubapi.com/crm/v3/objects/contacts",
        json=payload,
        headers=headers,
        timeout=TIMEOUT,
    )

    if resp.status_code not in (200, 201):
        return False, f"HubSpot contact create failed: {resp.status_code} {resp.text[:200]}"

    contact_id = resp.json().get("id")

    # If pipeline configured, create a deal
    pipeline_id = (brand.get("crm_pipeline_id") or "").strip()
    if pipeline_id and contact_id:
        deal_payload = {
            "properties": {
                "dealname": f"{lead_data.get('first_name', '')} {lead_data.get('last_name', '')}".strip() or "New Lead",
                "pipeline": pipeline_id,
                "dealstage": "appointmentscheduled",
            },
            "associations": [
                {
                    "to": {"id": contact_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}],
                }
            ],
        }
        deal_resp = requests.post(
            "https://api.hubapi.com/crm/v3/objects/deals",
            json=deal_payload,
            headers=headers,
            timeout=TIMEOUT,
        )
        if deal_resp.status_code not in (200, 201):
            log.warning("HubSpot deal create failed: %s", deal_resp.text[:200])

    return True, f"Contact created: {contact_id}"


def _push_webhook(brand, lead_data):
    webhook_url = (brand.get("crm_webhook_url") or "").strip()
    if not webhook_url:
        return False, "Webhook URL not configured"

    payload = {
        "brand": brand.get("display_name"),
        "lead": lead_data,
    }

    resp = requests.post(
        webhook_url,
        json=payload,
        timeout=TIMEOUT,
    )

    if resp.status_code not in (200, 201, 202, 204):
        return False, f"Webhook returned {resp.status_code}: {resp.text[:200]}"

    return True, f"Webhook delivered ({resp.status_code})"


def _razorsync_server_name(brand):
    raw = (brand.get("crm_server_url") or brand.get("crm_pipeline_id") or "").strip()
    raw = re.sub(r"^https?://", "", raw, flags=re.I)
    raw = raw.split("/")[0].strip()
    raw = raw.replace(".0.razorsync.com", "").replace(".razorsync.com", "")
    return raw


def _razorsync_base_url(brand):
    server_name = _razorsync_server_name(brand)
    if not server_name:
        return ""
    return f"https://{server_name}.0.razorsync.com/ApiService.svc"


def _razorsync_api(brand, method, path, json_body=None, params=None):
    token = (brand.get("crm_api_key") or "").strip()
    server_name = _razorsync_server_name(brand)
    base_url = _razorsync_base_url(brand)
    if not token:
        return None, "RazorSync API token not configured"
    if not server_name or not base_url:
        return None, "RazorSync portal/server name not configured"

    headers = {
        "Token": token,
        "ServerName": server_name,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{base_url}/{str(path or '').lstrip('/')}"
    try:
        resp = requests.request(method, url, json=json_body, params=params, headers=headers, timeout=TIMEOUT)
    except requests.RequestException as exc:
        return None, f"RazorSync API request failed: {str(exc)[:150]}"

    if resp.status_code not in (200, 201, 204):
        return None, f"RazorSync API returned {resp.status_code}: {resp.text[:300]}"
    if resp.status_code == 204:
        return {}, None
    try:
        return resp.json(), None
    except ValueError:
        return {"raw": resp.text[:500]}, None


def razorsync_test_connection(brand):
    result, error = _razorsync_api(brand, "GET", "Settings")
    if error:
        return "", error
    portal = ""
    if isinstance(result, dict):
        portal = result.get("PortalName") or result.get("PortalId") or result.get("CompanyName") or ""
    server = _razorsync_server_name(brand)
    label = f" ({portal})" if portal else (f" portal {server}" if server else "")
    return f"Connected to RazorSync{label}.", None


def _push_razorsync(brand, lead_data):
    name = (lead_data.get("name") or "").strip()
    first_name = (lead_data.get("first_name") or "").strip()
    last_name = (lead_data.get("last_name") or "").strip()
    if not (first_name or last_name) and name:
        parts = name.split()
        first_name = parts[0]
        last_name = " ".join(parts[1:])
    if not name:
        name = f"{first_name} {last_name}".strip() or "New Lead"

    notes = lead_data.get("notes") or lead_data.get("summary") or ""
    customer_payload = {
        "FullName": name,
        "FirstName": first_name or name,
        "LastName": last_name,
        "CompanyName": lead_data.get("company") or lead_data.get("company_name") or "",
        "Email": lead_data.get("email") or "",
        "Phone": lead_data.get("phone") or "",
        "AddressLine1": lead_data.get("address") or lead_data.get("street") or "",
        "City": lead_data.get("city") or "",
        "State": lead_data.get("state") or "",
        "Zip": lead_data.get("zip") or lead_data.get("postal_code") or "",
        "Notes": notes,
        "Source": lead_data.get("source") or "GroMore WARREN",
    }
    customer_payload = {k: v for k, v in customer_payload.items() if v not in (None, "")}

    result, error = _razorsync_api(brand, "POST", "Customer/Create", json_body=customer_payload)
    if error:
        return False, error

    customer_id = ""
    if isinstance(result, dict):
        customer_id = str(
            result.get("Id")
            or result.get("ID")
            or result.get("CustomerId")
            or result.get("customer_id")
            or ""
        ).strip()

    if customer_id and (notes or lead_data.get("service") or lead_data.get("price")):
        quote_payload = {
            "CustomerId": customer_id,
            "Title": lead_data.get("source") or "New Warren lead",
            "Summary": lead_data.get("service") or lead_data.get("service_needed") or "",
            "Description": notes,
            "Amount": _money_to_float(lead_data.get("price") or lead_data.get("monthly_price")),
        }
        quote_payload = {k: v for k, v in quote_payload.items() if v not in (None, "", 0)}
        _, quote_error = _razorsync_api(brand, "POST", "Quote/Create", json_body=quote_payload)
        if quote_error:
            log.info("RazorSync quote create skipped/failed for customer %s: %s", customer_id, quote_error)

    return True, f"RazorSync customer created{f': {customer_id}' if customer_id else ''}"


def _razorsync_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "Data", "items", "Items", "Invoices", "Payments", "results", "Results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _razorsync_records(value)
            if nested:
                return nested
    return []


def pull_razorsync_revenue(brand, month=None):
    month_prefix, start, end = _month_bounds(month)
    invoices_payload, error = _razorsync_api(brand, "GET", "Invoice/List")
    if error:
        return 0, 0, error

    total = 0.0
    payment_count = 0
    invoices = _razorsync_records(invoices_payload)
    for invoice in invoices[:500]:
        if not isinstance(invoice, dict):
            continue
        invoice_id = invoice.get("Id") or invoice.get("ID") or invoice.get("InvoiceId") or invoice.get("invoice_id")
        invoice_date = _parse_razorsync_date(
            invoice.get("PaidDate") or invoice.get("PaymentDate") or invoice.get("DatePaid") or invoice.get("CreatedDate")
        )
        status = str(invoice.get("Status") or invoice.get("PaymentStatus") or "").lower()
        invoice_amount = _money_to_float(
            invoice.get("AmountPaid") or invoice.get("PaidAmount") or invoice.get("Total") or invoice.get("Amount")
        )
        if invoice_date and start <= invoice_date < end and invoice_amount and status in ("", "paid", "settled", "complete", "completed", "success"):
            total += invoice_amount
            payment_count += 1
            continue

        if not invoice_id:
            continue
        payments_payload, payment_error = _razorsync_api(brand, "GET", f"Payment/List/{invoice_id}")
        if payment_error:
            log.info("RazorSync payment list failed for invoice %s: %s", invoice_id, payment_error)
            continue
        for payment in _razorsync_records(payments_payload):
            if not isinstance(payment, dict):
                continue
            paid_at = _parse_razorsync_date(payment.get("PaymentDate") or payment.get("Date") or payment.get("CreatedDate"))
            if not paid_at or not (start <= paid_at < end):
                continue
            status = str(payment.get("Status") or payment.get("PaymentStatus") or "").lower()
            if status and status not in ("paid", "settled", "complete", "completed", "success"):
                continue
            total += _money_to_float(payment.get("Amount") or payment.get("PaymentAmount") or payment.get("Total"))
            payment_count += 1

    if not invoices:
        return 0, 0, f"No RazorSync invoice records returned for {month_prefix}"
    return round(total, 2), payment_count, None


def _payment_provider(brand):
    return (brand.get("payment_provider") or "").strip().lower()


def stripe_payment_test_connection(brand):
    api_key = (brand.get("payment_api_key") or "").strip()
    if not api_key:
        return "", "Stripe secret key not configured"
    try:
        resp = requests.get("https://api.stripe.com/v1/account", auth=(api_key, ""), timeout=TIMEOUT)
    except requests.RequestException as exc:
        return "", f"Stripe request failed: {str(exc)[:150]}"
    if resp.status_code != 200:
        return "", f"Stripe returned {resp.status_code}: {resp.text[:200]}"
    data = resp.json()
    account_id = data.get("id") or ""
    return f"Connected to Stripe account {account_id}.", None


def pull_stripe_payment_revenue(brand, month=None):
    api_key = (brand.get("payment_api_key") or "").strip()
    if not api_key:
        return 0, 0, "Stripe secret key not configured"
    _month, start, end = _month_bounds(month)
    params = {
        "limit": 100,
        "created[gte]": int(start.timestamp()),
        "created[lt]": int(end.timestamp()),
    }
    total = 0.0
    count = 0
    while True:
        try:
            resp = requests.get("https://api.stripe.com/v1/charges", auth=(api_key, ""), params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            return 0, 0, f"Stripe request failed: {str(exc)[:150]}"
        if resp.status_code != 200:
            return 0, 0, f"Stripe returned {resp.status_code}: {resp.text[:200]}"
        payload = resp.json()
        rows = payload.get("data") or []
        for charge in rows:
            if not charge.get("paid") or charge.get("refunded"):
                continue
            amount = int(charge.get("amount") or 0) - int(charge.get("amount_refunded") or 0)
            total += max(amount, 0) / 100.0
            count += 1
        if not payload.get("has_more") or not rows:
            break
        params["starting_after"] = rows[-1].get("id")
    return round(total, 2), count, None


def _square_headers(brand):
    token = (brand.get("payment_api_key") or "").strip()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def square_payment_test_connection(brand):
    api_key = (brand.get("payment_api_key") or "").strip()
    if not api_key:
        return "", "Square access token not configured"
    try:
        resp = requests.get("https://connect.squareup.com/v2/locations", headers=_square_headers(brand), timeout=TIMEOUT)
    except requests.RequestException as exc:
        return "", f"Square request failed: {str(exc)[:150]}"
    if resp.status_code != 200:
        return "", f"Square returned {resp.status_code}: {resp.text[:200]}"
    locations = (resp.json() or {}).get("locations") or []
    return f"Connected to Square - {len(locations)} location{'s' if len(locations) != 1 else ''} accessible.", None


def pull_square_payment_revenue(brand, month=None):
    api_key = (brand.get("payment_api_key") or "").strip()
    if not api_key:
        return 0, 0, "Square access token not configured"
    _month, start, end = _month_bounds(month)
    params = {
        "begin_time": start.isoformat() + "Z",
        "end_time": end.isoformat() + "Z",
        "limit": 100,
    }
    location_id = (brand.get("payment_location_id") or "").strip()
    if location_id:
        params["location_id"] = location_id

    total = 0.0
    count = 0
    while True:
        try:
            resp = requests.get("https://connect.squareup.com/v2/payments", headers=_square_headers(brand), params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            return 0, 0, f"Square request failed: {str(exc)[:150]}"
        if resp.status_code != 200:
            return 0, 0, f"Square returned {resp.status_code}: {resp.text[:200]}"
        payload = resp.json() or {}
        for payment in payload.get("payments") or []:
            if str(payment.get("status") or "").upper() != "COMPLETED":
                continue
            total_money = payment.get("total_money") or {}
            refunded_money = payment.get("refunded_money") or {}
            amount = int(total_money.get("amount") or 0) - int(refunded_money.get("amount") or 0)
            total += max(amount, 0) / 100.0
            count += 1
        cursor = payload.get("cursor")
        if not cursor:
            break
        params["cursor"] = cursor
    return round(total, 2), count, None


def payment_provider_test_connection(brand):
    provider = _payment_provider(brand)
    if provider == "stripe":
        return stripe_payment_test_connection(brand)
    if provider == "square":
        return square_payment_test_connection(brand)
    return "", "Choose Stripe or Square first."


def pull_payment_provider_revenue(brand, month=None):
    provider = _payment_provider(brand)
    if provider == "stripe":
        return pull_stripe_payment_revenue(brand, month)
    if provider == "square":
        return pull_square_payment_revenue(brand, month)
    return 0, 0, "No supported payment provider configured"


# ──────────────────────────────────────────────
# Sweep and Go (Direct Open API - https://openapi.sweepandgo.com)
# ──────────────────────────────────────────────

SNG_BASE = "https://openapi.sweepandgo.com"


def _sng_api(brand, method, path, json_body=None, params=None):
    """Make a direct call to the Sweep and Go Open API."""
    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return None, "Sweep and Go API token not configured"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    url = f"{SNG_BASE}/{path.lstrip('/')}"

    try:
        resp = requests.request(
            method, url,
            json=json_body,
            params=params,
            headers=headers,
            timeout=TIMEOUT,
        )
    except Exception as e:
        return None, f"SNG API request failed: {str(e)[:150]}"

    if resp.status_code not in (200, 201):
        return None, f"SNG API returned {resp.status_code}: {resp.text[:200]}"

    try:
        return resp.json(), None
    except (ValueError, TypeError):
        return {"raw": resp.text[:500]}, None


def _push_sweepandgo(brand, lead_data):
    """Onboard a new residential client in Sweep and Go via Open API."""
    org_slug = (brand.get("crm_pipeline_id") or "").strip()
    first_name = (lead_data.get("first_name") or "").strip()
    last_name = (lead_data.get("last_name") or "").strip()
    if not first_name and not last_name:
        name_parts = str(lead_data.get("name") or "").strip().split()
        if name_parts:
            first_name = name_parts[0]
            last_name = " ".join(name_parts[1:])

    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "email": lead_data.get("email", ""),
        "cell_phone_number": lead_data.get("phone", ""),
        "home_address": lead_data.get("address", ""),
        "city": lead_data.get("city", ""),
        "state": lead_data.get("state", ""),
        "zip_code": lead_data.get("zip", ""),
        "number_of_dogs": 1,
        "last_time_yard_was_thoroughly_cleaned": "one_week",
        "clean_up_frequency": "once_a_week",
        "initial_cleanup_required": 1,
        "marketing_allowed": 1,
        "marketing_allowed_source": "open_api",
    }

    # Add UTM tracking if available
    source = lead_data.get("source", "")
    if source:
        payload["tracking_field"] = f"utm_source=gromore&utm_campaign={source}"
        payload["how_heard_about_us"] = "social_media"
        payload["how_heard_answer"] = source

    if lead_data.get("notes"):
        payload["additional_comment"] = lead_data["notes"]

    result, error = _sng_api(brand, "PUT", "api/v1/residential/onboarding", json_body=payload)
    if error:
        return False, error

    if isinstance(result, dict) and result.get("success"):
        return True, "SNG client onboarded successfully"
    return False, f"SNG onboarding response: {str(result)[:200]}"


def sng_get_active_clients(brand, page=1):
    """Get paginated list of active clients."""
    return _sng_api(brand, "GET", "api/v1/clients/active", params={"page": page})


def sng_get_inactive_clients(brand, page=1):
    """Get paginated list of inactive clients."""
    return _sng_api(brand, "GET", "api/v1/clients/inactive", params={"page": page})


def sng_get_active_no_subscription(brand, page=1):
    """Get active clients without a subscription (upsell targets)."""
    return _sng_api(brand, "GET", "api/v1/clients/active_no_subscription", params={"page": page})


def sng_get_client_details(brand, client_id):
    """Get full client details including payment history."""
    return _sng_api(brand, "POST", "api/v2/clients/client_details", json_body={"client": client_id})


def sng_search_client(brand, email, status=None):
    """Search for a client by email."""
    body = {"email": email}
    if status:
        body["status"] = status
    return _sng_api(brand, "POST", "api/v2/clients/client_search", json_body=body)


def sng_get_leads(brand, page=1):
    """Get paginated list of leads."""
    return _sng_api(brand, "GET", "api/v1/leads/list", params={"page": page})


def sng_get_out_of_area_leads(brand, page=1):
    """Get leads outside service area (ad targeting feedback)."""
    return _sng_api(brand, "GET", "api/v1/leads/out_of_service", params={"page": page})


def sng_get_dispatch_board(brand, date_str):
    """Get all jobs for a given date (YYYY-MM-DD)."""
    return _sng_api(brand, "GET", "api/v1/dispatch_board/jobs_for_date", params={"date": date_str})


def sng_get_free_quotes(brand):
    """Get list of free quote requests."""
    return _sng_api(brand, "GET", "api/v2/free_quotes")


def sng_count_active_clients(brand):
    """Get total active client count."""
    return _sng_api(brand, "GET", "api/v2/report/count_active_clients")


def sng_count_happy_clients(brand):
    """Get total happy client count."""
    return _sng_api(brand, "GET", "api/v2/report/count_happy_clients")


def sng_count_happy_dogs(brand):
    """Get total happy dog count."""
    return _sng_api(brand, "GET", "api/v2/report/count_happy_dogs")


def sng_count_jobs(brand):
    """Get total completed job count."""
    return _sng_api(brand, "GET", "api/v2/report/jobs_count")


def sng_get_staff(brand):
    """Get list of active staff members."""
    return _sng_api(brand, "GET", "api/v2/report/staff_select_list")


def sng_list_webhook_events(brand, page=1):
    """List previously triggered Sweep and Go webhook events."""
    return _sng_api(brand, "GET", "api/v1/webhooks/list", params={"page": page})


def sng_welcome_v2(brand):
    """Test endpoint for auth/connectivity."""
    return _sng_api(brand, "GET", "api/v2/welcome")


def sng_check_token(brand):
    """Return token details (useful for debugging token issues).

    Note: SNG requires BOTH Authorization: Bearer <token> and token=<token> query param.
    """
    api_key = (brand.get("crm_api_key") or "").strip()
    return _sng_api(brand, "GET", "api/v2/check_token", params={"token": api_key})


def sng_create_coupon(brand, coupon_id=None, name=None, coupon_type="percent",
                      duration="once", percent_off=None, amount_off=None,
                      redeem_by=None, max_redemptions=None):
    """Create a coupon for residential subscriptions."""
    body = {"coupon_type": coupon_type, "duration": duration}
    if coupon_id:
        body["coupon_id"] = coupon_id
    if name:
        body["name"] = name
    if percent_off is not None:
        body["percent_off"] = str(percent_off)
    if amount_off is not None:
        body["amount_off"] = str(amount_off)
    if redeem_by:
        body["redeem_by"] = redeem_by
    if max_redemptions is not None:
        body["max_redemptions"] = int(max_redemptions)
    return _sng_api(brand, "POST", "api/v2/coupon", json_body=body)


def sng_check_zip(brand, org_slug, zip_code):
    """Check if a ZIP code is in the service area."""
    return _sng_api(brand, "POST", "api/v2/client_on_boarding/check_zip_code_exists",
                    json_body={"organization": org_slug, "value": zip_code})


def sng_get_org_data(brand, org_slug):
    """Get organization branding info."""
    return _sng_api(brand, "GET", "api/v2/client_on_boarding/organization_data",
                    params={"organization": org_slug})


def _sng_collect_all_client_ids(brand):
    """Paginate through active clients and return all client string IDs."""
    all_ids = []
    page = 1
    while True:
        result, error = sng_get_active_clients(brand, page)
        if error or not isinstance(result, dict):
            break
        for c in (result.get("data") or []):
            cid = c.get("client") or c.get("id") or c.get("client_id")
            if cid:
                all_ids.append(cid)
        paginate = result.get("paginate") or {}
        if page >= (paginate.get("total_pages") or 1):
            break
        page += 1
    return all_ids


def _sng_collect_active_no_subscription_ids(brand):
    """Paginate through active_no_subscription and return client string IDs."""
    all_ids = []
    page = 1
    while True:
        result, error = sng_get_active_no_subscription(brand, page=page)
        if error or not isinstance(result, dict):
            break
        for c in (result.get("data") or []):
            cid = c.get("client") or c.get("id") or c.get("client_id")
            if cid:
                all_ids.append(cid)
        paginate = result.get("paginate") or {}
        if page >= (paginate.get("total_pages") or 1):
            break
        page += 1
    return all_ids


def _sng_sum_payments_for_month(brand, client_ids, month_prefix):
    """Call client_details for each client and sum succeeded payments in given month.
    month_prefix is like '2026-03'. Returns (total_revenue, payment_count, diagnostics)."""
    def _month_from_date(value):
        if not value:
            return None
        if isinstance(value, (int, float)):
            # Epoch seconds or ms
            ts = int(value)
            if ts > 10**12:
                ts = ts / 1000.0
            try:
                return datetime.utcfromtimestamp(ts).strftime("%Y-%m")
            except Exception:
                return None
        if not isinstance(value, str):
            return None
        s = value.strip()
        if len(s) >= 7 and s[4] == "-" and s[7:8] in ("", "-"):
            return s[:7]
        # ISO-ish
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m")
        except Exception:
            pass
        # Common US formats
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
            try:
                return datetime.strptime(s[:10], fmt).strftime("%Y-%m")
            except Exception:
                continue
        return None

    total_revenue = 0.0
    payment_count = 0
    diag = {
        "errors": 0,
        "clients_with_payments": 0,
        "clients_without_payments": 0,
        "all_payment_statuses": {},
        "all_payment_months": {},
        "sample_response_keys": None,
        "sample_payment": None,
        "sample_payment_keys": None,
        "first_error": None,
    }
    for i, cid in enumerate(client_ids):
        result, error = sng_get_client_details(brand, cid)
        if error:
            diag["errors"] += 1
            if not diag["first_error"]:
                diag["first_error"] = f"{cid}: {error}"
            continue
        normalized_result = _sng_normalize_detail_payload(result)
        if not isinstance(normalized_result, dict):
            diag["errors"] += 1
            if not diag["first_error"]:
                diag["first_error"] = f"{cid}: non-dict response ({type(result).__name__})"
            continue

        # Capture the keys from first successful response
        if diag["sample_response_keys"] is None:
            diag["sample_response_keys"] = list(normalized_result.keys())

        payments = _sng_extract_payments(normalized_result)
        if payments:
            diag["clients_with_payments"] += 1
            # Capture first payment as sample
            if diag["sample_payment"] is None:
                diag["sample_payment"] = {k: str(v)[:100] for k, v in payments[0].items()}
            diag["sample_payment_keys"] = list(payments[0].keys()) if isinstance(payments[0], dict) else None
        else:
            diag["clients_without_payments"] += 1

        for pmt in payments:
            # Track all statuses and months seen
            status = (pmt.get("status") or "unknown")
            status_norm = str(status).strip().lower()
            diag["all_payment_statuses"][status] = diag["all_payment_statuses"].get(status, 0) + 1

            pmt_date = pmt.get("date") or pmt.get("created_at") or pmt.get("createdAt") or ""
            pmt_month = _month_from_date(pmt_date)
            if pmt_month:
                diag["all_payment_months"][pmt_month] = diag["all_payment_months"].get(pmt_month, 0) + 1

            if status_norm not in (
                "succeeded",
                "success",
                "successful",
                "paid",
                "completed",
            ):
                continue
            if pmt_month != month_prefix:
                continue
            total_revenue += _sng_payment_amount(pmt)
            payment_count += 1

    log.info("SNG payment sum: %d clients, %d payments, $%.2f for %s | diag: with_pmts=%d, without=%d, errors=%d",
             len(client_ids), payment_count, total_revenue, month_prefix,
             diag["clients_with_payments"], diag["clients_without_payments"], diag["errors"])
    return round(total_revenue, 2), payment_count, diag


def sng_sync_revenue(brand, db, max_sample=50, month=None):
    """Revenue sync: samples up to max_sample clients from the previous complete
    month, then extrapolates to the full client base. Stores results in
    brand_month_finance + settings cache.
    Designed to complete within ~60s (50 clients x ~1s each).
    Returns the snapshot dict."""
    import json
    from datetime import datetime, timedelta

    brand_id = brand.get("id") or brand.get("brand_id")
    now = datetime.now()

    def _valid_month(s):
        try:
            if not s or len(s) != 7 or s[4] != "-":
                return False
            int(s[:4])
            m = int(s[5:7])
            return 1 <= m <= 12
        except Exception:
            return False

    if _valid_month(month):
        rev_month = month
    else:
        # Default: previous complete month (current month may have incomplete billing)
        first_of_this_month = now.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        rev_month = last_month_end.strftime("%Y-%m")

    log.info("SNG revenue sync for brand %s, month %s (sample=%d)",
             brand_id, rev_month, max_sample)

    # Get counts (fast, single calls each)
    active_count = 0
    r, _ = sng_count_active_clients(brand)
    if isinstance(r, dict):
        active_count = r.get("data", 0) or 0

    # Note: SNG provides an active_no_subscription endpoint, but in practice we
    # have seen it be inconsistent across accounts. For revenue scaling we rely
    # on `subscription_names` from the active clients list (docs include it).
    no_sub_total = 0

    inactive_count = 0
    r, _ = sng_get_inactive_clients(brand, page=1)
    if isinstance(r, dict):
        inactive_count = (r.get("paginate") or {}).get("total", len(r.get("data", [])))

    jobs_count = 0
    r, _ = sng_count_jobs(brand)
    if isinstance(r, dict):
        jobs_count = r.get("data", 0) or 0

    # Collect sample IDs and compute subscribed/no-sub counts from active clients.
    sample_ids = []
    subscribed_count = 0
    derived_no_sub = 0
    page = 1
    while True:
        result, error = sng_get_active_clients(brand, page)
        if error or not isinstance(result, dict):
            break

        rows = (result.get("data") or [])
        for c in rows:
            cid = c.get("client") or c.get("id") or c.get("client_id")
            subs_raw = c.get("subscription_names")
            subs = (str(subs_raw).strip() if subs_raw is not None else "")
            is_subscribed = bool(subs) and subs.lower() not in ("none", "null")

            if is_subscribed:
                subscribed_count += 1
                if cid and len(sample_ids) < max_sample:
                    sample_ids.append(cid)
            else:
                derived_no_sub += 1

        paginate = result.get("paginate") or {}
        total_pages = paginate.get("total_pages") or 1
        if page >= total_pages:
            break
        page += 1

    no_sub_total = derived_no_sub
    sample_size = len(sample_ids)
    log.info(
        "SNG sync: got %d sample subscribed client IDs (active=%d, no_sub=%d, subscribed=%d)",
        sample_size, active_count, no_sub_total, subscribed_count
    )

    # If we couldn't find subscribed clients via subscription_names, fall back
    # to sampling any active clients so revenue sync doesn't become a no-op.
    if sample_size == 0:
        page = 1
        while len(sample_ids) < max_sample:
            result, error = sng_get_active_clients(brand, page)
            if error or not isinstance(result, dict):
                break
            for c in (result.get("data") or []):
                cid = c.get("client") or c.get("id") or c.get("client_id")
                if cid:
                    sample_ids.append(cid)
                if len(sample_ids) >= max_sample:
                    break
            paginate = result.get("paginate") or {}
            if page >= (paginate.get("total_pages") or 1):
                break
            page += 1
        sample_size = len(sample_ids)
        subscribed_count = max(active_count - no_sub_total, 0) or active_count

    sample_revenue = 0.0
    sample_payments = 0
    diag = {}
    revenue_source = "client_details"
    webhook_revenue = 0.0
    webhook_payments = 0
    webhook_diag = {}
    if sample_ids:
        sample_revenue, sample_payments, diag = _sng_sum_payments_for_month(brand, sample_ids, rev_month)

        # If previous month is $0 but we can see payments in other months,
        # fall back to the most recent payment month from the sample.
        if (
            sample_revenue == 0
            and isinstance(diag, dict)
            and (diag.get("all_payment_months") or {})
        ):
            months_seen = sorted((diag.get("all_payment_months") or {}).keys())
            latest_month = months_seen[-1] if months_seen else None
            current_month = now.strftime("%Y-%m")

            # Only auto-fallback if the latest month is recent (current or previous)
            if latest_month and latest_month != rev_month and latest_month in (current_month, rev_month):
                alt_revenue, alt_payments, alt_diag = _sng_sum_payments_for_month(brand, sample_ids, latest_month)
                if alt_revenue > 0:
                    rev_month = latest_month
                    sample_revenue, sample_payments, diag = alt_revenue, alt_payments, alt_diag

    if sample_revenue == 0:
        webhook_revenue, webhook_payments, webhook_diag = _sng_sum_webhook_payments_for_month(brand, rev_month)
        if webhook_revenue > 0:
            sample_revenue = webhook_revenue
            sample_payments = webhook_payments
            revenue_source = "webhook_history"

    debug_note = ""
    auth_debug = {}
    try:
        if sample_ids and (sample_revenue == 0) and isinstance(diag, dict):
            months_seen = sorted((diag.get("all_payment_months") or {}).keys())
            statuses_seen = list((diag.get("all_payment_statuses") or {}).keys())
            first_error = diag.get("first_error")

            # Lightweight auth check hints (only when we otherwise got $0)
            try:
                w, we = sng_welcome_v2(brand)
                auth_debug["welcome_v2_ok"] = bool(w) and not we
                if we:
                    auth_debug["welcome_v2_error"] = we
            except Exception:
                pass
            try:
                ct, cte = sng_check_token(brand)
                auth_debug["check_token_ok"] = bool(ct) and not cte
                if cte:
                    auth_debug["check_token_error"] = cte
            except Exception:
                pass

            auth_bits = []
            if auth_debug.get("welcome_v2_ok") is False:
                auth_bits.append("welcome_v2 failed")
            if auth_debug.get("check_token_ok") is False:
                auth_bits.append("check_token failed")
            auth_suffix = f" Auth: {', '.join(auth_bits)}." if auth_bits else ""

            if first_error:
                debug_note = f"SNG sync warning: {first_error}.{auth_suffix}" if auth_suffix else f"SNG sync warning: {first_error}"
            elif months_seen:
                debug_note = (
                    f"SNG sync found payments but none matched month={rev_month} + succeeded status. "
                    f"Months seen in sample: {', '.join(months_seen[-6:])}. "
                    f"Statuses seen: {', '.join(statuses_seen[:8])}"
                )
            elif webhook_diag.get("first_error"):
                debug_note = f"SNG sync returned no client_details payments and webhook history failed: {webhook_diag['first_error']}"
            elif webhook_diag.get("rows_seen"):
                event_types_seen = sorted((webhook_diag.get("event_types_seen") or {}).keys())
                debug_note = (
                    "SNG sync returned no payments in sampled client_details responses and found no "
                    f"accepted-payment events in webhook history for {rev_month}. "
                    f"Webhook events seen: {', '.join(event_types_seen[:8])}"
                )
            else:
                debug_note = f"SNG sync returned no payments in the sample client_details responses.{auth_suffix}"
    except Exception:
        debug_note = ""

    # Extrapolate from sample to full subscribed client base
    if revenue_source == "webhook_history":
        scale = 1
        mrr = sample_revenue
        payment_count = sample_payments
    elif sample_size > 0 and subscribed_count > 0:
        scale = subscribed_count / sample_size
        mrr = round(sample_revenue * scale, 2)
        payment_count = int(sample_payments * scale)
    else:
        mrr = sample_revenue
        payment_count = sample_payments
        scale = 1

    # Calculate derived metrics
    avg_client_monthly = round(mrr / subscribed_count, 2) if subscribed_count > 0 and mrr > 0 else 0
    avg_retention_months = 18
    estimated_clv = round(avg_client_monthly * avg_retention_months, 2)
    churn_cost = round(inactive_count * estimated_clv, 2) if mrr > 0 else 0

    snapshot = {
        "active_clients": active_count,
        "no_subscription_clients": no_sub_total,
        "subscribed_clients": subscribed_count,
        "inactive_clients": inactive_count,
        "total_jobs": jobs_count,
        "mrr": mrr,
        "payment_count": payment_count,
        "sample_size": sample_size,
        "sample_revenue": sample_revenue,
        "scale_factor": round(scale, 2),
        "revenue_month": rev_month,
        "avg_client_monthly_value": avg_client_monthly,
        "estimated_clv": estimated_clv,
        "churn_cost_total": churn_cost,
        "avg_retention_months": avg_retention_months,
        "synced_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "sync_status": "done",
        "data_source": (
            "webhook_payment_history"
            if revenue_source == "webhook_history"
            else ("real_payments_sampled" if sample_size < active_count else "real_payments_full")
        ),
        "diagnostics": diag,
        "webhook_diagnostics": webhook_diag,
        "debug_note": debug_note,
        "auth_debug": auth_debug,
    }

    # Store in brand_month_finance for ROAS pipeline
    if db and brand_id:
        try:
            db.upsert_brand_month_finance(
                brand_id, rev_month,
                closed_revenue=mrr,
                closed_deals=payment_count,
                notes=(
                    "SNG sync from webhook payment history"
                    if revenue_source == "webhook_history"
                    else f"SNG sync ({sample_size} of {active_count} clients sampled)"
                )
            )
        except Exception as exc:
            log.warning("Failed to upsert brand_month_finance: %s", exc)

        # Cache the full snapshot in settings table
        try:
            cache_key = f"sng_revenue_cache_{brand_id}"
            db.save_setting(cache_key, json.dumps(snapshot))
        except Exception as exc:
            log.warning("Failed to cache revenue snapshot: %s", exc)

    log.info("SNG revenue sync done: brand=%s month=%s revenue=$%.2f (sample=%d, scale=%.1fx)",
             brand_id, rev_month, mrr, sample_size, scale)
    return snapshot


def sng_get_cached_revenue(brand, db):
    """Read cached revenue snapshot from the settings table.
    Fast - no API calls. Returns the cached dict or empty dict.
    Also merges live KPIs (active clients, jobs) for freshness."""
    import json

    brand_id = brand.get("id") or brand.get("brand_id")
    cache_key = f"sng_revenue_cache_{brand_id}"

    cached = {}
    try:
        raw = db.get_setting(cache_key, "")
        if raw:
            cached = json.loads(raw)
    except Exception:
        pass

    # Always get live KPIs (these are single fast API calls)
    active_count = 0
    r, _ = sng_count_active_clients(brand)
    if isinstance(r, dict):
        active_count = r.get("data", 0) or 0

    inactive_count = 0
    r, _ = sng_get_inactive_clients(brand, page=1)
    if isinstance(r, dict):
        inactive_count = (r.get("paginate") or {}).get("total", len(r.get("data", [])))

    jobs_count = 0
    r, _ = sng_count_jobs(brand)
    if isinstance(r, dict):
        jobs_count = r.get("data", 0) or 0

    # Merge live counts into cached data
    cached["active_clients"] = active_count
    cached["inactive_clients"] = inactive_count
    cached["total_jobs"] = jobs_count

    # Recalculate churn cost with current inactive count if we have revenue data
    if cached.get("avg_client_monthly_value") and cached["avg_client_monthly_value"] > 0:
        avg_retention = cached.get("avg_retention_months", 18)
        clv = cached["avg_client_monthly_value"] * avg_retention
        cached["estimated_clv"] = round(clv, 2)
        cached["churn_cost_total"] = round(inactive_count * clv, 2)

    return cached


def pull_sweepandgo_revenue(brand, month=None):
    """Pull real revenue from Sweep and Go payment history for a month.
    Iterates ALL active clients, calls client_details, sums succeeded payments.
    Returns (revenue, payment_count, error_or_None)."""
    from datetime import datetime

    if not month:
        month = datetime.now().strftime("%Y-%m")

    try:
        int(month[:4])
        int(month[5:7])
    except (ValueError, IndexError):
        return 0, 0, f"Invalid month format: {month}"

    client_ids = _sng_collect_all_client_ids(brand)
    if not client_ids:
        return 0, 0, "No active clients found"

    total_revenue, payment_count, _diag = _sng_sum_payments_for_month(brand, client_ids, month)
    return total_revenue, payment_count, None


def pull_sweepandgo_customers(brand, page=1):
    """Pull active customers from Sweep and Go.
    Returns (customers_list, error_or_None)."""
    result, error = sng_get_active_clients(brand, page)
    if error:
        return [], error

    customers = []
    if isinstance(result, dict):
        customers = result.get("data", [])

    return customers, None


# ──────────────────────────────────────────────
# Jobber (GraphQL API)
# ──────────────────────────────────────────────

JOBBER_TOKEN_URL = "https://api.getjobber.com/api/oauth/token"
JOBBER_GRAPHQL_URL = "https://api.getjobber.com/api/graphql"


def _current_db():
    try:
        return current_app.db
    except RuntimeError:
        return None


def jobber_access_token_expires_at(access_token):
    """Return the JWT exp timestamp as UTC ISO text when Jobber includes one."""
    token = (access_token or "").strip()
    try:
        payload_part = token.split(".")[1]
        padding = "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(f"{payload_part}{padding}").decode("utf-8"))
        exp = int(payload.get("exp") or 0)
        if exp > 0:
            return datetime.utcfromtimestamp(exp).isoformat()
    except Exception:
        pass
    return ""


def _jobber_token_should_refresh(brand):
    raw = (brand.get("jobber_token_expires_at") or "").strip()
    if not raw:
        return False
    try:
        expires_at = datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return False
    return expires_at <= datetime.utcnow() + timedelta(minutes=5)


def _update_jobber_brand_tokens(brand, access_token, refresh_token=""):
    brand["crm_api_key"] = access_token
    if refresh_token:
        brand["jobber_refresh_token"] = refresh_token
    expires_at = jobber_access_token_expires_at(access_token)
    brand["jobber_token_expires_at"] = expires_at

    db = _current_db()
    brand_id = brand.get("id")
    if db and brand_id:
        db.update_brand_text_field(brand_id, "crm_api_key", access_token)
        db.update_brand_text_field(brand_id, "jobber_token_expires_at", expires_at)
        if refresh_token:
            db.update_brand_text_field(brand_id, "jobber_refresh_token", refresh_token)


def _refresh_jobber_access_token(brand):
    client_id = (brand.get("jobber_client_id") or "").strip()
    client_secret = (brand.get("jobber_client_secret") or "").strip()
    refresh_token = (brand.get("jobber_refresh_token") or "").strip()
    if not client_id or not client_secret or not refresh_token:
        return False, "Jobber refresh token flow is not configured. Reconnect Jobber OAuth or paste a fresh access token."

    try:
        resp = requests.post(
            JOBBER_TOKEN_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            headers={"Accept": "application/json"},
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        return False, f"Jobber token refresh failed: {str(exc)[:150]}"

    if resp.status_code != 200:
        return False, f"Jobber token refresh returned {resp.status_code}: {resp.text[:180]}"

    data = resp.json()
    access_token = (data.get("access_token") or "").strip()
    new_refresh_token = (data.get("refresh_token") or refresh_token).strip()
    if not access_token:
        return False, "Jobber token refresh did not return an access token."

    _update_jobber_brand_tokens(brand, access_token, new_refresh_token)
    return True, None


def _jobber_graphql_request(api_key, query, variables=None):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-JOBBER-GRAPHQL-VERSION": "2024-01-08",
    }

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    return requests.post(
        JOBBER_GRAPHQL_URL,
        json=payload,
        headers=headers,
        timeout=TIMEOUT,
    )


def _jobber_graphql(brand, query, variables=None):
    """Execute a Jobber GraphQL query."""
    if _jobber_token_should_refresh(brand):
        _refresh_jobber_access_token(brand)

    api_key = (brand.get("crm_api_key") or "").strip()
    if not api_key:
        return None, "Jobber API key not configured"

    try:
        resp = _jobber_graphql_request(api_key, query, variables)
    except Exception as e:
        return None, f"Jobber request failed: {str(e)[:150]}"

    if resp.status_code == 401 and (brand.get("jobber_refresh_token") or "").strip():
        refreshed, refresh_error = _refresh_jobber_access_token(brand)
        if refreshed:
            try:
                resp = _jobber_graphql_request((brand.get("crm_api_key") or "").strip(), query, variables)
            except Exception as e:
                return None, f"Jobber request failed after token refresh: {str(e)[:150]}"
        elif refresh_error:
            return None, refresh_error

    if resp.status_code != 200:
        return None, f"Jobber returned {resp.status_code}: {resp.text[:200]}"

    data = resp.json()
    if data.get("errors"):
        msg = data["errors"][0].get("message", str(data["errors"][0]))
        return None, f"Jobber GraphQL error: {msg[:200]}"

    return data.get("data", {}), None


def jobber_account_label(brand):
    """Return the connected Jobber account label when the token has account access."""
    query = """
    query JobberAccountLabel {
        account {
            id
            name
        }
    }
    """
    result, error = _jobber_graphql(brand, query)
    if error:
        return "", error
    account = (result or {}).get("account") or {}
    name = (account.get("name") or "").strip()
    account_id = (account.get("id") or "").strip()
    if name and account_id:
        return f"{name} ({account_id})", None
    return name or account_id or "", None


def _push_jobber(brand, lead_data):
    """Create a client in Jobber, optionally create a request."""
    first_name = lead_data.get("first_name", "")
    last_name = lead_data.get("last_name", "")

    # Build phones list
    phones = []
    if lead_data.get("phone"):
        phones.append({"number": lead_data["phone"], "primary": True})

    # Build emails list
    emails = []
    if lead_data.get("email"):
        emails.append({"address": lead_data["email"], "primary": True})

    mutation = """
    mutation CreateClient($input: ClientCreateInput!) {
        clientCreate(input: $input) {
            client {
                id
                firstName
                lastName
            }
            userErrors {
                message
                path
            }
        }
    }
    """

    variables = {
        "input": {
            "firstName": first_name or "New",
            "lastName": last_name or "Lead",
            "phones": phones,
            "emails": emails,
        }
    }

    result, error = _jobber_graphql(brand, mutation, variables)
    if error:
        return False, error

    client_data = (result.get("clientCreate") or {})
    user_errors = client_data.get("userErrors", [])
    if user_errors:
        return False, f"Jobber: {user_errors[0].get('message', 'Validation error')}"

    client = client_data.get("client", {})
    client_id = client.get("id", "")

    # Create a request (work order) if we have a source/notes
    if client_id and (lead_data.get("source") or lead_data.get("notes")):
        req_title = lead_data.get("source", "New Lead from Ad Platform")
        req_details = lead_data.get("notes", "")
        _jobber_create_request(brand, client_id, req_title, req_details)

    return True, f"Jobber client created: {client_id}"


def _jobber_create_request(brand, client_id, title, details=""):
    """Create a request (work order) for an existing Jobber client."""
    mutation = """
    mutation CreateRequest($input: RequestCreateInput!) {
        requestCreate(input: $input) {
            request {
                id
                title
            }
            userErrors {
                message
                path
            }
        }
    }
    """

    variables = {
        "input": {
            "clientId": client_id,
            "title": title[:255] if title else "New Lead",
            "details": details[:2000] if details else "",
        }
    }

    result, error = _jobber_graphql(brand, mutation, variables)
    if error:
        log.warning("Jobber request create failed: %s", error)
    return result, error


def jobber_test_connection(brand):
    """Verify the Jobber token can reach the GraphQL API."""
    query = """
    query JobberConnectionTest {
        clients(first: 1) {
            totalCount
            nodes {
                id
            }
        }
    }
    """
    result, error = _jobber_graphql(brand, query)
    if error:
        return "", error

    clients = result.get("clients") if isinstance(result, dict) else {}
    total = (clients or {}).get("totalCount")
    if total is None:
        return "Connected to Jobber.", None
    return f"Connected - {total} client record{'s' if int(total or 0) != 1 else ''} accessible.", None


def pull_jobber_revenue(brand, month=None):
    """Pull completed invoice revenue from Jobber for a given month.
    Returns (revenue, invoice_count, error_or_None)."""
    if not month:
        from datetime import datetime
        month = datetime.now().strftime("%Y-%m")

    query = """
    query Invoices($filter: InvoiceFilterAttributes) {
        invoices(filter: $filter, first: 200) {
            nodes {
                id
                total
                depositTotal
                createdAt
            }
            totalCount
        }
    }
    """

    variables = {
        "filter": {
            "status": "paid",
            "createdAtRange": {
                "from": f"{month}-01",
                "to": f"{month}-31",
            },
        }
    }

    result, error = _jobber_graphql(brand, query, variables)
    if error:
        return 0, 0, error

    invoices_data = result.get("invoices", {})
    nodes = invoices_data.get("nodes", [])

    total_revenue = 0.0
    for inv in nodes:
        amount = inv.get("total", 0)
        try:
            total_revenue += float(amount or 0)
        except (TypeError, ValueError):
            pass

    return total_revenue, len(nodes), None


# ── GoHighLevel Revenue Pull ──────────────────────────────────

def ghl_list_pipelines(brand):
    """List all pipelines in the GHL sub-account. Returns (list, error)."""
    token = (brand.get("crm_api_key") or "").strip()
    if not token:
        return [], "GoHighLevel token not configured"

    location_id = _ghl_location_id(brand)
    if location_id:
        try:
            resp = requests.get(
                f"{GHL_LEADCONNECTOR_BASE}/opportunities/pipelines",
                params={"locationId": location_id},
                headers=_ghl_lc_headers(token),
                timeout=TIMEOUT,
            )
            if resp.status_code == 200:
                payload = resp.json()
                if isinstance(payload, list):
                    return payload, None
                if isinstance(payload, dict):
                    return payload.get("pipelines", []) or payload.get("data", []) or [], None
                return [], "Unexpected pipelines response"
            # If PIT call fails, fall through to legacy endpoint for backward compatibility
            log.info("GHL LeadConnector pipelines failed (%s), falling back", resp.status_code)
        except requests.RequestException as exc:
            log.info("GHL LeadConnector pipelines network error, falling back: %s", exc)

    # Legacy v1 endpoint (API key style)
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(
            "https://rest.gohighlevel.com/v1/pipelines/",
            headers=headers, timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            return [], f"GHL API error {resp.status_code}: {resp.text[:200]}"
        return resp.json().get("pipelines", []), None
    except requests.RequestException as exc:
        return [], f"Network error: {exc}"


def ghl_test_connection(brand):
    """Quick connection test: list pipelines. Returns (message, error)."""
    pipelines, error = ghl_list_pipelines(brand)
    if error:
        return None, error
    return f"Connected - {len(pipelines)} pipeline(s) found", None


def pull_gohighlevel_revenue(brand, month=None):
    """Pull won/closed opportunity revenue from GoHighLevel for a given month.
    Uses the pipeline configured in crm_pipeline_id (or first pipeline if blank).
    Returns (revenue, deal_count, error_or_None)."""
    from datetime import datetime
    import calendar

    token = (brand.get("crm_api_key") or "").strip()
    if not token:
        return 0, 0, "GoHighLevel token not configured"

    if not month:
        month = datetime.now().strftime("%Y-%m")

    try:
        year = int(month[:4])
        mon = int(month[5:7])
    except (ValueError, IndexError):
        return 0, 0, f"Invalid month format: {month}"

    # Epoch-ms range for the target month
    start_ts = int(datetime(year, mon, 1).timestamp() * 1000)
    last_day = calendar.monthrange(year, mon)[1]
    end_ts = int(datetime(year, mon, last_day, 23, 59, 59).timestamp() * 1000)

    location_id = _ghl_location_id(brand)

    # Prefer LeadConnector when Location ID is present (PIT-compatible)
    if location_id:
        # Resolve pipeline
        pipeline_id = (brand.get("crm_pipeline_id") or "").strip()
        if not pipeline_id:
            pipelines, err = ghl_list_pipelines(brand)
            if err:
                return 0, 0, err
            if not pipelines:
                return 0, 0, "No pipelines found in GoHighLevel account"
            first = pipelines[0]
            pipeline_id = (first.get("id") or first.get("_id") or "").strip()

        total_revenue = 0.0
        deal_count = 0
        fetched = 0
        page = 0
        limit = 100

        while True:
            body = {
                "locationId": location_id,
                "query": "",
                "limit": limit,
                "page": page,
                "searchAfter": [],
                "additionalDetails": {
                    "notes": False,
                    "tasks": False,
                    "calendarEvents": False,
                    "unReadConversations": False,
                },
            }

            try:
                resp = requests.post(
                    f"{GHL_LEADCONNECTOR_BASE}/opportunities/search",
                    json=body,
                    headers=_ghl_lc_headers(token),
                    timeout=TIMEOUT,
                )
            except requests.RequestException as exc:
                return 0, 0, f"Network error: {exc}"

            if resp.status_code != 200:
                return 0, 0, f"GHL API error {resp.status_code}: {resp.text[:200]}"

            data = resp.json() if resp.content else {}
            opportunities = []
            total = None
            if isinstance(data, dict):
                opportunities = data.get("opportunities") or []
                total = data.get("total")
            elif isinstance(data, list):
                opportunities = data

            if not opportunities:
                break

            fetched += len(opportunities)

            for opp in opportunities:
                if not isinstance(opp, dict):
                    continue

                opp_pipeline_id = (opp.get("pipelineId") or opp.get("pipeline_id") or "").strip()
                if pipeline_id and opp_pipeline_id != pipeline_id:
                    continue

                status = (opp.get("status") or "").lower()
                if status not in ("won", "closed"):
                    continue

                closed_at = (
                    opp.get("lastStatusChangeAt")
                    or opp.get("last_status_change_at")
                    or opp.get("updatedAt")
                    or opp.get("updated_at")
                )
                closed_ms = _parse_ts_ms(closed_at)
                if start_ts <= closed_ms <= end_ts:
                    try:
                        total_revenue += float(opp.get("monetaryValue") or opp.get("monetary_value") or 0)
                    except (TypeError, ValueError):
                        pass
                    deal_count += 1

            if len(opportunities) < limit:
                break
            if isinstance(total, (int, float)) and fetched >= int(total):
                break

            page += 1

        return total_revenue, deal_count, None

    headers = {"Authorization": f"Bearer {token}"}

    # Resolve pipeline
    pipeline_id = (brand.get("crm_pipeline_id") or "").strip()
    if not pipeline_id:
        pipelines, err = ghl_list_pipelines(brand)
        if err:
            return 0, 0, err
        if not pipelines:
            return 0, 0, "No pipelines found in GoHighLevel account"
        pipeline_id = pipelines[0].get("id", "")

    # Fetch opportunities from that pipeline
    total_revenue = 0.0
    deal_count = 0
    page = 1

    while True:
        try:
            resp = requests.get(
                f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities",
                headers=headers, timeout=TIMEOUT,
            )
        except requests.RequestException as exc:
            return 0, 0, f"Network error: {exc}"

        if resp.status_code != 200:
            return 0, 0, f"GHL API error {resp.status_code}: {resp.text[:200]}"

        data = resp.json()
        opportunities = data.get("opportunities", [])

        for opp in opportunities:
            status = (opp.get("status") or "").lower()
            if status not in ("won", "closed"):
                continue
            # Check if the opportunity closed within our month
            closed_at = opp.get("lastStatusChangeAt") or opp.get("updatedAt") or 0
            if isinstance(closed_at, str):
                try:
                    closed_at = int(datetime.fromisoformat(closed_at.replace("Z", "+00:00")).timestamp() * 1000)
                except (ValueError, TypeError):
                    closed_at = 0
            if start_ts <= closed_at <= end_ts:
                try:
                    total_revenue += float(opp.get("monetaryValue") or 0)
                except (TypeError, ValueError):
                    pass
                deal_count += 1

        # GHL v1 opportunities endpoint does not paginate - break after first call
        break

    return total_revenue, deal_count, None
