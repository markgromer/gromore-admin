import logging
import re
import time

from webapp.crm_bridge import _sng_extract_client_record, sng_get_active_clients, sng_get_client_details

log = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 300
_CONTACT_POLICY_CACHE = {}


def _normalize_email(value):
    return (value or "").strip().lower()


def _normalize_phone(value):
    raw = (value or "").strip()
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


def _coerce_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0

    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y", "on", "allowed", "allow"}:
        return True
    if lowered in {"0", "false", "no", "n", "off", "blocked", "deny", "denied", "opted_out", "opted out"}:
        return False
    return None


def _first_record_value(record, keys):
    if not isinstance(record, dict):
        return ""
    for key in keys:
        if key not in record:
            continue
        value = record.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _record_matches(record, normalized_phone, normalized_email):
    email_candidates = (
        "email",
        "email_address",
        "emailAddress",
    )
    phone_candidates = (
        "cell_phone",
        "cellPhone",
        "cell_phone_number",
        "cellPhoneNumber",
        "mobile",
        "mobile_phone",
        "mobilePhone",
        "phone",
        "phone_number",
        "phoneNumber",
        "home_phone",
        "homePhone",
    )

    if normalized_email:
        record_email = _normalize_email(_first_record_value(record, email_candidates))
        if record_email and record_email == normalized_email:
            return "email"

    if normalized_phone:
        for key in phone_candidates:
            record_phone = _normalize_phone(record.get(key))
            if record_phone and record_phone == normalized_phone:
                return "phone"

    return ""


def _record_marketing_allowed(record):
    for key in (
        "marketing_allowed",
        "marketingAllowed",
        "sms_marketing_allowed",
        "smsMarketingAllowed",
        "allow_marketing",
        "allowMarketing",
    ):
        if key in record:
            return _coerce_bool(record.get(key))
    return None


def _record_dnd(record):
    for key in (
        "do_not_disturb",
        "doNotDisturb",
        "dnd",
        "do_not_contact",
        "doNotContact",
        "contact_dnd",
        "contactDnd",
    ):
        if key in record:
            return bool(_coerce_bool(record.get(key)))
    return False


def _active_client_rows(brand, max_pages=25):
    page = 1
    while page <= max_pages:
        result, error = sng_get_active_clients(brand, page=page)
        if error or not isinstance(result, dict):
            if error:
                log.warning("Warren contact policy: active client lookup failed for brand %s: %s", brand.get("id"), error)
            break

        rows = result.get("data") or []
        for row in rows:
            if isinstance(row, dict):
                yield row

        paginate = result.get("paginate") or {}
        total_pages = int(paginate.get("total_pages") or 1)
        if page >= total_pages or not rows:
            break
        page += 1


def _lookup_active_client(brand, normalized_phone, normalized_email):
    for row in _active_client_rows(brand):
        match_field = _record_matches(row, normalized_phone, normalized_email)
        if not match_field:
            continue

        client_id = row.get("client") or row.get("id") or row.get("client_id") or ""
        detail_record = {}
        if client_id:
            details, error = sng_get_client_details(brand, client_id)
            if not error and isinstance(details, dict):
                detail_record = _sng_extract_client_record(details)

        merged = dict(row)
        merged.update(detail_record or {})
        return merged, match_field

    return {}, ""


def lookup_contact_policy(db, brand, thread):
    """Resolve whether this thread should receive marketing-style automation.

    Returns a dict with CRM/contact flags. Active clients always suppress
    lead-pitch, quote, and nurture behavior.
    """
    brand_id = (brand or {}).get("id")
    normalized_phone = _normalize_phone((thread or {}).get("lead_phone"))
    normalized_email = _normalize_email((thread or {}).get("lead_email"))

    policy = {
        "matched": False,
        "match_field": "",
        "client_id": "",
        "client_name": "",
        "is_active_client": False,
        "marketing_allowed": None,
        "contact_dnd": False,
        "is_opted_out": False,
        "suppress_marketing": False,
        "reason": "",
        "subscription_names": "",
        "source": "none",
    }

    if not brand_id:
        return policy

    cache_key = (brand_id, normalized_phone, normalized_email)
    cached = _CONTACT_POLICY_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and cached[0] > now_ts:
        return dict(cached[1])

    if normalized_phone and db.is_opted_out(brand_id, normalized_phone):
        policy["is_opted_out"] = True
        policy["suppress_marketing"] = True
        policy["reason"] = "opted_out"
        policy["source"] = "sms_consent"

    crm_api_key = ((brand or {}).get("crm_api_key") or "").strip()
    if crm_api_key and (normalized_phone or normalized_email):
        record, match_field = _lookup_active_client(brand, normalized_phone, normalized_email)
        if record:
            marketing_allowed = _record_marketing_allowed(record)
            contact_dnd = _record_dnd(record)
            policy.update({
                "matched": True,
                "match_field": match_field,
                "client_id": str(record.get("client") or record.get("id") or record.get("client_id") or "").strip(),
                "client_name": " ".join(
                    part for part in (
                        str(record.get("first_name") or "").strip(),
                        str(record.get("last_name") or "").strip(),
                    ) if part
                ).strip() or str(record.get("name") or record.get("client_name") or "").strip(),
                "is_active_client": True,
                "marketing_allowed": marketing_allowed,
                "contact_dnd": contact_dnd,
                "subscription_names": str(record.get("subscription_names") or "").strip(),
                "source": "sng_active_clients",
            })

            if contact_dnd:
                policy["suppress_marketing"] = True
                policy["reason"] = "contact_dnd"
            elif marketing_allowed is False:
                policy["suppress_marketing"] = True
                policy["reason"] = "marketing_disabled"
            else:
                policy["suppress_marketing"] = True
                policy["reason"] = "active_client"

    _CONTACT_POLICY_CACHE[cache_key] = (now_ts + _CACHE_TTL_SECONDS, dict(policy))
    return policy