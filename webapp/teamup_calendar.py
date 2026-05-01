"""Teamup Calendar adapter.

Teamup's direct API gives Warren read/write access to events. Realtime event
triggers are normally handled by Make/Zapier, so Warren also exposes a secured
incoming webhook endpoint in ``warren_webhooks.py`` for event notifications.
"""

import re
from datetime import date, datetime, timedelta
from urllib.parse import quote

import requests


DEFAULT_TEAMUP_BASE_URL = "https://api.teamup.com"


def _clean(value):
    return str(value or "").strip()


def teamup_config_from_row(row):
    return dict((row or {}).get("config") or {})


def teamup_config(db, brand_id):
    return teamup_config_from_row(db.get_brand_integration_config(brand_id, "teamup_calendar"))


def teamup_missing_fields(config):
    config = config or {}
    missing = []
    if not _clean(config.get("calendar_key")):
        missing.append("calendar_key")
    if not _clean(config.get("api_key")):
        missing.append("api_key")
    return missing


def teamup_base_url(config):
    base_url = _clean((config or {}).get("base_url")) or DEFAULT_TEAMUP_BASE_URL
    return base_url.rstrip("/")


def teamup_headers(config):
    api_key = _clean((config or {}).get("api_key"))
    return {
        "Teamup-Token": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _calendar_path(config, suffix=""):
    calendar_key = quote(_clean((config or {}).get("calendar_key")), safe="")
    suffix = str(suffix or "").strip("/")
    return f"/{calendar_key}/{suffix}" if suffix else f"/{calendar_key}"


def _request(config, method, path, *, params=None, json_body=None, timeout=15):
    missing = teamup_missing_fields(config)
    if missing:
        raise ValueError(f"Teamup missing required field(s): {', '.join(missing)}")
    url = f"{teamup_base_url(config)}{path}"
    resp = requests.request(
        method.upper(),
        url,
        headers=teamup_headers(config),
        params=params or None,
        json=json_body if json_body is not None else None,
        timeout=timeout,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Teamup returned {resp.status_code}: {(resp.text or '')[:240]}")
    if resp.status_code == 204 or not (resp.text or "").strip():
        return {}
    try:
        return resp.json() or {}
    except ValueError:
        return {"raw": resp.text}


def _date_value(value):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _clean(value)
    if len(text) >= 10:
        return text[:10]
    return text


def _datetime_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return _clean(value)


def _extract_list(payload, *keys):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def teamup_list_subcalendars(config):
    payload = _request(config, "GET", _calendar_path(config, "subcalendars"))
    return _extract_list(payload, "subcalendars", "subCalendars", "calendars")


def teamup_list_events(config, start_date=None, end_date=None, *, subcalendar_id=None):
    start_date = start_date or date.today()
    end_date = end_date or (date.today() + timedelta(days=14))
    params = {
        "startDate": _date_value(start_date),
        "endDate": _date_value(end_date),
    }
    subcalendar_id = _clean(subcalendar_id or (config or {}).get("subcalendar_id"))
    if subcalendar_id:
        params["subcalendarId"] = subcalendar_id
    payload = _request(config, "GET", _calendar_path(config, "events"), params=params)
    return [teamup_normalize_event(event) for event in _extract_list(payload, "events")]


def teamup_create_event(
    config,
    *,
    title,
    start_dt,
    end_dt,
    notes="",
    location="",
    who="",
    subcalendar_id=None,
    extra_fields=None,
):
    subcalendar_id = _clean(subcalendar_id or (config or {}).get("subcalendar_id"))
    if not subcalendar_id:
        raise ValueError("Teamup subcalendar_id is required to create events.")
    payload = {
        "title": _clean(title),
        "start_dt": _datetime_value(start_dt),
        "end_dt": _datetime_value(end_dt),
        "subcalendar_ids": [int(subcalendar_id) if str(subcalendar_id).isdigit() else subcalendar_id],
    }
    for key, value in {
        "notes": notes,
        "location": location,
        "who": who,
    }.items():
        if _clean(value):
            payload[key] = _clean(value)
    if isinstance(extra_fields, dict):
        payload.update(extra_fields)
    created = _request(config, "POST", _calendar_path(config, "events"), json_body=payload)
    event = created.get("event") if isinstance(created, dict) else None
    return teamup_normalize_event(event or created)


def teamup_update_event(config, event_id, fields):
    event_id = quote(_clean(event_id), safe="")
    if not event_id:
        raise ValueError("Teamup event_id is required.")
    payload = dict(fields or {})
    if "start_dt" in payload:
        payload["start_dt"] = _datetime_value(payload["start_dt"])
    if "end_dt" in payload:
        payload["end_dt"] = _datetime_value(payload["end_dt"])
    updated = _request(config, "PUT", _calendar_path(config, f"events/{event_id}"), json_body=payload)
    event = updated.get("event") if isinstance(updated, dict) else None
    return teamup_normalize_event(event or updated)


def teamup_delete_event(config, event_id):
    event_id = quote(_clean(event_id), safe="")
    if not event_id:
        raise ValueError("Teamup event_id is required.")
    _request(config, "DELETE", _calendar_path(config, f"events/{event_id}"))
    return True


def teamup_normalize_event(raw):
    raw = raw or {}
    if not isinstance(raw, dict):
        return {"raw": raw}
    event_id = raw.get("id") or raw.get("event_id") or raw.get("eventId")
    return {
        "external_event_id": _clean(event_id),
        "title": _clean(raw.get("title")),
        "start_dt": _clean(raw.get("start_dt") or raw.get("startDate") or raw.get("start")),
        "end_dt": _clean(raw.get("end_dt") or raw.get("endDate") or raw.get("end")),
        "all_day": bool(raw.get("all_day") or raw.get("allDay")),
        "notes": _clean(raw.get("notes") or raw.get("description")),
        "location": _clean(raw.get("location")),
        "who": _clean(raw.get("who")),
        "subcalendar_ids": raw.get("subcalendar_ids") or raw.get("subcalendarIds") or [],
        "raw": raw,
    }


def teamup_test_connection(config):
    missing = teamup_missing_fields(config)
    if missing:
        labels = ", ".join(key.replace("_", " ").title() for key in missing)
        return False, f"Teamup {labels} required."
    try:
        subcalendars = teamup_list_subcalendars(config)
        if subcalendars:
            return True, f"Connected to Teamup. Found {len(subcalendars)} sub-calendar(s)."
        events = teamup_list_events(config, date.today(), date.today() + timedelta(days=7))
        return True, f"Connected to Teamup. Event API returned {len(events)} upcoming event(s)."
    except requests.RequestException as exc:
        return False, f"Teamup network error: {str(exc)[:180]}"
    except Exception as exc:
        return False, f"Teamup test failed: {str(exc)[:180]}"


def _first_email(*values):
    for value in values:
        match = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", str(value or ""), re.IGNORECASE)
        if match:
            return match.group(0)
    return ""


def _first_phone(*values):
    for value in values:
        digits = re.sub(r"\D", "", str(value or ""))
        if len(digits) >= 10:
            if len(digits) == 10:
                return f"+1{digits}"
            if len(digits) == 11 and digits.startswith("1"):
                return f"+{digits}"
            return f"+{digits}"
    return ""


def _appointment_date(value, fallback):
    text = _clean(value)
    return text[:10] if len(text) >= 10 else fallback.isoformat()


def teamup_day_ahead_appointment_candidates(db, brand, target_date, max_events=None):
    """Return Teamup events in Warren's appointment-reminder candidate shape."""
    config = teamup_config(db, brand.get("id"))
    missing = teamup_missing_fields(config)
    if missing:
        return [], f"Teamup Calendar missing required field(s): {', '.join(missing)}"
    try:
        events = teamup_list_events(config, target_date, target_date + timedelta(days=1))
    except Exception as exc:
        return [], f"Teamup Calendar lookup failed: {str(exc)[:180]}"

    candidates = []
    for event in events[: int(max_events or len(events) or 0) or None]:
        raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
        text_sources = (
            event.get("who"),
            event.get("notes"),
            raw.get("description"),
            raw.get("custom"),
            raw.get("title"),
        )
        email = _first_email(*text_sources)
        phone = _first_phone(*text_sources)
        event_id = event.get("external_event_id") or raw.get("id") or raw.get("event_id")
        title = event.get("title") or "Teamup appointment"
        candidates.append({
            "appointment_key": f"teamup:{event_id or title}:{event.get('start_dt') or target_date.isoformat()}",
            "appointment_date": _appointment_date(event.get("start_dt"), target_date),
            "appointment_date_obj": target_date,
            "client_name": event.get("who") or title,
            "client_email": email,
            "client_phone": phone,
            "preferred_channel": "sms" if phone else "email" if email else "",
            "assigned_to_name": "",
            "address": event.get("location") or "",
            "job_id": str(event_id or ""),
            "status_name": "teamup_event",
            "source": "teamup_calendar",
            "event_title": title,
        })
    return candidates, None
