"""Connection health evaluation for client-facing integration settings."""

from datetime import datetime, timedelta, timezone


def _parse_dt(value):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_label(value, now=None):
    parsed = _parse_dt(value)
    if not parsed:
        return ""
    now = now or datetime.now(timezone.utc)
    seconds = max(0, int((now - parsed).total_seconds()))
    if seconds < 90:
        return "just now"
    minutes = seconds // 60
    if minutes < 90:
        return f"{minutes} min ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours} hr ago"
    days = hours // 24
    return f"{days} days ago"


def _token_health(label, connection, now):
    if not connection or connection.get("status") != "connected":
        return {
            "key": f"{label.lower()}_oauth",
            "label": f"{label} OAuth",
            "status": "info",
            "detail": f"{label} is not connected.",
        }
    expiry = _parse_dt(connection.get("token_expiry"))
    if not expiry:
        return {
            "key": f"{label.lower()}_oauth",
            "label": f"{label} OAuth",
            "status": "ok",
            "detail": f"{label} is connected. No expiry timestamp was stored.",
        }
    if expiry <= now:
        return {
            "key": f"{label.lower()}_oauth",
            "label": f"{label} OAuth",
            "status": "fail",
            "detail": f"{label} token expired at {expiry.isoformat()}. Reconnect or refresh is required.",
            "metadata": {"token_expiry": expiry.isoformat()},
        }
    if expiry <= now + timedelta(days=3):
        return {
            "key": f"{label.lower()}_oauth",
            "label": f"{label} OAuth",
            "status": "warn",
            "detail": f"{label} token expires soon: {expiry.isoformat()}.",
            "metadata": {"token_expiry": expiry.isoformat()},
        }
    return {
        "key": f"{label.lower()}_oauth",
        "label": f"{label} OAuth",
        "status": "ok",
        "detail": f"{label} is connected. Token expiry: {expiry.isoformat()}.",
        "metadata": {"token_expiry": expiry.isoformat()},
    }


def _health_item(key, label, status, detail, metadata=None):
    return {
        "key": key,
        "label": label,
        "status": status,
        "detail": detail,
        "metadata": metadata or {},
    }


def evaluate_brand_connection_health(db, brand, persist=True):
    """Evaluate health for the major connection paths on one brand."""
    brand = brand or {}
    brand_id = brand.get("id")
    if not brand_id:
        return []

    now = datetime.now(timezone.utc)
    connections = db.get_brand_connections(brand_id)
    items = [
        _token_health("Google", connections.get("google"), now),
        _token_health("Meta", connections.get("meta"), now),
    ]

    lead_deliveries = db.get_lead_webhook_deliveries(brand_id, limit=50)
    incoming_secret = (brand.get("sales_bot_incoming_webhook_secret") or "").strip()
    if not incoming_secret:
        items.append(_health_item(
            "lead_webhook_intake",
            "Lead Webhook Intake",
            "warn",
            "Incoming lead webhook secret is not configured. Make, Zapier, and form posts cannot be accepted.",
        ))
    elif not lead_deliveries:
        items.append(_health_item(
            "lead_webhook_intake",
            "Lead Webhook Intake",
            "warn",
            "Incoming lead webhook is configured, but no deliveries have been recorded yet.",
        ))
    else:
        latest = lead_deliveries[0]
        latest_age = _age_label(latest.get("received_at"), now)
        if (latest.get("status") or "").lower() == "rejected":
            items.append(_health_item(
                "lead_webhook_intake",
                "Lead Webhook Intake",
                "fail",
                f"Latest lead webhook delivery was rejected {latest_age}: {latest.get('reason') or 'No reason recorded.'}",
                {"latest_delivery_id": latest.get("id")},
            ))
        else:
            accepted_count = sum(1 for row in lead_deliveries if (row.get("status") or "").lower() == "accepted")
            items.append(_health_item(
                "lead_webhook_intake",
                "Lead Webhook Intake",
                "ok",
                f"Receiving lead webhooks. Last accepted delivery was {latest_age}.",
                {"latest_delivery_id": latest.get("id"), "recent_accepted": accepted_count},
            ))

    crm_deliveries = db.get_crm_push_deliveries(brand_id, limit=50)
    if crm_deliveries:
        latest = crm_deliveries[0]
        failed = [row for row in crm_deliveries if (row.get("status") or "").lower() == "failed"]
        latest_age = _age_label(latest.get("created_at"), now)
        if (latest.get("status") or "").lower() == "failed":
            items.append(_health_item(
                "crm_handoffs",
                "CRM Handoffs",
                "fail",
                f"Latest CRM handoff failed {latest_age}: {latest.get('detail') or 'No detail recorded.'}",
                {"latest_delivery_id": latest.get("id"), "recent_failed": len(failed)},
            ))
        elif failed:
            items.append(_health_item(
                "crm_handoffs",
                "CRM Handoffs",
                "warn",
                f"{len(failed)} recent CRM handoff(s) failed. Latest delivery was {latest.get('status')} {latest_age}.",
                {"latest_delivery_id": latest.get("id"), "recent_failed": len(failed)},
            ))
        else:
            items.append(_health_item(
                "crm_handoffs",
                "CRM Handoffs",
                "ok",
                f"Recent CRM handoffs are delivering. Latest delivery was {latest_age}.",
                {"latest_delivery_id": latest.get("id")},
            ))
    else:
        items.append(_health_item(
            "crm_handoffs",
            "CRM Handoffs",
            "info",
            "No Warren-to-CRM handoff attempts have been recorded yet.",
        ))

    crm_type = (brand.get("crm_type") or "").strip().lower()
    if crm_type == "jobber":
        token_expiry = _parse_dt(brand.get("jobber_token_expires_at"))
        if not (brand.get("crm_api_key") or "").strip():
            items.append(_health_item("jobber", "Jobber", "fail", "Jobber is selected, but no access token is saved."))
        elif not (brand.get("jobber_refresh_token") or "").strip():
            items.append(_health_item(
                "jobber",
                "Jobber",
                "fail",
                "Jobber is using a manual access token. Reconnect OAuth so tokens refresh automatically.",
            ))
        elif token_expiry and token_expiry <= now + timedelta(hours=6):
            items.append(_health_item(
                "jobber",
                "Jobber",
                "warn" if token_expiry > now else "fail",
                f"Jobber access token {'expires soon' if token_expiry > now else 'expired'}: {token_expiry.isoformat()}. Refresh token is present.",
                {"token_expiry": token_expiry.isoformat()},
            ))
        else:
            items.append(_health_item("jobber", "Jobber", "ok", "OAuth refresh token is present and Jobber handoffs can use auto-refresh."))
    elif crm_type == "sweepandgo":
        if not (brand.get("crm_api_key") or "").strip():
            items.append(_health_item("sweepandgo", "Sweep and Go", "fail", "Sweep and Go is selected, but no API token is saved."))
        else:
            sng_events = db.get_sng_webhook_events(brand_id, limit=10)
            if sng_events:
                items.append(_health_item(
                    "sweepandgo",
                    "Sweep and Go",
                    "ok",
                    f"API token is saved and latest webhook event was {_age_label(sng_events[0].get('received_at'), now)}.",
                    {"latest_event_id": sng_events[0].get("id")},
                ))
            else:
                items.append(_health_item(
                    "sweepandgo",
                    "Sweep and Go",
                    "warn",
                    "API token is saved, but no SNG webhook events have been recorded yet.",
                ))

    if (brand.get("quo_api_key") or "").strip() or (brand.get("quo_phone_number") or "").strip():
        missing = []
        if not (brand.get("quo_api_key") or "").strip():
            missing.append("API key")
        if not (brand.get("quo_phone_number") or "").strip():
            missing.append("phone number")
        if not (brand.get("sales_bot_quo_webhook_secret") or "").strip():
            missing.append("webhook secret")
        items.append(_health_item(
            "openphone",
            "OpenPhone / Quo",
            "warn" if missing else "ok",
            f"Missing: {', '.join(missing)}." if missing else "SMS credentials and inbound webhook secret are configured.",
        ))

    if persist:
        for item in items:
            db.upsert_connection_health(
                brand_id,
                item["key"],
                item["label"],
                item["status"],
                item["detail"],
                item.get("metadata") or {},
            )
    return items


def refresh_all_connection_health(db):
    stats = {"brands": 0, "items": 0, "fail": 0, "warn": 0, "healthy": 0, "info": 0}
    for brand in db.get_all_brands():
        items = evaluate_brand_connection_health(db, brand, persist=True)
        stats["brands"] += 1
        stats["items"] += len(items)
        for item in items:
            status = item.get("status") or "info"
            status_key = "healthy" if status == "ok" else status
            stats[status_key] = stats.get(status_key, 0) + 1
    return stats
