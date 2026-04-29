"""
Warren Sender - outbound message delivery via Quo SMS and Meta Messenger.

Handles sending Warren's replies to leads through the appropriate channel
and logging the delivery status.
"""
import json
import logging
import time
from datetime import datetime, timedelta, timezone
import requests

log = logging.getLogger(__name__)


def _parse_iso_timestamp(value):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _messenger_response_window_open(thread, now=None):
    """Return True when the lead's last inbound Messenger message is within 24 hours."""
    now = now or datetime.utcnow()
    last_inbound = _parse_iso_timestamp(thread.get("last_inbound_at", "")) if thread else None
    if not last_inbound:
        return False

    if last_inbound.tzinfo is not None:
        now = datetime.now(last_inbound.tzinfo)

    return now - last_inbound <= timedelta(hours=24)


def _mark_delivery(db, message_id, *, status, channel="", detail="", recipient="", auto_sent=False):
    if not message_id:
        return
    db.update_lead_message_metadata(
        message_id,
        {
            "delivery_status": status,
            "delivery_channel": channel,
            "delivery_detail": str(detail or "")[:500],
            "delivery_recipient": recipient or "",
            "delivery_checked_at": datetime.now(timezone.utc).isoformat(),
            "auto_sent": bool(auto_sent),
        },
    )


def send_reply(db, brand, thread_id, message_text, channel="sms", recipient_id=None, page_id=None, skip_dnd=False, logged_message_id=None):
    """Send an outbound reply through the appropriate channel.

    If DND is active and skip_dnd is False, the message is logged but NOT sent.

    Args:
        db: WebDB instance
        brand: brand dict
        thread_id: lead_thread ID
        message_text: text to send
        channel: 'sms' or 'messenger'
        recipient_id: Messenger PSID (required for messenger)
        page_id: Facebook Page ID (required for messenger)
        skip_dnd: if True, bypass DND check (used for manual sends)

    Returns:
        (success, detail_string)
    """
    thread = db.get_lead_thread(thread_id)
    if not thread:
        _mark_delivery(db, logged_message_id, status="failed", channel=channel, detail="Thread not found")
        return False, "Thread not found"

    # Messenger policy guard: only send RESPONSE messages inside the 24-hour response window.
    if channel == "messenger" and not _messenger_response_window_open(thread):
        db.add_lead_event(
            brand["id"], thread_id,
            "messenger_blocked_policy",
            event_value=message_text[:200],
            metadata={"reason": "outside_24h_response_window"},
        )
        log.info("Blocked Messenger send outside 24h response window: brand=%s thread=%s", brand.get("id"), thread_id)
        _mark_delivery(db, logged_message_id, status="blocked", channel=channel, detail="outside_24h_response_window")
        return False, "outside_24h_response_window"

    # DND check (automated sends only)
    if not skip_dnd:
        from webapp.warren_nurture import _is_dnd
        if _is_dnd(brand):
            if logged_message_id:
                _mark_delivery(db, logged_message_id, status="held", channel=channel, detail="dnd_held")
            else:
                db.add_lead_message(
                    thread_id, "outbound", "assistant", message_text,
                    channel=channel,
                    metadata={"dnd_held": True, "held_at": datetime.now(timezone.utc).isoformat(), "delivery_status": "held"},
                )
            log.info("DND active for brand %s - message held for thread %s", brand.get("id"), thread_id)
            return False, "dnd_held"

    # A2P opt-out check for SMS (blocks automated sends, warns on manual)
    if channel == "sms":
        to_phone = (thread.get("lead_phone") or "") if thread else ""
        if to_phone and db.is_opted_out(brand.get("id"), to_phone):
            if not skip_dnd:
                log.info("Blocked SMS to opted-out phone %s for brand %s", to_phone, brand.get("id"))
                if logged_message_id:
                    _mark_delivery(db, logged_message_id, status="blocked", channel=channel, detail="opted_out", recipient=to_phone)
                else:
                    db.add_lead_message(
                        thread_id, "outbound", "assistant", message_text,
                        channel=channel,
                        metadata={"blocked_opted_out": True, "delivery_status": "blocked", "delivery_detail": "opted_out"},
                    )
                return False, "opted_out"
            else:
                log.warning("Manual send to opted-out phone %s - allowed but flagged", to_phone)

    if not skip_dnd:
        try:
            delay_seconds = max(0.0, min(300.0, float(brand.get("sales_bot_reply_delay_seconds") or 0)))
        except (TypeError, ValueError):
            delay_seconds = 0.0
        if delay_seconds > 0:
            log.info("Warren auto-reply delay: brand=%s thread=%s delay=%.1fs", brand.get("id"), thread_id, delay_seconds)
            time.sleep(delay_seconds)

    if channel == "sms":
        return _send_sms(db, brand, thread_id, message_text, logged_message_id=logged_message_id)
    elif channel == "messenger":
        return _send_messenger(db, brand, thread_id, message_text, recipient_id, page_id, logged_message_id=logged_message_id)
    else:
        log.warning("Warren sender: unsupported channel '%s'", channel)
        _mark_delivery(db, logged_message_id, status="failed", channel=channel, detail=f"Unsupported channel: {channel}")
        return False, f"Unsupported channel: {channel}"


def _send_sms(db, brand, thread_id, message_text, logged_message_id=None):
    """Send SMS via Quo/OpenPhone."""
    from webapp.quo_sms import send_sms

    api_key = (brand.get("quo_api_key") or "").strip()
    from_number = (brand.get("quo_phone_number") or "").strip()

    if not api_key or not from_number:
        log.warning("Warren sender: Quo not configured for brand %s", brand.get("id"))
        _mark_delivery(db, logged_message_id, status="failed", channel="sms", detail="Quo SMS not configured (missing API key or phone number)")
        return False, "Quo SMS not configured (missing API key or phone number)"

    # Get the lead's phone number from the thread
    thread = db.get_lead_thread(thread_id)
    if not thread:
        _mark_delivery(db, logged_message_id, status="failed", channel="sms", detail="Thread not found")
        return False, "Thread not found"

    to_phone = (thread.get("lead_phone") or "").strip()
    if not to_phone:
        # Try to find phone from messages metadata
        messages = db.get_lead_messages(thread_id, limit=10)
        for msg in messages:
            try:
                meta = json.loads(msg.get("metadata_json", "{}"))
                phone = meta.get("from", "")
                if phone and phone.startswith("+"):
                    to_phone = phone
                    break
            except (json.JSONDecodeError, TypeError):
                pass

    if not to_phone:
        _mark_delivery(db, logged_message_id, status="failed", channel="sms", detail="No phone number found for lead")
        return False, "No phone number found for lead"

    # Append opt-out footer for A2P compliance
    footer = (brand.get("sales_bot_sms_opt_out_footer") or "").strip()
    if footer:
        full_text = f"{message_text}\n\n{footer}"
    else:
        full_text = message_text

    success, detail = send_sms(api_key, from_number, to_phone, full_text)
    _mark_delivery(
        db,
        logged_message_id,
        status="sent" if success else "failed",
        channel="sms",
        detail=detail,
        recipient=to_phone,
        auto_sent=success,
    )

    # Log delivery event
    db.add_lead_event(
        brand["id"], thread_id,
        "sms_sent" if success else "sms_failed",
        event_value=message_text[:200],
        metadata={"to": to_phone, "success": success, "detail": str(detail)[:500]},
    )

    if success:
        log.info("Warren SMS sent: thread=%s to=%s", thread_id, to_phone)
    else:
        log.warning("Warren SMS failed: thread=%s to=%s err=%s", thread_id, to_phone, detail)

    return success, str(detail)


def _send_messenger(db, brand, thread_id, message_text, recipient_id=None, page_id=None, logged_message_id=None):
    """Send a reply via Facebook Messenger."""
    if not recipient_id:
        # Try to get from thread/messages
        thread = db.get_lead_thread(thread_id)
        if thread and thread.get("external_thread_id"):
            recipient_id = thread["external_thread_id"]

    if not recipient_id:
        _mark_delivery(db, logged_message_id, status="failed", channel="messenger", detail="No Messenger recipient ID")
        return False, "No Messenger recipient ID"

    # Get page access token
    if not page_id:
        page_id = (brand.get("facebook_page_id") or "").strip()

    if not page_id:
        _mark_delivery(db, logged_message_id, status="failed", channel="messenger", detail="No Facebook Page ID configured", recipient=recipient_id)
        return False, "No Facebook Page ID configured"

    page_token = _get_page_token(db, brand, page_id)
    if not page_token:
        _mark_delivery(db, logged_message_id, status="failed", channel="messenger", detail="Could not get Facebook Page token", recipient=recipient_id)
        return False, "Could not get Facebook Page token"

    try:
        resp = requests.post(
            f"https://graph.facebook.com/v21.0/{page_id}/messages",
            params={"access_token": page_token},
            json={
                "recipient": {"id": recipient_id},
                "message": {"text": message_text},
                "messaging_type": "RESPONSE",
            },
            timeout=15,
        )

        success = resp.status_code == 200
        detail = resp.json() if success else resp.text[:300]
        _mark_delivery(
            db,
            logged_message_id,
            status="sent" if success else "failed",
            channel="messenger",
            detail=detail,
            recipient=recipient_id,
            auto_sent=success,
        )

        # Log delivery event
        db.add_lead_event(
            brand["id"], thread_id,
            "messenger_sent" if success else "messenger_failed",
            event_value=message_text[:200],
            metadata={"recipient_id": recipient_id, "success": success, "detail": str(detail)[:500]},
        )

        if success:
            log.info("Warren Messenger sent: thread=%s to=%s", thread_id, recipient_id)
        else:
            log.warning("Warren Messenger failed: thread=%s err=%s", thread_id, detail)

        return success, str(detail)

    except Exception as exc:
        log.exception("Warren Messenger error: %s", exc)
        _mark_delivery(db, logged_message_id, status="failed", channel="messenger", detail=str(exc), recipient=recipient_id)
        db.add_lead_event(
            brand["id"], thread_id, "messenger_failed",
            event_value=str(exc)[:200],
        )
        return False, str(exc)


def _get_page_token(db, brand, page_id):
    """Get a valid Facebook Page access token."""
    brand_id = brand["id"]
    conn_data = db.get_brand_connections(brand_id).get("meta")
    if not conn_data or conn_data.get("status") != "connected":
        return None

    from webapp.api_bridge import _get_meta_token, _get_page_access_token

    user_token = _get_meta_token(db, brand_id, conn_data)
    if not user_token:
        return None

    return _get_page_access_token(page_id, user_token)


def send_manual_reply(db, brand_id, thread_id, message_text, channel=None):
    """Send a manual reply from the inbox UI (client-initiated).

    This bypasses Warren's brain and sends directly.
    Returns (success, detail).
    """
    brand = db.get_brand(brand_id)
    if not brand:
        return False, "Brand not found"

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return False, "Thread not found"

    # Determine channel from thread if not specified
    if not channel:
        channel = thread.get("channel", "sms")

    # Get Messenger-specific IDs
    recipient_id = None
    page_id = None
    if channel == "messenger":
        recipient_id = thread.get("external_thread_id", "")
        page_id = (brand.get("facebook_page_id") or "").strip()

    # Log the manual message
    message_id = db.add_lead_message(
        thread_id,
        direction="outbound",
        role="user",
        content=message_text,
        channel=channel,
        metadata={"manual": True, "delivery_status": "pending"},
    )

    # Send it (manual sends bypass DND)
    success, detail = send_reply(db, brand, thread_id, message_text,
                                  channel=channel, recipient_id=recipient_id,
                                  page_id=page_id, skip_dnd=True,
                                  logged_message_id=message_id)

    return success, detail


# ─────────────────────────────────────────────
# A2P Compliance Replies (STOP / START / HELP)
# ─────────────────────────────────────────────

def _send_raw_sms(brand, to_phone, text):
    """Send an SMS directly without thread context or opt-out checks.
    Used only for STOP/START/HELP compliance responses.
    """
    from webapp.quo_sms import send_sms

    api_key = (brand.get("quo_api_key") or "").strip()
    from_number = (brand.get("quo_phone_number") or "").strip()
    if not api_key or not from_number:
        log.warning("Cannot send compliance SMS - Quo not configured for brand %s", brand.get("id"))
        return False, "not_configured"
    return send_sms(api_key, from_number, to_phone, text)


def send_transactional_sms(db, brand, to_phone, text, append_opt_out_footer=True):
    """Send a direct SMS outside the lead-thread system for transactional notices."""
    to_phone = (to_phone or "").strip()
    if not to_phone:
        return False, "missing_phone"
    if db.is_opted_out(brand.get("id"), to_phone):
        return False, "opted_out"

    full_text = text or ""
    if append_opt_out_footer:
        footer = (brand.get("sales_bot_sms_opt_out_footer") or "").strip()
        if footer:
            full_text = f"{full_text}\n\n{footer}"
    return _send_raw_sms(brand, to_phone, full_text)


def send_opt_out_confirmation(db, brand, phone):
    """TCPA-required: confirm the user has been unsubscribed."""
    brand_name = brand.get("name", "")
    msg = f"You have been unsubscribed from {brand_name} messages. Reply START to re-subscribe."
    ok, detail = _send_raw_sms(brand, phone, msg)
    if ok:
        log.info("Opt-out confirmation sent to %s for brand %s", phone, brand.get("id"))
    else:
        log.warning("Failed to send opt-out confirmation to %s: %s", phone, detail)
    return ok


def send_opt_in_confirmation(db, brand, phone):
    """Confirm the user has re-subscribed."""
    brand_name = brand.get("name", "")
    msg = f"You have been re-subscribed to {brand_name} messages. Reply STOP to opt out."
    ok, detail = _send_raw_sms(brand, phone, msg)
    if ok:
        log.info("Opt-in confirmation sent to %s for brand %s", phone, brand.get("id"))
    else:
        log.warning("Failed to send opt-in confirmation to %s: %s", phone, detail)
    return ok


def send_help_reply(db, brand, phone):
    """TCPA-required: respond to HELP with contact info."""
    brand_name = brand.get("name", "")
    contact_phone = (brand.get("phone") or brand.get("business_phone") or "").strip()
    if contact_phone:
        msg = f"{brand_name}: For help, call {contact_phone}. Msg frequency varies. Reply STOP to opt out."
    else:
        msg = f"{brand_name}: Msg frequency varies. Msg & data rates may apply. Reply STOP to opt out."
    ok, detail = _send_raw_sms(brand, phone, msg)
    if ok:
        log.info("HELP reply sent to %s for brand %s", phone, brand.get("id"))
    else:
        log.warning("Failed to send HELP reply to %s: %s", phone, detail)
    return ok
