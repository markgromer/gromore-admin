"""
Warren Sender - outbound message delivery via Quo SMS and Meta Messenger.

Handles sending Warren's replies to leads through the appropriate channel
and logging the delivery status.
"""
import json
import logging
import requests

log = logging.getLogger(__name__)


def send_reply(db, brand, thread_id, message_text, channel="sms", recipient_id=None, page_id=None, skip_dnd=False):
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
    # DND check (automated sends only)
    if not skip_dnd:
        from webapp.warren_nurture import _is_dnd
        if _is_dnd(brand):
            db.add_lead_message(
                thread_id, "outbound", "assistant", message_text,
                channel=channel,
                metadata_json=json.dumps({"dnd_held": True, "held_at": __import__("datetime").datetime.utcnow().isoformat()}),
            )
            log.info("DND active for brand %s - message held for thread %s", brand.get("id"), thread_id)
            return False, "dnd_held"

    if channel == "sms":
        return _send_sms(db, brand, thread_id, message_text)
    elif channel == "messenger":
        return _send_messenger(db, brand, thread_id, message_text, recipient_id, page_id)
    else:
        log.warning("Warren sender: unsupported channel '%s'", channel)
        return False, f"Unsupported channel: {channel}"


def _send_sms(db, brand, thread_id, message_text):
    """Send SMS via Quo/OpenPhone."""
    from webapp.quo_sms import send_sms

    api_key = (brand.get("quo_api_key") or "").strip()
    from_number = (brand.get("quo_phone_number") or "").strip()

    if not api_key or not from_number:
        log.warning("Warren sender: Quo not configured for brand %s", brand.get("id"))
        return False, "Quo SMS not configured (missing API key or phone number)"

    # Get the lead's phone number from the thread
    thread = db.get_lead_thread(thread_id)
    if not thread:
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
        return False, "No phone number found for lead"

    success, detail = send_sms(api_key, from_number, to_phone, message_text)

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


def _send_messenger(db, brand, thread_id, message_text, recipient_id=None, page_id=None):
    """Send a reply via Facebook Messenger."""
    if not recipient_id:
        # Try to get from thread/messages
        thread = db.get_lead_thread(thread_id)
        if thread and thread.get("external_thread_id"):
            recipient_id = thread["external_thread_id"]

    if not recipient_id:
        return False, "No Messenger recipient ID"

    # Get page access token
    if not page_id:
        page_id = (brand.get("facebook_page_id") or "").strip()

    if not page_id:
        return False, "No Facebook Page ID configured"

    page_token = _get_page_token(db, brand, page_id)
    if not page_token:
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
    db.add_lead_message(
        thread_id,
        direction="outbound",
        role="user",
        content=message_text,
        channel=channel,
        metadata={"manual": True},
    )

    # Send it (manual sends bypass DND)
    success, detail = send_reply(db, brand, thread_id, message_text,
                                  channel=channel, recipient_id=recipient_id,
                                  page_id=page_id, skip_dnd=True)

    return success, detail
