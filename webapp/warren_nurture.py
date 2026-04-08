"""
Warren Nurture Engine - automated follow-up for stale or cold leads.

Runs as a background job (called from the job scheduler) to:
1. Follow up on leads that haven't responded
2. Re-engage cold leads
3. Send gentle nudges to quoted-but-not-booked leads

Uses Warren's brain to generate contextual follow-ups rather than templates.
"""
import logging
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# Nurture timing rules (hours since last outbound with no reply)
NURTURE_RULES = [
    {"stage": "engaged", "hours_since_last": 4, "max_attempts": 3, "event": "nurture_followup"},
    {"stage": "quoted", "hours_since_last": 24, "max_attempts": 2, "event": "nurture_quote_followup"},
    {"stage": "qualified", "hours_since_last": 48, "max_attempts": 2, "event": "nurture_qualified_followup"},
    {"stage": "new", "hours_since_last": 2, "max_attempts": 2, "event": "nurture_first_touch"},
]


def process_nurture_queue(db):
    """Check all brands for leads that need follow-up.

    Returns (sent_count, skipped_count).
    """
    brands = _get_active_brands(db)
    total_sent = 0
    total_skipped = 0

    for brand in brands:
        sent, skipped = _process_brand_nurture(db, brand)
        total_sent += sent
        total_skipped += skipped

    return total_sent, total_skipped


def _get_active_brands(db):
    """Get all brands with the sales bot enabled."""
    conn = db._conn()
    rows = conn.execute(
        "SELECT * FROM brands WHERE sales_bot_enabled = 1"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _process_brand_nurture(db, brand):
    """Process nurture for a single brand. Returns (sent, skipped)."""
    brand_id = brand["id"]
    sent = 0
    skipped = 0

    for rule in NURTURE_RULES:
        threads = _find_stale_threads(db, brand_id, rule)
        for thread in threads:
            thread_id = thread["id"]

            # Check nurture attempt count
            attempts = _count_nurture_attempts(db, thread_id, rule["event"])
            if attempts >= rule["max_attempts"]:
                skipped += 1
                continue

            # Generate and send follow-up
            success = _send_nurture(db, brand, thread, rule)
            if success:
                sent += 1
            else:
                skipped += 1

    return sent, skipped


def _find_stale_threads(db, brand_id, rule):
    """Find threads matching the nurture rule criteria."""
    cutoff = (datetime.utcnow() - timedelta(hours=rule["hours_since_last"])).isoformat()

    conn = db._conn()
    rows = conn.execute(
        """
        SELECT * FROM lead_threads
        WHERE brand_id = ?
          AND status = ?
          AND last_outbound_at != ''
          AND last_outbound_at < ?
          AND (last_inbound_at = '' OR last_inbound_at < last_outbound_at)
        ORDER BY last_outbound_at ASC
        LIMIT 50
        """,
        (brand_id, rule["stage"], cutoff),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _count_nurture_attempts(db, thread_id, event_type):
    """Count how many nurture events of this type exist for this thread."""
    conn = db._conn()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM lead_events WHERE thread_id = ? AND event_type = ?",
        (thread_id, event_type),
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def _send_nurture(db, brand, thread, rule):
    """Generate a nurture follow-up and send it.

    Returns True if sent successfully.
    """
    from webapp.warren_brain import process_and_respond
    from webapp.warren_sender import send_reply

    thread_id = thread["id"]
    brand_id = brand["id"]
    channel = thread.get("channel", "sms")

    # Add a system message to guide Warren's follow-up
    db.add_lead_message(
        thread_id,
        direction="inbound",
        role="system",
        content=f"[System: The lead has not responded in {rule['hours_since_last']} hours. Generate a brief, natural follow-up. Do not repeat your last message. Keep it short and low-pressure.]",
        channel=channel,
    )

    result = process_and_respond(db, brand_id, thread_id, channel=channel)
    if not result or not result.get("reply"):
        return False

    # Log the nurture event
    db.add_lead_event(
        brand_id, thread_id, rule["event"],
        event_value=result["reply"][:200],
    )

    # Send if confidence is high enough
    if result.get("should_send"):
        if channel == "sms":
            success, _ = send_reply(db, brand, thread_id, result["reply"], channel="sms")
            return success
        elif channel == "messenger":
            recipient_id = thread.get("external_thread_id", "")
            page_id = (brand.get("facebook_page_id") or "").strip()
            success, _ = send_reply(db, brand, thread_id, result["reply"],
                                     channel="messenger", recipient_id=recipient_id,
                                     page_id=page_id)
            return success

    return False


def check_for_ghosted_leads(db, brand_id, ghost_hours=72):
    """Mark leads as 'lost' if they haven't responded in ghost_hours.

    Only marks leads that Warren has already followed up on at least twice.
    Returns count of leads marked as ghosted.
    """
    from webapp.warren_pipeline import advance_stage

    cutoff = (datetime.utcnow() - timedelta(hours=ghost_hours)).isoformat()

    conn = db._conn()
    rows = conn.execute(
        """
        SELECT * FROM lead_threads
        WHERE brand_id = ?
          AND status NOT IN ('won', 'lost', 'booked')
          AND last_outbound_at != ''
          AND last_outbound_at < ?
          AND (last_inbound_at = '' OR last_inbound_at < last_outbound_at)
        """,
        (brand_id, cutoff),
    ).fetchall()
    conn.close()

    ghosted = 0
    for row in rows:
        thread = dict(row)
        thread_id = thread["id"]

        # Only ghost if we've sent at least 2 follow-ups
        attempts = _count_nurture_attempts(db, thread_id, "nurture_followup")
        if attempts < 2:
            continue

        advance_stage(db, thread_id, brand_id, "lead_ghosted")
        ghosted += 1
        log.info("Lead ghosted: thread=%s brand=%s", thread_id, brand_id)

    return ghosted
