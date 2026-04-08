"""
Warren Nurture Engine - automated follow-up for stale or cold leads.

Runs as a background job (called from the job scheduler) to:
1. Follow up on leads that haven't responded
2. Re-engage cold leads
3. Send gentle nudges to quoted-but-not-booked leads

Uses Warren's brain to generate contextual follow-ups rather than templates.
Cadence (hot/warm/cold) and DND settings are per-brand, configured in settings.
"""
import logging
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

_SPOUSE_CHECK_PATTERNS = (
    "check with my wife",
    "check with wife",
    "talk to my wife",
    "ask my wife",
    "run it by my wife",
    "check with my husband",
    "check with husband",
    "talk to my husband",
    "ask my husband",
    "run it by my husband",
    "check with my spouse",
    "talk to my spouse",
    "ask my spouse",
    "check with my partner",
    "talk to my partner",
    "ask my partner",
    "run it by them",
    "talk it over",
    "sleep on it",
)

# Fallback defaults if brand settings are missing
_DEFAULTS = {
    "hot_hours": 2, "hot_max": 3,
    "warm_hours": 24, "warm_max": 2,
    "cold_hours": 48, "cold_max": 2,
    "ghost_hours": 72,
}

# Map pipeline stages to temperature tiers
_STAGE_TIER = {
    "new": "hot",
    "engaged": "hot",
    "quoted": "warm",
    "qualified": "cold",
}

# Event name per tier for tracking attempts
_TIER_EVENT = {
    "hot": "nurture_followup",
    "warm": "nurture_quote_followup",
    "cold": "nurture_qualified_followup",
}


def _hours_since_timestamp(value):
    """Return hours elapsed since a stored ISO/datetime string."""
    if not value:
        return None
    try:
        then = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return max(0.0, (datetime.utcnow() - then).total_seconds() / 3600.0)


def _recent_lead_text(messages, limit=3):
    """Return a compact string of the most recent lead-authored messages."""
    lead_parts = []
    for msg in reversed(messages):
        if msg.get("role") == "system":
            continue
        if msg.get("direction") == "inbound" or msg.get("role") in {"lead", "user"}:
            content = (msg.get("content") or "").strip()
            if content:
                lead_parts.append(content.lower())
            if len(lead_parts) >= limit:
                break
    lead_parts.reverse()
    return "\n".join(lead_parts)


def _detect_contextual_nudge_plan(thread, messages):
    """Return a context-specific nurture plan, if one matches the conversation."""
    if not messages:
        return None

    recent_lead_text = _recent_lead_text(messages)
    non_system_messages = [m for m in messages if m.get("role") != "system" and (m.get("content") or "").strip()]

    if recent_lead_text and any(pattern in recent_lead_text for pattern in _SPOUSE_CHECK_PATTERNS):
        return {
            "event": "nurture_spouse_followup",
            "wait_hours": 4.0,
            "max_attempts": 1,
            "prompt": (
                "The lead said they need to check with their spouse or partner. "
                "Send a low-pressure follow-up that acknowledges that directly. "
                "Offer one useful detail that helps them decide, like a quick price recap, what is included, "
                "availability, warranty, or the easiest next step. Keep it supportive, not pushy."
            ),
        }

    if thread.get("status") in {"new", "engaged"} and len(non_system_messages) >= 4:
        return {
            "event": "nurture_soft_close",
            "wait_hours": 0.25,
            "max_attempts": 1,
            "prompt": (
                "The lead went quiet in the middle of an active conversation. "
                "Send a simple, human nudge that gives them an easy out. "
                "Use the spirit of: 'Want me to leave this open or close it out for now?' "
                "Keep it brief, casual, and non-pushy."
            ),
        }

    return None


def _build_nurture_system_message(db, brand_id, thread, rule, messages=None):
    """Build the system instruction that guides a nurture follow-up."""
    channel = thread.get("channel", "sms")
    thread_id = thread["id"]
    hours = rule["hours_since_last"]
    context_prompt = rule.get(
        "prompt",
        f"The lead has not responded in {hours} hours. Generate a brief, natural follow-up. Do not repeat your last message. Keep it short and low-pressure.",
    )
    nudge = f"[System: {context_prompt}"

    objection_events = db.get_lead_events(brand_id, thread_id, event_type="objection_detected")
    if objection_events:
        past = [e.get("event_value", "") for e in objection_events[:3] if e.get("event_value")]
        if past:
            nudge += f"\n\nThis lead had these concerns: {', '.join(past)}. Address one of them naturally, don't ignore the elephant in the room."

    missing = []
    if not thread.get("lead_name"):
        missing.append("their name")
    if not thread.get("lead_phone") and channel == "messenger":
        missing.append("their phone number")
    if missing:
        nudge += f"\n\nWe still don't know {' or '.join(missing)}. Try to get it naturally in this follow-up."

    if messages:
        recent_lead_text = _recent_lead_text(messages, limit=2)
        if recent_lead_text:
            nudge += f"\n\nRecent lead context: {recent_lead_text}"

    nudge += "]"
    return nudge


def _brand_nurture_rules(brand):
    """Build nurture rules from per-brand settings (with fallbacks)."""
    rules = []
    for stage, tier in _STAGE_TIER.items():
        hours = brand.get(f"sales_bot_nurture_{tier}_hours") or _DEFAULTS[f"{tier}_hours"]
        max_att = brand.get(f"sales_bot_nurture_{tier}_max") or _DEFAULTS[f"{tier}_max"]
        rules.append({
            "stage": stage,
            "hours_since_last": float(hours),
            "max_attempts": int(max_att),
            "event": _TIER_EVENT[tier],
        })
    return rules


def _is_dnd(brand):
    """Check if the brand is currently in a Do Not Disturb window.

    Returns True if Warren should hold messages right now.
    """
    if not brand.get("sales_bot_dnd_enabled"):
        return False

    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    tz_name = brand.get("sales_bot_dnd_timezone") or "America/New_York"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("America/New_York")

    now = datetime.now(tz)

    # Weekend check
    if brand.get("sales_bot_dnd_weekends") and now.weekday() >= 5:
        return True

    # Time window check
    dnd_start_str = brand.get("sales_bot_dnd_start") or "21:00"
    dnd_end_str = brand.get("sales_bot_dnd_end") or "08:00"
    try:
        start_h, start_m = map(int, dnd_start_str.split(":"))
        end_h, end_m = map(int, dnd_end_str.split(":"))
    except (ValueError, AttributeError):
        start_h, start_m = 21, 0
        end_h, end_m = 8, 0

    start_minutes = start_h * 60 + start_m
    end_minutes = end_h * 60 + end_m
    now_minutes = now.hour * 60 + now.minute

    if start_minutes > end_minutes:
        # Overnight window (e.g. 21:00 - 08:00)
        return now_minutes >= start_minutes or now_minutes < end_minutes
    else:
        # Same-day window (e.g. 12:00 - 14:00)
        return start_minutes <= now_minutes < end_minutes


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
    suppressed_thread_ids = set()

    # Check nurture enabled
    if not brand.get("sales_bot_nurture_enabled", 1):
        return sent, skipped

    # Check DND
    if _is_dnd(brand):
        log.info("Brand %s is in DND window, skipping nurture", brand_id)
        return sent, skipped

    for thread in _find_contextual_candidates(db, brand_id):
        thread_id = thread["id"]
        messages = db.get_lead_messages(thread_id)
        plan = _detect_contextual_nudge_plan(thread, messages)
        if not plan:
            continue

        attempts = _count_nurture_attempts(db, thread_id, plan["event"])
        if attempts < plan["max_attempts"]:
            suppressed_thread_ids.add(thread_id)

        last_outbound_hours = _hours_since_timestamp(thread.get("last_outbound_at"))
        if last_outbound_hours is None or last_outbound_hours < plan["wait_hours"]:
            continue
        if attempts >= plan["max_attempts"]:
            continue

        custom_rule = {
            "event": plan["event"],
            "hours_since_last": plan["wait_hours"],
            "prompt": plan["prompt"],
        }
        success = _send_nurture(db, brand, thread, custom_rule, messages=messages)
        if success:
            sent += 1
        else:
            skipped += 1

    rules = _brand_nurture_rules(brand)

    for rule in rules:
        threads = _find_stale_threads(db, brand_id, rule)
        for thread in threads:
            thread_id = thread["id"]

            if thread_id in suppressed_thread_ids:
                skipped += 1
                continue

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


def _find_contextual_candidates(db, brand_id):
        """Find open threads that may qualify for fast, context-aware nudges."""
        cutoff = (datetime.utcnow() - timedelta(minutes=15)).isoformat()

        conn = db._conn()
        rows = conn.execute(
                """
                SELECT * FROM lead_threads
                WHERE brand_id = ?
                    AND status IN ('new', 'engaged', 'quoted', 'qualified')
                    AND last_outbound_at != ''
                    AND last_outbound_at < ?
                    AND (last_inbound_at = '' OR last_inbound_at < last_outbound_at)
                ORDER BY last_outbound_at ASC
                LIMIT 100
                """,
                (brand_id, cutoff),
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


def _send_nurture(db, brand, thread, rule, messages=None):
    """Generate a nurture follow-up and send it.

    Returns True if sent successfully.
    """
    from webapp.warren_brain import process_and_respond
    from webapp.warren_sender import send_reply

    thread_id = thread["id"]
    brand_id = brand["id"]
    channel = thread.get("channel", "sms")

    # A2P: skip opted-out leads
    if channel == "sms":
        lead_phone = (thread.get("lead_phone") or "").strip()
        if lead_phone and db.is_opted_out(brand_id, lead_phone):
            log.info("Skipping nurture for opted-out phone %s thread %s", lead_phone, thread_id)
            return False

    if messages is None:
        messages = db.get_lead_messages(thread_id)

    nudge = _build_nurture_system_message(db, brand_id, thread, rule, messages=messages)

    # Add a system message to guide Warren's follow-up
    db.add_lead_message(
        thread_id,
        direction="inbound",
        role="system",
        content=nudge,
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


def check_for_ghosted_leads(db, brand_id, ghost_hours=None):
    """Mark leads as 'lost' if they haven't responded in ghost_hours.

    Only marks leads that Warren has already followed up on at least twice.
    If ghost_hours is None, reads from brand settings.
    Returns count of leads marked as ghosted.
    """
    from webapp.warren_pipeline import advance_stage

    if ghost_hours is None:
        conn = db._conn()
        row = conn.execute("SELECT sales_bot_nurture_ghost_hours FROM brands WHERE id = ?", (brand_id,)).fetchone()
        conn.close()
        ghost_hours = float(row["sales_bot_nurture_ghost_hours"]) if row and row["sales_bot_nurture_ghost_hours"] else 72

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
