"""
Warren Pipeline Engine - manages lead lifecycle stages.

Pipeline stages:
  new        -> Lead just arrived, no Warren interaction yet
  engaged    -> Warren has replied at least once, conversation active
  quoted     -> A quote/price range has been shared with the lead
  qualified  -> Lead confirmed interest and fits the service profile
  booked     -> Appointment, job, or recurring service is scheduled
  won        -> Job completed or contract signed
  lost       -> Lead ghosted, declined, or disqualified

Transitions are driven by conversation events and can be
auto-advanced by Warren or manually set by the client.
"""
import logging
from datetime import datetime

log = logging.getLogger(__name__)

# Ordered pipeline stages
PIPELINE_STAGES = [
    "new",
    "engaged",
    "quoted",
    "qualified",
    "booked",
    "won",
    "lost",
]

STAGE_INDEX = {s: i for i, s in enumerate(PIPELINE_STAGES)}

# Events that trigger auto-advance
AUTO_ADVANCE_MAP = {
    "warren_replied":   "engaged",
    "quote_sent":       "quoted",
    "lead_confirmed":   "qualified",
    "appointment_set":  "booked",
    "job_completed":    "won",
    "lead_declined":    "lost",
    "lead_ghosted":     "lost",
    "handoff_triggered": None,  # no auto-advance, just log
}


def can_advance(current_stage, target_stage):
    """Check if moving from current_stage to target_stage is valid.

    Rules:
    - Can always move to 'lost' from any stage
    - Can move forward (higher index) freely
    - Can move backward only via manual override (returns False for auto)
    """
    if target_stage == "lost":
        return True
    ci = STAGE_INDEX.get(current_stage, 0)
    ti = STAGE_INDEX.get(target_stage, 0)
    return ti > ci


def advance_stage(db, thread_id, brand_id, event_type, metadata=None):
    """Process a pipeline event and auto-advance the stage if appropriate.

    Returns (new_stage_or_None, event_id).
    """
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        log.warning("advance_stage: thread %s not found for brand %s", thread_id, brand_id)
        return None, None

    current = thread.get("status", "new")
    target = AUTO_ADVANCE_MAP.get(event_type)

    # Log the event regardless
    event_id = db.add_lead_event(
        brand_id, thread_id, event_type,
        event_value=target or "",
        metadata=metadata,
    )

    if target and can_advance(current, target):
        db.update_lead_thread_status(thread_id, status=target)
        log.info(
            "Pipeline advanced: thread=%s brand=%s %s -> %s (event=%s)",
            thread_id, brand_id, current, target, event_type,
        )
        return target, event_id

    return None, event_id


def manual_stage_change(db, thread_id, brand_id, new_stage, changed_by="client"):
    """Manually set a pipeline stage (client or admin override).

    Returns (success, event_id).
    """
    if new_stage not in STAGE_INDEX:
        return False, None

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return False, None

    old_stage = thread.get("status", "new")
    db.update_lead_thread_status(thread_id, status=new_stage)
    event_id = db.add_lead_event(
        brand_id, thread_id, "stage_manual_change",
        event_value=new_stage,
        metadata={"from": old_stage, "to": new_stage, "changed_by": changed_by},
    )
    log.info(
        "Pipeline manual change: thread=%s %s -> %s by %s",
        thread_id, old_stage, new_stage, changed_by,
    )
    return True, event_id


def get_pipeline_summary(db, brand_id):
    """Get counts per pipeline stage for a brand. Returns dict of stage -> count."""
    threads = db.get_lead_threads(brand_id, limit=10000)
    summary = {stage: 0 for stage in PIPELINE_STAGES}
    for t in threads:
        stage = t.get("status", "new")
        if stage in summary:
            summary[stage] += 1
        else:
            summary["new"] += 1
    return summary


def get_pipeline_metrics(db, brand_id):
    """Calculate pipeline performance metrics for reporting."""
    threads = db.get_lead_threads(brand_id, limit=10000)
    if not threads:
        return {
            "total_leads": 0,
            "stage_counts": {s: 0 for s in PIPELINE_STAGES},
            "conversion_rate": 0,
            "avg_response_time_minutes": 0,
            "channels": {},
        }

    stage_counts = {s: 0 for s in PIPELINE_STAGES}
    channels = {}
    response_times = []

    for t in threads:
        stage = t.get("status", "new")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1

        ch = t.get("channel", "unknown")
        channels[ch] = channels.get(ch, 0) + 1

        # Response time: time between first inbound and first outbound
        inbound = t.get("last_inbound_at", "")
        outbound = t.get("last_outbound_at", "")
        if inbound and outbound:
            try:
                t_in = datetime.fromisoformat(inbound)
                t_out = datetime.fromisoformat(outbound)
                diff = (t_out - t_in).total_seconds() / 60.0
                if 0 < diff < 1440:  # only count if under 24 hours
                    response_times.append(diff)
            except (ValueError, TypeError):
                pass

    won = stage_counts.get("won", 0)
    total = len(threads)
    lost = stage_counts.get("lost", 0)
    decided = won + lost

    return {
        "total_leads": total,
        "stage_counts": stage_counts,
        "conversion_rate": round((won / decided * 100) if decided > 0 else 0, 1),
        "avg_response_time_minutes": round(sum(response_times) / len(response_times), 1) if response_times else 0,
        "channels": channels,
        "active_leads": total - won - lost,
    }
