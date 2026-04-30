"""Deterministic Warren operating brain.

This layer turns tasks and long-term memories into a current business state for
chat prompts and Google Sheet tables. It is separate from the lead-reply Warren
brain so sales conversations keep their existing behavior.
"""

import json
import logging
import re
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}(?!\d)")


def _clean(value, limit=500):
    return str(value or "").strip()[:limit]


def _prompt_clean(value, limit=500):
    text = _EMAIL_RE.sub("[email]", str(value or ""))
    text = _PHONE_RE.sub("[phone]", text)
    return text.strip()[:limit]


def _parse_dt(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    for fmt, length in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%dT%H:%M:%S", 19), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(text[:length], fmt)
        except Exception:
            continue
    return None


def _task_steps(task):
    try:
        parsed = json.loads((task or {}).get("steps_json") or "[]")
    except Exception:
        parsed = []
    return parsed if isinstance(parsed, list) else []


def _steps_summary(task, limit=1200):
    lines = []
    for step in _task_steps(task):
        if not isinstance(step, dict):
            continue
        text = _clean(step.get("text"), 220)
        if text:
            lines.append(f"[{'done' if step.get('done') else 'open'}] {text}")
    return "\n".join(lines)[:limit]


def _task_dict(task):
    status = _clean(task.get("status"), 40).lower() or "open"
    priority = _clean(task.get("priority"), 40).lower() or "normal"
    return {
        "id": task.get("id"),
        "title": _clean(task.get("title"), 160),
        "status": status,
        "priority": priority,
        "source": _clean(task.get("source"), 80),
        "source_ref": _clean(task.get("source_ref"), 120),
        "due_date": _clean(task.get("due_date"), 40),
        "created_at": _clean(task.get("created_at"), 40),
        "updated_at": _clean(task.get("updated_at"), 40),
        "completed_at": _clean(task.get("completed_at"), 40),
        "description": _clean(task.get("description"), 900),
        "completion_notes": _clean(task.get("completion_notes"), 1200),
        "steps": _steps_summary(task),
    }


def _load_tasks(db, brand_id, task_limit):
    combined = {}
    for task in db.get_brand_tasks(brand_id, limit=task_limit) or []:
        combined[task.get("id")] = task
    for status in ("done", "in_progress", "open"):
        for task in db.get_brand_tasks(brand_id, status=status, limit=50) or []:
            combined[task.get("id")] = task
    return [_task_dict(task) for task in combined.values() if task.get("id")]


def build_warren_operating_brain(db, brand_id, *, task_limit=120, memory_limit=40):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    tasks = _load_tasks(db, brand_id, task_limit)
    memories = db.get_warren_memories(brand_id, limit=memory_limit) or []

    open_tasks = [t for t in tasks if t["status"] not in {"done", "cancelled", "canceled"}]
    completed_tasks = [t for t in tasks if t["status"] == "done"]
    high_priority_open = [t for t in open_tasks if t["priority"] in {"urgent", "high"}]
    completed_without_notes = [t for t in completed_tasks if not t["completion_notes"]]

    stale_tasks = []
    overdue_tasks = []
    for task in open_tasks:
        updated = _parse_dt(task.get("updated_at")) or _parse_dt(task.get("created_at"))
        due = _parse_dt(task.get("due_date"))
        if updated and (now - updated).days >= 14:
            stale_tasks.append(task)
        if due and due.date() < now.date():
            overdue_tasks.append(task)

    recent_learnings = [
        {
            "category": _clean(memory.get("category"), 60),
            "title": _clean(memory.get("title"), 160),
            "content": _clean(memory.get("content"), 900),
            "updated_at": _clean(memory.get("updated_at") or memory.get("created_at"), 40),
        }
        for memory in memories[:12]
    ]

    focus_rules = []
    if overdue_tasks:
        focus_rules.append(f"Resolve overdue work first: {len(overdue_tasks)} open task(s) are past due.")
    if high_priority_open:
        focus_rules.append(f"Protect focus: {len(high_priority_open)} urgent/high-priority task(s) are open. Recommend the next one before adding new work.")
    if stale_tasks:
        focus_rules.append(f"Follow up: {len(stale_tasks)} open task(s) have not moved in 14+ days. Ask what is blocking them or suggest reassignment.")
    if completed_without_notes:
        focus_rules.append(f"Close the feedback loop: {len(completed_without_notes)} completed task(s) lack notes. Ask what changed so future advice improves.")
    if completed_tasks:
        focus_rules.append("Use completed work as history. Do not recommend the same action unless the next step is verification or iteration.")
    if not open_tasks:
        focus_rules.append("No open work is queued. Recommend one high-leverage next mission from current data, not a long list.")
    focus_rules.append("Every recommendation should include the intended business outcome and the metric or signal Warren will check later.")

    summary_rows = [
        ["Generated At", now.strftime("%Y-%m-%d %H:%M:%S"), ""],
        ["Open Tasks", str(len(open_tasks)), "Tasks not done or cancelled"],
        ["Urgent/High Open", str(len(high_priority_open)), "Work Warren should protect"],
        ["Overdue Open Tasks", str(len(overdue_tasks)), "Open tasks past due"],
        ["Stale Open Tasks", str(len(stale_tasks)), "Open tasks untouched for 14+ days"],
        ["Completed Tasks", str(len(completed_tasks)), "Completed work in the current task table"],
        ["Completed Without Notes", str(len(completed_without_notes)), "Done tasks missing outcome context"],
    ]
    for idx, rule in enumerate(focus_rules, start=1):
        summary_rows.append([f"Operating Rule {idx}", rule, ""])

    return {
        "summary": {
            "open_count": len(open_tasks),
            "high_priority_open_count": len(high_priority_open),
            "overdue_count": len(overdue_tasks),
            "stale_count": len(stale_tasks),
            "completed_count": len(completed_tasks),
            "completed_without_notes_count": len(completed_without_notes),
        },
        "focus_rules": focus_rules,
        "open_tasks": open_tasks[:30],
        "high_priority_open": high_priority_open[:12],
        "overdue_tasks": overdue_tasks[:12],
        "stale_tasks": stale_tasks[:12],
        "completed_tasks": completed_tasks[:30],
        "recent_learnings": recent_learnings,
        "sheet_summary_rows": summary_rows,
    }


def brain_prompt_text(brain):
    if not brain:
        return ""
    brain = sanitize_operating_brain_for_prompt(brain)
    summary = brain.get("summary") or {}
    lines = [
        "WARREN OPERATING BRAIN:",
        "Current work state: "
        f"{summary.get('open_count', 0)} open, "
        f"{summary.get('high_priority_open_count', 0)} urgent/high, "
        f"{summary.get('overdue_count', 0)} overdue, "
        f"{summary.get('stale_count', 0)} stale, "
        f"{summary.get('completed_count', 0)} completed.",
    ]
    for rule in (brain.get("focus_rules") or [])[:4]:
        lines.append(f"- {rule}")
    if brain.get("open_tasks"):
        lines.append("Top open work:")
        for task in brain["open_tasks"][:3]:
            detail = task.get("description") or task.get("steps") or ""
            lines.append(f"- #{task['id']} [{task['priority']}/{task['status']}] {task['title']}: {detail[:160]}")
    if brain.get("completed_tasks"):
        lines.append("Recent completions to remember:")
        for task in brain["completed_tasks"][:3]:
            note = task.get("completion_notes") or "No completion notes."
            lines.append(f"- #{task['id']} {task['title']}: {note[:180]}")
    if brain.get("recent_learnings"):
        lines.append("Recent memories/learnings:")
        for memory in brain["recent_learnings"][:3]:
            lines.append(f"- [{memory['category']}] {memory['title']}: {memory['content'][:180]}")
    lines.append("Use this brain before giving advice. Recommend action, verification, or follow-up based on this state.")
    return "\n".join(lines)[:3500]


def _prompt_task(task, *, notes_limit=360):
    return {
        "id": task.get("id"),
        "title": _prompt_clean(task.get("title"), 140),
        "status": _prompt_clean(task.get("status"), 30),
        "priority": _prompt_clean(task.get("priority"), 30),
        "source": _prompt_clean(task.get("source"), 60),
        "due_date": _prompt_clean(task.get("due_date"), 30),
        "updated_at": _prompt_clean(task.get("updated_at"), 30),
        "completed_at": _prompt_clean(task.get("completed_at"), 30),
        "description": _prompt_clean(task.get("description"), 260),
        "completion_notes": _prompt_clean(task.get("completion_notes"), notes_limit),
        "steps": _prompt_clean(task.get("steps"), 420),
    }


def sanitize_operating_brain_for_prompt(brain):
    """Return a compact, redacted operating brain safe for chat prompts."""
    if not isinstance(brain, dict):
        return {}
    return {
        "summary": dict(brain.get("summary") or {}),
        "focus_rules": [_prompt_clean(rule, 240) for rule in (brain.get("focus_rules") or [])[:6]],
        "open_tasks": [_prompt_task(task) for task in (brain.get("open_tasks") or [])[:10]],
        "high_priority_open": [_prompt_task(task) for task in (brain.get("high_priority_open") or [])[:6]],
        "overdue_tasks": [_prompt_task(task) for task in (brain.get("overdue_tasks") or [])[:6]],
        "stale_tasks": [_prompt_task(task) for task in (brain.get("stale_tasks") or [])[:6]],
        "completed_tasks": [_prompt_task(task, notes_limit=420) for task in (brain.get("completed_tasks") or [])[:8]],
        "recent_learnings": [
            {
                "category": _prompt_clean(memory.get("category"), 40),
                "title": _prompt_clean(memory.get("title"), 120),
                "content": _prompt_clean(memory.get("content"), 360),
                "updated_at": _prompt_clean(memory.get("updated_at"), 30),
            }
            for memory in (brain.get("recent_learnings") or [])[:6]
        ],
    }


def sync_warren_brain_sheet(db, brand_id):
    """Refresh Warren's table-style Sheet tabs. Best effort."""
    brain = build_warren_operating_brain(db, brand_id)
    try:
        from webapp.google_drive import write_sheet_table

        write_sheet_table(db, brand_id, "Warren Brain", ["Metric", "Value", "Notes"], brain["sheet_summary_rows"])
        write_sheet_table(
            db,
            brand_id,
            "Warren Open Tasks",
            ["Task ID", "Priority", "Status", "Title", "Description", "Steps", "Due Date", "Updated At", "Source", "Source Ref"],
            [
                [
                    task["id"],
                    task["priority"],
                    task["status"],
                    task["title"],
                    task["description"],
                    task["steps"],
                    task["due_date"],
                    task["updated_at"],
                    task["source"],
                    task["source_ref"],
                ]
                for task in brain["open_tasks"]
            ],
        )
        write_sheet_table(
            db,
            brand_id,
            "Warren Completed",
            ["Task ID", "Completed At", "Priority", "Title", "Completion Notes", "Steps", "Source", "Source Ref"],
            [
                [
                    task["id"],
                    task["completed_at"],
                    task["priority"],
                    task["title"],
                    task["completion_notes"],
                    task["steps"],
                    task["source"],
                    task["source_ref"],
                ]
                for task in brain["completed_tasks"]
            ],
        )
    except Exception as exc:
        log.warning("Failed to sync Warren brain sheet for brand %s: %s", brand_id, exc)
    return brain
