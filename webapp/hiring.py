"""Hiring Hub - AI-powered hiring funnel.

Blueprint for job creation, WARREN-generated postings, public application,
AI text-interview engine (5-signal behavioral scoring), candidate management,
and one-push scheduling/offer/rejection actions.
"""

import json
import logging
import os
import re
import time
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urlparse

import requests as _requests
from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)

log = logging.getLogger(__name__)

hiring_bp = Blueprint(
    "hiring",
    __name__,
    template_folder="templates",
)


@hiring_bp.context_processor
def inject_hiring_globals():
    """Provide the same base-template variables that client_bp injects."""
    import re
    brand_id = session.get("client_brand_id")
    assistant_enabled = False
    assistant_messages = []
    assistant_model_chat = "gpt-4o-mini"
    month = (request.args.get("month") or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", month):
        month = datetime.now().strftime("%Y-%m")

    if brand_id:
        try:
            db = _get_db()
            brand = db.get_brand(brand_id) or {}
            api_key = _get_api_key(brand)
            assistant_enabled = bool(api_key)
            rows = db.get_ai_chat_messages(brand_id, month, limit=30)
            assistant_messages = [
                {"role": r.get("role"), "content": r.get("content", "")}
                for r in rows if r.get("content")
            ]
        except Exception:
            pass

    return {
        "client_user": session.get("client_name"),
        "client_brand": session.get("client_brand_name"),
        "now": datetime.now(),
        "assistant_enabled": assistant_enabled,
        "assistant_messages": assistant_messages,
        "assistant_month": month,
        "assistant_model_chat": assistant_model_chat,
        "assistant_models": ["gpt-4o-mini", "gpt-4o"],
    }

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_db():
    return current_app.db


def _require_client_login():
    """Return (brand, client_user) or abort 401."""
    brand_id = session.get("client_brand_id")
    user_id = session.get("client_user_id")
    if not brand_id or not user_id:
        abort(401)
    db = _get_db()
    brand = db.get_brand(brand_id)
    if not brand:
        abort(401)
    return brand, user_id


def _get_api_key(brand=None):
    brand_key = ((brand or {}).get("openai_api_key") or "").strip()
    if brand_key:
        return brand_key
    try:
        return (current_app.config.get("OPENAI_API_KEY", "") or "").strip()
    except RuntimeError:
        return os.environ.get("OPENAI_API_KEY", "").strip()


def _ai_call(api_key, system, user_content, model=None, temperature=0.5, timeout=60):
    """Generic OpenAI chat completion returning parsed JSON or None."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        resp = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": model or "gpt-4o-mini",
                "temperature": temperature,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_content},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            log.warning("Hiring AI failed (%s): %s", resp.status_code, resp.text[:300])
            return None
        data = resp.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        return json.loads(content)
    except Exception as exc:
        log.warning("Hiring AI error: %s", exc)
        return None


def _cors_json(data, status=200):
    resp = jsonify(data)
    resp.status_code = status
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


def _cors_preflight():
    resp = jsonify({"ok": True})
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


def _send_hiring_email(brand, recipient_email, recipient_name, subject, body_html):
    """Send an email using the app's SMTP config."""
    try:
        from webapp.email_sender import send_report_email
        # Build a minimal "report" dict so we can reuse send_report_email
        # Actually, let's call SMTP directly for flexibility
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        cfg = current_app.config
        smtp_host = cfg.get("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(cfg.get("SMTP_PORT", 587))
        smtp_user = cfg.get("SMTP_USER", "")
        smtp_pass = cfg.get("SMTP_PASSWORD", "")
        from_name = brand.get("display_name") or cfg.get("SMTP_FROM_NAME", "GroMore")
        from_email = cfg.get("SMTP_FROM_EMAIL", smtp_user)

        if not smtp_user or not smtp_pass:
            log.warning("SMTP not configured, skipping hiring email to %s", recipient_email)
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = recipient_email
        msg.attach(MIMEText(body_html, "html"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, recipient_email, msg.as_string())
        return True
    except Exception as exc:
        log.exception("Hiring email error: %s", exc)
        return False


def _send_quo_sms(brand, to_phone, content):
    """Send SMS via Quo if configured on the brand."""
    api_key = (brand.get("quo_api_key") or "").strip()
    from_number = (brand.get("quo_phone_number") or "").strip()
    if not api_key or not from_number or not to_phone:
        return False
    from webapp.quo_sms import send_sms
    ok, _ = send_sms(api_key, from_number, to_phone, content)
    return ok


# ---------------------------------------------------------------------------
# WARREN: Job Prefill (smart form draft from minimal input)
# ---------------------------------------------------------------------------

JOB_PREFILL_PROMPT = """You are WARREN, an AI hiring assistant for local service businesses.
The business owner wants to create a job posting. They gave you a job title and maybe some notes.
Your job: fill out the entire job form with smart, realistic defaults they can edit.

Use your knowledge of the industry, the role, and local hiring norms.
Be practical, not corporate. Think like a small business owner hiring.

RULES:
- Salary should be realistic annual ranges for the role and region (if they mention hourly, convert to annual for the fields but note the hourly equivalent in benefits)
- Must-haves should be 3-5 practical, non-obvious requirements (not just "must be reliable")
- Dealbreakers should be 2-3 real disqualifiers an owner would care about
- Culture notes should reflect what kind of personality thrives in this role
- Physical requirements only if the role actually involves physical work
- No em dashes. Use commas, periods, or colons.
- Keep each field concise. These are form inputs, not essays.

Return ONLY valid JSON:
{
    "title": "Clean job title",
    "department": "Department name",
    "job_type": "full-time|part-time|contract",
    "location": "City, ST (use their input or leave empty)",
    "remote": "no|yes|hybrid",
    "salary_min": 35000,
    "salary_max": 50000,
    "benefits": "Realistic benefits for this type of role",
    "must_haves": "3-5 practical must-haves, one per line",
    "dealbreakers": "2-3 real dealbreakers, one per line",
    "culture_notes": "What kind of person thrives here",
    "physical_requirements": "Physical demands if applicable, or empty string"
}"""


def prefill_job_draft(brand, title, location="", owner_notes=""):
    """Generate a smart form prefill from just a title + optional notes."""
    api_key = _get_api_key(brand)
    if not api_key:
        return None
    context = {
        "title": title,
        "location": location,
        "owner_notes": owner_notes,
        "company_name": brand.get("display_name", ""),
        "industry": brand.get("industry", ""),
    }
    model = brand.get("openai_model_ads") or brand.get("openai_model") or "gpt-4o-mini"
    return _ai_call(api_key, JOB_PREFILL_PROMPT, json.dumps(context), model=model, temperature=0.6)


# ---------------------------------------------------------------------------
# WARREN: Job Posting Generator
# ---------------------------------------------------------------------------

JOB_GENERATION_PROMPT = """You are WARREN, an AI hiring assistant inside GroMore.
Your task: generate a compelling, honest job posting based on the owner's criteria.

RULES:
- Title must be clear and professional (no "Rockstar", "Ninja", "Guru")
- Be specific about responsibilities - no "other duties as assigned"
- Separate hard requirements from nice-to-haves clearly
- Include compensation if provided (even a range)
- Mention remote/hybrid/on-site and location
- Be authentic to a small/local business tone - not corporate speak
- No em dashes. Use commas, periods, or colons instead.
- Keep it concise. Applicants skim.

Return ONLY valid JSON:
{
    "title": "Clean job title",
    "description": "Full job posting text (2-4 paragraphs, no markdown)",
    "requirements": ["requirement 1", "requirement 2"],
    "nice_to_haves": ["nice 1", "nice 2"],
    "interview_questions_seed": ["3-5 screening questions tailored to this role"],
    "red_flags_to_watch": ["behavioral red flags specific to this role"]
}"""


def generate_job_posting(brand, criteria):
    """Generate a job posting from owner criteria using WARREN.

    criteria = {
        "title": "...", "department": "...", "job_type": "...",
        "location": "...", "remote": "...", "salary_min": 0, "salary_max": 0,
        "benefits": "...", "must_haves": "...", "dealbreakers": "...",
        "culture_notes": "...", "physical_requirements": "...",
    }
    """
    api_key = _get_api_key(brand)
    if not api_key:
        return None

    context = {
        "company_name": brand.get("display_name", ""),
        "industry": brand.get("industry", ""),
        **criteria,
    }
    model = brand.get("openai_model_ads") or brand.get("openai_model") or "gpt-4o-mini"
    return _ai_call(api_key, JOB_GENERATION_PROMPT, json.dumps(context), model=model, temperature=0.7)


# ---------------------------------------------------------------------------
# WARREN: Interview Brain (the core)
# ---------------------------------------------------------------------------

GATE_QUESTION_PROMPT = """You are WARREN, generating gate questions for a job screening.
These are quick, practical, non-negotiable qualifying questions.
They filter out people who physically cannot do the job BEFORE the real interview starts.

RULES:
- 4-6 questions max. Each one should actually eliminate someone.
- Questions come directly from the job's must-haves, dealbreakers, physical requirements, and schedule.
- Format: simple yes/no or pick-one. No essays. No "tell me about yourself."
- If the job is outdoors, ask about weather/conditions. If early mornings, ask about schedule. If physical, ask about lifting/standing.
- Include one "commitment" question that states a real condition (e.g. "This role requires Saturday availability. Are you available?")
- Each question has a "dealbreaker_answer" - the answer that disqualifies them.
- Be direct. "Can you lift 50 lbs repeatedly?" not "How do you feel about physical labor?"
- No em dashes. Keep it short and blunt.

Return ONLY valid JSON:
{
    "gate_questions": [
        {
            "id": 1,
            "question": "The question text",
            "type": "yes_no",
            "options": ["Yes", "No"],
            "dealbreaker_answer": "No",
            "signal": "What this question filters for"
        },
        {
            "id": 2,
            "question": "Pick your preferred shift:",
            "type": "pick_one",
            "options": ["6 AM - 2 PM", "9 AM - 5 PM", "2 PM - 10 PM", "No preference"],
            "dealbreaker_answer": null,
            "signal": "Schedule flexibility"
        }
    ]
}"""


def generate_gate_questions(job, brand):
    """Generate practical gate questions from job criteria."""
    api_key = _get_api_key(brand)
    if not api_key:
        return None
    context = {
        "job_title": job.get("title", ""),
        "job_type": job.get("job_type", "full-time"),
        "location": job.get("location", ""),
        "remote": job.get("remote", "no"),
        "description": job.get("description", ""),
        "screening_criteria": job.get("screening_criteria", "{}"),
    }
    model = brand.get("openai_model_chat") or brand.get("openai_model") or "gpt-4o-mini"
    return _ai_call(api_key, GATE_QUESTION_PROMPT, json.dumps(context), model=model, temperature=0.5)


def _load_screening_criteria(job_or_text):
    """Return the editable hiring screening JSON as a dict."""
    raw = job_or_text
    if isinstance(job_or_text, dict):
        raw = job_or_text.get("screening_criteria", "{}")
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _screening_criteria_from_form(data):
    return json.dumps({
        "must_haves": data.get("must_haves", ""),
        "dealbreakers": data.get("dealbreakers", ""),
        "culture_notes": data.get("culture_notes", ""),
        "physical_requirements": data.get("physical_requirements", ""),
        "interview_focus": data.get("interview_focus", ""),
        "interview_questions": data.get("interview_questions", ""),
        "interview_avoid": data.get("interview_avoid", ""),
    })


def _interview_guidance_from_job(job):
    criteria = _load_screening_criteria(job)
    return {
        "focus_on": str(criteria.get("interview_focus") or "").strip(),
        "ask_or_cover": str(criteria.get("interview_questions") or "").strip(),
        "avoid": str(criteria.get("interview_avoid") or "").strip(),
    }


INTERVIEW_SYSTEM_PROMPT = """You are WARREN, an AI screening engine for hiring.
You run interactive, quiz-style screening interviews. Not boring Q&A. Think fast-paced assessment.

## YOUR MISSION
Score this candidate on 5 signals (each 0-20, total 0-100):
1. RELIABILITY - Will they show up, on time, repeatedly, without hand-holding?
2. OWNERSHIP - When something goes wrong, do they fix it or finger-point?
3. WORK ETHIC - Can they grind through repetitive, unglamorous work without fading?
4. COMMUNICATION CLARITY - Can they say what they mean simply and directly?
5. RESPONSIVENESS - (System-measured via response_time data you receive. Factor it in.)

## QUESTION TYPES (rotate between these, use the type field)
- **scenario**: "You show up to a job site and the customer says they never booked the appointment. What do you do?" Real situations, not hypotheticals about feelings.
- **pick_one**: Give them 2-4 concrete options. "Which one is closer to how you'd handle it? A) Call your boss immediately B) Try to resolve it yourself first C) Leave and go to the next job" Force a choice.
- **rapid_fire**: Quick 1-sentence questions that need quick 1-sentence answers. Stack 2-3 in a row. "Quick round: What time do you naturally wake up? What's the longest you've stayed at one job? What makes you quit a job?"
- **rank_it**: "Rank these from most important to least: Showing up on time, getting along with coworkers, quality of work, speed." Reveals real priorities.
- **real_talk**: Blunt, direct questions. "What's the worst job you've ever had and why did you leave?" "Be honest: are you looking for something temporary or long-term?"

## RED FLAG DETECTION (penalize hard)
- Blame-first language ("it wasn't my fault", "the customer was wrong")
- Dodging responsibility or parts of questions
- Polished but empty answers (sounds good, says nothing concrete)
- Mentions discomfort before solutions
- Defensive or combative tone
- Inconsistency between early and later answers

## HIGH-SIGNAL INDICATORS (reward)
- Takes responsibility immediately, even hypothetically
- Mentions proactive communication ("I'd call them", "I'd let them know")
- Clear, simple, direct answers (not rambling)
- Accepts tough conditions and focuses on getting the job done
- Short, structured responses that actually answer the question
- Specificity (times, examples, real situations from their past)

## WHAT YOU RECEIVE
- Job description, requirements, screening criteria
- Interview guidance from the hiring manager, including what to focus on, questions/topics to ask, and topics/language to avoid
- Full conversation so far (all messages + response times)
- Current question number
- Gate question answers (if available)

## WHAT YOU RETURN (strict JSON)
{
    "signal_scores": {
        "reliability": 0-20,
        "ownership": 0-20,
        "work_ethic": 0-20,
        "communication_clarity": 0-20,
        "responsiveness": 0-20
    },
    "red_flags": ["specific red flags from THIS answer, empty array if clean"],
    "next_question": "Your next question text",
    "question_type": "scenario|pick_one|rapid_fire|rank_it|real_talk",
    "choices": ["Option A", "Option B", "Option C"],
    "question_targets": ["which signals this question probes"],
    "is_final": false,
    "running_score": 0-100,
    "evaluation_notes": "Brief note: what you learned from this answer and what gap remains"
}

IMPORTANT: When question_type is "pick_one" or "rank_it", you MUST include a "choices" array.
For other types, set "choices" to an empty array [].

## FLOW RULES
- 8-12 questions. You decide when you have enough signal. Don't drag it out.
- Start with a friendly scenario question (ease in). Escalate pressure from there.
- After 3-4 questions, throw in a rapid_fire round - changes the pace, catches people off guard.
- If you spot a red flag, probe it with ONE follow-up. Don't belabor it.
- Adapt based on their answers. Strong candidate? Push harder. Weak? Confirm the pattern quick and wrap up.
- Mix question types. Never do the same type twice in a row.
- Never repeat a question or scenario that already appears in asked_questions or asked_question_topics.
- If a prior answer was too short, ask a different follow-up about the gap instead of re-asking the same setup.
- Treat interview_guidance.focus_on as priority signals to probe during the interview.
- Work interview_guidance.ask_or_cover into the interview when it is relevant, without dumping all questions at once.
- Respect interview_guidance.avoid. Do not ask about avoided topics, do not use avoided phrasing, and do not create scenarios that conflict with it.
- For the FINAL question (is_final: true), also include:
  "final_evaluation": "2-3 sentence honest assessment of this candidate. Be specific about what you saw.",
  "recommendation": "interview|waitlist|reject",
  "suggested_interview_questions": ["3-5 specific questions for the in-person, based on gaps or flags"],
  "signal_reasoning": {
      "reliability": "1-2 sentence explanation of why this score, citing specific answers",
      "ownership": "1-2 sentence explanation",
      "work_ethic": "1-2 sentence explanation",
      "communication_clarity": "1-2 sentence explanation",
      "responsiveness": "1-2 sentence explanation"
  },
  "key_moments": [
      {"quote": "their exact words or paraphrase", "signal": "which signal it revealed", "impact": "positive|negative", "note": "why this matters"},
      ...up to 5 key moments
  ],
  "detailed_recommendation": "3-5 sentences. What this candidate brings, what concerns you, and what the hiring manager should dig into during in-person."
- Do NOT reveal scores or that you're scoring them.
- Keep questions conversational but direct. One question at a time. 2-3 sentences max.
- No em dashes. No corporate fluff."""


_INTERVIEW_TOPIC_PATTERNS = [
    ("vehicle_problem", ("vehicle", "car", "truck", "break", "broke", "mechanic", "flat tire", "on the way")),
    ("forgot_equipment", ("forgot", "equipment", "supplies", "tools")),
    ("unhappy_customer", ("unhappy", "satisfied", "complain", "customer is upset", "not happy")),
    ("short_notice_route", ("short notice", "cover", "route")),
    ("customer_denies_booking", ("never booked", "appointment", "job site")),
    ("aggressive_dog", ("dog", "barking", "aggressive")),
    ("work_history", ("worst job", "leave", "quit", "longest", "one job")),
    ("schedule_habits", ("wake up", "naturally wake", "morning", "late")),
    ("priority_ranking", ("rank", "most important", "least")),
    ("independent_work", ("independently", "supervisor", "boss", "hand holding")),
    ("quality_vs_speed", ("quality", "speed", "fast", "rushing")),
    ("customer_property", ("gate", "key", "access", "property")),
    ("weather_conditions", ("weather", "heat", "rain", "cold")),
    ("coworker_issue", ("coworker", "team member", "cutting corners")),
]

_INTERVIEW_FALLBACK_QUESTIONS = [
    {
        "topic": "attendance_reliability",
        "question_type": "scenario",
        "question": "You wake up feeling rough but you are scheduled for a full route. What do you do in the first 10 minutes after realizing you may not be at 100%?",
        "choices": [],
        "question_targets": ["reliability", "communication_clarity"],
    },
    {
        "topic": "quality_vs_speed",
        "question_type": "pick_one",
        "question": "Which is closer to how you work when nobody is watching?",
        "choices": ["A) Move fast and keep the route on schedule", "B) Slow down enough to make sure the job is done right", "C) Ask for help whenever the decision is not obvious"],
        "question_targets": ["work_ethic", "ownership"],
    },
    {
        "topic": "directions_unclear",
        "question_type": "scenario",
        "question": "You get route notes that are unclear and the customer is not answering. What do you do before you decide to skip or improvise?",
        "choices": [],
        "question_targets": ["ownership", "communication_clarity"],
    },
    {
        "topic": "weather_conditions",
        "question_type": "real_talk",
        "question": "Real talk: this job can be repetitive and uncomfortable in bad weather. What part of that would be hardest for you to stick with?",
        "choices": [],
        "question_targets": ["work_ethic", "communication_clarity"],
    },
    {
        "topic": "customer_property",
        "question_type": "scenario",
        "question": "A gate code does not work and the customer is not responding, but the stop is on your route. What steps do you take?",
        "choices": [],
        "question_targets": ["ownership", "reliability"],
    },
    {
        "topic": "coworker_issue",
        "question_type": "scenario",
        "question": "You notice another worker skipping part of the process to finish faster. What do you do?",
        "choices": [],
        "question_targets": ["ownership", "work_ethic"],
    },
    {
        "topic": "communication_style",
        "question_type": "rapid_fire",
        "question": "Quick round: How early is early enough for a shift? What is one thing a supervisor should never have to remind you about? How do you prefer to get feedback?",
        "choices": [],
        "question_targets": ["reliability", "communication_clarity"],
    },
    {
        "topic": "schedule_conflict",
        "question_type": "pick_one",
        "question": "You realize you have a personal conflict with a scheduled shift. What is the first move?",
        "choices": ["A) Tell the supervisor as soon as possible", "B) Try to trade shifts yourself first", "C) Wait until you know for sure you cannot make it"],
        "question_targets": ["reliability", "communication_clarity"],
    },
]


def _avoid_terms_from_guidance(guidance):
    avoid_text = str((guidance or {}).get("avoid") or "").lower()
    pieces = re.split(r"[\n,;]+", avoid_text)
    terms = []
    for piece in pieces:
        cleaned = _normalize_interview_question(piece)
        if len(cleaned) >= 4:
            terms.append(cleaned)
    return terms


def _question_conflicts_with_guidance(question, guidance):
    normalized = _normalize_interview_question(question)
    if not normalized:
        return False
    for term in _avoid_terms_from_guidance(guidance):
        if term in normalized or SequenceMatcher(None, normalized, term).ratio() >= 0.82:
            return True
    return False


def _normalize_interview_question(text):
    text = re.sub(r"\s+", " ", str(text or "").lower()).strip()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _interview_question_topic(text):
    normalized = _normalize_interview_question(text)
    if not normalized:
        return ""
    for topic, needles in _INTERVIEW_TOPIC_PATTERNS:
        hits = sum(1 for needle in needles if needle in normalized)
        if hits >= 2 or (len(needles) <= 3 and hits >= 1):
            return topic
    return ""


def _asked_interview_questions(messages):
    questions = []
    for message in messages or []:
        if message.get("direction") != "outbound":
            continue
        content = str(message.get("content") or "").strip()
        if not content:
            continue
        if message.get("is_question") or content.endswith("?") or "?" in content:
            questions.append(content)
    return questions


def _is_repeated_interview_question(next_question, prior_questions):
    next_norm = _normalize_interview_question(next_question)
    if not next_norm:
        return False
    next_topic = _interview_question_topic(next_question)
    for prior in prior_questions or []:
        prior_norm = _normalize_interview_question(prior)
        if not prior_norm:
            continue
        if next_norm == prior_norm:
            return True
        if SequenceMatcher(None, next_norm, prior_norm).ratio() >= 0.84:
            return True
        if next_topic and next_topic == _interview_question_topic(prior):
            return True
    return False


def _repair_repeated_interview_question(result, prior_questions, current_question_number, guidance=None):
    if not result or result.get("is_final"):
        return result
    next_question = str(result.get("next_question") or "").strip()
    if (
        next_question
        and not _is_repeated_interview_question(next_question, prior_questions)
        and not _question_conflicts_with_guidance(next_question, guidance)
    ):
        return result

    used_topics = {_interview_question_topic(q) for q in prior_questions or []}
    used_topics.discard("")
    for fallback in _INTERVIEW_FALLBACK_QUESTIONS:
        if fallback["topic"] in used_topics:
            continue
        if _is_repeated_interview_question(fallback["question"], prior_questions):
            continue
        if _question_conflicts_with_guidance(fallback["question"], guidance):
            continue
        repaired = dict(result)
        repaired["next_question"] = fallback["question"]
        repaired["question_type"] = fallback["question_type"]
        repaired["choices"] = fallback["choices"]
        repaired["question_targets"] = fallback["question_targets"]
        repaired.setdefault("evaluation_notes", "")
        repaired["evaluation_notes"] = (
            str(repaired.get("evaluation_notes") or "").strip()
            + f" Server replaced a repeated question before question {current_question_number}."
        ).strip()
        return repaired

    repaired = dict(result)
    repaired["next_question"] = "Give me one specific example from a past job where something went wrong and you had to decide what to do next."
    repaired["question_type"] = "real_talk"
    repaired["choices"] = []
    repaired["question_targets"] = ["ownership", "communication_clarity"]
    return repaired


def _repair_first_interview_question(result, guidance=None):
    if not result:
        return result
    first_question = str(result.get("first_question") or "").strip()
    if first_question and not _question_conflicts_with_guidance(first_question, guidance):
        return result
    repaired = dict(result)
    for fallback in _INTERVIEW_FALLBACK_QUESTIONS:
        if _question_conflicts_with_guidance(fallback["question"], guidance):
            continue
        repaired["first_question"] = fallback["question"]
        repaired["question_type"] = fallback["question_type"]
        repaired["choices"] = fallback["choices"]
        repaired["question_targets"] = fallback["question_targets"]
        return repaired
    repaired["first_question"] = "Tell me about a time a job did not go according to plan. What did you do next?"
    repaired["question_type"] = "real_talk"
    repaired["choices"] = []
    repaired["question_targets"] = ["ownership", "communication_clarity"]
    return repaired


def conduct_interview_step(interview, messages, job, candidate, brand):
    """Run one step of the WARREN interview.

    Returns dict with next_question, signal_scores, etc. or None on failure.
    """
    api_key = _get_api_key(brand)
    if not api_key:
        return None

    # Build conversation context for WARREN
    conversation = []
    for m in messages:
        role = "assistant" if m["direction"] == "outbound" else "user"
        entry = {"role": role, "content": m["content"]}
        if m.get("response_time_sec") is not None:
            entry["response_time_sec"] = m["response_time_sec"]
        conversation.append(entry)

    asked_questions = _asked_interview_questions(messages)
    asked_topics = sorted({_interview_question_topic(q) for q in asked_questions if _interview_question_topic(q)})
    interview_guidance = _interview_guidance_from_job(job)
    context = {
        "job_title": job.get("title", ""),
        "job_description": job.get("description", ""),
        "job_requirements": job.get("requirements", "[]"),
        "screening_criteria": _load_screening_criteria(job),
        "interview_guidance": interview_guidance,
        "candidate_name": candidate.get("name", ""),
        "cover_letter": candidate.get("cover_letter", ""),
        "conversation": conversation,
        "current_question_number": interview.get("current_question", 0),
        "avg_response_time_sec": candidate.get("response_time_avg_sec", 0),
        "gate_answers": interview.get("gate_answers", "{}"),
        "asked_questions": asked_questions,
        "asked_question_topics": asked_topics,
        "disallowed_next_questions": asked_questions[-8:],
    }

    model = brand.get("openai_model_chat") or brand.get("openai_model") or "gpt-4o-mini"
    result = _ai_call(
        api_key,
        INTERVIEW_SYSTEM_PROMPT,
        json.dumps(context),
        model=model,
        temperature=0.5,
        timeout=45,
    )
    return _repair_repeated_interview_question(
        result,
        asked_questions,
        int(interview.get("current_question", 0) or 0) + 1,
        interview_guidance,
    )


def generate_first_question(job, candidate, brand):
    """Generate the opening welcome + first question for an interview."""
    api_key = _get_api_key(brand)
    if not api_key:
        return None

    interview_guidance = _interview_guidance_from_job(job)
    context = {
        "job_title": job.get("title", ""),
        "job_description": job.get("description", ""),
        "screening_criteria": _load_screening_criteria(job),
        "interview_guidance": interview_guidance,
        "candidate_name": candidate.get("name", ""),
    }

    system = """You are WARREN, starting a quiz-style screening interview.
Generate a brief, friendly welcome (1-2 sentences) and your first question.
Make it feel like a quick, interactive assessment, not a boring interview.
Start with a scenario question to ease them in.
Respect interview_guidance. If focus_on is provided, make the opening scenario relevant to it. If ask_or_cover is provided, use it when it fits naturally. Do not ask about anything in avoid.
No em dashes.

Return JSON:
{
    "welcome_message": "Hey [name]! Thanks for applying for [role]. Let's run through a quick screening - a mix of scenarios, quick-fire questions, and a few choices. Just be honest, there are no trick questions.",
    "first_question": "Your first scenario question here",
    "question_type": "scenario",
    "choices": [],
    "question_targets": ["reliability", "ownership"]
}"""

    model = brand.get("openai_model_chat") or brand.get("openai_model") or "gpt-4o-mini"
    result = _ai_call(api_key, system, json.dumps(context), model=model, temperature=0.6)
    return _repair_first_interview_question(result, interview_guidance)


# ---------------------------------------------------------------------------
# Routes: Hiring Hub (authenticated client portal)
# ---------------------------------------------------------------------------

@hiring_bp.route("/")
def hiring_dashboard():
    brand, user_id = _require_client_login()
    db = _get_db()
    jobs = db.get_hiring_jobs(brand["id"])

    # Attach candidate counts and top scores per job
    for job in jobs:
        candidates = db.get_hiring_candidates(brand["id"], job_id=job["id"])
        job["candidate_count"] = len(candidates)
        scores = [c["ai_score"] or 0 for c in candidates]
        job["top_score"] = max(scores, default=0)
        job["avg_score"] = round(sum(scores) / len(scores)) if scores else 0

    # Overview stats
    all_candidates = db.get_hiring_candidates(brand["id"])
    all_scores = [c["ai_score"] or 0 for c in all_candidates]
    stats = {
        "total_jobs": len(jobs),
        "active_jobs": sum(1 for j in jobs if j["status"] == "active"),
        "total_candidates": len(all_candidates),
        "avg_score": round(sum(all_scores) / len(all_scores)) if all_scores else 0,
        "top_candidates": [c for c in all_candidates if (c["ai_score"] or 0) >= 80],
    }

    return render_template("client/client_hiring.html", brand=brand, jobs=jobs, stats=stats)


@hiring_bp.route("/design", methods=["GET", "POST"])
def interview_design():
    """Brand-level interview design settings: colors, logo choice, card style."""
    brand, user_id = _require_client_login()
    db = _get_db()

    if request.method == "POST":
        design = {
            "primary_color": (request.form.get("primary_color") or "#7c3aed").strip()[:7],
            "secondary_color": (request.form.get("secondary_color") or "#60a5fa").strip()[:7],
            "bg_color": (request.form.get("bg_color") or "#0a0a14").strip()[:7],
            "card_bg": (request.form.get("card_bg") or "#12121e").strip()[:7],
            "text_color": (request.form.get("text_color") or "#e2e2f0").strip()[:7],
            "show_logo": request.form.get("show_logo") == "1",
            "logo_variant": (request.form.get("logo_variant") or "primary").strip()[:32],
            "company_label": (request.form.get("company_label") or "").strip()[:80],
            "border_radius": (request.form.get("border_radius") or "20").strip()[:3],
        }
        db.update_brand_text_field(brand["id"], "hiring_design", json.dumps(design))
        flash("Interview design saved.", "success")
        return redirect(url_for("hiring.interview_design"))

    design = {}
    try:
        design = json.loads(brand.get("hiring_design") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass

    logo_variants = []
    try:
        logo_variants = json.loads(brand.get("logo_variants") or "[]")
    except (json.JSONDecodeError, TypeError):
        pass

    return render_template("client/client_hiring_design.html",
                           brand=brand, design=design, logo_variants=logo_variants,
                           jobs=[], stats={})


@hiring_bp.route("/jobs/create", methods=["GET", "POST"])
def create_job():
    brand, user_id = _require_client_login()
    db = _get_db()

    if request.method == "POST":
        data = request.form
        screening = _screening_criteria_from_form(data)
        def _parse_salary(val):
            """Parse salary from free text: '45000', '45,000', '18/hr', '$52k'."""
            if not val:
                return 0
            val = val.strip().replace("$", "").replace(",", "").lower()
            try:
                if "/hr" in val or "/hour" in val:
                    hourly = float(val.split("/")[0].strip())
                    return round(hourly * 2080)
                if val.endswith("k"):
                    return float(val[:-1]) * 1000
                return float(val)
            except (ValueError, TypeError):
                return 0

        job_id = db.create_hiring_job(
            brand_id=brand["id"],
            title=data.get("title", "").strip(),
            department=data.get("department", "").strip(),
            job_type=data.get("job_type", "full-time"),
            location=data.get("location", "").strip(),
            remote=data.get("remote", "no"),
            description=data.get("description", "").strip(),
            requirements=data.get("requirements", "[]"),
            nice_to_haves=data.get("nice_to_haves", "[]"),
            salary_min=_parse_salary(data.get("salary_min")),
            salary_max=_parse_salary(data.get("salary_max")),
            benefits=data.get("benefits", "").strip(),
            screening_criteria=screening,
            scheduling_link=data.get("scheduling_link", "").strip(),
            status=data.get("status", "draft"),
            generated_post=data.get("generated_post", ""),
            created_by=user_id,
        )
        # Save gate questions if provided
        gate_qs = data.get("gate_questions", "")
        if gate_qs:
            db.update_hiring_job(job_id, gate_questions=gate_qs)

        # Save auto-send interview setting
        db.update_hiring_job(job_id, auto_send_interview=1 if data.get("auto_send_interview") == "on" else 0)

        flash("Job created!", "success")
        return redirect(url_for("hiring.job_detail", job_id=job_id))

    return render_template("client/client_hiring.html", brand=brand, jobs=[], stats={}, show_create=True)


@hiring_bp.route("/jobs/ai-prefill", methods=["POST"])
def ai_prefill_job():
    """WARREN drafts form fields from just a title + notes."""
    brand, user_id = _require_client_login()
    data = request.get_json(silent=True) or {}
    title = (data.get("title") or "").strip()
    if not title:
        return jsonify({"ok": False, "error": "Job title is required."})

    draft = prefill_job_draft(
        brand,
        title=title,
        location=data.get("location", ""),
        owner_notes=data.get("owner_notes", ""),
    )
    if not draft:
        return jsonify({"ok": False, "error": "No OpenAI key configured. Fill the form manually or add your key in Settings."})

    return jsonify({"ok": True, "draft": draft})


@hiring_bp.route("/jobs/<int:job_id>")
def job_detail(job_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    job = db.get_hiring_job(job_id)
    if not job or job["brand_id"] != brand["id"]:
        abort(404)
    candidates = db.get_hiring_candidates(brand["id"], job_id=job_id)
    return render_template("client/client_hiring.html", brand=brand, job=job, candidates=candidates, jobs=[], stats={})


@hiring_bp.route("/jobs/<int:job_id>/edit")
def edit_job(job_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    job = db.get_hiring_job(job_id)
    if not job or job["brand_id"] != brand["id"]:
        abort(404)
    screening = {}
    try:
        screening = json.loads(job.get("screening_criteria") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass
    gate_questions = []
    try:
        gate_questions = json.loads(job.get("gate_questions") or "[]")
    except (json.JSONDecodeError, TypeError):
        pass
    candidates = db.get_hiring_candidates(brand["id"], job_id=job_id)
    return render_template("client/client_hiring.html", brand=brand, job=job,
                           screening=screening, gate_questions=gate_questions,
                           candidates=candidates, jobs=[], stats={}, show_edit=True)


@hiring_bp.route("/jobs/<int:job_id>/update", methods=["POST"])
def update_job(job_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    job = db.get_hiring_job(job_id)
    if not job or job["brand_id"] != brand["id"]:
        abort(404)

    data = request.form
    fields = {}
    for key in ("title", "department", "job_type", "location", "remote",
                 "description", "requirements", "nice_to_haves", "benefits",
                 "scheduling_link", "status", "generated_post"):
        if key in data:
            fields[key] = data[key].strip() if isinstance(data[key], str) else data[key]
    if "salary_min" in data:
        fields["salary_min"] = float(data["salary_min"] or 0)
    if "salary_max" in data:
        fields["salary_max"] = float(data["salary_max"] or 0)
    if "must_haves" in data:
        fields["screening_criteria"] = _screening_criteria_from_form(data)
    if "gate_questions" in data:
        fields["gate_questions"] = data["gate_questions"]

    # Checkbox: present = on, absent = off
    fields["auto_send_interview"] = 1 if data.get("auto_send_interview") == "on" else 0

    if fields:
        db.update_hiring_job(job_id, **fields)
        flash("Job updated.", "success")
    return redirect(url_for("hiring.job_detail", job_id=job_id))


@hiring_bp.route("/jobs/<int:job_id>/delete", methods=["POST"])
def delete_job(job_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    job = db.get_hiring_job(job_id)
    if not job or job["brand_id"] != brand["id"]:
        abort(404)
    db.delete_hiring_job(job_id)
    flash("Job deleted.", "success")
    return redirect(url_for("hiring.hiring_dashboard"))


@hiring_bp.route("/jobs/<int:job_id>/generate", methods=["POST"])
def generate_posting(job_id):
    """WARREN generates a job posting from the owner's criteria."""
    brand, user_id = _require_client_login()
    db = _get_db()
    job = db.get_hiring_job(job_id)
    if not job or job["brand_id"] != brand["id"]:
        return jsonify({"ok": False, "error": "Job not found"}), 404

    criteria = {
        "title": job["title"],
        "department": job["department"],
        "job_type": job["job_type"],
        "location": job["location"],
        "remote": job["remote"],
        "salary_min": job["salary_min"],
        "salary_max": job["salary_max"],
        "benefits": job["benefits"],
    }
    sc = json.loads(job.get("screening_criteria") or "{}")
    criteria.update(sc)

    result = generate_job_posting(brand, criteria)
    if not result:
        return jsonify({"ok": False, "error": "AI generation failed. Check your OpenAI API key."}), 500

    # Save generated content back to the job
    updates = {}
    if result.get("description"):
        updates["description"] = result["description"]
        updates["generated_post"] = result["description"]
    if result.get("requirements"):
        updates["requirements"] = json.dumps(result["requirements"])
    if result.get("nice_to_haves"):
        updates["nice_to_haves"] = json.dumps(result["nice_to_haves"])
    if updates:
        db.update_hiring_job(job_id, **updates)

    # Auto-generate gate questions alongside the posting
    gate_result = generate_gate_questions(job, brand)
    if gate_result and gate_result.get("gate_questions"):
        db.update_hiring_job(job_id, gate_questions=json.dumps(gate_result["gate_questions"]))

    return jsonify({"ok": True, "result": result})


@hiring_bp.route("/jobs/<int:job_id>/generate-gates", methods=["POST"])
def generate_gates(job_id):
    """WARREN generates gate questions from job criteria."""
    brand, user_id = _require_client_login()
    db = _get_db()
    job = db.get_hiring_job(job_id)
    if not job or job["brand_id"] != brand["id"]:
        return jsonify({"ok": False, "error": "Job not found"}), 404

    result = generate_gate_questions(job, brand)
    if not result or not result.get("gate_questions"):
        return jsonify({"ok": False, "error": "Failed to generate gate questions."}), 500

    gate_qs = result["gate_questions"]
    db.update_hiring_job(job_id, gate_questions=json.dumps(gate_qs))
    return jsonify({"ok": True, "gate_questions": gate_qs})


# ---------------------------------------------------------------------------
# Routes: Candidates
# ---------------------------------------------------------------------------

@hiring_bp.route("/candidates")
def candidates_list():
    brand, user_id = _require_client_login()
    db = _get_db()
    job_id = request.args.get("job_id", type=int)
    status = request.args.get("status")
    q = request.args.get("q", "").strip()

    if q:
        candidates = db.search_hiring_candidates(brand["id"], q)
    else:
        candidates = db.get_hiring_candidates(brand["id"], job_id=job_id, status=status)

    jobs = db.get_hiring_jobs(brand["id"])
    return render_template("client/client_hiring.html", brand=brand, candidates=candidates,
                           jobs=jobs, stats={}, view="candidates")


@hiring_bp.route("/candidates/<int:candidate_id>")
def candidate_detail(candidate_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)

    job = db.get_hiring_job(candidate["job_id"]) if candidate.get("job_id") else None
    interviews = db.get_hiring_interviews_for_candidate(candidate_id)

    # Get messages for the latest interview
    messages = []
    latest_interview = interviews[0] if interviews else None
    if latest_interview:
        messages = db.get_hiring_messages(latest_interview["id"])

    return render_template("client/client_hiring_candidate.html",
                           brand=brand, candidate=candidate, job=job,
                           interviews=interviews, messages=messages,
                           interview=latest_interview)


@hiring_bp.route("/candidates/<int:candidate_id>/notes", methods=["POST"])
def update_candidate_notes(candidate_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)
    notes = request.form.get("notes", "")
    db.update_hiring_candidate(candidate_id, notes=notes)
    flash("Notes saved.", "success")
    return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))


@hiring_bp.route("/candidates/<int:candidate_id>/status", methods=["POST"])
def update_candidate_status(candidate_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)

    new_status = request.form.get("status", "").strip()
    valid = ("applied", "screening", "interviewing", "interviewed", "rejected", "offer", "hired", "archived")
    if new_status not in valid:
        flash("Invalid status.", "danger")
        return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))

    updates = {"status": new_status}
    if new_status == "hired":
        updates["hired_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    db.update_hiring_candidate(candidate_id, **updates)
    flash(f"Status updated to {new_status}.", "success")
    return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))


# ---------------------------------------------------------------------------
# One-push actions
# ---------------------------------------------------------------------------

@hiring_bp.route("/candidates/<int:candidate_id>/schedule-interview", methods=["POST"])
def schedule_interview_action(candidate_id):
    """Send scheduling invite via email + SMS."""
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)

    job = db.get_hiring_job(candidate["job_id"]) if candidate.get("job_id") else None
    scheduling_link = (job or {}).get("scheduling_link", "")
    company = brand.get("display_name", "the company")
    job_title = (job or {}).get("title", "the position")

    if not scheduling_link:
        flash("No scheduling link configured for this job. Add one in job settings.", "warning")
        return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))

    subject = f"Interview Invitation - {job_title} at {company}"
    body = f"""<p>Hi {candidate['name']},</p>
<p>Great news! We'd like to invite you to interview for the <strong>{job_title}</strong> position at {company}.</p>
<p>Please pick a time that works for you:</p>
<p><a href="{scheduling_link}" style="display:inline-block;padding:12px 24px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;">Schedule Your Interview</a></p>
<p>We look forward to meeting you.</p>
<p>Best,<br>{company}</p>"""

    _send_hiring_email(brand, candidate["email"], candidate["name"], subject, body)

    if candidate.get("phone"):
        sms_text = f"Hi {candidate['name']}! {company} would like to interview you for {job_title}. Pick a time here: {scheduling_link}"
        _send_quo_sms(brand, candidate["phone"], sms_text)

    db.update_hiring_candidate(candidate_id,
                               status="interviewing",
                               interview_scheduled_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

    # Log the action
    db.add_hiring_message(0, candidate_id, "outbound", "email",
                          f"[Schedule Interview] Sent invite with link: {scheduling_link}")

    flash("Interview invite sent!", "success")
    return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))


@hiring_bp.route("/candidates/<int:candidate_id>/reject", methods=["POST"])
def reject_candidate(candidate_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)

    job = db.get_hiring_job(candidate["job_id"]) if candidate.get("job_id") else None
    company = brand.get("display_name", "the company")
    job_title = (job or {}).get("title", "the position")

    subject = f"Update on your application - {company}"
    body = f"""<p>Hi {candidate['name']},</p>
<p>Thank you for your interest in the {job_title} position at {company} and for taking the time to go through our process.</p>
<p>After careful consideration, we've decided to move forward with other candidates whose experience more closely matches what we're looking for right now.</p>
<p>We appreciate your time and wish you the best in your job search.</p>
<p>Best,<br>{company}</p>"""

    _send_hiring_email(brand, candidate["email"], candidate["name"], subject, body)
    db.update_hiring_candidate(candidate_id, status="rejected")
    flash("Rejection sent.", "success")
    return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))


@hiring_bp.route("/candidates/<int:candidate_id>/offer", methods=["POST"])
def send_offer(candidate_id):
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)

    job = db.get_hiring_job(candidate["job_id"]) if candidate.get("job_id") else None
    company = brand.get("display_name", "the company")
    job_title = (job or {}).get("title", "the position")

    subject = f"Offer Letter - {job_title} at {company}"
    body = f"""<p>Hi {candidate['name']},</p>
<p>We're excited to offer you the <strong>{job_title}</strong> position at {company}!</p>
<p>We were impressed with your responses during our screening process and believe you'd be a great fit for our team.</p>
<p>Please reply to this email to discuss next steps, compensation details, and start date.</p>
<p>Welcome aboard!</p>
<p>Best,<br>{company}</p>"""

    _send_hiring_email(brand, candidate["email"], candidate["name"], subject, body)

    if candidate.get("phone"):
        sms_text = f"Hi {candidate['name']}! Great news from {company} - we'd like to offer you the {job_title} role! Check your email for details."
        _send_quo_sms(brand, candidate["phone"], sms_text)

    db.update_hiring_candidate(candidate_id, status="offer")
    flash("Offer sent!", "success")
    return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))


# ---------------------------------------------------------------------------
# Helper: send interview link (used by manual start + auto-send)
# ---------------------------------------------------------------------------

def _send_interview_link(db, brand, job, candidate):
    """Create an interview, email+SMS the link, update candidate status.

    Returns (interview_id, token) on success, or (None, None) on failure.
    """
    candidate_id = candidate["id"]
    interview_id, token = db.create_hiring_interview(candidate_id, brand["id"], job["id"])
    interview_url = url_for("hiring.interview_page", token=token, _external=True)

    company = brand.get("display_name", "the company")
    job_title = job.get("title", "the position")

    # Send via email
    subject = f"Quick Screening - {job_title} at {company}"
    body = f"""<p>Hi {candidate['name']},</p>
<p>Thanks for applying to {company}! We have a quick screening step - just a few questions that should take about 10-15 minutes.</p>
<p><a href="{interview_url}" style="display:inline-block;padding:12px 24px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;">Start Screening</a></p>
<p>The link expires in 48 hours, so the sooner the better.</p>
<p>Best,<br>{company}</p>"""

    _send_hiring_email(brand, candidate["email"], candidate["name"], subject, body)

    # Send via SMS if phone exists
    if candidate.get("phone"):
        sms_text = f"Hi {candidate['name']}! {company} here. Quick screening for the {job_title} role - takes ~10 min: {interview_url}"
        _send_quo_sms(brand, candidate["phone"], sms_text)

    db.update_hiring_candidate(candidate_id, status="screening",
                               screening_started_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    return interview_id, token


# ---------------------------------------------------------------------------
# Routes: Start AI Interview
# ---------------------------------------------------------------------------

@hiring_bp.route("/candidates/<int:candidate_id>/start-interview", methods=["POST"])
def start_interview(candidate_id):
    """Create an interview and send the link to the candidate."""
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)

    job = db.get_hiring_job(candidate["job_id"]) if candidate.get("job_id") else None
    if not job:
        flash("Candidate must be attached to a job first.", "warning")
        return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))

    _send_interview_link(db, brand, job, candidate)

    flash("Interview link sent!", "success")
    return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))


# ---------------------------------------------------------------------------
# Routes: Public Application (no auth)
# ---------------------------------------------------------------------------

@hiring_bp.route("/apply", methods=["POST", "OPTIONS"])
def public_apply():
    """Public JSON endpoint for job applications."""
    if request.method == "OPTIONS":
        return _cors_preflight()

    data = request.get_json(silent=True) or {}
    job_id = data.get("job_id")
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    phone = (data.get("phone") or "").strip()
    cover_letter = (data.get("cover_letter") or "").strip()
    source = (data.get("source") or "website").strip()

    if not job_id or not name or not email:
        return _cors_json({"ok": False, "error": "Name, email, and job are required."}, 400)

    db = _get_db()
    job = db.get_hiring_job(job_id)
    if not job or job["status"] != "active":
        return _cors_json({"ok": False, "error": "This position is no longer accepting applications."}, 400)

    # Check for duplicate
    existing = db.get_candidate_by_email_and_job(email, job_id)
    if existing:
        return _cors_json({"ok": False, "error": "You've already applied for this position."}, 400)

    candidate_id = db.create_hiring_candidate(
        brand_id=job["brand_id"],
        job_id=job_id,
        name=name,
        email=email,
        phone=phone,
        source=source,
        cover_letter=cover_letter,
    )

    # Send confirmation to applicant
    brand = db.get_brand(job["brand_id"])
    company = (brand or {}).get("display_name", "the company")

    # Auto-send interview if enabled on this job
    if job.get("auto_send_interview"):
        candidate = db.get_hiring_candidate(candidate_id)
        try:
            _send_interview_link(db, brand, job, candidate)
            log.info("Auto-sent interview to candidate %s for job %s", candidate_id, job_id)
        except Exception as exc:
            log.warning("Auto-send interview failed for candidate %s: %s", candidate_id, exc)
            # Fall back to confirmation email
            _send_hiring_email(
                brand or {},
                email, name,
                f"Application Received - {job['title']} at {company}",
                f"""<p>Hi {name},</p>
<p>We've received your application for the <strong>{job['title']}</strong> position at {company}. Thanks for your interest!</p>
<p>We'll be in touch with next steps soon.</p>
<p>Best,<br>{company}</p>""",
            )
    else:
        _send_hiring_email(
            brand or {},
            email, name,
            f"Application Received - {job['title']} at {company}",
            f"""<p>Hi {name},</p>
<p>We've received your application for the <strong>{job['title']}</strong> position at {company}. Thanks for your interest!</p>
<p>We'll be in touch with next steps soon.</p>
<p>Best,<br>{company}</p>""",
        )

    return _cors_json({"ok": True, "candidate_id": candidate_id})


# ---------------------------------------------------------------------------
# Routes: Public Job Listing (no auth)
# ---------------------------------------------------------------------------

@hiring_bp.route("/jobs/public/<brand_slug>")
def public_jobs(brand_slug):
    """Public page listing active jobs for a brand."""
    db = _get_db()
    # Find brand by slug
    conn = db._conn()
    row = conn.execute("SELECT * FROM brands WHERE slug = ?", (brand_slug,)).fetchone()
    conn.close()
    if not row:
        abort(404)
    brand = dict(row)
    jobs = db.get_hiring_jobs(brand["id"], status="active")
    return render_template("client/client_hiring_public.html", brand=brand, jobs=jobs, job=None)


@hiring_bp.route("/jobs/public/<brand_slug>/<int:job_id>")
def public_job_detail(brand_slug, job_id):
    """Public page for a specific job with application form."""
    db = _get_db()
    conn = db._conn()
    row = conn.execute("SELECT * FROM brands WHERE slug = ?", (brand_slug,)).fetchone()
    conn.close()
    if not row:
        abort(404)
    brand = dict(row)
    job = db.get_hiring_job(job_id)
    if not job or job["brand_id"] != brand["id"] or job["status"] != "active":
        abort(404)
    return render_template("client/client_hiring_public.html", brand=brand, jobs=[], job=job)


# ---------------------------------------------------------------------------
# Routes: Public logo serving (for interview pages, no login required)
# ---------------------------------------------------------------------------

@hiring_bp.route("/assets/logo/<path:filename>")
def public_logo(filename):
    """Serve logo files publicly so interview pages can display brand logos."""
    from pathlib import Path
    uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
    logo_dir = uploads_dir / "logos"
    # Prevent directory traversal
    safe_path = Path(filename)
    if ".." in safe_path.parts:
        abort(404)
    return send_from_directory(str(logo_dir), filename)


# ---------------------------------------------------------------------------
# Routes: Interview Chat Page (public, token-authenticated)
# ---------------------------------------------------------------------------

@hiring_bp.route("/interview/<token>/debug")
def interview_debug(token):
    """Debug endpoint - shows what design data the interview page would use."""
    db = _get_db()
    interview = db.get_hiring_interview_by_token(token)
    if not interview:
        abort(404)
    brand = db.get_brand(interview["brand_id"])
    design = {}
    try:
        design = json.loads((brand or {}).get("hiring_design") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass
    variants_raw = (brand or {}).get("logo_variants", "[]")
    logo_path = (brand or {}).get("logo_path", "")
    # Resolve logo URL same way as interview_page
    logo_url = ""
    if design.get("show_logo") and brand:
        variant_key = design.get("logo_variant", "primary")
        try:
            variants = json.loads(variants_raw or "[]")
        except (json.JSONDecodeError, TypeError):
            variants = []
        match = next((v for v in variants if v.get("key") == variant_key), None)
        logo_rel = None
        if match and match.get("path"):
            logo_rel = match["path"]
        elif brand.get("logo_path"):
            logo_rel = brand["logo_path"]
        if logo_rel and logo_rel.startswith("logos/"):
            logo_url = url_for("hiring.public_logo", filename=logo_rel[len("logos/"):])
        elif logo_rel:
            logo_url = f"/uploads/{logo_rel}"
    return jsonify({
        "design": design,
        "logo_url": logo_url,
        "logo_path": logo_path,
        "logo_variants_raw": variants_raw,
        "company": (brand or {}).get("display_name", ""),
        "brand_id": (brand or {}).get("id"),
    })

@hiring_bp.route("/interview/<token>")
def interview_page(token):
    """Public interview chat page - token is the auth."""
    db = _get_db()
    interview = db.get_hiring_interview_by_token(token)
    if not interview:
        abort(404)

    brand = db.get_brand(interview["brand_id"])
    company = (brand or {}).get("display_name", "")

    # Load hiring design settings
    design = {}
    try:
        design = json.loads((brand or {}).get("hiring_design") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass

    # Resolve logo URL (use public route so no login needed)
    logo_url = ""
    if design.get("show_logo") and brand:
        variant_key = design.get("logo_variant", "primary")
        try:
            variants = json.loads(brand.get("logo_variants") or "[]")
        except (json.JSONDecodeError, TypeError):
            variants = []
        match = next((v for v in variants if v.get("key") == variant_key), None)
        logo_rel = None
        if match and match.get("path"):
            logo_rel = match["path"]
        elif brand.get("logo_path"):
            logo_rel = brand["logo_path"]
        if logo_rel and logo_rel.startswith("logos/"):
            logo_url = url_for("hiring.public_logo", filename=logo_rel[len("logos/"):])
        elif logo_rel:
            logo_url = f"/uploads/{logo_rel}"

    # Let brand override the displayed company name
    if design.get("company_label"):
        company = design["company_label"]

    common = dict(interview=interview, token=token, brand=brand, company=company,
                  design=design, logo_url=logo_url)

    # Check expiry
    if interview["status"] == "expired":
        return render_template("client/client_hiring_interview.html",
                               expired=True, completed=False, **common)
    if interview["status"] == "completed":
        return render_template("client/client_hiring_interview.html",
                               expired=False, completed=True, **common)

    # Check time-based expiry (48h if pending, 72h if in_progress)
    created = datetime.strptime(interview["created_at"], "%Y-%m-%d %H:%M:%S")
    if interview["status"] == "pending" and datetime.utcnow() - created > timedelta(hours=48):
        db.update_hiring_interview(interview["id"], status="expired",
                                   expired_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        return render_template("client/client_hiring_interview.html",
                               expired=True, completed=False, **common)

    if interview["status"] == "in_progress" and interview.get("started_at"):
        started = datetime.strptime(interview["started_at"], "%Y-%m-%d %H:%M:%S")
        if datetime.utcnow() - started > timedelta(hours=72):
            db.update_hiring_interview(interview["id"], status="expired",
                                       expired_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            return render_template("client/client_hiring_interview.html",
                                   expired=True, completed=False, **common)

    messages = db.get_hiring_messages(interview["id"])

    # Load gate questions from job
    gate_questions = []
    if interview.get("job_id"):
        job = db.get_hiring_job(interview["job_id"])
        if job:
            try:
                gate_questions = json.loads(job.get("gate_questions") or "[]")
            except (json.JSONDecodeError, TypeError):
                gate_questions = []

    # Has the candidate already completed the gate?
    gate_completed = bool(interview.get("gate_answers") and interview["gate_answers"] != "{}")

    return render_template("client/client_hiring_interview.html",
                           messages=messages,
                           expired=False, completed=False,
                           gate_questions=gate_questions,
                           gate_completed=gate_completed,
                           **common)


@hiring_bp.route("/interview/<token>/gate", methods=["POST"])
def interview_gate(token):
    """Submit gate question answers before the real interview starts."""
    db = _get_db()
    interview = db.get_hiring_interview_by_token(token)
    if not interview or interview["status"] != "pending":
        return jsonify({"ok": False, "error": "Interview not available."}), 400

    data = request.get_json(silent=True) or {}
    answers = data.get("answers", {})

    if not answers:
        return jsonify({"ok": False, "error": "No answers provided."}), 400

    # Load gate questions from job to check for dealbreakers
    job = db.get_hiring_job(interview["job_id"]) if interview.get("job_id") else None
    gate_questions = []
    if job:
        try:
            gate_questions = json.loads(job.get("gate_questions") or "[]")
        except (json.JSONDecodeError, TypeError):
            pass

    # Check for dealbreaker answers
    dealbreaker_hit = False
    flags = []
    for gq in gate_questions:
        qid = str(gq.get("id", ""))
        candidate_answer = answers.get(qid, "")
        db_answer = gq.get("dealbreaker_answer")
        if db_answer and candidate_answer == db_answer:
            dealbreaker_hit = True
            flags.append(gq.get("signal", gq.get("question", "")))

    # Save gate answers on the interview
    gate_passed = 0 if dealbreaker_hit else 1
    db.update_hiring_interview(
        interview["id"],
        gate_answers=json.dumps(answers),
        gate_passed=gate_passed,
    )

    # If dealbreaker hit, flag on candidate but still allow interview to proceed
    # (the hiring manager sees the flag; we don't auto-reject)
    if dealbreaker_hit:
        candidate = db.get_hiring_candidate(interview["candidate_id"])
        existing_notes = (candidate or {}).get("notes", "") or ""
        flag_note = f"[GATE FLAG] Dealbreaker answers on: {', '.join(flags)}"
        if flag_note not in existing_notes:
            db.update_hiring_candidate(
                interview["candidate_id"],
                notes=f"{existing_notes}\n{flag_note}".strip(),
            )

    return jsonify({
        "ok": True,
        "gate_passed": gate_passed == 1,
        "flags": flags,
    })


@hiring_bp.route("/interview/<token>/start", methods=["POST"])
def interview_start(token):
    """Applicant clicks 'Start' - generate first question."""
    db = _get_db()
    interview = db.get_hiring_interview_by_token(token)
    if not interview or interview["status"] not in ("pending",):
        return jsonify({"ok": False, "error": "Interview not available."}), 400

    brand = db.get_brand(interview["brand_id"])
    job = db.get_hiring_job(interview["job_id"]) if interview.get("job_id") else {}
    candidate = db.get_hiring_candidate(interview["candidate_id"])

    # Mark as in progress
    db.update_hiring_interview(interview["id"],
                               status="in_progress",
                               started_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

    # Generate first question
    result = generate_first_question(job or {}, candidate or {}, brand or {})
    if not result:
        welcome = f"Hi {(candidate or {}).get('name', 'there')}! Thanks for applying. Let's get started with a few questions."
        first_q = "Tell me - if you showed up to a job site and realized the customer wasn't home, what would you do?"
    else:
        welcome = result.get("welcome_message", "Hi! Let's get started.")
        first_q = result.get("first_question", "What interests you about this role?")

    full_message = f"{welcome}\n\n{first_q}"

    # Save the outbound message
    db.add_hiring_message(
        interview_id=interview["id"],
        candidate_id=interview["candidate_id"],
        direction="outbound",
        channel="web_chat",
        content=full_message,
        is_question=1,
        question_number=1,
    )
    db.update_hiring_interview(interview["id"], current_question=1)

    return jsonify({"ok": True, "message": full_message})


@hiring_bp.route("/interview/<token>/respond", methods=["POST"])
def interview_respond(token):
    """Applicant sends an answer - WARREN scores it and asks the next question."""
    db = _get_db()
    interview = db.get_hiring_interview_by_token(token)
    if not interview or interview["status"] != "in_progress":
        return jsonify({"ok": False, "error": "Interview not active."}), 400

    data = request.get_json(silent=True) or {}
    answer = (data.get("answer") or data.get("message") or "").strip()
    if not answer:
        return jsonify({"ok": False, "error": "Please type an answer."}), 400

    # Calculate response time
    messages = db.get_hiring_messages(interview["id"])
    last_outbound = None
    for m in reversed(messages):
        if m["direction"] == "outbound":
            last_outbound = m
            break

    response_time = None
    if last_outbound and last_outbound.get("sent_at"):
        try:
            sent = datetime.strptime(last_outbound["sent_at"], "%Y-%m-%d %H:%M:%S")
            response_time = int((datetime.utcnow() - sent).total_seconds())
        except Exception:
            pass

    # Save the applicant's answer
    db.add_hiring_message(
        interview_id=interview["id"],
        candidate_id=interview["candidate_id"],
        direction="inbound",
        channel="web_chat",
        content=answer,
        is_question=0,
        response_time_sec=response_time,
    )

    # Update avg response time on candidate
    all_msgs = db.get_hiring_messages(interview["id"])
    response_times = [m["response_time_sec"] for m in all_msgs
                      if m["direction"] == "inbound" and m.get("response_time_sec")]
    if response_times:
        avg_rt = int(sum(response_times) / len(response_times))
        db.update_hiring_candidate(interview["candidate_id"], response_time_avg_sec=avg_rt)

    # Call WARREN for scoring + next question
    brand = db.get_brand(interview["brand_id"])
    job = db.get_hiring_job(interview["job_id"]) if interview.get("job_id") else {}
    candidate = db.get_hiring_candidate(interview["candidate_id"])

    result = conduct_interview_step(
        interview=interview,
        messages=all_msgs,
        job=job or {},
        candidate=candidate or {},
        brand=brand or {},
    )

    if not result:
        # Fallback: end the interview if AI fails
        db.update_hiring_interview(
            interview["id"],
            status="completed",
            completed_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return jsonify({
            "ok": True,
            "message": "Thanks for your responses! We'll review everything and be in touch.",
            "completed": True,
        })

    # Save signal scores on the inbound message (update last message)
    if result.get("signal_scores"):
        # Score the answer in the last inbound message
        inbound_msgs = [m for m in all_msgs if m["direction"] == "inbound"]
        if inbound_msgs:
            last_inbound_id = inbound_msgs[-1]["id"]
            conn = db._conn()
            conn.execute("UPDATE hiring_messages SET signal_scores = ? WHERE id = ?",
                         (json.dumps(result["signal_scores"]), last_inbound_id))
            conn.commit()
            conn.close()

    is_final = result.get("is_final", False)
    next_q = result.get("next_question", "")
    current_q = interview.get("current_question", 0) + 1

    if is_final or current_q > 12:
        # Interview complete
        final_eval = result.get("final_evaluation", result.get("evaluation_notes", ""))
        recommendation = result.get("recommendation", "waitlist")
        total_score = result.get("running_score", 0)
        score_breakdown = json.dumps(result.get("signal_scores", {}))
        signal_reasoning = json.dumps(result.get("signal_reasoning", {}))
        key_moments = json.dumps(result.get("key_moments", []))
        detailed_rec = result.get("detailed_recommendation", "")

        db.update_hiring_interview(
            interview["id"],
            status="completed",
            completed_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            current_question=current_q,
            total_score=total_score,
            score_breakdown=score_breakdown,
            ai_evaluation=final_eval,
        )

        # Build rich summary: detailed_recommendation + final_evaluation
        full_summary = final_eval
        if detailed_rec:
            full_summary = f"{detailed_rec}\n\n{final_eval}" if final_eval else detailed_rec

        # Update candidate with final scores + detailed reasoning
        db.update_hiring_candidate(
            interview["candidate_id"],
            ai_score=total_score,
            score_breakdown=score_breakdown,
            ai_summary=full_summary,
            ai_recommendation=recommendation,
            signal_reasoning=signal_reasoning,
            key_moments=key_moments,
            status="interviewed",
            screening_completed_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        )

        # Store suggested interview questions if provided
        suggested = result.get("suggested_interview_questions", [])
        if suggested:
            db.update_hiring_candidate(
                interview["candidate_id"],
                interview_questions=json.dumps(suggested),
            )

        # Send completion message
        completion_msg = "Thanks for your time! We've reviewed your responses and will be in touch soon about next steps."
        if next_q:
            completion_msg = next_q  # WARREN may have a custom closing

        db.add_hiring_message(
            interview_id=interview["id"],
            candidate_id=interview["candidate_id"],
            direction="outbound",
            channel="web_chat",
            content=completion_msg,
            is_question=0,
        )

        return jsonify({"ok": True, "message": completion_msg, "completed": True})

    # Not final - save WARREN's next question
    db.add_hiring_message(
        interview_id=interview["id"],
        candidate_id=interview["candidate_id"],
        direction="outbound",
        channel="web_chat",
        content=next_q,
        is_question=1,
        question_number=current_q,
    )
    db.update_hiring_interview(interview["id"], current_question=current_q)

    return jsonify({
        "ok": True,
        "message": next_q,
        "completed": False,
        "question_number": current_q,
        "question_type": result.get("question_type", "scenario"),
        "choices": result.get("choices", []),
    })


# ---------------------------------------------------------------------------
# Routes: Quo SMS settings (JSON endpoints for settings page)
# ---------------------------------------------------------------------------

@hiring_bp.route("/quo/test-connection", methods=["POST"])
def quo_test_connection():
    brand, user_id = _require_client_login()
    api_key = (brand.get("quo_api_key") or "").strip()
    if not api_key:
        return jsonify({"ok": False, "error": "No Quo API key configured."})
    from webapp.quo_sms import test_connection
    ok, detail = test_connection(api_key)
    return jsonify({"ok": ok, "message": detail if ok else None, "error": None if ok else detail})


@hiring_bp.route("/quo/save-settings", methods=["POST"])
def quo_save_settings():
    brand, user_id = _require_client_login()
    db = _get_db()
    api_key = (request.form.get("quo_api_key") or "").strip()
    phone = (request.form.get("quo_phone_number") or "").strip()
    updates = {}
    if api_key:
        updates["quo_api_key"] = api_key
    if phone:
        updates["quo_phone_number"] = phone
    if updates:
        conn = db._conn()
        for col, val in updates.items():
            conn.execute(f"UPDATE brands SET {col} = ? WHERE id = ?", (val, brand["id"]))
        conn.commit()
        conn.close()
    return redirect(url_for("client.client_settings"))


@hiring_bp.route("/quo/phone-numbers", methods=["GET"])
def quo_phone_numbers():
    brand, user_id = _require_client_login()
    api_key = (brand.get("quo_api_key") or "").strip()
    if not api_key:
        return jsonify({"ok": False, "numbers": [], "error": "No API key"})
    from webapp.quo_sms import get_phone_numbers
    numbers, err = get_phone_numbers(api_key)
    return jsonify({"ok": not err, "numbers": numbers, "error": err})


# ---------------------------------------------------------------------------
# Social Profile Scan
# ---------------------------------------------------------------------------

_SOCIAL_DOMAINS = {
    "facebook.com": "Facebook",
    "instagram.com": "Instagram",
    "twitter.com": "Twitter/X",
    "x.com": "Twitter/X",
    "linkedin.com": "LinkedIn",
    "tiktok.com": "TikTok",
    "reddit.com": "Reddit",
    "youtube.com": "YouTube",
    "threads.net": "Threads",
    "nextdoor.com": "Nextdoor",
}


def _classify_url(url):
    """Return (platform_name, url) or None if not a social platform."""
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        for domain, name in _SOCIAL_DOMAINS.items():
            if host.endswith(domain):
                return name, url
    except Exception:
        pass
    return None


def _duckduckgo_search(query, max_results=20):
    """Search DuckDuckGo HTML lite and return a list of {title, url, snippet} dicts."""
    results = []
    try:
        resp = _requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=10,
        )
        if resp.status_code != 200:
            return results
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        for r in soup.select(".result")[:max_results]:
            link_el = r.select_one(".result__a")
            snippet_el = r.select_one(".result__snippet")
            if not link_el:
                continue
            href = link_el.get("href", "")
            # DuckDuckGo wraps links in a redirect - extract actual URL
            if "uddg=" in href:
                from urllib.parse import parse_qs, urlparse as _up
                qs = parse_qs(_up(href).query)
                href = qs.get("uddg", [href])[0]
            results.append({
                "title": link_el.get_text(strip=True),
                "url": href,
                "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
            })
    except Exception as exc:
        log.warning("DuckDuckGo search failed: %s", exc)
    return results


def _fetch_page_text(url, max_chars=5000):
    """Fetch a public page and return its visible text, truncated."""
    try:
        resp = _requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=8,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        ct = resp.headers.get("Content-Type", "")
        if "text/html" not in ct:
            return None
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return text[:max_chars] if text else None
    except Exception:
        return None


@hiring_bp.route("/candidates/<int:candidate_id>/social-scan", methods=["POST"])
def social_scan(candidate_id):
    """Run a social media profile scan for a candidate."""
    brand, user_id = _require_client_login()
    db = _get_db()
    candidate = db.get_hiring_candidate(candidate_id)
    if not candidate or candidate["brand_id"] != brand["id"]:
        abort(404)

    api_key = _get_openai_key()
    if not api_key:
        flash("OpenAI API key required for social scanning.", "danger")
        return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))

    name = candidate["name"]
    location = brand.get("service_area", "")
    manual_urls = request.form.get("social_urls", "").strip()

    # ── Phase 1: Discover profiles via web search ──
    search_query = f'"{name}"'
    if location:
        search_query += f" {location}"
    search_query += " site:facebook.com OR site:instagram.com OR site:linkedin.com OR site:twitter.com OR site:x.com OR site:tiktok.com OR site:reddit.com"

    search_results = _duckduckgo_search(search_query)

    # Classify found URLs
    found_profiles = {}
    for r in search_results:
        match = _classify_url(r["url"])
        if match:
            platform, url = match
            if platform not in found_profiles:
                found_profiles[platform] = {
                    "url": url,
                    "title": r["title"],
                    "snippet": r["snippet"],
                }

    # Add manually provided URLs
    if manual_urls:
        for line in manual_urls.replace(",", "\n").split("\n"):
            url = line.strip()
            if not url:
                continue
            match = _classify_url(url)
            if match:
                platform, url = match
                found_profiles[platform] = {
                    "url": url,
                    "title": "(manually provided)",
                    "snippet": "",
                }
            else:
                found_profiles[f"Other ({urlparse(url).netloc})"] = {
                    "url": url,
                    "title": "(manually provided)",
                    "snippet": "",
                }

    # ── Phase 2: Fetch public page content where possible ──
    profile_content = {}
    for platform, info in found_profiles.items():
        text = _fetch_page_text(info["url"])
        if text and len(text) > 50:
            profile_content[platform] = text[:3000]

    # ── Phase 3: AI analysis ──
    profile_summary = ""
    for platform, info in found_profiles.items():
        profile_summary += f"\n--- {platform} ---\nURL: {info['url']}\nSearch result title: {info['title']}\nSearch snippet: {info['snippet']}\n"
        if platform in profile_content:
            profile_summary += f"Page content excerpt: {profile_content[platform][:2000]}\n"

    if not found_profiles:
        scan_result = {
            "status": "no_profiles_found",
            "profiles": [],
            "analysis": "No public social media profiles were found for this candidate.",
            "red_flags": [],
            "scanned_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
    else:
        system_prompt = """You are a professional HR screening assistant. You are reviewing publicly available social media profiles found for a job candidate. Your job is to identify potential red flags that might indicate the candidate is not a good fit for a professional workplace.

Red flags to look for:
- Discriminatory, hateful, or violent language
- Illegal activity or drug references (beyond legal substances)
- Extreme unprofessional behavior
- Dishonesty indicators (claims that contradict their application)
- Repeated complaints about employers, coworkers, or customers
- Sharing confidential business information from past jobs

Things that are NOT red flags (do not flag these):
- Normal personal life, hobbies, relationships
- Political opinions (unless extreme/hateful)
- Religious beliefs
- Humor, memes, or casual language
- Gaps in posting or minimal social presence

Be fair and objective. Do not discriminate based on protected characteristics. Only flag genuinely concerning content.

Return JSON:
{
  "profiles": [{"platform": "...", "url": "...", "likely_match": true/false, "confidence": "high/medium/low"}],
  "red_flags": ["concise description of each flag"],
  "positive_signals": ["anything that reflects well on the candidate"],
  "overall_assessment": "clean / minor_concerns / significant_concerns",
  "summary": "2-3 sentence overview"
}"""

        user_msg = f"Candidate name: {name}\nJob applied for: {(db.get_hiring_job(candidate['job_id']) or {}).get('title', 'Unknown')}\nCompany: {brand.get('display_name', brand.get('name', 'Unknown'))}\n\nProfiles found:\n{profile_summary}"

        ai_result = _ai_call(api_key, system_prompt, user_msg, temperature=0.3, timeout=30)

        if ai_result:
            scan_result = {
                "status": "completed",
                "profiles": ai_result.get("profiles", []),
                "red_flags": ai_result.get("red_flags", []),
                "positive_signals": ai_result.get("positive_signals", []),
                "overall_assessment": ai_result.get("overall_assessment", "unknown"),
                "summary": ai_result.get("summary", ""),
                "scanned_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        else:
            scan_result = {
                "status": "error",
                "profiles": [{"platform": p, "url": info["url"]} for p, info in found_profiles.items()],
                "red_flags": [],
                "analysis": "AI analysis failed. Profiles were found but could not be analyzed.",
                "scanned_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }

    db.update_hiring_candidate(candidate_id, social_scan=json.dumps(scan_result))

    n_profiles = len(scan_result.get("profiles", []))
    n_flags = len(scan_result.get("red_flags", []))
    if n_flags:
        flash(f"Scan complete: {n_profiles} profile(s) found, {n_flags} red flag(s) detected.", "warning")
    elif n_profiles:
        flash(f"Scan complete: {n_profiles} profile(s) found, no red flags.", "success")
    else:
        flash("Scan complete: no public profiles found.", "info")

    return redirect(url_for("hiring.candidate_detail", candidate_id=candidate_id))


# ---------------------------------------------------------------------------
# Background job: expire stale interviews
# ---------------------------------------------------------------------------

def expire_stale_interviews(db):
    """Called from jobs.py scheduler - expire pending (48h) and stalled (72h) interviews."""
    expired = db.get_expired_hiring_interviews(pending_hours=48, active_hours=72)
    for iv in expired:
        db.update_hiring_interview(iv["id"],
                                   status="expired",
                                   expired_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        # Update candidate status back
        candidate = db.get_hiring_candidate(iv["candidate_id"])
        if candidate and candidate["status"] == "screening":
            db.update_hiring_candidate(iv["candidate_id"], status="applied")
    if expired:
        log.info("Expired %d stale hiring interviews", len(expired))
    return len(expired)
