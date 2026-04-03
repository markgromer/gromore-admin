"""Hiring Hub - AI-powered hiring funnel.

Blueprint for job creation, WARREN-generated postings, public application,
AI text-interview engine (5-signal behavioral scoring), candidate management,
and one-push scheduling/offer/rejection actions.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta

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

INTERVIEW_SYSTEM_PROMPT = """You are WARREN, a behavioral screening AI for hiring.
You are conducting a text-based interview for the following position.

## YOUR MISSION
Evaluate this candidate on exactly 5 signals. Each scores 0-20 (total 0-100).
You are NOT measuring personality. You are measuring:
1. RELIABILITY - Will they show up, on time, repeatedly, without supervision?
2. OWNERSHIP - Do they take responsibility when something goes wrong?
3. WORK ETHIC - Can they perform repetitive work consistently without drop-off?
4. COMMUNICATION CLARITY - Can they communicate simply and effectively?
5. RESPONSIVENESS - (Measured by system, not your questions. Factor in the response_time data.)

## YOUR TECHNIQUES (use these, rotate between them)
- Scenario Projection: Simulate reality instead of asking about traits. "You arrive at a job and..."
- Constraint Framing: Introduce friction (late, heat, complaints). Stress reveals true decision-making.
- Binary Pressure: Forced A vs B choices. Eliminates vague answers.
- Commitment Trap: State real job conditions after they're invested. Filters weak candidates.
- Behavioral Consistency: Compare early answers to later answers. Inconsistency = risk.

## RED FLAG DETECTION (heavily penalize these)
- Blame-first language ("it wasn't my fault", "the customer was wrong")
- Avoidance of responsibility
- Overly polished but empty answers (sounds good, says nothing)
- Ignoring parts of questions
- Mentions discomfort before solutions
- Defensive tone

## HIGH-SIGNAL INDICATORS (reward these)
- Immediate acknowledgment of responsibility
- Mentions communication proactively ("I'd let them know")
- Clear, simple, direct answers
- Accepts conditions without complaint, focuses on completing the job
- Short, structured responses

## WHAT YOU RECEIVE
- Job description and requirements
- Owner's screening criteria (must-haves, dealbreakers, culture notes)
- The full conversation so far (all messages + response times per message)
- Current question number

## WHAT YOU MUST RETURN (strict JSON)
{
    "signal_scores": {
        "reliability": 0-20,
        "ownership": 0-20,
        "work_ethic": 0-20,
        "communication_clarity": 0-20,
        "responsiveness": 0-20
    },
    "red_flags": ["list of red flags spotted in this answer, empty if none"],
    "next_question": "Your next question text (adapt based on what you've learned)",
    "question_technique": "scenario_projection|constraint_framing|binary_pressure|commitment_trap|consistency_check",
    "question_targets": ["which signals this question probes"],
    "is_final": false,
    "running_score": 0-100,
    "evaluation_notes": "Brief internal note about what you've observed so far"
}

## FLOW RULES
- Ask 8-12 questions total. You decide when you have enough signal.
- Start with an easy warm-up (scenario projection), escalate pressure gradually.
- If you spot a red flag, probe it with a follow-up before moving on.
- Adapt your questions to what you've learned. Don't repeat signal areas you've already scored confidently.
- For the FINAL question (is_final: true), also include:
  "final_evaluation": "2-3 sentence summary of this candidate",
  "recommendation": "interview|waitlist|reject",
  "suggested_interview_questions": ["3-5 questions for the in-person interview based on gaps you noticed"]
- Do NOT reveal scores or that you're scoring them.
- Keep questions conversational, not robotic. One question at a time. 2-3 sentences max per question.
- Do NOT use em dashes in your questions."""


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

    context = {
        "job_title": job.get("title", ""),
        "job_description": job.get("description", ""),
        "job_requirements": job.get("requirements", "[]"),
        "screening_criteria": job.get("screening_criteria", "{}"),
        "candidate_name": candidate.get("name", ""),
        "cover_letter": candidate.get("cover_letter", ""),
        "conversation": conversation,
        "current_question_number": interview.get("current_question", 0),
        "avg_response_time_sec": candidate.get("response_time_avg_sec", 0),
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
    return result


def generate_first_question(job, candidate, brand):
    """Generate the opening welcome + first question for an interview."""
    api_key = _get_api_key(brand)
    if not api_key:
        return None

    context = {
        "job_title": job.get("title", ""),
        "job_description": job.get("description", ""),
        "screening_criteria": job.get("screening_criteria", "{}"),
        "candidate_name": candidate.get("name", ""),
    }

    system = """You are WARREN, starting a text-based screening interview.
Generate a friendly, brief welcome message and your first screening question.
Keep the welcome to 1-2 sentences. Then ask one scenario-based question.
Do NOT use em dashes.

Return JSON:
{
    "welcome_message": "Hi [name]! Thanks for applying for [role]. I have a few quick questions to get to know you better.",
    "first_question": "Your first scenario question here",
    "question_technique": "scenario_projection",
    "question_targets": ["reliability", "ownership"]
}"""

    model = brand.get("openai_model_chat") or brand.get("openai_model") or "gpt-4o-mini"
    return _ai_call(api_key, system, json.dumps(context), model=model, temperature=0.6)


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


@hiring_bp.route("/jobs/create", methods=["GET", "POST"])
def create_job():
    brand, user_id = _require_client_login()
    db = _get_db()

    if request.method == "POST":
        data = request.form
        screening = json.dumps({
            "must_haves": data.get("must_haves", ""),
            "dealbreakers": data.get("dealbreakers", ""),
            "culture_notes": data.get("culture_notes", ""),
            "physical_requirements": data.get("physical_requirements", ""),
        })
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
        fields["screening_criteria"] = json.dumps({
            "must_haves": data.get("must_haves", ""),
            "dealbreakers": data.get("dealbreakers", ""),
            "culture_notes": data.get("culture_notes", ""),
            "physical_requirements": data.get("physical_requirements", ""),
        })

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

    return jsonify({"ok": True, "result": result})


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
# Routes: Interview Chat Page (public, token-authenticated)
# ---------------------------------------------------------------------------

@hiring_bp.route("/interview/<token>")
def interview_page(token):
    """Public interview chat page - token is the auth."""
    db = _get_db()
    interview = db.get_hiring_interview_by_token(token)
    if not interview:
        abort(404)

    brand = db.get_brand(interview["brand_id"])
    company = (brand or {}).get("display_name", "")
    common = dict(interview=interview, token=token, brand=brand, company=company)

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

    return render_template("client/client_hiring_interview.html",
                           messages=messages,
                           expired=False, completed=False, **common)


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

        db.update_hiring_interview(
            interview["id"],
            status="completed",
            completed_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            current_question=current_q,
            total_score=total_score,
            score_breakdown=score_breakdown,
            ai_evaluation=final_eval,
        )

        # Update candidate with final scores
        db.update_hiring_candidate(
            interview["candidate_id"],
            ai_score=total_score,
            score_breakdown=score_breakdown,
            ai_summary=final_eval,
            ai_recommendation=recommendation,
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
