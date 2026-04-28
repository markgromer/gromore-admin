"""
Warren Brain - AI response generation for lead conversations.

Builds a system prompt from the brand's sales_bot_* settings and
generates contextual responses using OpenAI. Decides whether to:
  - Reply with a conversational message
  - Send a quote/price range
  - Ask a qualifying question
  - Trigger a human handoff
  - Nurture with a follow-up
"""
import json
import logging
import re
import requests
from datetime import datetime

from flask import current_app
from webapp.warren_contact_policy import lookup_contact_policy

log = logging.getLogger(__name__)

DEFAULT_MODEL = "gpt-4o-mini"


def _safe_json_object(raw_value):
    if isinstance(raw_value, dict):
        return dict(raw_value)
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_phone_list(raw_value):
    cleaned = []
    seen = set()
    for chunk in re.split(r"[,;\n]+", str(raw_value or "")):
        phone = chunk.strip()
        if not phone:
            continue
        phone = re.sub(r"[^\d+]", "", phone)
        if not phone:
            continue
        if not phone.startswith("+") and phone.isdigit() and len(phone) == 10:
            phone = "+1" + phone
        if phone in seen:
            continue
        seen.add(phone)
        cleaned.append(phone)
    return cleaned


def _split_lead_name(name):
    parts = str(name or "").strip().split()
    if not parts:
        return "", ""
    return parts[0], " ".join(parts[1:])


def _has_successful_crm_push(db, brand_id, thread_id):
    for event in db.get_lead_events(brand_id, thread_id, event_type="crm_push", limit=20):
        metadata = _safe_json_object(event.get("metadata_json"))
        status = str(metadata.get("status") or event.get("event_value") or "").strip().lower()
        if status in {"success", "auto", "sent", "pushed"} or metadata.get("success") is True:
            return True
    return False


def _brand_wants_crm_push(brand, response):
    explicit_action = (response.get("closing_action") or "").strip().lower()
    if explicit_action in {"push_crm", "both"}:
        return explicit_action

    configured_action = (brand.get("sales_bot_closing_action") or "none").strip().lower()
    stage_suggestion = (response.get("stage_suggestion") or "").strip().lower()
    action = (response.get("action") or "").strip().lower()
    if configured_action in {"push_crm", "both"} and (stage_suggestion == "booked" or action == "qualify"):
        return configured_action

    if brand.get("sales_bot_auto_push_crm") and stage_suggestion == "booked":
        return "push_crm"
    return ""


def _build_crm_lead_payload(thread, response, channel):
    first_name, last_name = _split_lead_name(thread.get("lead_name", ""))
    info = response.get("info_collected") or {}
    profile = _safe_json_object(thread.get("commercial_data_json"))
    quote = response.get("_quote") or {}
    notes = []
    if response.get("internal_notes"):
        notes.append(response["internal_notes"])
    if thread.get("summary"):
        notes.append(f"Thread summary: {thread['summary']}")
    if quote:
        amount_low = quote.get("amount_low") or 0
        amount_high = quote.get("amount_high") or 0
        if amount_low or amount_high:
            notes.append(f"Warren quote range: ${amount_low:g}-${amount_high:g}")
    service_needed = info.get("service_needed") or profile.get("service_needed") or ""
    if service_needed:
        notes.append(f"Service needed: {service_needed}")

    return {
        "name": thread.get("lead_name", ""),
        "first_name": first_name,
        "last_name": last_name,
        "phone": thread.get("lead_phone", ""),
        "email": thread.get("lead_email", ""),
        "address": info.get("address") or profile.get("service_address") or profile.get("address") or "",
        "source": f"warren_{channel}",
        "notes": "\n".join(part for part in notes if part).strip(),
    }


def _maybe_push_thread_to_crm(db, brand, brand_id, thread_id, response, channel, contact_policy):
    closing_action = _brand_wants_crm_push(brand, response)
    if not closing_action or contact_policy.get("suppress_marketing"):
        return None
    if _has_successful_crm_push(db, brand_id, thread_id):
        return {"success": True, "detail": "already_pushed", "skipped": True, "closing_action": closing_action}

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return {"success": False, "detail": "thread_not_found", "closing_action": closing_action}

    lead_data = _build_crm_lead_payload(thread, response, channel)
    if not (lead_data.get("phone") or lead_data.get("email")):
        detail = "missing_phone_or_email"
        db.add_lead_event(
            brand_id,
            thread_id,
            "crm_push_failed",
            event_value=detail,
            metadata={"closing_action": closing_action, "crm_type": brand.get("crm_type") or ""},
        )
        return {"success": False, "detail": detail, "closing_action": closing_action}

    try:
        from webapp.crm_bridge import push_lead

        success, detail = push_lead(brand, lead_data)
    except Exception as exc:
        success, detail = False, str(exc)

    event_type = "crm_push" if success else "crm_push_failed"
    db.add_lead_event(
        brand_id,
        thread_id,
        event_type,
        event_value="success" if success else str(detail)[:200],
        metadata={
            "success": bool(success),
            "detail": str(detail)[:500],
            "closing_action": closing_action,
            "crm_type": brand.get("crm_type") or "",
            "source": f"warren_{channel}",
        },
    )
    if success:
        log.info("Warren auto-pushed lead %s to CRM: %s", thread_id, detail)
    else:
        log.warning("Warren CRM push returned failure for thread %s: %s", thread_id, detail)
    return {"success": bool(success), "detail": str(detail), "closing_action": closing_action}


def _build_owner_handoff_alert(thread, reason, channel):
    lead_name = (thread.get("lead_name") or "Unknown lead").strip()
    lead_phone = (thread.get("lead_phone") or "").strip()
    lead_email = (thread.get("lead_email") or "").strip()
    lead_summary = (thread.get("summary") or "").strip()
    last_inbound = ""
    for message in reversed(thread.get("_messages") or []):
        if message.get("direction") == "inbound":
            last_inbound = (message.get("content") or "").strip()
            break

    subject = f"W.A.R.R.E.N. handoff needed - {lead_name}"

    lines = [
        "W.A.R.R.E.N. needs you to interrupt a live lead conversation.",
        "",
        f"Lead: {lead_name}",
        f"Channel: {(channel or thread.get('channel') or 'sms').upper()}",
    ]
    if lead_phone:
        lines.append(f"Phone: {lead_phone}")
    if lead_email:
        lines.append(f"Email: {lead_email}")
    lines.append(f"Reason: {reason}")
    if lead_summary:
        lines.append(f"Thread summary: {lead_summary}")
    if last_inbound:
        lines.extend(["", "Latest lead message:", last_inbound[:500]])
    lines.extend(["", "Please open the inbox and take over this thread now."])
    email_text = "\n".join(lines)

    sms_bits = [
        f"WARREN handoff needed for {lead_name}.",
        f"Reason: {reason}.",
    ]
    if lead_phone:
        sms_bits.append(f"Lead phone: {lead_phone}.")
    sms_bits.append("Open the inbox and interrupt the conversation now.")
    sms_text = " ".join(bit.strip() for bit in sms_bits if bit.strip())
    return subject, email_text, sms_text[:320]


def _notify_owner_handoff(db, brand, thread, reason, channel):
    from webapp.email_sender import send_simple_email
    from webapp.warren_crm_events import get_internal_alert_recipients, load_crm_event_rules
    from webapp.warren_sender import send_transactional_sms

    subject, email_text, sms_text = _build_owner_handoff_alert(thread, reason, channel)
    email_recipients = get_internal_alert_recipients(db, brand, load_crm_event_rules(brand))
    sms_recipients = _parse_phone_list(brand.get("sales_bot_handoff_alert_phones"))

    sent_email_count = 0
    sent_sms_count = 0
    failed_targets = []

    app_config = {}
    try:
        app_config = current_app.config
    except RuntimeError:
        app_config = {}

    for email in email_recipients:
        try:
            send_simple_email(app_config, email, subject, email_text)
            sent_email_count += 1
        except Exception as exc:
            log.warning("Owner handoff email failed for %s: %s", email, exc)
            failed_targets.append(f"email:{email}")

    for phone in sms_recipients:
        ok, detail = send_transactional_sms(db, brand, phone, sms_text, append_opt_out_footer=False)
        if ok:
            sent_sms_count += 1
        else:
            log.warning("Owner handoff SMS failed for %s: %s", phone, detail)
            failed_targets.append(f"sms:{phone}")

    detail_bits = []
    if sent_email_count:
        detail_bits.append(f"email x{sent_email_count}")
    if sent_sms_count:
        detail_bits.append(f"sms x{sent_sms_count}")
    if failed_targets:
        detail_bits.append("failed: " + ", ".join(failed_targets[:4]))
    if not detail_bits:
        detail_bits.append("no recipients configured")

    db.add_lead_event(
        brand.get("id"),
        thread.get("id"),
        "owner_handoff_alert",
        event_value=" | ".join(detail_bits),
        metadata={
            "reason": reason,
            "email_count": sent_email_count,
            "sms_count": sent_sms_count,
            "failed_targets": failed_targets,
        },
    )

    return {
        "email_count": sent_email_count,
        "sms_count": sent_sms_count,
        "failed_targets": failed_targets,
    }


def _get_openai_key(db):
    """Get the OpenAI API key from app settings."""
    return (db.get_setting("openai_api_key", "") or "").strip()


def _build_active_client_reply(thread, contact_policy, channel):
    client_name = (contact_policy.get("client_name") or thread.get("lead_name") or "").strip()
    greeting = f"Thanks, {client_name}." if client_name else "Thanks for the reply."
    if channel == "messenger":
        return (
            f"{greeting} I can see you're already an active client, so I am not going to send sales info or a quote here. "
            "I flagged this for the team and they will handle the conversation directly."
        )
    return (
        f"{greeting} You're already an active client, so I am not going to send sales info or a quote here. "
        "I flagged this for the team and they will handle it directly."
    )


def _build_system_prompt(brand, contact_policy=None):
    """Build Warren's lead-handling system prompt from brand settings."""
    name = brand.get("display_name", "our company")
    industry = brand.get("industry", "home services")
    service_area = brand.get("service_area", "")
    primary_services = brand.get("primary_services", "")

    tone = brand.get("sales_bot_reply_tone", "") or "friendly, professional, and direct"
    service_menu = brand.get("sales_bot_service_menu", "") or ""
    pricing_notes = brand.get("sales_bot_pricing_notes", "") or ""
    guardrails = brand.get("sales_bot_guardrails", "") or ""
    example_language = brand.get("sales_bot_example_language", "") or ""
    disallowed = brand.get("sales_bot_disallowed_language", "") or ""
    handoff_rules = brand.get("sales_bot_handoff_rules", "") or ""
    objection_playbook = brand.get("sales_bot_objection_playbook", "") or ""
    message_templates = brand.get("sales_bot_message_templates", "") or ""
    quote_mode = brand.get("sales_bot_quote_mode", "hybrid") or "hybrid"
    avg_price = brand.get("crm_avg_service_price", 0) or 0
    business_hours = brand.get("sales_bot_business_hours", "") or ""

    prompt = f"""You are Warren, the lead assistant for {name}, a {industry} company.
Your job is to handle inbound leads via text/chat: greet them, qualify them, provide pricing,
and move them toward booking a service. You are not a generic chatbot. You represent this specific business.

TONE: {tone}

ABOUT THE BUSINESS:
- Company: {name}
- Industry: {industry}"""

    if service_area:
        prompt += f"\n- Service area: {service_area}"
    if primary_services:
        prompt += f"\n- Core services: {primary_services}"
    if avg_price:
        prompt += f"\n- Average service price: ${avg_price:.0f}"
    if business_hours:
        prompt += f"\n- Business hours: {business_hours}"

    if service_menu:
        prompt += f"\n\nSERVICE MENU AND PRICING:\n{service_menu}"

    if pricing_notes:
        prompt += f"\n\nPRICING STRATEGY:\n{pricing_notes}"

    prompt += f"""

QUOTING MODE: {quote_mode}
- "simple": Give a single ballpark number when asked
- "hybrid": Give a price range and explain what affects the final number
- "structured": Break down each line item with individual pricing"""

    if guardrails:
        prompt += f"\n\nGUARDRAILS (hard rules you must follow):\n{guardrails}"

    if handoff_rules:
        prompt += f"\n\nHUMAN HANDOFF (stop and escalate immediately when):\n{handoff_rules}"

    if example_language:
        prompt += f"\n\nEXAMPLE LANGUAGE (match this style):\n{example_language}"

    if disallowed:
        prompt += f"\n\nNEVER SAY (language to avoid):\n{disallowed}"

    if objection_playbook:
        prompt += f"\n\nOBJECTION HANDLING (use these responses when a lead pushes back):\n{objection_playbook}"

    if message_templates:
        prompt += f"\n\nMESSAGE TEMPLATES (use as starting points, personalize based on context):\n{message_templates}"

    # Closing procedure
    closing_procedure = brand.get("sales_bot_closing_procedure", "") or ""
    closing_action = brand.get("sales_bot_closing_action", "") or "none"
    onboarding_link = brand.get("sales_bot_onboarding_link", "") or ""
    if closing_procedure:
        prompt += f"\n\nCLOSING PROCEDURE (follow these steps when the lead is ready to book):\n{closing_procedure}"
    if closing_action == "send_onboarding" and onboarding_link:
        prompt += f"\n\nAfter confirming the booking, send them this onboarding link: {onboarding_link}"
    elif closing_action == "both" and onboarding_link:
        prompt += f"\n\nAfter confirming the booking, send them this onboarding link: {onboarding_link}"
        prompt += "\n(The system will also push the lead to the connected CRM automatically.)"
    elif closing_action == "push_crm":
        prompt += "\n\nAfter confirming the booking, the system will push the lead to the connected CRM automatically. Just confirm with the lead that they're booked."

    # Booking success message
    booking_success = brand.get("sales_bot_booking_success_message", "") or ""
    if booking_success:
        prompt += f"\n\nBOOKING CONFIRMATION MESSAGE (use this exact message when a lead confirms they want to book, personalize the placeholders):\n{booking_success}"

    # Service area schedule
    schedule_raw = brand.get("sales_bot_service_area_schedule", "") or ""
    if schedule_raw:
        try:
            schedule = json.loads(schedule_raw) if isinstance(schedule_raw, str) else schedule_raw
            if schedule and isinstance(schedule, dict):
                lines = []
                day_order = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                for day in day_order:
                    areas = schedule.get(day, "")
                    if areas:
                        lines.append(f"  {day.capitalize()}: {areas}")
                if lines:
                    prompt += "\n\nSERVICE AREA SCHEDULE (which areas we service on which days):\n" + "\n".join(lines)
                    prompt += "\n- If a lead mentions their location and it matches a scheduled day, tell them which day(s) you service their area."
                    prompt += "\n- Don't bring up the schedule unprompted. Only mention it when relevant to the conversation."
        except (json.JSONDecodeError, TypeError):
            pass

    # Info collection strategy
    collect_fields = brand.get("sales_bot_collect_fields", "") or "name,phone"
    if collect_fields:
        field_labels = {"name": "Name", "phone": "Phone number", "email": "Email", "address": "Service address", "service_needed": "What service they need"}
        fields_list = [f.strip() for f in collect_fields.split(",") if f.strip()]
        readable = ", ".join(field_labels.get(f, f) for f in fields_list)
        prompt += f"\n\nINFORMATION TO COLLECT: {readable}"
        prompt += "\n- Only ask for ONE piece of missing info per message. Be casual about it."
        prompt += "\n- If on Messenger and you need their phone, say something like 'Want me to text you the details? What's the best number?'"
        prompt += "\n- If you don't have their name yet, work it in: 'By the way, who am I chatting with?'"
        prompt += "\n- Never demand info. If they don't share it, move on."

    if contact_policy and contact_policy.get("is_active_client"):
        subscription_names = (contact_policy.get("subscription_names") or "").strip()
        prompt += "\n\nCONTACT POLICY:"
        prompt += "\n- This person is already an active client in the CRM."
        if contact_policy.get("client_name"):
            prompt += f"\n- Active client name: {contact_policy['client_name']}"
        if subscription_names:
            prompt += f"\n- Current services/subscriptions: {subscription_names}"
        prompt += "\n- Do not pitch, qualify, nurture, or quote this person."
        prompt += "\n- Treat this as an existing-customer support or coordination conversation."
        prompt += "\n- If you are unsure, keep the reply short, helpful, and route to the team instead of selling."
    elif contact_policy and contact_policy.get("suppress_marketing"):
        reason = (contact_policy.get("reason") or "marketing_restricted").strip()
        prompt += "\n\nCONTACT POLICY:"
        prompt += f"\n- This contact is marketing-restricted ({reason})."
        prompt += "\n- Do not send promotional nudges, sales pressure, or proactive quote-pushing."
        prompt += "\n- Only answer the specific inbound question in a restrained, non-promotional way."

    prompt += """

RESPONSE FORMAT:
You must respond with valid JSON containing these fields:
{
    "reply": "Your message to the lead (plain text, conversational, under 300 chars for SMS)",
    "action": "reply|quote|qualify|handoff|nurture",
    "confidence": 0.0-1.0,
    "quote_low": null or number,
    "quote_high": null or number,
    "stage_suggestion": null or "engaged|quoted|qualified|booked",
    "handoff_reason": null or "reason string",
    "closing_action": null or "push_crm|send_onboarding|both",
    "internal_notes": "Brief note about what you observed and why you chose this action",
    "info_collected": {"name": null, "phone": null, "email": null, "address": null, "service_needed": null},
    "objection_detected": null
}

INFO_COLLECTED RULES:
- Only set a field if the lead EXPLICITLY provided that info in THIS message.
- "name" = their first name or full name. null if not given.
- "phone" = phone number they shared. null if not given.
- "email" = email they shared. null if not given.
- "address" = service address or location. null if not given.
- "service_needed" = what service they want (e.g. "drain cleaning", "AC repair"). null if not clear.

OBJECTION_DETECTED RULES:
- Set to a short label when the lead pushes back. Examples: "too expensive", "need to think about it", "got another quote", "not ready yet", "bad reviews", "timing"
- null if no objection in this message.
- Use the objection playbook to respond, but always log the objection here.

RULES:
- Keep SMS replies under 300 characters. Be concise.
- For Messenger, you can be slightly longer but still keep it tight.
- If the lead sends photos, analyze what you can actually see and use it to improve your reply. Be helpful, but do not overclaim or pretend you can inspect hidden details.
- If the lead asks about pricing, quote per the quoting mode.
- If a handoff rule matches, set action to "handoff" immediately.
- If the lead seems cold or hasn't replied, set action to "nurture".
- Never invent services or prices not in the service menu.
- Never promise guarantees or exact timelines unless the business hours confirm them.
- Never use em dashes. Use regular dashes, commas, or periods instead.
- Sound like a real person, not a corporate bot."""

    return prompt


def _build_conversation_context(messages, max_messages=20):
    """Build the conversation history for the AI context window."""
    recent = messages[-max_messages:] if len(messages) > max_messages else messages
    context = []
    for msg in recent:
        role = "assistant" if msg.get("role") == "assistant" else "user"
        content = msg.get("content", "")
        metadata = msg.get("metadata") or {}
        if not metadata:
            raw_metadata = msg.get("metadata_json", "")
            if raw_metadata:
                try:
                    metadata = json.loads(raw_metadata)
                except (TypeError, ValueError, json.JSONDecodeError):
                    metadata = {}
        image_urls = metadata.get("image_urls") or []
        if content or image_urls:
            if role == "user" and image_urls:
                parts = []
                if content:
                    parts.append({"type": "text", "text": content})
                else:
                    parts.append({"type": "text", "text": "Lead sent image(s)."})
                for image_url in image_urls[:3]:
                    if image_url:
                        parts.append(
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": image_url,
                                    "detail": "high",
                                },
                            }
                        )
                context.append({"role": role, "content": parts})
            else:
                context.append({"role": role, "content": content})
    return context


def generate_response(db, brand, thread, messages, channel="sms"):
    """Generate Warren's response to a lead conversation.

    Args:
        db: WebDB instance
        brand: brand dict
        thread: lead_thread dict
        messages: list of lead_message dicts (chronological)
        channel: 'sms', 'messenger', 'lead_form'

    Returns:
        dict with keys: reply, action, confidence, quote_low, quote_high,
                       stage_suggestion, handoff_reason, internal_notes
        Or None if generation fails.
    """
    api_key = _get_openai_key(db)
    if not api_key:
        log.error("Warren brain: no OpenAI API key configured")
        return None

    system_prompt = _build_system_prompt(brand, thread.get("_contact_policy") or {})

    # Add channel-specific context
    if channel == "sms":
        system_prompt += "\n\nCHANNEL: SMS. Keep replies under 300 characters. No markdown."
    elif channel == "messenger":
        system_prompt += "\n\nCHANNEL: Facebook Messenger. Can be slightly longer. No markdown."
    elif channel == "lead_form":
        system_prompt += "\n\nCHANNEL: Meta Lead Form. This is a first-touch reply to a form submission. Greet warmly, confirm their interest, and move toward qualifying."

    # Add lead context
    lead_name = thread.get("lead_name", "")
    lead_phone = thread.get("lead_phone", "")
    lead_email = thread.get("lead_email", "")
    source = thread.get("source", "")
    if lead_name or lead_phone or lead_email or source:
        system_prompt += "\n\nLEAD INFO (what we already know):"
        if lead_name:
            system_prompt += f"\n- Name: {lead_name}"
        else:
            system_prompt += "\n- Name: UNKNOWN - try to get it naturally"
        if lead_phone:
            system_prompt += f"\n- Phone: {lead_phone}"
        elif channel == "messenger":
            system_prompt += "\n- Phone: UNKNOWN - we're on Messenger, try to get their number so we can text them details"
        if lead_email:
            system_prompt += f"\n- Email: {lead_email}"
        if source:
            system_prompt += f"\n- Source: {source}"
    else:
        system_prompt += "\n\nLEAD INFO: Nothing known yet. Try to get their name naturally."

    # Add past objection context if any
    past_objections = thread.get("_objections", [])
    if past_objections:
        system_prompt += "\n\nPAST OBJECTIONS FROM THIS LEAD:"
        for obj in past_objections[-5:]:
            system_prompt += f"\n- {obj}"
        system_prompt += "\nAddress these concerns naturally. Don't ignore them."

    # Build conversation history
    conversation = _build_conversation_context(messages)

    # If no conversation yet (e.g., lead form), add a synthetic first message
    if not conversation:
        summary = thread.get("summary", "")
        if summary:
            conversation = [{"role": "user", "content": summary}]
        else:
            conversation = [{"role": "user", "content": "Hi, I'm interested in your services."}]

    all_messages = [{"role": "system", "content": system_prompt}] + conversation

    model = (db.get_setting("openai_model_chat", "") or DEFAULT_MODEL).strip() or DEFAULT_MODEL

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "temperature": 0.4,
                "messages": all_messages,
                "response_format": {"type": "json_object"},
            },
            timeout=30,
        )

        if resp.status_code != 200:
            log.error("Warren brain OpenAI error (%s): %s", resp.status_code, resp.text[:300])
            return None

        data = resp.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")

        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            log.error("Warren brain: invalid JSON response: %s", content[:500])
            return None

        # Normalize info_collected - only keep non-null values
        raw_info = result.get("info_collected") or {}
        info_collected = {}
        for k in ("name", "phone", "email", "address", "service_needed"):
            v = raw_info.get(k)
            if v and isinstance(v, str) and v.strip():
                info_collected[k] = v.strip()

        # Normalize the response
        return {
            "reply": (result.get("reply") or "").strip(),
            "action": (result.get("action") or "reply").strip().lower(),
            "confidence": float(result.get("confidence") or 0.7),
            "quote_low": result.get("quote_low"),
            "quote_high": result.get("quote_high"),
            "stage_suggestion": result.get("stage_suggestion"),
            "handoff_reason": result.get("handoff_reason"),
            "closing_action": result.get("closing_action"),
            "internal_notes": (result.get("internal_notes") or "").strip(),
            "info_collected": info_collected,
            "objection_detected": (result.get("objection_detected") or "").strip() or None,
        }

    except Exception as exc:
        log.exception("Warren brain error: %s", exc)
        return None


def process_and_respond(db, brand_id, thread_id, channel="sms", allow_auto_send=True):
    """Full pipeline: generate response, log it, advance pipeline, return action.

    This is the main entry point called by webhook handlers.

    Returns:
        dict with: reply, action, thread_id, should_send, handoff_reason
        Or None on failure.
    """
    from webapp.warren_pipeline import advance_stage

    brand = db.get_brand(brand_id)
    if not brand:
        return None

    # Check if assistant is enabled
    if not brand.get("sales_bot_enabled"):
        log.info("Warren brain: assistant disabled for brand %s", brand_id)
        return None

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return None

    if str(thread.get("assigned_to") or "").strip().lower() == "human":
        log.info("Warren brain: thread %s already assigned to human, skipping auto-response", thread_id)
        return None

    messages = db.get_lead_messages(thread_id)
    thread["_messages"] = messages
    contact_policy = lookup_contact_policy(db, brand, thread)
    thread["_contact_policy"] = contact_policy

    # Load past objections into thread context for the brain
    objection_events = db.get_lead_events(brand_id, thread_id, event_type="objection_detected")
    thread["_objections"] = [e.get("event_value", "") for e in (objection_events or []) if e.get("event_value")]

    if contact_policy.get("is_active_client"):
        reply_text = _build_active_client_reply(thread, contact_policy, channel)
        should_send = allow_auto_send and bool(reply_text)
        db.update_lead_thread_status(thread_id, assigned_to="human")
        db.add_lead_event(
            brand_id,
            thread_id,
            "marketing_suppressed",
            event_value=contact_policy.get("reason") or "active_client",
            metadata={
                "source": "warren_brain",
                "contact_policy": contact_policy,
            },
        )
        if reply_text:
            db.add_lead_message(
                thread_id,
                direction="outbound",
                role="assistant",
                content=reply_text,
                channel=channel,
                metadata={
                    "action": "reply",
                    "confidence": 1.0,
                    "auto_sent": should_send,
                    "internal_notes": "Active CRM client detected. Routed away from sales automation.",
                    "contact_policy_reason": contact_policy.get("reason") or "active_client",
                },
            )
        return {
            "reply": reply_text,
            "action": "reply",
            "thread_id": thread_id,
            "should_send": should_send,
            "handoff_reason": "active_client",
            "closing_action": None,
            "confidence": 1.0,
        }

    response = generate_response(db, brand, thread, messages, channel=channel)
    if not response:
        return None

    action = response.get("action", "reply")
    reply_text = response.get("reply", "")

    # Save any info the AI collected from this message
    info = response.get("info_collected") or {}
    if info:
        update_data = {}
        if info.get("name") and not thread.get("lead_name"):
            update_data["lead_name"] = info["name"]
        if info.get("phone") and not thread.get("lead_phone"):
            update_data["lead_phone"] = info["phone"]
        if info.get("email") and not thread.get("lead_email"):
            update_data["lead_email"] = info["email"]
        profile = _safe_json_object(thread.get("commercial_data_json"))
        profile_changed = False
        if info.get("address") and not (profile.get("service_address") or profile.get("address")):
            profile["service_address"] = info["address"]
            profile_changed = True
        if info.get("service_needed") and not profile.get("service_needed"):
            profile["service_needed"] = info["service_needed"]
            profile_changed = True
        if profile_changed:
            db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(profile, separators=(",", ":")))
            thread["commercial_data_json"] = json.dumps(profile, separators=(",", ":"))
        if update_data:
            db.update_lead_thread_profile_fields(thread_id, brand_id, **update_data)
            thread.update(update_data)
            log.info("Warren collected info for thread %s: %s", thread_id, update_data)

    # Log any objection detected
    objection = response.get("objection_detected")
    if objection:
        db.add_lead_event(brand_id, thread_id, "objection_detected", event_value=objection)
        log.info("Warren detected objection for thread %s: %s", thread_id, objection)

    # Determine if we should auto-send or hold for review
    confidence = response.get("confidence", 0)
    should_send = allow_auto_send and confidence >= 0.7 and action != "handoff"

    if contact_policy.get("suppress_marketing") and action == "nurture":
        action = "reply"
        response["action"] = "reply"
        should_send = allow_auto_send and confidence >= 0.7
        response["internal_notes"] = (
            (response.get("internal_notes") or "").strip() + " Marketing nurture suppressed by contact policy."
        ).strip()
    if contact_policy.get("suppress_marketing"):
        response["closing_action"] = None

    # Log Warren's response as an outbound message
    if reply_text:
        db.add_lead_message(
            thread_id,
            direction="outbound",
            role="assistant",
            content=reply_text,
            channel=channel,
            metadata={
                "action": action,
                "confidence": confidence,
                "auto_sent": should_send,
                "internal_notes": response.get("internal_notes", ""),
                "contact_policy_reason": contact_policy.get("reason") or "",
            },
        )

    # Handle quoting
    if action == "quote" and not contact_policy.get("suppress_marketing") and (response.get("quote_low") or response.get("quote_high")):
        quote = db.upsert_lead_quote(
            brand_id, thread_id,
            status="sent" if should_send else "draft",
            quote_mode=brand.get("sales_bot_quote_mode", "hybrid"),
            amount_low=response.get("quote_low") or 0,
            amount_high=response.get("quote_high") or 0,
            summary=response.get("internal_notes", ""),
            follow_up_text=reply_text,
            sent_at=datetime.now().isoformat() if should_send else "",
        )
        response["_quote"] = quote
        advance_stage(db, thread_id, brand_id, "quote_sent")

    # Handle handoff
    if action == "handoff":
        reason = response.get("handoff_reason", "Handoff triggered")
        db.add_lead_event(brand_id, thread_id, "handoff_triggered", event_value=reason)
        db.update_lead_thread_status(thread_id, assigned_to="human")
        thread["assigned_to"] = "human"
        _notify_owner_handoff(db, brand, thread, reason, channel)

    # Auto-advance pipeline based on action
    if action in ("reply", "qualify", "nurture") and thread.get("status") == "new":
        advance_stage(db, thread_id, brand_id, "warren_replied")

    # Stage suggestion from AI
    stage_suggestion = response.get("stage_suggestion")
    if stage_suggestion and not contact_policy.get("suppress_marketing"):
        stage_event_map = {
            "engaged": "warren_replied",
            "quoted": "quote_sent",
            "qualified": "lead_confirmed",
            "booked": "appointment_set",
        }
        stage_event = stage_event_map.get(str(stage_suggestion or "").strip().lower())
        if stage_event:
            advance_stage(db, thread_id, brand_id, stage_event)

    # Handle closing actions (CRM push / onboarding link)
    crm_push_result = _maybe_push_thread_to_crm(db, brand, brand_id, thread_id, response, channel, contact_policy)
    closing_action = (crm_push_result or {}).get("closing_action") or response.get("closing_action")

    return {
        "reply": reply_text,
        "action": action,
        "thread_id": thread_id,
        "should_send": should_send,
        "handoff_reason": response.get("handoff_reason"),
        "closing_action": closing_action,
        "crm_push": crm_push_result,
        "confidence": confidence,
    }
