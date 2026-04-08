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
import requests
from datetime import datetime

log = logging.getLogger(__name__)

DEFAULT_MODEL = "gpt-4o-mini"


def _get_openai_key(db):
    """Get the OpenAI API key from app settings."""
    return (db.get_setting("openai_api_key", "") or "").strip()


def _build_system_prompt(brand):
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
    "internal_notes": "Brief note about what you observed and why you chose this action"
}

RULES:
- Keep SMS replies under 300 characters. Be concise.
- For Messenger, you can be slightly longer but still keep it tight.
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
        if content:
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

    system_prompt = _build_system_prompt(brand)

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
    source = thread.get("source", "")
    if lead_name or lead_phone or source:
        system_prompt += "\n\nLEAD INFO:"
        if lead_name:
            system_prompt += f"\n- Name: {lead_name}"
        if lead_phone:
            system_prompt += f"\n- Phone: {lead_phone}"
        if source:
            system_prompt += f"\n- Source: {source}"

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

        # Normalize the response
        return {
            "reply": (result.get("reply") or "").strip(),
            "action": (result.get("action") or "reply").strip().lower(),
            "confidence": float(result.get("confidence") or 0.7),
            "quote_low": result.get("quote_low"),
            "quote_high": result.get("quote_high"),
            "stage_suggestion": result.get("stage_suggestion"),
            "handoff_reason": result.get("handoff_reason"),
            "internal_notes": (result.get("internal_notes") or "").strip(),
        }

    except Exception as exc:
        log.exception("Warren brain error: %s", exc)
        return None


def process_and_respond(db, brand_id, thread_id, channel="sms"):
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

    messages = db.get_lead_messages(thread_id)
    response = generate_response(db, brand, thread, messages, channel=channel)
    if not response:
        return None

    action = response.get("action", "reply")
    reply_text = response.get("reply", "")

    # Determine if we should auto-send or hold for review
    confidence = response.get("confidence", 0)
    should_send = confidence >= 0.7 and action != "handoff"

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
            },
        )

    # Handle quoting
    if action == "quote" and (response.get("quote_low") or response.get("quote_high")):
        db.upsert_lead_quote(
            brand_id, thread_id,
            status="sent" if should_send else "draft",
            quote_mode=brand.get("sales_bot_quote_mode", "hybrid"),
            amount_low=response.get("quote_low") or 0,
            amount_high=response.get("quote_high") or 0,
            summary=response.get("internal_notes", ""),
            follow_up_text=reply_text,
            sent_at=datetime.utcnow().isoformat() if should_send else "",
        )
        advance_stage(db, thread_id, brand_id, "quote_sent")

    # Handle handoff
    if action == "handoff":
        reason = response.get("handoff_reason", "Handoff triggered")
        db.add_lead_event(brand_id, thread_id, "handoff_triggered", event_value=reason)
        db.update_lead_thread_status(thread_id, assigned_to="human")

    # Auto-advance pipeline based on action
    if action in ("reply", "qualify", "nurture") and thread.get("status") == "new":
        advance_stage(db, thread_id, brand_id, "warren_replied")

    # Stage suggestion from AI
    stage_suggestion = response.get("stage_suggestion")
    if stage_suggestion:
        advance_stage(db, thread_id, brand_id, f"lead_{stage_suggestion}" if stage_suggestion != "engaged" else "warren_replied")

    return {
        "reply": reply_text,
        "action": action,
        "thread_id": thread_id,
        "should_send": should_send,
        "handoff_reason": response.get("handoff_reason"),
        "confidence": confidence,
    }
