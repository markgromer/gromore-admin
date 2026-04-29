"""
Warren Webhooks - inbound message receivers for Quo (OpenPhone) and Meta.

Endpoints:
    POST /webhooks/leads/<brand_slug>             - Generic inbound lead form submissions
    POST /webhooks/sng/<brand_slug>/<secret>      - Sweep and Go inbound events
    POST /webhooks/quo/sms/<brand_slug>           - Quo/OpenPhone inbound SMS
    POST /webhooks/meta/leadgen                   - Meta Lead Form submissions
    POST /webhooks/meta/messenger                 - Meta Messenger inbound messages
    GET  /webhooks/meta/messenger                 - Meta webhook verification

All webhook endpoints are public (no login) but verified via signatures
or secrets configured per-brand.
"""
import hashlib
import hmac
import json
import logging
import re
import threading
import base64

from flask import Blueprint, request, jsonify, current_app, abort

log = logging.getLogger(__name__)

webhooks_bp = Blueprint("webhooks", __name__)


def _get_db():
    return current_app.db


def _meta_verify_token_is_valid(db, token):
    token = (token or "").strip()
    if not token:
        return False

    global_token = (db.get_setting("meta_webhook_verify_token", "") or "").strip()
    return bool(global_token and hmac.compare_digest(token, global_token))


def _handle_meta_webhook_verification():
    db = _get_db()
    mode = request.args.get("hub.mode", "")
    token = request.args.get("hub.verify_token", "")
    challenge = request.args.get("hub.challenge", "")

    if mode == "subscribe" and _meta_verify_token_is_valid(db, token):
        log.info("Meta webhook verified")
        return challenge, 200

    log.warning("Meta webhook verification failed")
    abort(403)


def _looks_like_image_url(url):
    lowered = (url or "").lower()
    return any(token in lowered for token in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic", "/photo", "/image"))


def _extract_image_urls(payload):
    """Extract image attachment URLs from a webhook payload fragment."""
    found = []

    def _walk(value):
        if isinstance(value, dict):
            url = (value.get("url") or "").strip()
            mime_type = (
                value.get("mimeType") or value.get("mime_type") or
                value.get("contentType") or value.get("content_type") or ""
            ).strip().lower()
            attachment_type = (value.get("type") or "").strip().lower()
            if url and (attachment_type == "image" or mime_type.startswith("image/") or _looks_like_image_url(url)):
                found.append(url)
            for child_key in ("attachments", "attachment", "media", "files", "payload", "data", "object"):
                child = value.get(child_key)
                if child:
                    _walk(child)
        elif isinstance(value, list):
            for item in value:
                _walk(item)

    _walk(payload)

    urls = []
    seen = set()
    for url in found:
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def _stringify_webhook_value(value):
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [_stringify_webhook_value(item) for item in value]
        return ", ".join(part for part in parts if part)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return str(value).strip()


def _payload_value(payload, *keys):
    for key in keys:
        value = _stringify_webhook_value((payload or {}).get(key))
        if value:
            return value
    return ""


def _payload_block(payload, key):
    value = (payload or {}).get(key)
    return value if isinstance(value, dict) else {}


def _merge_nested_fields(target, value):
    if isinstance(value, str):
        raw = value.strip()
        if raw.startswith("{"):
            try:
                value = json.loads(raw)
            except Exception:
                return
    if not isinstance(value, dict):
        return
    for key, item in value.items():
        text = _stringify_webhook_value(item)
        if text:
            target[str(key).strip()] = text


def _extract_generic_form_submission(payload, raw_body):
    payload = payload or {}
    lead_block = _payload_block(payload, "lead")
    service_block = _payload_block(payload, "service")
    metadata_block = _payload_block(payload, "metadata")
    original_payload = _payload_block(payload, "original_payload")

    lead_name = (
        _payload_value(payload, "name", "full_name", "lead_name") or
        _payload_value(lead_block, "name", "full_name", "lead_name") or
        _payload_value(original_payload, "lead_name", "name", "full_name")
    )
    if not lead_name:
        first_name = (
            _payload_value(payload, "first_name", "firstname") or
            _payload_value(lead_block, "first_name", "firstname") or
            _payload_value(original_payload, "first_name", "firstname")
        )
        last_name = (
            _payload_value(payload, "last_name", "lastname") or
            _payload_value(lead_block, "last_name", "lastname") or
            _payload_value(original_payload, "last_name", "lastname")
        )
        lead_name = " ".join(part for part in (first_name, last_name) if part).strip()

    lead_email = (
        _payload_value(payload, "email", "email_address") or
        _payload_value(lead_block, "email", "email_address") or
        _payload_value(original_payload, "email", "email_address")
    )
    lead_phone = (
        _payload_value(payload, "phone_e164", "phone_number", "phone", "mobile", "cell", "cellphone") or
        _payload_value(lead_block, "phone_e164", "phone_number", "phone", "mobile", "cell", "cellphone") or
        _payload_value(original_payload, "phone_e164", "phone_number", "phone", "mobile", "cell", "cellphone")
    )
    message_text = (
        _payload_value(payload, "message", "notes", "note", "details", "description", "comments", "inquiry", "text") or
        _payload_value(lead_block, "message", "notes", "note", "details", "description", "comments", "inquiry", "text") or
        _payload_value(original_payload, "message", "notes", "note", "details", "description", "comments", "inquiry", "text")
    )
    source = (
        _payload_value(payload, "source", "form_name", "form", "page", "campaign") or
        _payload_value(metadata_block, "source", "form_name", "form", "page", "campaign") or
        "incoming_webhook"
    )
    external_id = (
        _payload_value(payload, "external_id", "submission_id", "lead_id", "entry_id", "id") or
        _payload_value(lead_block, "external_id", "submission_id", "lead_id", "entry_id", "id") or
        _payload_value(metadata_block, "external_id", "submission_id", "lead_id", "entry_id", "id") or
        _payload_value(original_payload, "external_id", "submission_id", "lead_id", "entry_id", "id")
    )

    extra_fields = {}
    for nested_key in ("fields", "custom_fields", "metadata"):
        _merge_nested_fields(extra_fields, payload.get(nested_key))
    for nested_key, nested_payload in (
        ("lead", lead_block),
        ("service", service_block),
        ("metadata", metadata_block),
        ("original", original_payload),
    ):
        for key, value in nested_payload.items():
            text = _stringify_webhook_value(value)
            if text:
                extra_fields[f"{nested_key}_{str(key).strip()}"] = text

    reserved = {
        "name", "full_name", "first_name", "firstname", "last_name", "lastname",
        "lead_name", "email", "email_address", "phone_e164", "phone_number", "phone", "mobile", "cell", "cellphone",
        "message", "notes", "note", "details", "description", "comments", "inquiry", "text",
        "source", "form_name", "form", "page", "campaign",
        "external_id", "submission_id", "lead_id", "entry_id", "id",
        "fields", "custom_fields", "metadata", "lead", "service", "original_payload",
        "secret", "webhook_secret",
    }
    for key, value in payload.items():
        if key in reserved:
            continue
        text = _stringify_webhook_value(value)
        if text:
            extra_fields[str(key).strip()] = text

    if not external_id:
        event_name = _payload_value(payload, "event", "event_type", "type")
        submitted_at = _payload_value(payload, "submitted_at", "created_at", "timestamp")
        organization = _payload_value(metadata_block, "organization", "org", "brand", "brand_slug")
        if (
            "titan" in source.lower() or
            str(event_name or "").strip().lower() in {"partial_quote", "quote_started", "quote_not_signed_up"}
        ) and (submitted_at or lead_phone or lead_email):
            external_id = ":".join(
                part for part in ("titan", organization, event_name, submitted_at, lead_phone or lead_email) if part
            )[:255]

    if not external_id:
        payload_bytes = raw_body or json.dumps(payload, sort_keys=True).encode("utf-8")
        external_id = f"payload_{hashlib.sha1(payload_bytes).hexdigest()[:20]}"

    return {
        "lead_name": lead_name,
        "lead_email": lead_email,
        "lead_phone": lead_phone,
        "message_text": message_text,
        "source": source,
        "external_id": external_id,
        "extra_fields": extra_fields,
    }


def _build_lead_submission_summary(lead_name, lead_email, lead_phone, message_text, extra_fields):
    summary_parts = []
    if lead_name:
        summary_parts.append(f"Name: {lead_name}")
    if lead_email:
        summary_parts.append(f"Email: {lead_email}")
    if lead_phone:
        summary_parts.append(f"Phone: {lead_phone}")
    if message_text:
        summary_parts.append(f"Message: {message_text}")
    for key, value in (extra_fields or {}).items():
        label = str(key).replace("_", " ").strip().title()
        summary_parts.append(f"{label}: {value}")
    return "\n".join(summary_parts).strip()


def _normalize_profile_key(key):
    text = str(key or "").strip()
    text = re.sub(r"^(service|metadata|original|lead)_", "", text)
    return text


def _ingest_lead_submission(
    db,
    brand_id,
    brand,
    *,
    external_id,
    source,
    lead_name="",
    lead_email="",
    lead_phone="",
    message_text="",
    extra_fields=None,
    message_header="Lead Submission",
    external_message_id="",
    allow_auto_send=True,
):
    extra_fields = extra_fields or {}
    summary = _build_lead_submission_summary(
        lead_name,
        lead_email,
        lead_phone,
        message_text,
        extra_fields,
    )

    thread_id = db.upsert_lead_thread(
        brand_id,
        "lead_form",
        external_id,
        data={
            "lead_name": lead_name,
            "lead_email": lead_email,
            "lead_phone": lead_phone,
            "source": source,
            "summary": summary,
            "commercial_data_json": json.dumps({
                "incoming_webhook_fields": {
                    _normalize_profile_key(key): value
                    for key, value in (extra_fields or {}).items()
                    if str(value or "").strip()
                }
            }, separators=(",", ":")),
        },
    )

    message_body = summary or "Lead submitted a form without any structured details."
    db.add_lead_message(
        thread_id,
        direction="inbound",
        role="lead",
        content=f"[{message_header}]\n{message_body}",
        channel="lead_form",
        external_message_id=external_message_id or external_id,
        metadata={"source": source, "fields": extra_fields},
    )
    db.update_lead_thread_status(thread_id, summary=summary)

    if brand.get("sales_bot_enabled"):
        thread = db.get_lead_thread(thread_id, brand_id=brand_id)
        if not (thread and thread.get("is_private")):
            from webapp.warren_brain import process_and_respond
            from webapp.warren_sender import send_reply

            result = process_and_respond(
                db,
                brand_id,
                thread_id,
                channel="lead_form",
                allow_auto_send=allow_auto_send,
            )
            if result and result.get("should_send") and result.get("reply") and lead_phone:
                send_reply(
                    db,
                    brand,
                    thread_id,
                    result["reply"],
                    channel="sms",
                    logged_message_id=result.get("outbound_message_id"),
                )

    return thread_id


def _extract_incoming_webhook_secret():
    auth_header = (request.headers.get("Authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return (
        request.headers.get("X-GroMore-Webhook-Secret", "") or
        request.headers.get("X-Webhook-Secret", "") or
        request.headers.get("X-Incoming-Webhook-Secret", "") or
        request.values.get("webhook_secret", "") or
        request.values.get("secret", "")
    ).strip()


def _signature_candidates(secret, raw_body):
    digest = hmac.new(secret.encode("utf-8"), raw_body or b"", hashlib.sha256).digest()
    hex_digest = digest.hex()
    return {
        hex_digest,
        f"sha256={hex_digest}",
        base64.b64encode(digest).decode("ascii"),
    }


def _incoming_webhook_auth_valid(configured_secret, raw_body):
    presented_secret = _extract_incoming_webhook_secret()
    if presented_secret and hmac.compare_digest(presented_secret, configured_secret):
        return True

    # Titan Quote Tool / WP plugin compatibility: its partial quote webhook stores
    # the shared secret and signs the raw JSON body instead of sending Bearer auth.
    signature = (request.headers.get("X-TQT-Signature") or "").strip()
    if signature:
        for candidate in _signature_candidates(configured_secret, raw_body):
            if hmac.compare_digest(signature, candidate):
                return True
    return False


def _request_signature_present():
    return bool((request.headers.get("X-TQT-Signature") or "").strip())


def _payload_preview(raw_body, payload=None):
    if isinstance(payload, dict) and payload:
        try:
            return json.dumps(payload, separators=(",", ":"), sort_keys=True)[:2000]
        except Exception:
            pass
    try:
        return (raw_body or b"").decode("utf-8", errors="replace")[:2000]
    except Exception:
        return ""


def _record_lead_webhook_delivery(db, brand=None, **kwargs):
    try:
        db.record_lead_webhook_delivery(
            (brand or {}).get("id"),
            brand_slug=(brand or {}).get("slug") or kwargs.pop("brand_slug", ""),
            endpoint=request.path,
            signature_present=_request_signature_present(),
            remote_addr=request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip(),
            **kwargs,
        )
    except Exception:
        log.exception("Failed to record lead webhook delivery")


def _resolve_brand_from_incoming_webhook_secret(db, raw_body):
    presented_secret = _extract_incoming_webhook_secret()
    for brand in db.get_all_brands():
        configured_secret = (brand.get("sales_bot_incoming_webhook_secret") or "").strip()
        if not configured_secret:
            continue
        if presented_secret and hmac.compare_digest(presented_secret, configured_secret):
            return brand
        if _request_signature_present() and _incoming_webhook_auth_valid(configured_secret, raw_body):
            return brand
    return None


def _sng_find_first(payload, *keys):
    key_set = {str(key).strip().lower() for key in keys if str(key).strip()}
    if not key_set:
        return ""

    queue = [payload]
    depth = 0
    while queue and depth < 5:
        next_queue = []
        for item in queue:
            if isinstance(item, dict):
                for key, value in item.items():
                    if str(key).strip().lower() in key_set:
                        text = _stringify_webhook_value(value)
                        if text:
                            return text
                    if isinstance(value, (dict, list)):
                        next_queue.append(value)
            elif isinstance(item, list):
                next_queue.extend(value for value in item if isinstance(value, (dict, list)))
        queue = next_queue
        depth += 1
    return ""


def _extract_sng_event_type(payload):
    if isinstance(payload, dict):
        direct_value = _stringify_webhook_value(payload.get("event_type"))
        if direct_value:
            return direct_value[:255]

        event_block = payload.get("event")
        if isinstance(event_block, dict):
            for key in ("type", "name", "topic"):
                nested_value = _stringify_webhook_value(event_block.get(key))
                if nested_value:
                    return nested_value[:255]

        for key in ("type", "name", "topic"):
            direct_candidate = _stringify_webhook_value(payload.get(key))
            if direct_candidate and direct_candidate.lower() != "event":
                return direct_candidate[:255]

    for candidate in (
        _sng_find_first(payload, "event_type"),
        _sng_find_first(payload.get("event") if isinstance(payload, dict) else {}, "type", "name", "topic"),
        _sng_find_first(payload, "type", "topic"),
    ):
        text = str(candidate or "").strip()
        if text and text.lower() != "event":
            return text[:255]
    return "unknown"


def _extract_sng_event_id(payload, raw_body):
    event_id = _sng_find_first(payload, "event_id", "eventid", "id", "webhook_id", "webhookid")
    if event_id:
        return event_id[:255]
    digest = hashlib.sha1(raw_body or b"").hexdigest()[:24]
    return f"sng_{digest}"


def _extract_sng_summary(payload, event_type):
    client = payload.get("client") if isinstance(payload, dict) and isinstance(payload.get("client"), dict) else {}
    payment = payload.get("payment") if isinstance(payload, dict) and isinstance(payload.get("payment"), dict) else {}
    invoice = payload.get("invoice") if isinstance(payload, dict) and isinstance(payload.get("invoice"), dict) else {}
    subscription = payload.get("subscription") if isinstance(payload, dict) and isinstance(payload.get("subscription"), dict) else {}
    quote = payload.get("quote") if isinstance(payload, dict) and isinstance(payload.get("quote"), dict) else {}
    free_quote = payload.get("free_quote") if isinstance(payload, dict) and isinstance(payload.get("free_quote"), dict) else {}
    quote_block = quote or free_quote
    summary = {
        "event_type": event_type,
        "client_id": _sng_find_first(payload, "client_id", "clientid") or _stringify_webhook_value(client.get("id")),
        "client_name": _sng_find_first(payload, "client_name", "clientname") or _stringify_webhook_value(client.get("name")) or _sng_find_first(payload, "name"),
        "client_email": _sng_find_first(payload, "client_email", "clientemail") or _stringify_webhook_value(client.get("email")) or _sng_find_first(payload, "email"),
        "client_phone": _sng_find_first(payload, "client_phone", "clientphone") or _stringify_webhook_value(client.get("phone") or client.get("mobile")) or _sng_find_first(payload, "phone", "mobile"),
        "payment_id": _sng_find_first(payload, "payment_id", "paymentid") or _stringify_webhook_value(payment.get("id")),
        "invoice_id": _sng_find_first(payload, "invoice_id", "invoiceid") or _stringify_webhook_value(invoice.get("id")),
        "subscription_id": _sng_find_first(payload, "subscription_id", "subscriptionid") or _stringify_webhook_value(subscription.get("id")),
        "quote_id": _sng_find_first(payload, "quote_id", "quoteid", "free_quote_id", "freequoteid") or _stringify_webhook_value(quote_block.get("id")),
        "quote_amount": _sng_find_first(payload, "quote_amount", "quoteamount", "amount", "total", "price") or _stringify_webhook_value(quote_block.get("amount") or quote_block.get("total") or quote_block.get("price")),
        "quote_service": _sng_find_first(payload, "service", "service_type", "service_name", "quote_service") or _stringify_webhook_value(quote_block.get("service") or quote_block.get("service_type") or quote_block.get("service_name")),
        "quote_address": _sng_find_first(payload, "address", "service_address", "property_address") or _stringify_webhook_value(quote_block.get("address") or quote_block.get("service_address")),
        "status": _sng_find_first(payload, "status", "payment_status", "paymentstatus") or _stringify_webhook_value(payment.get("status") or invoice.get("status") or subscription.get("status")),
    }
    return {key: value for key, value in summary.items() if value}


def _build_sng_event_detail(event_type, summary):
    parts = [event_type or "Sweep and Go event"]
    if summary.get("client_name"):
        parts.append(summary["client_name"])
    elif summary.get("client_id"):
        parts.append(f"client {summary['client_id']}")
    if summary.get("status"):
        parts.append(summary["status"])
    return " - ".join(part for part in parts if part)[:1000]


def _handle_generic_lead_webhook(brand_slug=None):
    db = _get_db()
    raw_body = request.get_data() or b""
    brand = db.get_brand_by_slug(brand_slug) if brand_slug else _resolve_brand_from_incoming_webhook_secret(db, raw_body)
    if not brand:
        _record_lead_webhook_delivery(
            db,
            brand_slug=brand_slug or "",
            status="rejected",
            http_status=404 if brand_slug else 401,
            reason="Unknown brand slug." if brand_slug else "No brand matched the webhook secret/signature.",
            payload_preview=_payload_preview(raw_body),
        )
        if brand_slug:
            abort(404)
        return jsonify({"error": "No brand matched the webhook secret/signature. Use /webhooks/leads/<brand_slug> or verify the secret."}), 401

    configured_secret = (brand.get("sales_bot_incoming_webhook_secret") or "").strip()
    if not configured_secret:
        _record_lead_webhook_delivery(
            db,
            brand,
            status="rejected",
            http_status=409,
            reason="Incoming lead webhook is not configured for this brand.",
            payload_preview=_payload_preview(raw_body),
        )
        return jsonify({"error": "Incoming lead webhook is not configured for this brand."}), 409

    if not _incoming_webhook_auth_valid(configured_secret, raw_body):
        _record_lead_webhook_delivery(
            db,
            brand,
            status="rejected",
            http_status=401,
            reason="Secret/signature did not match.",
            payload_preview=_payload_preview(raw_body),
        )
        return jsonify({"error": "Webhook secret/signature did not match."}), 401

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        payload = request.form.to_dict(flat=True)
    if not isinstance(payload, dict) or not payload:
        _record_lead_webhook_delivery(
            db,
            brand,
            status="rejected",
            http_status=400,
            reason="Expected a JSON or form-encoded payload.",
            payload_preview=_payload_preview(raw_body, payload),
        )
        return jsonify({"error": "Expected a JSON or form-encoded payload."}), 400

    submission = _extract_generic_form_submission(payload, raw_body)
    thread_id = _ingest_lead_submission(
        db,
        brand["id"],
        brand,
        external_id=submission["external_id"],
        source=f"incoming_webhook:{submission['source']}",
        lead_name=submission["lead_name"],
        lead_email=submission["lead_email"],
        lead_phone=submission["lead_phone"],
        message_text=submission["message_text"],
        extra_fields=submission["extra_fields"],
        message_header="Inbound Lead Submission",
    )
    _record_lead_webhook_delivery(
        db,
        brand,
        status="accepted",
        http_status=200,
        reason="Lead added to Warren.",
        source=submission["source"],
        lead_name=submission["lead_name"],
        lead_email=submission["lead_email"],
        lead_phone=submission["lead_phone"],
        thread_id=thread_id,
        payload_preview=_payload_preview(raw_body, payload),
    )
    return jsonify({"ok": True, "thread_id": thread_id}), 200


@webhooks_bp.route("/leads", methods=["POST"])
@webhooks_bp.route("/leads/", methods=["POST"])
def generic_lead_webhook_without_slug():
    """Accept generic lead submissions when the shared secret identifies the brand."""
    return _handle_generic_lead_webhook()


@webhooks_bp.route("/leads/<brand_slug>", methods=["POST"])
def generic_lead_webhook(brand_slug):
    """Accept generic lead form submissions from websites and middleware tools."""
    return _handle_generic_lead_webhook(brand_slug)


@webhooks_bp.route("/sng/<brand_slug>/<secret>", methods=["POST"])
def sng_webhook(brand_slug, secret):
    db = _get_db()
    brand = db.get_brand_by_slug(brand_slug)
    if not brand:
        abort(404)

    expected_secret = (brand.get("sales_bot_sng_webhook_secret") or "").strip()
    if not expected_secret or not secret or not hmac.compare_digest(secret, expected_secret):
        abort(401)

    raw_body = request.get_data() or b""
    parsed = request.get_json(silent=True)
    if isinstance(parsed, dict):
        payload = parsed
    elif isinstance(parsed, list):
        payload = {"items": parsed}
    else:
        payload = {}

    if not payload:
        return jsonify({"error": "Expected a JSON payload."}), 400

    event_type = _extract_sng_event_type(payload)
    event_id = _extract_sng_event_id(payload, raw_body)
    summary = _extract_sng_summary(payload, event_type)
    detail = _build_sng_event_detail(event_type, summary)

    db.record_sng_webhook_event(
        brand["id"],
        event_id,
        event_type=event_type,
        status="received",
        detail=detail,
        summary=summary,
        payload=payload,
    )
    try:
        from webapp.warren_crm_events import process_incoming_sng_event

        process_incoming_sng_event(db, current_app.config, brand, event_id, event_type, summary, base_detail=detail)
    except Exception as exc:
        log.exception("SNG CRM event processing failed: brand=%s event=%s err=%s", brand.get("id"), event_id, exc)
        db.update_sng_webhook_event(
            brand["id"],
            event_id,
            status="failed",
            detail=f"CRM event processing failed: {str(exc)[:900]}",
        )
    return jsonify({"ok": True, "event_type": event_type, "event_id": event_id}), 200


# ─────────────────────────────────────────────
# Quo / OpenPhone SMS Webhook
# ─────────────────────────────────────────────

def _verify_quo_signature(payload_bytes, signature, secret):
    """Verify Quo webhook signature using HMAC-SHA256.

    OpenPhone signs with the webhook 'key' (base64-encoded secret).
    The signature header is a base64-encoded HMAC-SHA256 digest.
    """
    if not secret:
        return True  # no secret configured, accept (dev mode)
    if not signature:
        log.warning("Quo webhook: no signature header present - accepting anyway (secret is configured but sender didn't sign)")
        return True

    import base64

    try:
        # The secret from OpenPhone is base64-encoded
        try:
            secret_bytes = base64.b64decode(secret)
        except Exception:
            secret_bytes = secret.encode("utf-8")

        computed = hmac.new(
            secret_bytes,
            payload_bytes,
            hashlib.sha256,
        ).digest()

        # Try base64 comparison first (OpenPhone's format)
        computed_b64 = base64.b64encode(computed).decode("utf-8")
        if hmac.compare_digest(computed_b64, signature):
            return True

        # Fall back to hex comparison
        computed_hex = hmac.new(
            secret_bytes,
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        if hmac.compare_digest(computed_hex, signature):
            return True

        # Also try with the secret as plain UTF-8 + base64 output
        computed2 = hmac.new(
            secret.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).digest()
        computed2_b64 = base64.b64encode(computed2).decode("utf-8")
        if hmac.compare_digest(computed2_b64, signature):
            return True

        computed2_hex = hmac.new(
            secret.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        if hmac.compare_digest(computed2_hex, signature):
            return True

        log.warning("Quo signature mismatch. Header=%s, expected_b64=%s, expected_hex=%s",
                     signature[:20] + "...", computed_b64[:20] + "...", computed_hex[:20] + "...")
        return False
    except Exception as exc:
        log.exception("Quo signature verification error: %s", exc)
        return False


@webhooks_bp.route("/quo/sms/<brand_slug>", methods=["POST"])
def quo_sms_webhook(brand_slug):
    """Handle inbound SMS from Quo/OpenPhone.

    Quo webhook payload (message.received event):
    {
        "type": "message.received",
        "data": {
            "object": {
                "id": "msg_xxx",
                "conversationId": "conv_xxx",
                "from": "+15551234567",
                "to": "+15559876543",
                "body": "Hi, I need a quote",
                "direction": "incoming",
                "createdAt": "2026-04-07T12:00:00Z"
            }
        }
    }
    """
    db = _get_db()

    # Find the brand by slug
    brand = db.get_brand_by_slug(brand_slug)
    if not brand:
        log.warning("Quo webhook: unknown brand slug '%s'", brand_slug)
        abort(404)

    brand_id = brand["id"]

    # Verify signature
    secret = (brand.get("sales_bot_quo_webhook_secret") or "").strip()
    raw_body = request.get_data()

    # Try multiple possible signature header names
    signature = (
        request.headers.get("X-Openphone-Signature", "") or
        request.headers.get("X-Quo-Signature", "") or
        request.headers.get("X-Webhook-Signature", "") or
        request.headers.get("X-Signature", "")
    )
    if secret and not _verify_quo_signature(raw_body, signature, secret):
        log.warning("Quo webhook: signature verification failed for brand %s (sig_present=%s, headers=%s)",
                     brand_id, bool(signature),
                     {k: v for k, v in request.headers if k.lower().startswith("x-")})
        abort(401)

    payload = request.get_json(silent=True) or {}
    event_type = payload.get("type", "")

    # Only process incoming messages
    if event_type != "message.received":
        return jsonify({"ok": True, "skipped": event_type}), 200

    msg_data = (payload.get("data") or {}).get("object") or {}
    msg_body = (msg_data.get("body") or "").strip()
    from_phone = (msg_data.get("from") or "").strip()
    conversation_id = (msg_data.get("conversationId") or "").strip()
    message_id = (msg_data.get("id") or "").strip()
    image_urls = _extract_image_urls(msg_data)

    if (not msg_body and not image_urls) or not from_phone:
        return jsonify({"ok": True, "skipped": "empty_message"}), 200

    # ── A2P opt-out / opt-in keyword detection ──
    OPT_OUT_KEYWORDS = {"stop", "unsubscribe", "cancel", "end", "quit"}
    OPT_IN_KEYWORDS = {"start", "unstop", "subscribe"}
    HELP_KEYWORDS = {"help", "info"}
    body_lower = msg_body.lower().strip()

    if body_lower in OPT_OUT_KEYWORDS:
        db.record_opt_out(brand_id, from_phone, keyword=msg_body.upper())
        log.info("A2P opt-out recorded: brand=%s phone=%s keyword=%s", brand_id, from_phone, msg_body)
        # Send required confirmation
        from webapp.warren_sender import send_opt_out_confirmation
        app = current_app._get_current_object()
        def _send_optout():
            with app.app_context():
                send_opt_out_confirmation(db, brand, from_phone)
        threading.Thread(target=_send_optout, daemon=True).start()
        return jsonify({"ok": True, "action": "opted_out"}), 200

    if body_lower in OPT_IN_KEYWORDS:
        db.record_opt_in(brand_id, from_phone, source=msg_body.upper())
        log.info("A2P opt-in recorded: brand=%s phone=%s keyword=%s", brand_id, from_phone, msg_body)
        # Send opt-in confirmation
        from webapp.warren_sender import send_opt_in_confirmation
        app = current_app._get_current_object()
        def _send_optin():
            with app.app_context():
                send_opt_in_confirmation(db, brand, from_phone)
        threading.Thread(target=_send_optin, daemon=True).start()
        return jsonify({"ok": True, "action": "opted_in"}), 200

    if body_lower in HELP_KEYWORDS:
        from webapp.warren_sender import send_help_reply
        app = current_app._get_current_object()
        def _send_help():
            with app.app_context():
                send_help_reply(db, brand, from_phone)
        threading.Thread(target=_send_help, daemon=True).start()
        return jsonify({"ok": True, "action": "help_sent"}), 200

    # ── Check if this phone has opted out ──
    if db.is_opted_out(brand_id, from_phone):
        log.info("Inbound from opted-out phone %s, logging only (no auto-reply)", from_phone)
        external_id = conversation_id or from_phone
        thread_id = db.upsert_lead_thread(
            brand_id, "sms", external_id,
            data={"lead_phone": from_phone, "source": "openphone"},
        )
        db.add_lead_message(
            thread_id, direction="inbound", role="lead", content=(msg_body or "[Lead sent image]"),
            channel="sms", external_message_id=message_id,
            metadata={"from": from_phone, "conversation_id": conversation_id, "opted_out": True, "image_urls": image_urls},
        )
        return jsonify({"ok": True, "thread_id": thread_id, "auto_reply": False, "reason": "opted_out"}), 200

    # Upsert thread (keyed by channel + conversation ID)
    external_id = conversation_id or from_phone
    thread_id = db.upsert_lead_thread(
        brand_id, "sms", external_id,
        data={
            "lead_phone": from_phone,
            "source": "openphone",
        },
    )

    # Add the inbound message
    db.add_lead_message(
        thread_id,
        direction="inbound",
        role="lead",
        content=(msg_body or "[Lead sent image]"),
        channel="sms",
        external_message_id=message_id,
        metadata={"from": from_phone, "conversation_id": conversation_id, "image_urls": image_urls},
    )

    # Check if assistant is enabled
    if not brand.get("sales_bot_enabled"):
        return jsonify({"ok": True, "thread_id": thread_id, "auto_reply": False}), 200

    # Skip auto-reply for private threads (personal conversations)
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if thread and thread.get("is_private"):
        log.info("Skipping Warren for private thread %s", thread_id)
        return jsonify({"ok": True, "thread_id": thread_id, "auto_reply": False, "reason": "private"}), 200

    # Process in background thread to return 200 quickly
    app = current_app._get_current_object()

    def _process():
        with app.app_context():
            from webapp.warren_brain import process_and_respond
            from webapp.warren_sender import send_reply

            result = process_and_respond(db, brand_id, thread_id, channel="sms")
            if result and result.get("should_send") and result.get("reply"):
                send_reply(
                    db,
                    brand,
                    thread_id,
                    result["reply"],
                    channel="sms",
                    logged_message_id=result.get("outbound_message_id"),
                )

    threading.Thread(target=_process, daemon=True).start()

    return jsonify({"ok": True, "thread_id": thread_id, "auto_reply": True}), 200


# ─────────────────────────────────────────────
# Meta Lead Forms Webhook
# ─────────────────────────────────────────────

def _verify_meta_signature(payload_bytes, signature, app_secret):
    """Verify Meta webhook signature using HMAC-SHA256."""
    if not app_secret:
        return True
    if not signature:
        return False
    # Meta sends: sha256=<hex>
    if signature.startswith("sha256="):
        signature = signature[7:]
    expected = hmac.new(
        app_secret.encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def _get_meta_app_secret(db):
    """Get Meta app secret for webhook verification."""
    return (db.get_setting("meta_app_secret", "") or "").strip()


@webhooks_bp.route("/meta/leadgen", methods=["POST"])
def meta_leadgen_webhook():
    """Handle Meta Lead Form submissions (leadgen webhook).

    Meta sends:
    {
        "entry": [{
            "id": "page_id",
            "time": 1234567890,
            "changes": [{
                "field": "leadgen",
                "value": {
                    "form_id": "123",
                    "leadgen_id": "456",
                    "page_id": "789",
                    "created_time": 1234567890
                }
            }]
        }]
    }
    """
    db = _get_db()

    # Verify signature
    raw_body = request.get_data()
    signature = request.headers.get("X-Hub-Signature-256", "")
    app_secret = _get_meta_app_secret(db)
    if app_secret and not _verify_meta_signature(raw_body, signature, app_secret):
        log.warning("Meta leadgen webhook: invalid signature")
        abort(401)

    payload = request.get_json(silent=True) or {}
    entries = payload.get("entry") or []

    for entry in entries:
        page_id = str(entry.get("id", ""))
        changes = entry.get("changes") or []

        for change in changes:
            if change.get("field") != "leadgen":
                continue

            value = change.get("value") or {}
            leadgen_id = str(value.get("leadgen_id", ""))
            form_id = str(value.get("form_id", ""))

            if not leadgen_id:
                continue

            # Find brand by facebook_page_id
            brand = _find_brand_by_page_id(db, page_id)
            if not brand:
                log.warning("Meta leadgen: no brand found for page_id=%s", page_id)
                continue

            brand_id = brand["id"]

            # Check if lead forms are enabled
            if not brand.get("sales_bot_meta_lead_forms"):
                continue

            # Fetch the actual lead data from Meta Graph API
            app = current_app._get_current_object()

            def _process(bid=brand_id, lid=leadgen_id, fid=form_id, br=brand):
                with app.app_context():
                    _fetch_and_process_lead(db, bid, lid, fid, br)

            threading.Thread(target=_process, daemon=True).start()

    return jsonify({"ok": True}), 200


@webhooks_bp.route("/meta/leadgen", methods=["GET"])
def meta_leadgen_verify():
    """Meta leadgen webhook verification (GET challenge)."""
    return _handle_meta_webhook_verification()


def _find_brand_by_page_id(db, page_id):
    """Find a brand by its facebook_page_id."""
    if not page_id:
        return None
    conn = db._conn()
    row = conn.execute(
        "SELECT * FROM brands WHERE facebook_page_id = ?",
        (page_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _fetch_and_process_lead(db, brand_id, leadgen_id, form_id, brand):
    """Fetch lead data from Meta and create a thread."""
    from webapp.api_bridge import _get_meta_token

    conn_data = db.get_brand_connections(brand_id).get("meta")
    if not conn_data or conn_data.get("status") != "connected":
        log.warning("Meta leadgen: no Meta connection for brand %s", brand_id)
        return

    token = _get_meta_token(db, brand_id, conn_data)
    if not token:
        log.warning("Meta leadgen: no Meta token for brand %s", brand_id)
        return

    import requests as _req

    # Fetch lead data
    resp = _req.get(
        f"https://graph.facebook.com/v21.0/{leadgen_id}",
        params={"access_token": token, "fields": "field_data,created_time"},
        timeout=15,
    )
    if resp.status_code != 200:
        log.error("Meta leadgen: failed to fetch lead %s: %s", leadgen_id, resp.text[:300])
        return

    lead_data = resp.json()
    field_data = lead_data.get("field_data") or []

    # Extract fields
    lead_name = ""
    lead_email = ""
    lead_phone = ""
    extra_fields = {}

    for field in field_data:
        fname = (field.get("name") or "").lower()
        fvalues = field.get("values") or []
        fval = fvalues[0] if fvalues else ""

        if fname in ("full_name", "name"):
            lead_name = fval
        elif fname == "email":
            lead_email = fval
        elif fname in ("phone_number", "phone"):
            lead_phone = fval
        else:
            extra_fields[fname] = fval

    _ingest_lead_submission(
        db,
        brand_id,
        brand,
        external_id=leadgen_id,
        source=f"meta_lead_form:{form_id}",
        lead_name=lead_name,
        lead_email=lead_email,
        lead_phone=lead_phone,
        extra_fields=extra_fields,
        message_header="Meta Lead Form Submission",
        external_message_id=leadgen_id,
    )


# ─────────────────────────────────────────────
# Meta Messenger Webhook
# ─────────────────────────────────────────────

@webhooks_bp.route("/meta/messenger", methods=["GET"])
def meta_messenger_verify():
    """Meta webhook verification (GET challenge)."""
    return _handle_meta_webhook_verification()


@webhooks_bp.route("/meta/messenger", methods=["POST"])
def meta_messenger_webhook():
    """Handle inbound Messenger messages.

    Meta sends:
    {
        "object": "page",
        "entry": [{
            "id": "page_id",
            "time": 1234567890,
            "messaging": [{
                "sender": {"id": "user_psid"},
                "recipient": {"id": "page_id"},
                "timestamp": 1234567890,
                "message": {
                    "mid": "msg_id",
                    "text": "Hello"
                }
            }]
        }]
    }
    """
    db = _get_db()

    # Verify signature
    raw_body = request.get_data()
    signature = request.headers.get("X-Hub-Signature-256", "")
    app_secret = _get_meta_app_secret(db)
    if app_secret and not _verify_meta_signature(raw_body, signature, app_secret):
        log.warning("Meta Messenger webhook: invalid signature")
        abort(401)

    payload = request.get_json(silent=True) or {}

    if payload.get("object") != "page":
        return jsonify({"ok": True}), 200

    entries = payload.get("entry") or []

    for entry in entries:
        page_id = str(entry.get("id", ""))
        messaging_events = entry.get("messaging") or []

        for event in messaging_events:
            sender_id = (event.get("sender") or {}).get("id", "")
            message = event.get("message") or {}
            msg_text = (message.get("text") or "").strip()
            msg_id = message.get("mid", "")
            image_urls = _extract_image_urls(message)

            # Skip echoes (messages sent by the page itself)
            if message.get("is_echo"):
                continue

            if (not msg_text and not image_urls) or not sender_id:
                continue

            # Skip if sender is the page itself
            if sender_id == page_id:
                continue

            brand = _find_brand_by_page_id(db, page_id)
            if not brand:
                log.warning("Meta Messenger: no brand for page_id=%s", page_id)
                continue

            brand_id = brand["id"]

            if not brand.get("sales_bot_messenger_enabled"):
                continue

            # Upsert thread
            thread_id = db.upsert_lead_thread(
                brand_id, "messenger", sender_id,
                data={
                    "source": "messenger",
                },
            )

            # Add inbound message
            db.add_lead_message(
                thread_id,
                direction="inbound",
                role="lead",
                content=(msg_text or "[Lead sent image]"),
                channel="messenger",
                external_message_id=msg_id,
                metadata={"sender_psid": sender_id, "page_id": page_id, "image_urls": image_urls},
            )

            # Process with Warren
            if brand.get("sales_bot_enabled"):
                # Skip auto-reply for private threads
                m_thread = db.get_lead_thread(thread_id, brand_id=brand_id)
                if m_thread and m_thread.get("is_private"):
                    log.info("Skipping Warren for private Messenger thread %s", thread_id)
                    continue

                app = current_app._get_current_object()

                def _process(bid=brand_id, tid=thread_id, br=brand, pid=page_id, sid=sender_id):
                    with app.app_context():
                        from webapp.warren_brain import process_and_respond
                        from webapp.warren_sender import send_reply

                        result = process_and_respond(db, bid, tid, channel="messenger")
                        if result and result.get("should_send") and result.get("reply"):
                            send_reply(
                                db,
                                br,
                                tid,
                                result["reply"],
                                channel="messenger",
                                recipient_id=sid,
                                page_id=pid,
                                logged_message_id=result.get("outbound_message_id"),
                            )

                threading.Thread(target=_process, daemon=True).start()

    return jsonify({"ok": True}), 200
