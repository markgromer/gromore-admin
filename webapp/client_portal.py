"""
Client Portal Blueprint

Separate login and dashboard for clients (brand owners) to see their
ad performance, understand what the numbers mean, get step-by-step
action instructions, and manage their ad campaigns directly.
"""
import os
import json
import re
import html
import time
import threading
import logging
import uuid
from functools import wraps
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, urljoin, quote_plus

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, session, abort, jsonify, current_app,
    make_response, send_file,
)

from webapp.font_catalog import (
    GOOGLE_FONT_CHOICES,
    SITE_BUILDER_FONT_GROUPS,
    SITE_BUILDER_FONT_PAIR_CHOICES,
    build_editor_font_family_options,
    build_site_builder_font_preview_stylesheets,
    normalize_google_font_family,
)

client_bp = Blueprint(
    "client",
    __name__,
    template_folder="templates/client",
    url_prefix="/client",
)

client_public_bp = Blueprint(
    "client_public",
    __name__,
    template_folder="templates/client",
)


log = logging.getLogger(__name__)

_SITE_BUILDER_MAX_CONTENT_BYTES = 1024 * 1024
_SITE_BUILDER_MAX_EDITOR_JSON_BYTES = 2 * 1024 * 1024
_SITE_BUILDER_MAX_PAGE_CSS_BYTES = 256 * 1024
_SITE_BUILDER_MAX_TITLE_LENGTH = 255
_SITE_BUILDER_MAX_SEO_TITLE_LENGTH = 120
_SITE_BUILDER_MAX_SEO_DESCRIPTION_LENGTH = 320
_SITE_BUILDER_MAX_REWRITE_INSTRUCTIONS_LENGTH = 2000
_SITE_BUILDER_INTAKE_IMAGE_SLOTS = [
    {
        "key": "hero_desktop",
        "label": "Homepage hero image - desktop",
        "field_name": "hero_desktop_image",
        "accept": "image/*",
        "stock_query": "service business hero exterior truck crew",
        "help": "The main desktop hero image. Wide crops work best.",
    },
    {
        "key": "hero_mobile",
        "label": "Homepage hero image - mobile",
        "field_name": "hero_mobile_image",
        "accept": "image/*",
        "stock_query": "service business portrait mobile hero worker",
        "help": "Optional mobile-specific hero image for tighter crops and portrait framing.",
    },
    {
        "key": "about_team",
        "label": "About / team image",
        "field_name": "about_team_image",
        "accept": "image/*",
        "stock_query": "local service team portrait business owner",
        "help": "Use a team, founder, crew, or trust-building brand image.",
    },
    {
        "key": "services_overview",
        "label": "Services overview image",
        "field_name": "services_overview_image",
        "accept": "image/*",
        "stock_query": "local service work in progress tools technician",
        "help": "Supports the services section or services page overview.",
    },
    {
        "key": "proof_image",
        "label": "Proof / before-after image",
        "field_name": "proof_image",
        "accept": "image/*",
        "stock_query": "before after home service results",
        "help": "Best for results, before/after, completed work, or proof sections.",
    },
    {
        "key": "contact_location",
        "label": "Contact / location image",
        "field_name": "contact_location_image",
        "accept": "image/*",
        "stock_query": "local storefront neighborhood map service area",
        "help": "Use for contact, location, service-area, or local trust sections.",
    },
    {
        "key": "gallery_images",
        "label": "Gallery images",
        "field_name": "gallery_images",
        "accept": "image/*",
        "multiple": True,
        "stock_query": "local service gallery completed work",
        "help": "Optional extra images for gallery or proof grids. You can upload several.",
    },
]

SITE_BUILDER_EDITOR_FONT_OPTIONS = build_editor_font_family_options()
SITE_BUILDER_FONT_PREVIEW_STYLESHEETS = build_site_builder_font_preview_stylesheets()


def client_login_required(view_func):
    @wraps(view_func)
    def _wrapped(*args, **kwargs):
        if session.get("client_user_id") and session.get("client_brand_id"):
            return view_func(*args, **kwargs)

        wants_json = (
            request.path.startswith("/client/api/")
            or request.headers.get("X-Requested-With") in {"XMLHttpRequest", "PJAX"}
            or request.is_json
        )
        if wants_json:
            return jsonify({"error": "Authentication required"}), 401
        return redirect(url_for("client.client_login"))

    return _wrapped


def _require_role(*allowed_roles):
    current_role = str(session.get("client_role") or "owner").strip().lower()
    allowed = {
        str(role).strip().lower()
        for role in allowed_roles
        if str(role).strip()
    }
    if not allowed:
        return True
    return current_role in allowed


def _get_ad_connection_status(db, brand):
    brand_id = int((brand or {}).get("id") or 0)
    connections = db.get_brand_connections(brand_id) if brand_id else {}
    google_conn = connections.get("google", {}) if isinstance(connections, dict) else {}
    meta_conn = connections.get("meta", {}) if isinstance(connections, dict) else {}

    has_google = bool(
        google_conn.get("status") == "connected"
        and ((brand or {}).get("google_ads_customer_id") or "").strip()
    )
    has_meta = bool(meta_conn.get("status") == "connected")
    return has_google, has_meta


def _normalize_client_phone_number(value):
    phone = re.sub(r"[^\d+]", "", str(value or "").strip())
    if not phone:
        return ""
    if phone.startswith("+"):
        digits = "+" + re.sub(r"\D", "", phone[1:])
        return digits[:20]
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}" if digits else ""


def _coerce_action_key(value, fallback):
    text = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")
    return (text or fallback)[:80]


def _safe_json_object(raw_value):
    if isinstance(raw_value, dict):
        return dict(raw_value)
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _safe_json_list(raw_value):
    if isinstance(raw_value, list):
        return list(raw_value)
    if not raw_value:
        return []
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _normalize_site_builder_text(value, field_label, max_length=None, trim=True):
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    normalized = value.strip() if trim else value
    if max_length and len(normalized) > max_length:
        raise ValueError(f"{field_label} is too long.")
    return normalized


def _validate_site_builder_blob_size(value, field_label, max_bytes):
    size = len((value or "").encode("utf-8"))
    if size > max_bytes:
        raise ValueError(f"{field_label} is too large.")


def _normalize_site_builder_editor_json(raw_value):
    if raw_value in (None, ""):
        return ""
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
        except Exception as exc:
            raise ValueError("Editor state is not valid JSON.") from exc
    else:
        parsed = raw_value
    try:
        normalized = json.dumps(parsed)
    except Exception as exc:
        raise ValueError("Editor state could not be serialized.") from exc
    _validate_site_builder_blob_size(normalized, "Editor state", _SITE_BUILDER_MAX_EDITOR_JSON_BYTES)
    return normalized


def _normalize_site_builder_page_save_payload(data):
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError("Invalid page update payload.")

    update = {}
    if "content" in data:
        content = _normalize_site_builder_text(data.get("content"), "Page content", trim=False)
        _validate_site_builder_blob_size(content, "Page content", _SITE_BUILDER_MAX_CONTENT_BYTES)
        update["content"] = content
        update["full_html"] = content
    if "editor_json" in data:
        update["editor_json"] = _normalize_site_builder_editor_json(data.get("editor_json"))
    if "page_css" in data:
        page_css = _normalize_site_builder_text(data.get("page_css"), "Page CSS", trim=False)
        _validate_site_builder_blob_size(page_css, "Page CSS", _SITE_BUILDER_MAX_PAGE_CSS_BYTES)
        update["page_css"] = page_css
    if "seo_title" in data:
        update["seo_title"] = _normalize_site_builder_text(
            data.get("seo_title"),
            "SEO title",
            max_length=_SITE_BUILDER_MAX_SEO_TITLE_LENGTH,
        )
    if "seo_description" in data:
        update["seo_description"] = _normalize_site_builder_text(
            data.get("seo_description"),
            "SEO description",
            max_length=_SITE_BUILDER_MAX_SEO_DESCRIPTION_LENGTH,
        )
    if "title" in data:
        update["title"] = _normalize_site_builder_text(
            data.get("title"),
            "Page title",
            max_length=_SITE_BUILDER_MAX_TITLE_LENGTH,
        )
    return update


def _summarize_delivery_detail(raw_value):
    detail = _safe_json_object(raw_value)
    if not detail:
        text = str(raw_value or "").strip()
        return text[:160]

    result = detail.get("result")
    if isinstance(result, dict):
        status = str(result.get("status") or result.get("state") or result.get("type") or "").strip()
        message_id = str(result.get("id") or result.get("messageId") or result.get("message_id") or "").strip()
        if status and message_id:
            return f"{status} - {message_id}"[:160]
        if status:
            return status[:160]
        if message_id:
            return f"Message ID {message_id}"[:160]
        rendered = json.dumps(result, separators=(",", ":"))
        return rendered[:160]

    if result:
        return str(result)[:160]
    if detail.get("error"):
        return str(detail.get("error"))[:160]
    rendered = json.dumps(detail, separators=(",", ":"))
    return rendered[:160]


def _split_text_lines(value, limit=24):
    parts = []
    seen = set()
    for raw_part in re.split(r"[\n,;|]+", str(value or "")):
        item = re.sub(r"\s+", " ", raw_part).strip()
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        parts.append(item[:120])
        if len(parts) >= limit:
            break
    return parts


def _default_lead_form_service_options(brand):
    options = _split_text_lines((brand or {}).get("primary_services") or "")
    if options:
        return options
    options = _split_text_lines((brand or {}).get("sales_bot_service_menu") or "")
    cleaned = []
    for option in options:
        cleaned.append(re.sub(r"^[-*\d.)\s]+", "", option).strip())
        if len(cleaned) >= 8:
            break
    return [item for item in cleaned if item]


def _default_warren_lead_form_config(brand=None):
    brand = brand or {}
    brand_name = (brand.get("display_name") or "Your Team").strip() or "Your Team"
    return {
        "enabled": False,
        "headline": f"Get a fast quote from {brand_name}",
        "intro": "Tell us what you need, where the job is, and how to reach you. Warren will open the lead instantly and can text back a quote or next step when texting is enabled.",
        "cta_label": "Request My Quote",
        "success_title": "Request received",
        "success_message": "We have your request. If texting is enabled for this brand and you opted in, watch for a text with a quote or next step shortly.",
        "service_label": "What do you need help with?",
        "details_label": "Tell us about the job",
        "service_options": _default_lead_form_service_options(brand),
        "show_service": True,
        "show_email": True,
        "show_company": False,
        "show_address": True,
        "show_message": True,
        "auto_text_enabled": True,
        "require_sms_consent": True,
        "consent_label": "I agree to receive text messages about my request, scheduling, and pricing.",
    }


def _normalize_warren_lead_form_config(raw_config, brand=None):
    defaults = _default_warren_lead_form_config(brand)
    config = dict(defaults)
    if isinstance(raw_config, str):
        raw_config = _safe_json_object(raw_config)
    if not isinstance(raw_config, dict):
        raw_config = {}

    for key in (
        "enabled",
        "show_service",
        "show_email",
        "show_company",
        "show_address",
        "show_message",
        "auto_text_enabled",
        "require_sms_consent",
    ):
        if key in raw_config:
            config[key] = bool(raw_config.get(key))

    for key, max_len in (
        ("headline", 140),
        ("intro", 800),
        ("cta_label", 60),
        ("success_title", 80),
        ("success_message", 400),
        ("service_label", 80),
        ("details_label", 80),
        ("consent_label", 220),
    ):
        value = raw_config.get(key)
        if value is None:
            continue
        config[key] = str(value).strip()[:max_len] or defaults[key]

    options = raw_config.get("service_options")
    if isinstance(options, str):
        options = _split_text_lines(options)
    elif isinstance(options, list):
        options = _split_text_lines("\n".join(str(item or "") for item in options))
    else:
        options = []
    config["service_options"] = options or defaults["service_options"]
    return config


def _lead_form_config_for_brand(brand):
    return _normalize_warren_lead_form_config((brand or {}).get("sales_bot_lead_form_config") or "{}", brand=brand)


def _lead_form_share_payload(brand):
    public_url = f"{_external_app_url()}{url_for('client_public.public_lead_form', brand_slug=brand['slug'])}"
    embed_url = f"{public_url}?embed=1"
    embed_code = (
        f'<iframe src="{embed_url}" title="{brand.get("display_name") or "Warren lead form"}" '
        'width="100%" height="840" style="border:0;border-radius:18px;overflow:hidden;" loading="lazy"></iframe>'
    )
    return {
        "public_url": public_url,
        "embed_url": embed_url,
        "embed_code": embed_code,
    }


def _normalize_client_commercial_text(value, max_len=4000):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    return str(value).strip()[:max_len]


def _normalize_client_commercial_website(value):
    website = _normalize_client_commercial_text(value, 500)
    if not website:
        return ""
    parsed = urlparse(website if website.startswith(("http://", "https://")) else f"https://{website}")
    host = (parsed.netloc or parsed.path or "").strip().lower()
    path = parsed.path if parsed.netloc else ""
    normalized = f"{host}{path}".rstrip("/")
    if normalized.startswith("www."):
        normalized = normalized[4:]
    if not normalized:
        return ""
    if parsed.scheme in {"http", "https"}:
        return f"{parsed.scheme}://{normalized}"
    return f"https://{normalized}"


def _normalize_client_commercial_emails(raw_value):
    if isinstance(raw_value, (list, tuple, set)):
        parts = list(raw_value)
    else:
        parts = re.split(r"[;,\n]+", str(raw_value or ""))
    emails = []
    seen = set()
    for part in parts:
        email = _normalize_client_commercial_text(part, 255).lower().strip(".,;:()[]{}<>")
        if not email or "@" not in email or email in seen:
            continue
        seen.add(email)
        emails.append(email)
    return emails[:5]


def _normalize_client_commercial_list(raw_value, *, max_items=10, item_max_len=180):
    if isinstance(raw_value, list):
        parts = raw_value
    else:
        parts = re.split(r"[\n,;]+", raw_value or "")
    items = []
    seen = set()
    for part in parts:
        value = _normalize_client_commercial_text(part, item_max_len)
        if not value:
            continue
        dedupe_key = value.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        items.append(value)
    return items[:max_items]


def _normalize_client_commercial_payload(raw_item, *, default_service_area=""):
    item = dict(raw_item) if isinstance(raw_item, dict) else {}
    source_details = _safe_json_object(item.get("source_details_json"))
    audit_snapshot = item.get("audit_snapshot") if isinstance(item.get("audit_snapshot"), dict) else _safe_json_object(item.get("audit_snapshot_json"))
    pain_points = item.get("pain_points") if isinstance(item.get("pain_points"), list) else _safe_json_list(item.get("pain_points_json"))
    qualification_answers = _safe_json_object(item.get("qualification_answers_json"))
    required_add_ons = item.get("required_add_ons") if isinstance(item.get("required_add_ons"), list) else _safe_json_list(item.get("required_add_ons_json"))
    walkthrough_photos = item.get("walkthrough_photo_urls") if isinstance(item.get("walkthrough_photo_urls"), list) else _safe_json_list(item.get("walkthrough_photo_urls_json"))
    website = _normalize_client_commercial_website(item.get("website") or source_details.get("website"))
    business_name = _normalize_client_commercial_text(
        item.get("business_name") or item.get("name") or item.get("contact_name") or "Commercial Prospect",
        160,
    ) or "Commercial Prospect"
    account_type = _normalize_client_commercial_text(item.get("prospect_type") or item.get("account_type"), 80)
    industry = _normalize_client_commercial_text(item.get("prospect_type_label") or item.get("industry") or account_type or "Commercial Property", 120)
    emails = _normalize_client_commercial_emails(item.get("emails") or source_details.get("emails") or item.get("email"))
    primary_email = emails[0] if emails else _normalize_client_commercial_text(item.get("email"), 255).lower()
    phone = _normalize_client_commercial_text(item.get("phone") or source_details.get("phone"), 80)
    source_query = _normalize_client_commercial_text(item.get("source_query") or source_details.get("source_query"), 255)
    search_criteria = _normalize_client_commercial_text(item.get("search_criteria") or source_details.get("search_criteria"), 220)
    address = _normalize_client_commercial_text(item.get("address") or source_details.get("address"), 255)
    maps_url = _normalize_client_commercial_text(item.get("maps_url") or source_details.get("maps_url"), 500)
    service_area = _normalize_client_commercial_text(item.get("service_area") or source_details.get("service_area") or default_service_area, 160)
    normalized_source_details = {
        "emails": emails,
        "address": address,
        "phone": phone,
        "website": website,
        "service_area": service_area,
        "prospect_type": account_type,
        "prospect_type_label": industry,
        "source_query": source_query,
        "search_criteria": search_criteria,
        "rating": item.get("rating") if item.get("rating") is not None else source_details.get("rating"),
        "review_count": item.get("review_count") if item.get("review_count") is not None else source_details.get("review_count") or 0,
        "maps_url": maps_url,
        "site_signals": _normalize_client_commercial_list(item.get("site_signals") or source_details.get("site_signals") or [], max_items=8, item_max_len=120),
        "scraped_page_count": _parse_int_range(item.get("scraped_page_count") or source_details.get("scraped_page_count"), maximum=20, default=0),
        "service_frequency_hint": _normalize_client_commercial_text(item.get("service_frequency_hint") or source_details.get("service_frequency_hint"), 40).lower(),
        "service_days_hint": _normalize_client_commercial_text(item.get("service_days_hint") or source_details.get("service_days_hint"), 120),
        "public_mentions": (item.get("public_mentions") if isinstance(item.get("public_mentions"), list) else source_details.get("public_mentions") or [])[:6],
        "complaint_signals": (item.get("complaint_signals") if isinstance(item.get("complaint_signals"), list) else source_details.get("complaint_signals") or [])[:8],
        "decision_maker_contacts": (item.get("decision_maker_contacts") if isinstance(item.get("decision_maker_contacts"), list) else source_details.get("decision_maker_contacts") or [])[:6],
        "decision_maker_role_hint": _normalize_client_commercial_text(item.get("decision_maker_role") or item.get("decision_maker_role_hint") or source_details.get("decision_maker_role_hint"), 160),
        "management_company": _normalize_client_commercial_text(item.get("management_company") or source_details.get("management_company"), 160),
        "management_url": _normalize_client_commercial_text(item.get("management_url") or source_details.get("management_url"), 500),
        "contact_urls": _normalize_client_commercial_list(item.get("contact_urls") or source_details.get("contact_urls") or [], max_items=6, item_max_len=500),
    }
    return {
        "name": business_name,
        "email": primary_email,
        "phone": phone,
        "business_name": business_name,
        "website": website,
        "industry": industry,
        "account_type": account_type,
        "service_area": service_area,
        "stage": _normalize_client_commercial_text(item.get("stage") or item.get("status") or "new", 40).lower() or "new",
        "source": _normalize_client_commercial_text(item.get("source") or "commercial_prospecting", 80) or "commercial_prospecting",
        "summary": _normalize_client_commercial_text(item.get("summary"), 400),
        "search_criteria": search_criteria,
        "source_details_json": json.dumps(normalized_source_details),
        "audit_snapshot_json": json.dumps(audit_snapshot if isinstance(audit_snapshot, dict) else {}),
        "qualification_answers_json": json.dumps(qualification_answers),
        "property_count": _normalize_client_commercial_text(item.get("property_count"), 160),
        "walkthrough_property_label": _normalize_client_commercial_text(item.get("walkthrough_property_label"), 160),
        "walkthrough_waste_station_count": _parse_int_range(item.get("walkthrough_waste_station_count"), maximum=500, default=0),
        "walkthrough_common_area_count": _parse_int_range(item.get("walkthrough_common_area_count"), maximum=500, default=0),
        "walkthrough_relief_area_count": _parse_int_range(item.get("walkthrough_relief_area_count"), maximum=500, default=0),
        "pet_traffic_estimate": _normalize_client_commercial_text(item.get("pet_traffic_estimate"), 120),
        "site_condition": _normalize_client_commercial_text(item.get("site_condition"), 220),
        "access_notes": _normalize_client_commercial_text(item.get("access_notes"), 1000),
        "gate_notes": _normalize_client_commercial_text(item.get("gate_notes"), 500),
        "disposal_notes": _normalize_client_commercial_text(item.get("disposal_notes"), 500),
        "walkthrough_notes": _normalize_client_commercial_text(item.get("walkthrough_notes"), 1200),
        "required_add_ons_json": json.dumps(_normalize_client_commercial_list(required_add_ons, max_items=8, item_max_len=120)),
        "walkthrough_photo_urls_json": json.dumps(_normalize_client_commercial_list(walkthrough_photos, max_items=8, item_max_len=500)),
        "walkthrough_completed_at": _normalize_client_commercial_text(item.get("walkthrough_completed_at"), 40),
        "service_frequency_hint": _normalize_client_commercial_text(item.get("service_frequency_hint") or source_details.get("service_frequency_hint"), 40).lower(),
        "service_days_hint": _normalize_client_commercial_text(item.get("service_days_hint") or source_details.get("service_days_hint"), 120),
        "decision_maker_role": _normalize_client_commercial_text(item.get("decision_maker_role") or source_details.get("decision_maker_role_hint"), 160),
        "current_vendor_status": _normalize_client_commercial_text(item.get("current_vendor_status"), 220),
        "outreach_angle": _normalize_client_commercial_text(item.get("outreach_angle"), 160),
        "proposal_status": _normalize_client_commercial_text(item.get("proposal_status"), 80),
        "pain_points_json": json.dumps([_normalize_client_commercial_text(point, 220) for point in pain_points if _normalize_client_commercial_text(point, 220)]),
        "next_action": _normalize_client_commercial_text(item.get("next_action"), 220),
        "outreach_subject_override": _normalize_client_commercial_text(item.get("outreach_subject_override"), 255),
        "outreach_email_body_override": _normalize_client_commercial_text(item.get("outreach_email_body_override"), 6000),
        "outreach_call_opener_override": _normalize_client_commercial_text(item.get("outreach_call_opener_override"), 2000),
        "outreach_rewrite_prompt": _normalize_client_commercial_text(item.get("outreach_rewrite_prompt"), 1000),
        "outreach_last_rewrite_at": _normalize_client_commercial_text(item.get("outreach_last_rewrite_at"), 40),
        "proposal_builder_json": json.dumps(_normalize_client_commercial_proposal_builder(item.get("proposal_builder_json"), prospect=item)),
    }


def _merge_client_commercial_payload(existing_payload, incoming_payload):
    existing = _normalize_client_commercial_payload(existing_payload)
    incoming = _normalize_client_commercial_payload(incoming_payload, default_service_area=existing.get("service_area") or "")
    existing_source_details = _safe_json_object(existing.get("source_details_json"))
    incoming_source_details = _safe_json_object(incoming.get("source_details_json"))
    existing_audit_snapshot = _safe_json_object(existing.get("audit_snapshot_json"))
    incoming_audit_snapshot = _safe_json_object(incoming.get("audit_snapshot_json"))
    existing_answers = _safe_json_object(existing.get("qualification_answers_json"))
    incoming_answers = _safe_json_object(incoming.get("qualification_answers_json"))
    merged_emails = _normalize_client_commercial_emails(
        (incoming_source_details.get("emails") or [])
        + (existing_source_details.get("emails") or [])
        + [incoming.get("email"), existing.get("email")]
    )
    merged_source_details = {
        **existing_source_details,
        **incoming_source_details,
        "emails": merged_emails,
        "address": incoming_source_details.get("address") or existing_source_details.get("address") or "",
        "phone": incoming_source_details.get("phone") or existing_source_details.get("phone") or incoming.get("phone") or existing.get("phone") or "",
        "website": incoming_source_details.get("website") or existing_source_details.get("website") or incoming.get("website") or existing.get("website") or "",
        "service_area": incoming_source_details.get("service_area") or existing_source_details.get("service_area") or incoming.get("service_area") or existing.get("service_area") or "",
        "prospect_type": incoming_source_details.get("prospect_type") or existing_source_details.get("prospect_type") or incoming.get("account_type") or existing.get("account_type") or "",
        "prospect_type_label": incoming_source_details.get("prospect_type_label") or existing_source_details.get("prospect_type_label") or incoming.get("industry") or existing.get("industry") or "",
        "source_query": incoming_source_details.get("source_query") or existing_source_details.get("source_query") or "",
        "search_criteria": incoming_source_details.get("search_criteria") or existing_source_details.get("search_criteria") or incoming.get("search_criteria") or existing.get("search_criteria") or "",
        "maps_url": incoming_source_details.get("maps_url") or existing_source_details.get("maps_url") or "",
        "rating": incoming_source_details.get("rating") if incoming_source_details.get("rating") is not None else existing_source_details.get("rating"),
        "review_count": incoming_source_details.get("review_count") if incoming_source_details.get("review_count") not in (None, "") else existing_source_details.get("review_count") or 0,
        "public_mentions": (incoming_source_details.get("public_mentions") or existing_source_details.get("public_mentions") or [])[:6],
        "complaint_signals": (incoming_source_details.get("complaint_signals") or existing_source_details.get("complaint_signals") or [])[:8],
        "decision_maker_contacts": (incoming_source_details.get("decision_maker_contacts") or existing_source_details.get("decision_maker_contacts") or [])[:6],
        "decision_maker_role_hint": incoming_source_details.get("decision_maker_role_hint") or existing_source_details.get("decision_maker_role_hint") or "",
        "management_company": incoming_source_details.get("management_company") or existing_source_details.get("management_company") or "",
        "management_url": incoming_source_details.get("management_url") or existing_source_details.get("management_url") or "",
        "contact_urls": incoming_source_details.get("contact_urls") or existing_source_details.get("contact_urls") or [],
    }
    merged_payload = {
        "name": incoming.get("name") or existing.get("name") or "Commercial Prospect",
        "email": merged_emails[0] if merged_emails else incoming.get("email") or existing.get("email") or "",
        "phone": incoming.get("phone") or existing.get("phone") or "",
        "business_name": incoming.get("business_name") or existing.get("business_name") or incoming.get("name") or existing.get("name") or "Commercial Prospect",
        "website": incoming.get("website") or existing.get("website") or "",
        "industry": incoming.get("industry") or existing.get("industry") or "Commercial Property",
        "account_type": incoming.get("account_type") or existing.get("account_type") or "",
        "service_area": incoming.get("service_area") or existing.get("service_area") or "",
        "stage": existing.get("stage") or incoming.get("stage") or "new",
        "source": incoming.get("source") or existing.get("source") or "commercial_prospecting",
        "summary": incoming.get("summary") or existing.get("summary") or "",
        "search_criteria": incoming.get("search_criteria") or existing.get("search_criteria") or "",
        "source_details_json": json.dumps(merged_source_details),
        "audit_snapshot_json": json.dumps(incoming_audit_snapshot or existing_audit_snapshot),
        "qualification_answers_json": json.dumps(incoming_answers or existing_answers),
        "property_count": incoming.get("property_count") or existing.get("property_count") or "",
        "walkthrough_property_label": incoming.get("walkthrough_property_label") or existing.get("walkthrough_property_label") or "",
        "walkthrough_waste_station_count": incoming.get("walkthrough_waste_station_count") if incoming.get("walkthrough_waste_station_count") not in (None, "") else existing.get("walkthrough_waste_station_count") or 0,
        "walkthrough_common_area_count": incoming.get("walkthrough_common_area_count") if incoming.get("walkthrough_common_area_count") not in (None, "") else existing.get("walkthrough_common_area_count") or 0,
        "walkthrough_relief_area_count": incoming.get("walkthrough_relief_area_count") if incoming.get("walkthrough_relief_area_count") not in (None, "") else existing.get("walkthrough_relief_area_count") or 0,
        "pet_traffic_estimate": incoming.get("pet_traffic_estimate") or existing.get("pet_traffic_estimate") or "",
        "site_condition": incoming.get("site_condition") or existing.get("site_condition") or "",
        "access_notes": incoming.get("access_notes") or existing.get("access_notes") or "",
        "gate_notes": incoming.get("gate_notes") or existing.get("gate_notes") or "",
        "disposal_notes": incoming.get("disposal_notes") or existing.get("disposal_notes") or "",
        "walkthrough_notes": incoming.get("walkthrough_notes") or existing.get("walkthrough_notes") or "",
        "required_add_ons_json": incoming.get("required_add_ons_json") if _safe_json_list(incoming.get("required_add_ons_json")) else existing.get("required_add_ons_json") or "[]",
        "walkthrough_photo_urls_json": incoming.get("walkthrough_photo_urls_json") if _safe_json_list(incoming.get("walkthrough_photo_urls_json")) else existing.get("walkthrough_photo_urls_json") or "[]",
        "walkthrough_completed_at": incoming.get("walkthrough_completed_at") or existing.get("walkthrough_completed_at") or "",
        "service_frequency_hint": incoming.get("service_frequency_hint") or existing.get("service_frequency_hint") or "",
        "service_days_hint": incoming.get("service_days_hint") or existing.get("service_days_hint") or "",
        "decision_maker_role": incoming.get("decision_maker_role") or existing.get("decision_maker_role") or "",
        "current_vendor_status": incoming.get("current_vendor_status") or existing.get("current_vendor_status") or "",
        "outreach_angle": incoming.get("outreach_angle") or existing.get("outreach_angle") or "",
        "proposal_status": incoming.get("proposal_status") or existing.get("proposal_status") or "",
        "pain_points_json": incoming.get("pain_points_json") if _safe_json_list(incoming.get("pain_points_json")) else existing.get("pain_points_json") or "[]",
        "next_action": incoming.get("next_action") or existing.get("next_action") or "",
        "outreach_subject_override": incoming.get("outreach_subject_override") or existing.get("outreach_subject_override") or "",
        "outreach_email_body_override": incoming.get("outreach_email_body_override") or existing.get("outreach_email_body_override") or "",
        "outreach_call_opener_override": incoming.get("outreach_call_opener_override") or existing.get("outreach_call_opener_override") or "",
        "outreach_rewrite_prompt": incoming.get("outreach_rewrite_prompt") or existing.get("outreach_rewrite_prompt") or "",
        "outreach_last_rewrite_at": incoming.get("outreach_last_rewrite_at") or existing.get("outreach_last_rewrite_at") or "",
        "proposal_builder_json": incoming.get("proposal_builder_json") or existing.get("proposal_builder_json") or "{}",
    }
    return _normalize_client_commercial_payload(merged_payload)


def _build_client_commercial_summary(prospect, brief):
    type_label = prospect.get("industry") or prospect.get("account_type") or "Commercial"
    return f"Commercial target - {type_label}. Proposal: {brief['proposal_readiness']['label']}."


def _build_client_commercial_research(prospect):
    source_details = _safe_json_object((prospect or {}).get("source_details_json"))
    raw_mentions = source_details.get("public_mentions") or []
    raw_complaints = source_details.get("complaint_signals") or []
    raw_contacts = source_details.get("decision_maker_contacts") or []
    mentions = []
    for item in raw_mentions[:6]:
        if not isinstance(item, dict):
            continue
        title = _normalize_client_commercial_text(item.get("title"), 160)
        url = _normalize_client_commercial_text(item.get("url"), 500)
        snippet = _normalize_client_commercial_text(item.get("snippet"), 280)
        if not (title or url or snippet):
            continue
        mentions.append({
            "title": title or url,
            "url": url,
            "snippet": snippet,
            "query": _normalize_client_commercial_text(item.get("query"), 120),
            "same_domain": bool(item.get("same_domain")),
        })

    contacts = []
    for item in raw_contacts[:6]:
        if not isinstance(item, dict):
            continue
        contact = {
            "name": _normalize_client_commercial_text(item.get("name"), 120),
            "role": _normalize_client_commercial_text(item.get("role"), 120),
            "email": _normalize_client_commercial_text(item.get("email"), 255),
            "phone": _normalize_client_commercial_text(item.get("phone"), 80),
            "source_url": _normalize_client_commercial_text(item.get("source_url"), 500),
            "evidence": _normalize_client_commercial_text(item.get("evidence"), 200),
            "priority_score": int(item.get("priority_score") or 0),
            "priority_label": _normalize_client_commercial_text(item.get("priority_label"), 40),
        }
        if any(contact.values()):
            contacts.append(contact)

    complaints = []
    for item in raw_complaints[:8]:
        if not isinstance(item, dict):
            continue
        complaint = {
            "category": _normalize_client_commercial_text(item.get("category"), 40),
            "label": _normalize_client_commercial_text(item.get("label"), 120),
            "title": _normalize_client_commercial_text(item.get("title"), 160),
            "url": _normalize_client_commercial_text(item.get("url"), 500),
            "snippet": _normalize_client_commercial_text(item.get("snippet"), 220),
        }
        if complaint["label"] or complaint["snippet"]:
            complaints.append(complaint)

    return {
        "mentions": mentions,
        "complaints": complaints,
        "contacts": contacts,
        "decision_maker_role_hint": _normalize_client_commercial_text((prospect or {}).get("decision_maker_role") or source_details.get("decision_maker_role_hint"), 160),
        "management_company": _normalize_client_commercial_text(source_details.get("management_company"), 160),
        "management_url": _normalize_client_commercial_text(source_details.get("management_url"), 500),
        "contact_urls": _normalize_client_commercial_list(source_details.get("contact_urls") or [], max_items=6, item_max_len=500),
    }


def _promote_client_commercial_research_to_worksheet(prospect):
    updated = _normalize_client_commercial_payload(prospect)
    source_details = _safe_json_object(updated.get("source_details_json"))
    answers = _safe_json_object(updated.get("qualification_answers_json"))
    contacts = source_details.get("decision_maker_contacts") or []
    complaints = source_details.get("complaint_signals") or []
    categories = {(_normalize_client_commercial_text(item.get("category"), 40) or "") for item in complaints if isinstance(item, dict)}
    management_company = _normalize_client_commercial_text(source_details.get("management_company"), 160)

    if not updated.get("decision_maker_role"):
        updated["decision_maker_role"] = _normalize_client_commercial_text(source_details.get("decision_maker_role_hint"), 160)

    if not updated.get("current_vendor_status") and management_company:
        updated["current_vendor_status"] = f"Managed by {management_company}"

    if not answers.get("service_scope"):
        scope_items = []
        if int(updated.get("walkthrough_waste_station_count") or 0) > 0:
            scope_items.append("Waste station servicing")
        if int(updated.get("walkthrough_relief_area_count") or 0) > 0:
            scope_items.append("Dog area cleanup")
        if int(updated.get("walkthrough_common_area_count") or 0) > 0:
            scope_items.append("Common-area policing")
        if scope_items:
            answers["service_scope"] = ", ".join(scope_items)

    if not answers.get("commercial_goal") and categories:
        goal_bits = []
        if "dog_area" in categories:
            goal_bits.append("reduce dog-area complaints")
        if "cleanliness" in categories:
            goal_bits.append("improve cleanliness and odor control")
        if "pet_waste" in categories:
            goal_bits.append("tighten pet waste service proof")
        if goal_bits:
            answers["commercial_goal"] = ", ".join(goal_bits).capitalize()

    if not updated.get("site_condition") and categories:
        condition_bits = []
        if "dog_area" in categories:
            condition_bits.append("public review snippets point to dog-area friction")
        if "cleanliness" in categories:
            condition_bits.append("cleanliness or odor complaints")
        if "pet_waste" in categories:
            condition_bits.append("pet waste coverage issues")
        if condition_bits:
            updated["site_condition"] = "; ".join(condition_bits).capitalize()

    if not updated.get("walkthrough_notes") and complaints:
        note_lines = []
        for item in complaints[:3]:
            if not isinstance(item, dict):
                continue
            snippet = _normalize_client_commercial_text(item.get("snippet"), 180)
            label = _normalize_client_commercial_text(item.get("label"), 120)
            if snippet:
                note_lines.append(f"{label}: {snippet}")
        if note_lines:
            updated["walkthrough_notes"] = "Public complaint clues:\n" + "\n".join(note_lines)

    current_add_ons = _safe_json_list(updated.get("required_add_ons_json"))
    if not current_add_ons:
        suggested_add_ons = []
        if int(updated.get("walkthrough_waste_station_count") or 0) > 0:
            suggested_add_ons.append("Bag refill")
        if "cleanliness" in categories:
            suggested_add_ons.append("Deodorizer")
        if "require removal from the property" in (updated.get("disposal_notes") or "").lower():
            suggested_add_ons.append("Off-site waste haul")
        updated["required_add_ons_json"] = json.dumps(_normalize_client_commercial_list(suggested_add_ons, max_items=8, item_max_len=120))

    if not updated.get("contact_name") and contacts:
        top_contact = contacts[0] if isinstance(contacts[0], dict) else {}
        updated["contact_name"] = _normalize_client_commercial_text(top_contact.get("name"), 120)

    updated["qualification_answers_json"] = json.dumps(answers)
    return updated


def _client_commercial_identity_key(prospect):
    website = _normalize_client_commercial_website(prospect.get("website"))
    email = _normalize_client_commercial_text(prospect.get("email"), 255).lower()
    business_name = _normalize_client_commercial_text(prospect.get("business_name") or prospect.get("name"), 160).lower()
    return website or email or business_name


def _parse_int_range(value, *, minimum=0, maximum=100000, default=0):
    try:
        parsed = int(float(value or 0))
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _parse_float_range(value, *, minimum=0.0, maximum=1000000.0, default=0.0):
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _parse_bool_flag(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


COMMERCIAL_PROPOSAL_FREQUENCY = {
    "every_2_weeks": {"label": "Every 2 weeks", "visits_per_month": 2.17},
    "1x_week": {"label": "1x per week", "visits_per_month": 4.33},
    "2x_week": {"label": "2x per week", "visits_per_month": 8.66},
    "3x_week": {"label": "3x per week", "visits_per_month": 13.0},
    "5x_week": {"label": "5x per week", "visits_per_month": 21.67},
    "7x_week": {"label": "7x per week", "visits_per_month": 30.33},
}

COMMERCIAL_PROPOSAL_PACKAGES = {
    "basic": {
        "label": "Basic",
        "description": "Core cleanup coverage for common pet-waste problem areas.",
    },
    "standard": {
        "label": "Standard",
        "description": "Cleanup plus station support and manager-ready reporting.",
    },
    "premium": {
        "label": "Premium",
        "description": "Full presentation package with service-proof and deodorizer support.",
    },
}

COMMERCIAL_WORKSHEET_SITE_FIELDS = [
    {
        "key": "walkthrough_property_label",
        "label": "Site Label",
        "placeholder": "North dog park / courtyard loop / Building A grounds",
        "multiline": False,
        "help": "Use the label your team would recognize on the route sheet.",
    },
    {
        "key": "property_count",
        "label": "Units Or Portfolio Size",
        "placeholder": "214 units across 3 buildings",
        "multiline": False,
        "help": "This is the size anchor for scope and pricing.",
    },
    {
        "key": "pet_traffic_estimate",
        "label": "Traffic Pattern",
        "placeholder": "Heavy after work hours and weekends",
        "multiline": False,
        "help": "Keep this short and operational.",
    },
    {
        "key": "site_condition",
        "label": "Current Site Condition",
        "placeholder": "Complaint-prone near dog run, common areas mostly clean",
        "multiline": True,
        "help": "What is actually happening onsite right now?",
    },
    {
        "key": "access_notes",
        "label": "Access Notes",
        "placeholder": "Gate opened by leasing before 8am. Dumpster access through rear lane.",
        "multiline": True,
        "help": "Capture anything that affects service execution.",
    },
    {
        "key": "gate_notes",
        "label": "Gate / Security Notes",
        "placeholder": "Photo the latch after each visit. Call manager if gate is jammed.",
        "multiline": True,
        "help": "Only add this if it matters operationally.",
    },
    {
        "key": "disposal_notes",
        "label": "Disposal Notes",
        "placeholder": "Bag waste goes to rear dumpster enclosure by maintenance shed.",
        "multiline": True,
        "help": "Keep disposal instructions practical.",
    },
    {
        "key": "walkthrough_notes",
        "label": "Internal Notes",
        "placeholder": "Manager wants recap tied to tenant complaints and station refill proof.",
        "multiline": True,
        "help": "Use this for anything your team should not lose.",
    },
]

COMMERCIAL_WORKSHEET_SCOPE_FIELDS = [
    {
        "key": "required_add_ons",
        "label": "Add-Ons Needed",
        "placeholder": "Bag refill\nDeodorizer\nExtra common-area pass",
        "multiline": True,
        "help": "One item per line.",
    },
]


def _default_client_commercial_proposal_builder(brand=None, prospect=None):
    average_ticket = _parse_float_range((brand or {}).get("crm_avg_service_price"), minimum=0, maximum=5000, default=65.0)
    seed_rate = max(14.0, round(average_ticket / 4.0, 2))
    property_count = _normalize_client_commercial_text((prospect or {}).get("property_count"), 120)
    waste_station_count = _parse_int_range((prospect or {}).get("walkthrough_waste_station_count"), maximum=500, default=0)
    common_area_count = _parse_int_range((prospect or {}).get("walkthrough_common_area_count"), maximum=500, default=1)
    relief_area_count = _parse_int_range((prospect or {}).get("walkthrough_relief_area_count"), maximum=500, default=0)
    service_frequency_hint = _normalize_client_commercial_text((prospect or {}).get("service_frequency_hint"), 40).lower()
    if service_frequency_hint not in COMMERCIAL_PROPOSAL_FREQUENCY:
        service_frequency_hint = "5x_week"
    service_days_hint = _normalize_client_commercial_text((prospect or {}).get("service_days_hint"), 120)
    return {
        "selected_package": "standard",
        "service_frequency": service_frequency_hint,
        "service_days": service_days_hint or ("Monday-Friday" if service_frequency_hint in {"5x_week", "7x_week"} else ""),
        "property_count": property_count,
        "waste_station_count": waste_station_count,
        "waste_station_rate": seed_rate,
        "common_area_count": common_area_count,
        "common_area_rate": round(seed_rate * 2.25, 2),
        "relief_area_count": relief_area_count,
        "relief_area_rate": round(seed_rate * 2.75, 2),
        "bag_refill_included": True,
        "bag_refill_fee": round(seed_rate * 1.5, 2),
        "deodorizer_included": False,
        "deodorizer_fee": round(seed_rate * 1.2, 2),
        "initial_cleanup_required": True,
        "initial_cleanup_fee": round(seed_rate * 8.0, 2),
        "monthly_management_fee": 0.0,
        "notes": "",
        "scope_summary": "Commercial pet waste stations, common area policing, and service reporting.",
    }


def _build_client_commercial_worksheet(prospect, brief, brand=None):
    from webapp.commercial_strategy import COMMERCIAL_QUALIFICATION_CORE_FIELDS, COMMERCIAL_QUALIFICATION_FIELDS

    answers = _safe_json_object((prospect or {}).get("qualification_answers_json"))
    required_add_ons = _safe_json_list((prospect or {}).get("required_add_ons_json"))
    sections = []

    buyer_fields = []
    for field in COMMERCIAL_QUALIFICATION_CORE_FIELDS:
        value = _normalize_client_commercial_text((prospect or {}).get(field["key"]), 160)
        buyer_fields.append({
            "key": field["key"],
            "label": field["label"],
            "placeholder": field["placeholder"],
            "multiline": field["multiline"],
            "value": value,
            "help": field["prompt"],
            "complete": bool(value),
        })
    for field in COMMERCIAL_QUALIFICATION_FIELDS:
        value = _normalize_client_commercial_text(answers.get(field["key"]), 1200)
        buyer_fields.append({
            "key": field["key"],
            "label": field["label"],
            "placeholder": field.get("placeholder") or "",
            "multiline": field["multiline"],
            "value": value,
            "help": field["prompt"],
            "complete": bool(value),
        })

    site_fields = []
    for field in COMMERCIAL_WORKSHEET_SITE_FIELDS:
        value = _normalize_client_commercial_text((prospect or {}).get(field["key"]), 1200)
        site_fields.append({
            **field,
            "value": value,
            "complete": bool(value),
        })

    count_fields = [
        {
            "key": "walkthrough_waste_station_count",
            "label": "Waste Stations",
            "value": int((prospect or {}).get("walkthrough_waste_station_count") or 0),
        },
        {
            "key": "walkthrough_common_area_count",
            "label": "Common Areas",
            "value": int((prospect or {}).get("walkthrough_common_area_count") or 0),
        },
        {
            "key": "walkthrough_relief_area_count",
            "label": "Relief Areas",
            "value": int((prospect or {}).get("walkthrough_relief_area_count") or 0),
        },
    ]

    scope_fields = []
    for field in COMMERCIAL_WORKSHEET_SCOPE_FIELDS:
        value = "\n".join(required_add_ons) if field["key"] == "required_add_ons" else ""
        scope_fields.append({
            **field,
            "value": value,
            "complete": bool(value.strip()),
        })

    sections.append({
        "key": "buyer",
        "title": "Buyer Snapshot",
        "description": "Who buys, what they care about, and what has to be true before you quote.",
        "fields": buyer_fields,
        "complete": sum(1 for field in buyer_fields if field["complete"]),
        "total": len(buyer_fields),
    })
    sections.append({
        "key": "site",
        "title": "Site Reality",
        "description": "Just enough onsite detail for routing, operations, and a credible scope.",
        "fields": site_fields,
        "count_fields": count_fields,
        "complete": sum(1 for field in site_fields if field["complete"]) + sum(1 for field in count_fields if field["value"] > 0),
        "total": len(site_fields) + len(count_fields),
    })
    sections.append({
        "key": "scope",
        "title": "Offer Shape",
        "description": "What should be in scope if this turns into a real commercial account.",
        "fields": scope_fields,
        "complete": sum(1 for field in scope_fields if field["complete"]),
        "total": len(scope_fields),
    })

    total_complete = sum(section["complete"] for section in sections)
    total_fields = sum(section["total"] for section in sections)
    return {
        "sections": sections,
        "total_complete": total_complete,
        "total_fields": total_fields,
        "completion_percent": int(round((total_complete / total_fields) * 100)) if total_fields else 0,
        "service_pitch": (brief or {}).get("service_pitch") or "",
        "outreach_angle": (brief or {}).get("outreach_angle") or "",
        "next_action": ((brief or {}).get("next_actions") or [""])[0],
    }


def _apply_client_commercial_worksheet_form(prospect, form_data):
    from webapp.commercial_strategy import COMMERCIAL_QUALIFICATION_CORE_FIELDS, COMMERCIAL_QUALIFICATION_FIELDS

    updated = _normalize_client_commercial_payload(prospect)
    answers = _safe_json_object(updated.get("qualification_answers_json"))

    for field in COMMERCIAL_QUALIFICATION_CORE_FIELDS:
        if field["key"] in form_data:
            updated[field["key"]] = _normalize_client_commercial_text(form_data.get(field["key"]), 220)
    for field in COMMERCIAL_QUALIFICATION_FIELDS:
        if field["key"] in form_data:
            answers[field["key"]] = _normalize_client_commercial_text(form_data.get(field["key"]), 1200)

    updated["qualification_answers_json"] = json.dumps(answers)
    text_fields = {
        "walkthrough_property_label": 160,
        "pet_traffic_estimate": 120,
        "site_condition": 220,
        "access_notes": 1000,
        "gate_notes": 500,
        "disposal_notes": 500,
        "walkthrough_notes": 1200,
        "property_count": 160,
    }
    for key, max_len in text_fields.items():
        if key in form_data:
            updated[key] = _normalize_client_commercial_text(form_data.get(key), max_len)

    count_fields = (
        "walkthrough_waste_station_count",
        "walkthrough_common_area_count",
        "walkthrough_relief_area_count",
    )
    for key in count_fields:
        if key in form_data:
            updated[key] = _parse_int_range(form_data.get(key), maximum=500, default=updated.get(key) or 0)

    if "required_add_ons" in form_data:
        updated["required_add_ons_json"] = json.dumps(_normalize_client_commercial_list(form_data.get("required_add_ons") or "", max_items=8, item_max_len=120))

    if any([
        updated.get("walkthrough_property_label"),
        updated.get("walkthrough_waste_station_count"),
        updated.get("walkthrough_common_area_count"),
        updated.get("walkthrough_relief_area_count"),
        updated.get("site_condition"),
        updated.get("access_notes"),
        updated.get("walkthrough_notes"),
    ]):
        updated["walkthrough_completed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return updated


def _merge_client_commercial_ai_worksheet_suggestions(prospect, suggestions):
    from webapp.commercial_strategy import COMMERCIAL_QUALIFICATION_CORE_FIELDS, COMMERCIAL_QUALIFICATION_FIELDS

    updated = _normalize_client_commercial_payload(prospect)
    answers = _safe_json_object(updated.get("qualification_answers_json"))

    direct_keys = {field["key"] for field in COMMERCIAL_QUALIFICATION_CORE_FIELDS}
    direct_keys.update({
        "walkthrough_property_label",
        "pet_traffic_estimate",
        "site_condition",
        "access_notes",
        "gate_notes",
        "disposal_notes",
        "walkthrough_notes",
    })
    answer_keys = {field["key"] for field in COMMERCIAL_QUALIFICATION_FIELDS}

    for key in direct_keys:
        current_value = _normalize_client_commercial_text(updated.get(key), 1200)
        suggested_value = _normalize_client_commercial_text((suggestions or {}).get(key), 1200)
        if not current_value and suggested_value:
            updated[key] = suggested_value

    for key in answer_keys:
        current_value = _normalize_client_commercial_text(answers.get(key), 1200)
        suggested_value = _normalize_client_commercial_text((suggestions or {}).get(key), 1200)
        if not current_value and suggested_value:
            answers[key] = suggested_value

    suggested_add_ons = (suggestions or {}).get("required_add_ons")
    current_add_ons = _safe_json_list(updated.get("required_add_ons_json"))
    if not current_add_ons:
        updated["required_add_ons_json"] = json.dumps(_normalize_client_commercial_list(suggested_add_ons or "", max_items=8, item_max_len=120))

    updated["qualification_answers_json"] = json.dumps(answers)
    return updated


def _assist_client_commercial_worksheet(brand, prospect, brief):
    api_key = _get_openai_api_key(brand)
    if not api_key:
        raise ValueError("No OpenAI API key configured. Add one in Connections.")

    import requests as req

    model = _pick_ai_model(brand, "chat")
    prompt = {
        "brand": {
            "display_name": (brand or {}).get("display_name") or "",
            "industry": (brand or {}).get("industry") or "",
            "service_area": (brand or {}).get("service_area") or "",
            "primary_services": (brand or {}).get("primary_services") or "",
            "active_offers": (brand or {}).get("active_offers") or "",
            "sales_bot_service_menu": (brand or {}).get("sales_bot_service_menu") or "",
        },
        "prospect": {
            "business_name": (prospect or {}).get("business_name") or "",
            "industry": (prospect or {}).get("industry") or "",
            "account_type": (prospect or {}).get("account_type") or "",
            "service_area": (prospect or {}).get("service_area") or "",
            "website": (prospect or {}).get("website") or "",
            "source_details": _safe_json_object((prospect or {}).get("source_details_json")),
            "audit_snapshot": _safe_json_object((prospect or {}).get("audit_snapshot_json")),
            "current_answers": _safe_json_object((prospect or {}).get("qualification_answers_json")),
        },
        "brief": {
            "service_pitch": (brief or {}).get("service_pitch") or "",
            "outreach_angle": (brief or {}).get("outreach_angle") or "",
            "pain_points": (brief or {}).get("pain_points") or [],
            "audit_findings": (brief or {}).get("audit_findings") or [],
            "qualification_questions": (brief or {}).get("qualification_questions") or [],
        },
    }
    system_prompt = (
        "You are helping a local service operator fill out a commercial lead worksheet. "
        "Only suggest concise, plausible worksheet entries grounded in the provided context. "
        "Do not invent precise counts, addresses, or buyer names if they are not supported. "
        "Leave unknown fields as empty strings. Do not use em dashes. Return valid JSON only."
    )
    user_prompt = (
        "Fill in helpful draft values for missing commercial worksheet fields. Return JSON only with these keys: "
        "decision_maker_role, current_vendor_status, service_scope, buying_timeline, decision_process, commercial_goal, budget_range, "
        "walkthrough_property_label, pet_traffic_estimate, site_condition, access_notes, gate_notes, disposal_notes, walkthrough_notes, required_add_ons.\n\n"
        + json.dumps(prompt)
    )
    response = req.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "temperature": 0.4,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        },
        timeout=45,
    )
    if response.status_code >= 400:
        raise ValueError(f"OpenAI request failed ({response.status_code}): {response.text[:200]}")
    payload = response.json() if hasattr(response, "json") else {}
    content = ((((payload or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or "")
    parsed = _extract_json_object_from_ai_text(content)
    if not parsed:
        raise ValueError("AI worksheet assist returned an empty result.")
    return parsed


def _normalize_client_commercial_proposal_builder(raw_value, *, brand=None, prospect=None):
    defaults = _default_client_commercial_proposal_builder(brand=brand, prospect=prospect)
    raw = _safe_json_object(raw_value)
    package_key = _normalize_client_commercial_text(raw.get("selected_package"), 40).lower() or defaults["selected_package"]
    if package_key not in COMMERCIAL_PROPOSAL_PACKAGES:
        package_key = defaults["selected_package"]
    frequency_key = _normalize_client_commercial_text(raw.get("service_frequency"), 40).lower() or defaults["service_frequency"]
    if frequency_key not in COMMERCIAL_PROPOSAL_FREQUENCY:
        frequency_key = defaults["service_frequency"]
    normalized = {
        "selected_package": package_key,
        "service_frequency": frequency_key,
        "service_days": _normalize_client_commercial_text(raw.get("service_days"), 120) or defaults["service_days"],
        "property_count": _normalize_client_commercial_text(raw.get("property_count"), 120) or defaults["property_count"],
        "waste_station_count": _parse_int_range(raw.get("waste_station_count"), maximum=500, default=defaults["waste_station_count"]),
        "waste_station_rate": _parse_float_range(raw.get("waste_station_rate"), maximum=5000, default=defaults["waste_station_rate"]),
        "common_area_count": _parse_int_range(raw.get("common_area_count"), maximum=500, default=defaults["common_area_count"]),
        "common_area_rate": _parse_float_range(raw.get("common_area_rate"), maximum=5000, default=defaults["common_area_rate"]),
        "relief_area_count": _parse_int_range(raw.get("relief_area_count"), maximum=500, default=defaults["relief_area_count"]),
        "relief_area_rate": _parse_float_range(raw.get("relief_area_rate"), maximum=5000, default=defaults["relief_area_rate"]),
        "bag_refill_included": _parse_bool_flag(raw.get("bag_refill_included")) if raw else defaults["bag_refill_included"],
        "bag_refill_fee": _parse_float_range(raw.get("bag_refill_fee"), maximum=5000, default=defaults["bag_refill_fee"]),
        "deodorizer_included": _parse_bool_flag(raw.get("deodorizer_included")) if raw else defaults["deodorizer_included"],
        "deodorizer_fee": _parse_float_range(raw.get("deodorizer_fee"), maximum=5000, default=defaults["deodorizer_fee"]),
        "initial_cleanup_required": _parse_bool_flag(raw.get("initial_cleanup_required")) if raw else defaults["initial_cleanup_required"],
        "initial_cleanup_fee": _parse_float_range(raw.get("initial_cleanup_fee"), maximum=25000, default=defaults["initial_cleanup_fee"]),
        "monthly_management_fee": _parse_float_range(raw.get("monthly_management_fee"), maximum=25000, default=defaults["monthly_management_fee"]),
        "scope_summary": _normalize_client_commercial_text(raw.get("scope_summary"), 300) or defaults["scope_summary"],
        "notes": _normalize_client_commercial_text(raw.get("notes"), 1200),
    }
    return normalized


def _client_commercial_builder_for_package(builder, package_key):
    package = COMMERCIAL_PROPOSAL_PACKAGES.get(package_key, COMMERCIAL_PROPOSAL_PACKAGES["standard"])
    packaged = dict(builder)
    packaged["selected_package"] = package_key
    if package_key == "basic":
        packaged["bag_refill_included"] = False
        packaged["deodorizer_included"] = False
        packaged["monthly_management_fee"] = 0.0
    elif package_key == "standard":
        packaged["bag_refill_included"] = True
        packaged["monthly_management_fee"] = max(_parse_float_range(builder.get("monthly_management_fee"), maximum=25000, default=0.0), 35.0)
    elif package_key == "premium":
        packaged["bag_refill_included"] = True
        packaged["deodorizer_included"] = True
        packaged["monthly_management_fee"] = max(_parse_float_range(builder.get("monthly_management_fee"), maximum=25000, default=0.0), 95.0)
    packaged["package_label"] = package["label"]
    packaged["package_description"] = package["description"]
    return packaged


def _client_commercial_calculate_proposal(builder, prospect, frequency):
    line_items = []
    visits_per_month = frequency["visits_per_month"]

    def add_monthly_item(label, quantity, unit_rate, unit_label):
        quantity = int(quantity or 0)
        rate = float(unit_rate or 0)
        if quantity <= 0 or rate <= 0:
            return 0.0
        monthly_total = round(quantity * rate * visits_per_month, 2)
        line_items.append({
            "label": label,
            "quantity": quantity,
            "unit_rate": round(rate, 2),
            "unit_label": unit_label,
            "frequency": frequency["label"],
            "billing": "monthly",
            "amount": monthly_total,
        })
        return monthly_total

    monthly_total = 0.0
    monthly_total += add_monthly_item("Waste station servicing", builder["waste_station_count"], builder["waste_station_rate"], "per station / visit")
    monthly_total += add_monthly_item("Common area policing", builder["common_area_count"], builder["common_area_rate"], "per area / visit")
    monthly_total += add_monthly_item("Dog relief area treatment", builder["relief_area_count"], builder["relief_area_rate"], "per zone / visit")

    if builder["bag_refill_included"] and builder["bag_refill_fee"] > 0:
        monthly_total += round(builder["bag_refill_fee"], 2)
        line_items.append({
            "label": "Bag refill and consumables",
            "quantity": 1,
            "unit_rate": round(builder["bag_refill_fee"], 2),
            "unit_label": "monthly",
            "frequency": "Monthly",
            "billing": "monthly",
            "amount": round(builder["bag_refill_fee"], 2),
        })

    if builder["deodorizer_included"] and builder["deodorizer_fee"] > 0:
        monthly_total += round(builder["deodorizer_fee"], 2)
        line_items.append({
            "label": "Deodorizer treatment",
            "quantity": 1,
            "unit_rate": round(builder["deodorizer_fee"], 2),
            "unit_label": "monthly",
            "frequency": "Monthly",
            "billing": "monthly",
            "amount": round(builder["deodorizer_fee"], 2),
        })

    if builder["monthly_management_fee"] > 0:
        monthly_total += round(builder["monthly_management_fee"], 2)
        line_items.append({
            "label": "Site reporting and management",
            "quantity": 1,
            "unit_rate": round(builder["monthly_management_fee"], 2),
            "unit_label": "monthly",
            "frequency": "Monthly",
            "billing": "monthly",
            "amount": round(builder["monthly_management_fee"], 2),
        })

    setup_total = 0.0
    if builder["initial_cleanup_required"] and builder["initial_cleanup_fee"] > 0:
        setup_total = round(builder["initial_cleanup_fee"], 2)
        line_items.append({
            "label": "Initial cleanup and site setup",
            "quantity": 1,
            "unit_rate": setup_total,
            "unit_label": "one-time",
            "frequency": "One-time",
            "billing": "one_time",
            "amount": setup_total,
        })

    account_name = prospect.get("business_name") or prospect.get("name") or "Commercial account"
    scope_summary = builder["scope_summary"] or "Commercial pet waste services."
    property_context = builder["property_count"] or prospect.get("property_count") or "portfolio size pending confirmation"
    package_label = builder.get("package_label") or COMMERCIAL_PROPOSAL_PACKAGES[builder["selected_package"]]["label"]
    summary = (
        f"{package_label} proposal for {account_name}: {frequency['label']} commercial pet waste coverage for {property_context}. "
        f"Monthly recurring service totals ${monthly_total:,.2f}."
    )
    if setup_total > 0:
        summary += f" One-time setup is ${setup_total:,.2f}."

    follow_up_text = (
        f"We scoped the {package_label.lower()} package around {frequency['label']} service for {account_name}, covering waste stations, common areas, and onsite presentation. "
        f"If this scope looks right, we can finalize routing, start date, and site access details next."
    )
    if builder["notes"]:
        follow_up_text += f" Notes: {builder['notes']}"

    included_features = [
        f"{frequency['label']} service cadence",
        f"{builder['waste_station_count']} waste stations",
        f"{builder['common_area_count']} common areas",
    ]
    if builder["bag_refill_included"]:
        included_features.append("Bag refill support")
    if builder["deodorizer_included"]:
        included_features.append("Deodorizer treatment")
    if builder["monthly_management_fee"] > 0:
        included_features.append("Manager-facing reporting")

    return {
        "builder": builder,
        "scope_summary": scope_summary,
        "line_items": line_items,
        "monthly_total": round(monthly_total, 2),
        "setup_total": round(setup_total, 2),
        "grand_total": round(monthly_total + setup_total, 2),
        "summary": summary,
        "follow_up_text": follow_up_text,
        "included_features": included_features,
    }


def _build_client_commercial_proposal(prospect, *, brand=None, existing_quote=None):
    builder = _normalize_client_commercial_proposal_builder(
        prospect.get("proposal_builder_json"),
        brand=brand,
        prospect=prospect,
    )
    frequency = COMMERCIAL_PROPOSAL_FREQUENCY[builder["service_frequency"]]
    packages = []
    for package_key in ("basic", "standard", "premium"):
        package_builder = _client_commercial_builder_for_package(builder, package_key)
        package_preview = _client_commercial_calculate_proposal(package_builder, prospect, frequency)
        packages.append({
            "key": package_key,
            "label": package_builder["package_label"],
            "description": package_builder["package_description"],
            "monthly_total": package_preview["monthly_total"],
            "setup_total": package_preview["setup_total"],
            "grand_total": package_preview["grand_total"],
            "included_features": package_preview["included_features"],
        })

    selected_builder = _client_commercial_builder_for_package(builder, builder["selected_package"])
    selected_preview = _client_commercial_calculate_proposal(selected_builder, prospect, frequency)
    proposal_quote = {
        "status": (existing_quote or {}).get("status") or "draft",
        "quote_mode": "structured",
        "amount_low": selected_preview["monthly_total"],
        "amount_high": selected_preview["grand_total"],
        "currency": (existing_quote or {}).get("currency") or "USD",
        "line_items": selected_preview["line_items"],
        "summary": selected_preview["summary"],
        "follow_up_text": selected_preview["follow_up_text"],
    }
    return {
        "builder": selected_builder,
        "frequency": frequency,
        "scope_summary": selected_preview["scope_summary"],
        "packages": packages,
        "selected_package": builder["selected_package"],
        "monthly_total": selected_preview["monthly_total"],
        "setup_total": selected_preview["setup_total"],
        "grand_total": selected_preview["grand_total"],
        "quote": proposal_quote,
    }


def _apply_client_commercial_outreach_overrides(prospect, brief):
    updated = dict(brief or {})
    generated_subject = _normalize_client_commercial_text(updated.get("subject"), 255)
    generated_email_body = _normalize_client_commercial_text(updated.get("email_body"), 6000)
    generated_call_opener = _normalize_client_commercial_text(updated.get("call_opener"), 2000)
    subject_override = _normalize_client_commercial_text((prospect or {}).get("outreach_subject_override"), 255)
    email_override = _normalize_client_commercial_text((prospect or {}).get("outreach_email_body_override"), 6000)
    call_override = _normalize_client_commercial_text((prospect or {}).get("outreach_call_opener_override"), 2000)

    updated["generated_subject"] = generated_subject
    updated["generated_email_body"] = generated_email_body
    updated["generated_call_opener"] = generated_call_opener
    updated["subject"] = subject_override or generated_subject
    updated["email_body"] = email_override or generated_email_body
    updated["call_opener"] = call_override or generated_call_opener
    updated["rewrite_prompt"] = _normalize_client_commercial_text((prospect or {}).get("outreach_rewrite_prompt"), 1000)
    updated["has_outreach_overrides"] = bool(subject_override or email_override or call_override)
    updated["outreach_last_rewrite_at"] = _normalize_client_commercial_text((prospect or {}).get("outreach_last_rewrite_at"), 40)
    return updated


def _extract_json_object_from_ai_text(raw_text):
    text = (raw_text or "").strip()
    if not text:
        return {}
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
        except Exception:
            return {}
    return parsed if isinstance(parsed, dict) else {}


def _rewrite_client_commercial_outreach_assets(brand, prospect, brief, rewrite_prompt):
    api_key = _get_openai_api_key(brand)
    if not api_key:
        raise ValueError("No OpenAI API key configured. Add one in Connections.")

    import requests as req

    model = _pick_ai_model(brand, "chat")
    context = {
        "brand": {
            "display_name": (brand or {}).get("display_name") or "",
            "industry": (brand or {}).get("industry") or "",
            "service_area": (brand or {}).get("service_area") or "",
            "primary_services": (brand or {}).get("primary_services") or "",
            "active_offers": (brand or {}).get("active_offers") or "",
            "sales_bot_service_menu": (brand or {}).get("sales_bot_service_menu") or "",
        },
        "prospect": {
            "business_name": (prospect or {}).get("business_name") or (prospect or {}).get("name") or "Commercial Prospect",
            "industry": (prospect or {}).get("industry") or "",
            "account_type": (prospect or {}).get("account_type") or "",
            "service_area": (prospect or {}).get("service_area") or "",
            "website": (prospect or {}).get("website") or "",
            "email": (prospect or {}).get("email") or "",
        },
        "strategy": {
            "service_pitch": (brief or {}).get("service_pitch") or "",
            "outreach_angle": (brief or {}).get("outreach_angle") or "",
            "pain_points": (brief or {}).get("pain_points") or [],
            "next_actions": (brief or {}).get("next_actions") or [],
        },
        "current_assets": {
            "subject": (brief or {}).get("subject") or "",
            "email_body": (brief or {}).get("email_body") or "",
            "call_opener": (brief or {}).get("call_opener") or "",
        },
        "rewrite_prompt": _normalize_client_commercial_text(rewrite_prompt, 1000),
    }
    system_prompt = (
        "You rewrite commercial outreach for a local service brand selling its real services to commercial properties. "
        "Do not drift into generic agency language unless the brand clearly sells marketing services. "
        "Keep the copy practical, specific, and easy for an operator to use. Do not use em dashes. Return valid JSON only."
    )
    user_prompt = (
        "Rewrite the outreach assets using the supplied context and user instruction. Keep the email under 180 words and the call opener under 60 words. "
        "Return JSON with keys: subject, email_body, call_opener.\n\n"
        + json.dumps(context)
    )

    response = req.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "temperature": 0.7,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        },
        timeout=45,
    )
    if response.status_code >= 400:
        raise ValueError(f"OpenAI request failed ({response.status_code}): {response.text[:200]}")

    payload = response.json() if hasattr(response, "json") else {}
    content = ((((payload or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or "")
    parsed = _extract_json_object_from_ai_text(content)
    subject = _normalize_client_commercial_text(parsed.get("subject"), 255)
    email_body = _normalize_client_commercial_text(parsed.get("email_body"), 6000)
    call_opener = _normalize_client_commercial_text(parsed.get("call_opener"), 2000)
    if not subject or not email_body or not call_opener:
        raise ValueError("AI rewrite returned an incomplete outreach draft.")
    return {
        "subject": subject,
        "email_body": email_body,
        "call_opener": call_opener,
    }


def _build_client_commercial_email_html(message_text):
    body = html.escape(message_text or "").replace("\n", "<br>")
    return (
        "<div style=\"font-family:Arial,sans-serif;font-size:14px;line-height:1.6;color:#111827;\">"
        f"{body}"
        "</div>"
    )


def _prepare_client_commercial_service_visits(raw_visits):
    visits = []
    for visit in raw_visits or []:
        item = dict(visit)
        item["issues"] = _safe_json_list(item.get("issues_json"))
        item["photos"] = _safe_json_list(item.get("photos_json"))
        visits.append(item)
    return visits


def _build_client_commercial_service_recap(prospect, service_visits):
    visits = service_visits or []
    total_visits = len(visits)
    stations_serviced = sum(int(visit.get("waste_station_count_serviced") or 0) for visit in visits)
    bags_restocked_count = sum(1 for visit in visits if visit.get("bags_restocked"))
    gate_secured_count = sum(1 for visit in visits if visit.get("gate_secured"))
    issue_list = []
    seen = set()
    for visit in visits:
        for issue in visit.get("issues") or []:
            key = issue.lower()
            if key in seen:
                continue
            seen.add(key)
            issue_list.append(issue)
    account_name = prospect.get("business_name") or prospect.get("name") or "the property"
    last_service_date = visits[0].get("service_date") if visits else ""
    if visits:
        summary = (
            f"{account_name} received {total_visits} logged service visit{'s' if total_visits != 1 else ''}. "
            f"Teams serviced {stations_serviced} station stop{'s' if stations_serviced != 1 else ''} across the recorded visits, "
            f"with bag restocks completed on {bags_restocked_count} visit{'s' if bags_restocked_count != 1 else ''} and gate security confirmed on {gate_secured_count} visit{'s' if gate_secured_count != 1 else ''}."
        )
    else:
        summary = f"No commercial service visits have been logged for {account_name} yet."

    recommendations = []
    if issue_list:
        recommendations.append(f"Resolve the top open field issue: {issue_list[0]}.")
    if total_visits and gate_secured_count < total_visits:
        recommendations.append("Tighten gate-close confirmation on every visit.")
    if total_visits and bags_restocked_count == 0:
        recommendations.append("Confirm whether bag refill should be part of the active scope.")
    if not recommendations and total_visits:
        recommendations.append("Use this recap in monthly client reporting and renewal conversations.")

    return {
        "total_visits": total_visits,
        "stations_serviced": stations_serviced,
        "bags_restocked_count": bags_restocked_count,
        "gate_secured_count": gate_secured_count,
        "issues": issue_list[:5],
        "last_service_date": last_service_date,
        "summary": summary,
        "recommendations": recommendations,
    }


def _get_client_commercial_nurture_sequences(db):
    sequences = []
    for sequence in db.get_drip_sequences():
        if (sequence.get("trigger") or "").strip().lower() != "commercial":
            continue
        if not sequence.get("is_active"):
            continue
        sequences.append(sequence)
    return sequences


def _get_client_commercial_nurture_state(db, thread_id):
    enrollments = db.get_lead_drip_enrollments("client_commercial", thread_id)
    for enrollment in enrollments:
        enrollment["sends"] = db.get_drip_sends(enrollment_id=enrollment["id"], limit=10)
    return enrollments


def _parse_dog_count(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return max(0, int(value))
    text = str(value).strip().lower()
    if not text:
        return None
    digit_match = re.search(r"\b(\d{1,2})\b", text)
    if digit_match:
        return int(digit_match.group(1))
    word_map = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }
    for word, number in word_map.items():
        if re.search(rf"\b{word}\b", text):
            return number
    return None


def _extract_objections(text):
    lowered = (text or "").strip().lower()
    if not lowered:
        return []
    checks = [
        ("budget", ("budget", "too expensive", "too much", "price seems high", "cost is high", "can't afford")),
        ("needs partner approval", ("wife", "husband", "spouse", "partner", "need to ask", "need to check with")),
        ("timing", ("not ready", "later", "next month", "timing", "busy right now", "wait a bit")),
        ("shopping around", ("shopping around", "getting quotes", "comparing", "checking options", "other quotes")),
        ("schedule conflict", ("schedule", "calendar", "availability", "out of town", "not home")),
    ]
    found = []
    for label, phrases in checks:
        if any(phrase in lowered for phrase in phrases):
            found.append(label)
    return found


def _format_quote_amount(quote):
    if not quote:
        return ""
    try:
        low = float(quote.get("amount_low") or 0)
    except (TypeError, ValueError):
        low = 0.0
    try:
        high = float(quote.get("amount_high") or 0)
    except (TypeError, ValueError):
        high = 0.0
    currency = (quote.get("currency") or "USD").strip().upper()
    symbol = "$" if currency == "USD" else f"{currency} "
    if low > 0 and high > 0 and abs(low - high) >= 1:
        return f"{symbol}{int(low):,}-{symbol}{int(high):,}"
    value = high or low
    return f"{symbol}{int(value):,}" if value > 0 else ""


def _derive_waiting_on(thread, quote, messages):
    status = (thread.get("status") or "new").strip().lower()
    last_message = messages[-1] if messages else None
    last_direction = (last_message or {}).get("direction", "")

    if status == "won":
        return "Closed won"
    if status == "lost":
        return "Closed lost"
    if status == "booked":
        return "Waiting on service delivery"
    if status == "quoted":
        if quote and (quote.get("status") or "").lower() in {"accepted", "approved"}:
            return "Waiting on scheduling"
        return "Waiting on quote approval"
    if status == "qualified":
        return "Waiting on booking decision"
    if last_direction == "inbound":
        return "Waiting on team follow-up"
    if last_direction == "outbound":
        return "Waiting on lead reply"
    return "Waiting on first real conversation"


def _estimate_closeability(thread, quote, objections, messages, waiting_on):
    stage_scores = {
        "new": 18,
        "engaged": 36,
        "quoted": 58,
        "qualified": 72,
        "booked": 88,
        "won": 100,
        "lost": 5,
    }
    score = stage_scores.get((thread.get("status") or "new").strip().lower(), 20)
    drivers = []
    drivers.append(f"Stage: {(thread.get('status') or 'new').strip().title()} baseline")
    if (thread.get("lead_phone") or "").strip():
        score += 4
        drivers.append("Phone captured")
    if (thread.get("lead_email") or "").strip():
        score += 4
        drivers.append("Email captured")
    inbound_replies = sum(1 for message in messages if message.get("direction") == "inbound")
    if inbound_replies >= 2:
        score += 6
        drivers.append("Multiple inbound replies")
    if quote and _format_quote_amount(quote):
        score += 10
        drivers.append("Quote prepared or sent")
    if any((message.get("direction") == "inbound") and re.search(r"\b(schedule|book|when can|availability|tomorrow|this week)\b", message.get("content") or "", re.I) for message in messages):
        score += 8
        drivers.append("Lead asked about scheduling")
    if any((message.get("direction") == "inbound") and re.search(r"\b(yes|sounds good|let's do it|move forward|works for me)\b", message.get("content") or "", re.I) for message in messages):
        score += 10
        drivers.append("Positive buying language")
    if waiting_on == "Waiting on team follow-up":
        score -= 8
        drivers.append("Team still owes follow-up")
    objection_penalty = min(20, len(objections) * 7)
    if objection_penalty:
        score -= objection_penalty
        drivers.append("Open objections still unresolved")
    score = max(5, min(100, int(score)))
    return score, drivers[:5]


def _build_lead_profile(db, thread):
    thread = dict(thread or {})
    thread_id = int(thread.get("id") or 0)
    messages = db.get_lead_messages(thread_id, limit=120) if thread_id else []
    quote = db.get_lead_quote_for_thread(thread_id) if thread_id else None
    override = db.get_lead_profile_override(thread_id) if thread_id else None

    phone = (thread.get("lead_phone") or "").strip()
    email = (thread.get("lead_email") or "").strip().lower()
    name = (thread.get("lead_name") or "").strip()
    dog_count = None
    objections = []
    known_items = []

    for message in messages:
        metadata = _safe_json_object(message.get("metadata_json"))
        fields = _safe_json_object(metadata.get("fields"))
        combined = {**metadata, **fields}

        for key, value in combined.items():
            key_text = str(key or "").strip().lower()
            value_text = str(value or "").strip()
            if not value_text:
                continue
            if key_text in {"name", "full_name"} and not name:
                name = value_text
            elif key_text in {"email", "email_address"} and not email:
                email = value_text.lower()
            elif key_text in {"phone", "phone_number", "mobile", "cell"} and not phone:
                phone = value_text

            if dog_count is None and ("dog" in key_text or "pet" in key_text):
                dog_count = _parse_dog_count(value_text)

            if "objection" in key_text:
                objections.extend(_extract_objections(value_text) or [value_text.lower()])

            if key_text not in {"from", "conversation_id", "sender_psid", "page_id", "image_urls", "opted_out", "fields"}:
                label = str(key).replace("_", " ").strip().title()
                known_items.append((label, value_text))

        if message.get("direction") == "inbound":
            content = message.get("content") or ""
            if dog_count is None and re.search(r"\b(dog|dogs|pup|puppy|pet|pets)\b", content, re.I):
                dog_count = _parse_dog_count(content)
            objections.extend(_extract_objections(content))

    seen_known = set()
    deduped_known_items = []
    for label, value in known_items:
        marker = (label.lower(), value.lower())
        if marker in seen_known:
            continue
        seen_known.add(marker)
        deduped_known_items.append({"label": label, "value": value})

    seen_objections = set()
    deduped_objections = []
    for item in objections:
        label = str(item or "").strip().lower()
        if not label or label in seen_objections:
            continue
        seen_objections.add(label)
        deduped_objections.append(label)

    waiting_on = _derive_waiting_on(thread, quote, messages)
    quote_amount = _format_quote_amount(quote)
    quote_status = ((quote or {}).get("status") or thread.get("quote_status") or "").strip()
    closeability, closeability_drivers = _estimate_closeability(thread, quote, deduped_objections, messages, waiting_on)

    if override:
        if override.get("dog_count") is not None:
            dog_count = int(override.get("dog_count"))
        override_objections = [item.strip().lower() for item in re.split(r"[\n,;]+", override.get("objections_text") or "") if item.strip()]
        if override_objections:
            deduped_objections = override_objections
        if (override.get("waiting_on_text") or "").strip():
            waiting_on = override.get("waiting_on_text").strip()
        if override.get("closeability_pct") is not None:
            closeability = max(0, min(100, int(override.get("closeability_pct") or 0)))

    return {
        "thread_id": thread_id,
        "display_name": name or phone or email or "Unknown Lead",
        "lead_name": name,
        "lead_phone": phone,
        "lead_email": email,
        "status": (thread.get("status") or "new").strip().lower(),
        "source": thread.get("source") or "",
        "summary": thread.get("summary") or "",
        "dog_count": dog_count,
        "quoted_amount": quote_amount,
        "quote_status": quote_status,
        "objections": deduped_objections,
        "waiting_on": waiting_on,
        "closeability_pct": closeability,
        "closeability_drivers": closeability_drivers,
        "known_items": deduped_known_items[:6],
        "last_message_at": thread.get("last_message_at") or "",
        "profile_notes": (override or {}).get("profile_notes") or "",
    }


def _normalize_client_actions(actions):
    normalized = []
    if not isinstance(actions, list):
        return normalized

    for index, raw_action in enumerate(actions, start=1):
        if isinstance(raw_action, dict):
            action = dict(raw_action)
        elif isinstance(raw_action, str):
            action = {"title": raw_action}
        else:
            continue

        title = str(action.get("title") or action.get("mission_name") or "").strip() or f"Mission {index}"
        mission_name = str(action.get("mission_name") or title).strip() or title

        xp_value = action.get("xp", 100)
        try:
            xp = int(xp_value)
        except (TypeError, ValueError):
            xp = 100

        difficulty_value = action.get("difficulty", 0)
        try:
            difficulty = int(difficulty_value)
        except (TypeError, ValueError):
            difficulty = 0
        difficulty = max(0, min(difficulty, 3))

        steps_value = action.get("steps") or []
        if isinstance(steps_value, str):
            steps = [steps_value] if steps_value.strip() else []
        elif isinstance(steps_value, (list, tuple)):
            steps = []
            for step in steps_value:
                if step is None:
                    continue
                step_text = str(step).strip()
                if step_text:
                    steps.append(step_text)
        else:
            steps = []

        targets_value = action.get("exact_targets") or []
        if isinstance(targets_value, str):
            exact_targets = [targets_value.strip()] if targets_value.strip() else []
        elif isinstance(targets_value, (list, tuple)):
            exact_targets = [str(target).strip() for target in targets_value if str(target or "").strip()]
        else:
            exact_targets = []

        normalized.append(
            {
                **action,
                "title": title,
                "mission_name": mission_name,
                "priority": str(action.get("priority") or "Worth Doing Soon"),
                "priority_class": str(action.get("priority_class") or "warning"),
                "category": str(action.get("category") or "Marketing"),
                "what": str(action.get("what") or ""),
                "steps": steps,
                "impact": str(action.get("impact") or ""),
                "time": str(action.get("time") or ""),
                "data_point": str(action.get("data_point") or ""),
                "why": str(action.get("why") or ""),
                "reward": str(action.get("reward") or ""),
                "icon": str(action.get("icon") or "bi-star-fill"),
                "icon_color": str(action.get("icon_color") or "#6b7280"),
                "skill": str(action.get("skill") or "Marketing"),
                "platform_url": str(action.get("platform_url") or ""),
                "platform_label": str(action.get("platform_label") or ""),
                "execution_mode": str(action.get("execution_mode") or "direct"),
                "delegate_to": str(action.get("delegate_to") or ""),
                "delegate_message": str(action.get("delegate_message") or ""),
                "exact_targets": exact_targets,
                "xp": xp,
                "difficulty": difficulty,
                "key": _coerce_action_key(
                    str(action.get("key") or mission_name or title),
                    f"mission_{index}",
                ),
            }
        )

    return normalized


def _external_app_url() -> str:
    configured = (current_app.config.get("APP_URL", "") or "").rstrip("/")
    scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
    host = (request.headers.get("X-Forwarded-Host") or request.host or "").strip()
    request_base = f"{scheme}://{host}" if host else request.host_url.rstrip("/")
    if not configured or "localhost" in configured:
        return request_base.rstrip("/")
    return configured


def _normalize_scheduled_datetime(raw_value):
    raw = (raw_value or "").strip()
    if not raw:
        return None

    for fmt in (
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _warm_client_snapshots_async(*, brand_id: int, month: str) -> None:
    """Warm heavy caches in the background after login.

    Populates:
    - analysis + suggestions (monthly_summary)
    - campaigns list snapshot (settings)
    - full dashboard snapshot (dashboard_snapshots table)

    Best-effort: failures should never block the request.
    """

    try:
        app = current_app._get_current_object()
    except Exception:
        return

        # Adding outreach deletion confirmation
        flash("Commercial outreach deleted.", "success")
    def _runner():
        try:
            with app.app_context():
                db = getattr(app, "db", None)
                if not db:
                    return
                brand = db.get_brand(brand_id)
                if not brand:
                    return

                analysis = None
                suggestions = None
                try:
                    from webapp.report_runner import get_analysis_and_suggestions_for_brand

                    analysis, suggestions = get_analysis_and_suggestions_for_brand(
                        db, brand, month, force_refresh=True
                    )
                except Exception as exc:
                    log.info("Warm-up analysis failed (brand=%s month=%s): %s", brand_id, month, exc)

                campaigns_data = {}
                try:
                    campaigns_data = _get_campaigns_cached(db, brand, month, force_sync=True)
                except Exception as exc:
                    log.info("Warm-up campaigns failed (brand=%s month=%s): %s", brand_id, month, exc)

                # Build and save full dashboard snapshot
                if analysis:
                    try:
                        dashboard_data = _assemble_dashboard_payload(
                            db, brand, brand_id, month, analysis, suggestions, campaigns_data
                        )
                        db.upsert_dashboard_snapshot(
                            brand_id, month,
                            json.dumps(dashboard_data, default=str),
                            source="warmup",
                        )
                    except Exception as exc:
                        log.info("Warm-up snapshot save failed (brand=%s): %s", brand_id, exc)
        except Exception:
            return

    try:
        t = threading.Thread(target=_runner, name=f"warmup-{brand_id}-{month}", daemon=True)
        t.start()
    except Exception:
        return


def _assemble_dashboard_payload(db, brand, brand_id, month, analysis, suggestions, campaigns_data):
    """Build the full dashboard JSON payload from pre-fetched data.

    Shared by the /dashboard/data endpoint and the warm-up thread so the
    snapshot contains the exact same structure the frontend expects.
    """
    from webapp.client_advisor import build_client_dashboard

    dashboard_data = build_client_dashboard(analysis, suggestions, brand)

    # Store raw analysis in snapshot so missions can regenerate from it later
    dashboard_data["_analysis"] = analysis
    dashboard_data["_suggestions"] = suggestions

    dashboard_data["campaigns"] = {
        "google": [
            {
                "id": c.get("id", ""),
                "name": c.get("name", ""),
                "status": c.get("status", ""),
                "daily_budget": c.get("daily_budget", 0),
                "spend": c.get("spend", 0),
                "clicks": c.get("clicks", 0),
                "ctr": c.get("ctr", 0),
                "conversions": c.get("conversions", 0),
                "cpa": c.get("cpa", 0),
                "channel_type": c.get("channel_type", ""),
            }
            for c in (campaigns_data.get("google") or [])
        ],
        "meta": [
            {
                "id": c.get("id", ""),
                "name": c.get("name", ""),
                "status": c.get("status", ""),
                "daily_budget": c.get("daily_budget", 0),
                "spend": c.get("spend", 0),
                "clicks": c.get("clicks", 0),
                "ctr": c.get("ctr", 0),
                "conversions": c.get("conversions", 0),
                "cpa": c.get("cpa", 0),
                "objective": c.get("objective", ""),
            }
            for c in (campaigns_data.get("meta") or [])
        ],
    }

    try:
        dashboard_data["target_cpa"] = float(brand.get("kpi_target_cpa") or 0)
    except (ValueError, TypeError):
        dashboard_data["target_cpa"] = 0.0

    try:
        drafts_raw = db.get_campaign_drafts(brand_id) or []
        dashboard_data["drafts"] = [
            {
                "id": dr["id"],
                "platform": dr.get("platform", ""),
                "campaign_name": dr.get("campaign_name", "Untitled"),
                "status": dr.get("status", "draft"),
                "created_by": dr.get("created_by", ""),
                "updated_at": dr.get("updated_at", dr.get("created_at", "")),
            }
            for dr in drafts_raw
        ]
    except Exception:
        dashboard_data["drafts"] = []

    try:
        if brand.get("crm_type") == "sweepandgo" and brand.get("crm_api_key"):
            from webapp.crm_bridge import (
                sng_count_active_clients,
                sng_count_happy_clients,
                sng_count_happy_dogs,
                sng_count_jobs,
            )
            dashboard_data["sng"] = {
                "connected": True,
                "active_clients": sng_count_active_clients(brand) or 0,
                "happy_clients": sng_count_happy_clients(brand) or 0,
                "happy_dogs": sng_count_happy_dogs(brand) or 0,
                "completed_jobs": sng_count_jobs(brand) or 0,
            }
    except Exception:
        dashboard_data["sng"] = {"connected": False}

    try:
        latest_findings = db.get_agent_findings(brand_id, month=month, limit=50)
        briefing_critical = [f for f in latest_findings if f.get("severity") == "critical"]
        briefing_warning = [f for f in latest_findings if f.get("severity") == "warning"]
        briefing_positive = [f for f in latest_findings if f.get("severity") == "positive"]

        dashboard_data["warren_briefing"] = {
            "total_findings": len(latest_findings),
            "critical_count": len(briefing_critical),
            "warning_count": len(briefing_warning),
            "positive_count": len(briefing_positive),
            "top_critical": [
                {"title": f.get("title", ""), "detail": f.get("detail", ""), "agent": f.get("agent_key", "")}
                for f in briefing_critical[:3]
            ],
            "top_warnings": [
                {"title": f.get("title", ""), "detail": f.get("detail", ""), "agent": f.get("agent_key", "")}
                for f in briefing_warning[:3]
            ],
            "top_wins": [
                {"title": f.get("title", ""), "detail": f.get("detail", ""), "agent": f.get("agent_key", "")}
                for f in briefing_positive[:3]
            ],
        }
    except Exception:
        dashboard_data["warren_briefing"] = None

    try:
        hired_agents = json.loads(brand.get("hired_agents") or "{}")
        active_count = len([a for a in hired_agents.values() if a.get("trained")])
        dashboard_data["team_status"] = {
            "hired": len(hired_agents),
            "trained": active_count,
            "total_available": len(AGENT_ROSTER),
        }
    except Exception:
        dashboard_data["team_status"] = None

    return dashboard_data


ALLOWED_AI_MODELS = {
    "",
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4-turbo",
    "gpt-4.1-mini",
    "gpt-4.1",
    "o3-mini",
    "o4-mini",
}


def _pick_ai_model(brand, purpose, requested=""):
    candidate = (requested or "").strip()
    if candidate and candidate in ALLOWED_AI_MODELS:
        return candidate

    purpose_field = {
        "chat": "openai_model_chat",
        "images": "openai_model_images",
        "analysis": "openai_model_analysis",
        "ads": "openai_model_ads",
    }.get((purpose or "").strip().lower())

    if purpose_field:
        purpose_model = ((brand or {}).get(purpose_field) or "").strip()
        if purpose_model in ALLOWED_AI_MODELS and purpose_model:
            return purpose_model

    fallback_model = ((brand or {}).get("openai_model") or "").strip()
    if fallback_model in ALLOWED_AI_MODELS and fallback_model:
        return fallback_model
    return "gpt-4o-mini"


def _get_openai_api_key(brand):
    api_key = ((brand or {}).get("openai_api_key") or "").strip()
    if api_key:
        return api_key
    try:
        from flask import current_app
        return (current_app.config.get("OPENAI_API_KEY") or "").strip()
    except RuntimeError:
        return ""


def _assistant_month():
    month = (request.args.get("month") or request.form.get("month") or "").strip()
    if re.match(r"^\d{4}-\d{2}$", month):
        return month
    return datetime.now().strftime("%Y-%m")


def _compact_message_text(text, limit=160):
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


_FACEBOOK_POST_TYPE_OPTIONS = [
    {
        "key": "value",
        "label": "Value Post",
        "description": "Share one useful tip, lesson, or buyer insight people can use right away.",
        "prompt_focus": "Teach something practical and make the business sound helpful, not preachy.",
    },
    {
        "key": "faq",
        "label": "FAQ Post",
        "description": "Answer one common customer question in a way that removes friction and builds trust.",
        "prompt_focus": "Make the answer practical, clear, and grounded in how the business actually works.",
    },
    {
        "key": "special_offer",
        "label": "Special Post",
        "description": "Promote an offer, seasonal push, or short-term reason to reach out now.",
        "prompt_focus": "Make the offer specific, credible, and easy to act on.",
    },
    {
        "key": "testimonial",
        "label": "Testimonial Post",
        "description": "Turn customer proof into a trust-building post with a clear takeaway.",
        "prompt_focus": "Use believable social proof and connect it back to why someone should trust the team.",
    },
    {
        "key": "behind_the_scenes",
        "label": "Behind The Scenes",
        "description": "Show the people, process, prep work, or standards behind the service.",
        "prompt_focus": "Make the business feel real, local, and hands-on.",
    },
    {
        "key": "team_intro",
        "label": "Team Intro",
        "description": "Introduce an owner, tech, team member, or role so the business feels more personal.",
        "prompt_focus": "Highlight competence, personality, and why that person matters to customers.",
    },
    {
        "key": "community_spotlight",
        "label": "Community Spotlight",
        "description": "Highlight a neighborhood, local event, nearby project, or local partnership.",
        "prompt_focus": "Make the business feel present in the local community instead of abstract or corporate.",
    },
    {
        "key": "seasonal_reminder",
        "label": "Seasonal Reminder",
        "description": "Tie a timely reminder or preventive tip to the season, weather, or local calendar.",
        "prompt_focus": "Make the timing feel relevant and useful, not manufactured.",
    },
    {
        "key": "business_growth",
        "label": "Watch Our Business Grow",
        "description": "Share momentum, milestones, new tools, team growth, or progress updates.",
        "prompt_focus": "Show growth in a grounded way that reinforces trust and momentum.",
    },
    {
        "key": "character_spotlight",
        "label": "Recurring Character",
        "description": "Feature a recurring person, mascot, crew personality, or branded voice the audience should get to know.",
        "prompt_focus": "Use one recurring character to tell the story in a way that feels consistent, human, and memorable.",
    },
]
_FACEBOOK_POST_TYPE_MAP = {item["key"]: item for item in _FACEBOOK_POST_TYPE_OPTIONS}
_FACEBOOK_POST_TYPE_ALIASES = {
    "faq_post": "faq",
    "frequently_asked_question": "faq",
    "special": "special_offer",
    "offer": "special_offer",
    "special_post": "special_offer",
    "testimonial_post": "testimonial",
    "behind_the_scenes_post": "behind_the_scenes",
    "behind_the_scenes": "behind_the_scenes",
    "behind_the_scences": "behind_the_scenes",
    "behind_scenes": "behind_the_scenes",
    "team": "team_intro",
    "team_member": "team_intro",
    "team_member_intro": "team_intro",
    "community": "community_spotlight",
    "spotlight": "community_spotlight",
    "community_feature": "community_spotlight",
    "seasonal": "seasonal_reminder",
    "seasonal_post": "seasonal_reminder",
    "seasonal_tip": "seasonal_reminder",
    "watch_our_business_grow": "business_growth",
    "watch_business_grow": "business_growth",
    "growth": "business_growth",
    "character": "character_spotlight",
    "recurring_character": "character_spotlight",
    "character_post": "character_spotlight",
}
_FACEBOOK_POST_TYPE_RULES = {
    "value": "Lead with one practical lesson, warning sign, tip, or FAQ answer that helps a prospect make a better decision.",
    "faq": "Answer a real customer question clearly. Reduce hesitation and explain what the customer should do next if they need help.",
    "special_offer": "Make the offer concrete. Mention timing, audience fit, or what makes the offer worth acting on without sounding desperate.",
    "testimonial": "Write like a real owner highlighting a customer result or experience. Do not fabricate exact names unless the brief gives one.",
    "behind_the_scenes": "Describe the work, prep, standards, people, or process behind the service so the business feels tangible and trustworthy.",
    "team_intro": "Make the person feel competent and approachable. Tie their role back to the quality customers receive.",
    "community_spotlight": "Reference something local that feels authentic to the business, such as a neighborhood, event, nearby project, or local partner.",
    "seasonal_reminder": "Anchor the post in a real seasonal concern, weather pattern, or calendar moment that matters to local customers right now.",
    "business_growth": "Share a milestone, upgrade, new team member, new capability, or community momentum update that shows the business is growing for the right reasons.",
    "character_spotlight": "Make the recurring character feel consistent and real. Show how that person or persona reveals the brand without turning the post into a forced skit.",
}
_FACEBOOK_CONTENT_MIXES = [
    {
        "key": "balanced_local_presence",
        "label": "Balanced Local Presence",
        "description": "A steady mix of education, proof, local relevance, and business momentum.",
        "post_types": ["value", "faq", "testimonial", "behind_the_scenes", "community_spotlight", "business_growth"],
        "prompt_focus": "Keep the calendar balanced across education, trust, and local brand presence.",
    },
    {
        "key": "trust_and_proof",
        "label": "Trust And Proof",
        "description": "Lean harder on testimonials, team credibility, and how the work actually gets done.",
        "post_types": ["testimonial", "behind_the_scenes", "team_intro", "value", "faq", "community_spotlight"],
        "prompt_focus": "Bias the calendar toward trust-building proof, visible standards, and real people.",
    },
    {
        "key": "offer_and_conversion",
        "label": "Offer And Conversion",
        "description": "Use stronger offer, urgency, and decision-stage content without turning spammy.",
        "post_types": ["special_offer", "value", "faq", "testimonial", "seasonal_reminder", "behind_the_scenes"],
        "prompt_focus": "Keep the calendar oriented toward booked calls, seasonal demand, and credible reasons to act now.",
    },
    {
        "key": "community_momentum",
        "label": "Community Momentum",
        "description": "Emphasize local visibility, crew personality, and growth in the market.",
        "post_types": ["community_spotlight", "team_intro", "behind_the_scenes", "business_growth", "character_spotlight", "seasonal_reminder"],
        "prompt_focus": "Make the business feel active, visible, and rooted in the local community.",
    },
    {
        "key": "brand_story_engine",
        "label": "Brand Story Engine",
        "description": "Tell an ongoing story about the people, momentum, and personalities behind the business.",
        "post_types": ["business_growth", "behind_the_scenes", "character_spotlight", "team_intro", "community_spotlight", "value"],
        "prompt_focus": "Make the brand feel like an unfolding story customers want to follow, not just a stream of offers.",
    },
]
_FACEBOOK_CONTENT_MIX_MAP = {item["key"]: item for item in _FACEBOOK_CONTENT_MIXES}
_FACEBOOK_CALENDAR_DAY_OFFSETS = {
    2: [1, 4],
    3: [0, 2, 4],
    4: [0, 2, 4, 6],
}
_FACEBOOK_CALENDAR_TIMES = ["09:15:00", "11:45:00", "14:30:00", "16:15:00"]
_FACEBOOK_CONTENT_PERSONALITY_OPTIONS = [
    {
        "key": "straight_forward",
        "label": "Straight Forward",
        "description": "Professional, clear, no joking, no cute copy.",
        "instruction": "Keep the tone direct, grounded, and professional. Do not joke around or lean on playful copy.",
    },
    {
        "key": "warm_professional",
        "label": "Warm Professional",
        "description": "Competent and polished, but still human and approachable.",
        "instruction": "Keep the tone warm, competent, and polished. Let the posts feel human without slipping into goofy language.",
    },
    {
        "key": "light_personality",
        "label": "Light Personality",
        "description": "A little personality and charm, but still credible and service-first.",
        "instruction": "Allow light personality, charm, and small moments of humor, but keep the business credible and useful.",
    },
    {
        "key": "playful_funny",
        "label": "Playful And Funny",
        "description": "Use humor and memorable voice intentionally, without making the brand sound sloppy.",
        "instruction": "Use humor intentionally where it fits, but keep the post coherent, brand-safe, and still commercially useful.",
    },
]
_FACEBOOK_CONTENT_PERSONALITY_MAP = {item["key"]: item for item in _FACEBOOK_CONTENT_PERSONALITY_OPTIONS}
_FACEBOOK_CTA_STYLE_OPTIONS = [
    {
        "key": "subtle",
        "label": "Subtle",
        "description": "Sell without sounding like a sales pitch.",
        "instruction": "Use a subtle CTA. It should still sell, but it should feel observational or invitational, not pushy.",
    },
    {
        "key": "consultative",
        "label": "Consultative",
        "description": "Guide the reader toward the next step like a trusted advisor.",
        "instruction": "Use a consultative CTA. Encourage the next step like a trusted operator giving practical advice.",
    },
    {
        "key": "direct",
        "label": "Direct",
        "description": "Be clear and explicit about booking, calling, or messaging now.",
        "instruction": "Use a direct CTA. Tell the reader exactly what action to take next without hedging.",
    },
]
_FACEBOOK_CTA_STYLE_MAP = {item["key"]: item for item in _FACEBOOK_CTA_STYLE_OPTIONS}
_FACEBOOK_POST_LENGTH_OPTIONS = [
    {
        "key": "short",
        "label": "Short",
        "description": "Quick, punchy, easy to scan in-feed.",
        "instruction": "Keep it compact and punchy. Prioritize one sharp idea and a brief CTA.",
        "single_post_words": "between 45 and 80 words",
        "calendar_words": "between 45 and 80 words",
    },
    {
        "key": "medium",
        "label": "Medium",
        "description": "Balanced length for most day-to-day posts.",
        "instruction": "Keep it balanced. Give enough detail to feel useful without dragging.",
        "single_post_words": "between 80 and 140 words",
        "calendar_words": "between 80 and 140 words",
    },
    {
        "key": "long",
        "label": "Long",
        "description": "Deeper explanation with more detail and context.",
        "instruction": "Go deeper than normal. Add detail, examples, or context, but keep it readable.",
        "single_post_words": "between 140 and 220 words",
        "calendar_words": "between 140 and 220 words",
    },
    {
        "key": "story_time",
        "label": "Story Time",
        "description": "A fuller narrative post with room for setup, payoff, and CTA.",
        "instruction": "Write like a real story post. Use a clear hook, fuller body, and a closing CTA with breathing room between sections.",
        "single_post_words": "between 220 and 320 words",
        "calendar_words": "between 220 and 320 words",
    },
]
_FACEBOOK_POST_LENGTH_MAP = {item["key"]: item for item in _FACEBOOK_POST_LENGTH_OPTIONS}
_FACEBOOK_CHARACTER_CADENCE_OPTIONS = [
    {
        "key": "once_per_calendar",
        "label": "Once Per Calendar",
        "description": "Use this character once when the calendar has a strong fit.",
        "interval_posts": None,
    },
    {
        "key": "every_6_posts",
        "label": "Light Rotation",
        "description": "Roughly once every 6 posts.",
        "interval_posts": 6,
    },
    {
        "key": "every_4_posts",
        "label": "Regular Rotation",
        "description": "Roughly once every 4 posts.",
        "interval_posts": 4,
    },
    {
        "key": "every_3_posts",
        "label": "Heavy Rotation",
        "description": "Roughly once every 3 posts.",
        "interval_posts": 3,
    },
]
_FACEBOOK_CHARACTER_CADENCE_MAP = {item["key"]: item for item in _FACEBOOK_CHARACTER_CADENCE_OPTIONS}


def _normalize_facebook_post_type(value, default="value"):
    cleaned = (value or "").strip().lower().replace("-", "_").replace(" ", "_")
    cleaned = _FACEBOOK_POST_TYPE_ALIASES.get(cleaned, cleaned)
    if cleaned in _FACEBOOK_POST_TYPE_MAP:
        return cleaned
    return default if default is not None else cleaned


def _facebook_post_type_label(value):
    normalized = _normalize_facebook_post_type(value, default=None)
    if normalized in _FACEBOOK_POST_TYPE_MAP:
        return _FACEBOOK_POST_TYPE_MAP[normalized]["label"]
    raw_value = (value or "").strip()
    if not raw_value:
        return ""
    return raw_value.replace("_", " ").replace("-", " ").title()


def _clean_gbp_cid(value):
    return re.sub(r"\D", "", (value or "").strip())[:32]


def _extract_json_object(raw_text):
    raw = (raw_text or "").strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError("AI returned invalid format. Try again.")
    return json.loads(match.group())


def _default_post_link_url(brand, post_type):
    website = ((brand or {}).get("website_url") or (brand or {}).get("website") or "").strip()
    if not website:
        return ""
    if not website.startswith(("http://", "https://")):
        website = f"https://{website.lstrip('/')}"
    if post_type in {"value", "faq", "special_offer", "testimonial", "seasonal_reminder"}:
        return website
    return ""


def _coerce_int(value, default, minimum=None, maximum=None):
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def _normalize_facebook_post_type_list(values):
    if isinstance(values, str):
        values = [chunk.strip() for chunk in values.split(",")]
    normalized = []
    for value in values or []:
        key = _normalize_facebook_post_type(value, default=None)
        if key and key not in normalized:
            normalized.append(key)
    return normalized


def _normalize_facebook_content_mix(value, default="balanced_local_presence"):
    cleaned = (value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if cleaned in _FACEBOOK_CONTENT_MIX_MAP:
        return cleaned
    return default if default is not None else cleaned


def _normalize_facebook_content_personality(value, default="warm_professional"):
    cleaned = (value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if cleaned in _FACEBOOK_CONTENT_PERSONALITY_MAP:
        return cleaned
    return default if default is not None else cleaned


def _normalize_facebook_cta_style(value, default="subtle"):
    cleaned = (value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if cleaned in _FACEBOOK_CTA_STYLE_MAP:
        return cleaned
    return default if default is not None else cleaned


def _normalize_facebook_post_length(value, default="medium"):
    cleaned = (value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if cleaned in _FACEBOOK_POST_LENGTH_MAP:
        return cleaned
    return default if default is not None else cleaned


def _normalize_facebook_character_cadence(value, default="every_6_posts"):
    cleaned = (value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if cleaned in _FACEBOOK_CHARACTER_CADENCE_MAP:
        return cleaned
    return default if default is not None else cleaned


def _parse_facebook_recurring_characters(raw_value):
    raw = (raw_value or "").strip()
    if not raw:
        return []

    parsed = None
    if raw[:1] in ("[", "{"):
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None

    if isinstance(parsed, dict):
        if isinstance(parsed.get("characters"), list):
            parsed = parsed.get("characters")
        else:
            parsed = [parsed]

    characters = []

    def _append_character(name, profile, role="", cadence="every_6_posts"):
        cleaned_name = (name or "").strip()[:80]
        cleaned_role = (role or "").strip()[:120]
        cleaned_profile = re.sub(r"\s+", " ", (profile or "").strip())[:1200]
        cadence_key = _normalize_facebook_character_cadence(cadence)
        cadence_option = _FACEBOOK_CHARACTER_CADENCE_MAP.get(cadence_key, _FACEBOOK_CHARACTER_CADENCE_OPTIONS[1])
        if not cleaned_name and cleaned_profile:
            cleaned_name = f"Character {len(characters) + 1}"
        if cleaned_name:
            characters.append(
                {
                    "key": cleaned_name.lower().replace(" ", "_")[:80],
                    "name": cleaned_name,
                    "role": cleaned_role,
                    "profile": cleaned_profile,
                    "cadence": cadence_key,
                    "cadence_label": cadence_option["label"],
                }
            )

    def _json_profile_text(value):
        if value is None:
            return ""
        if isinstance(value, str):
            cleaned = re.sub(r"\s+", " ", value.strip())
            return f"JSON Profile: {cleaned}" if cleaned else ""
        try:
            return f"JSON Profile: {json.dumps(value, ensure_ascii=True, sort_keys=True)}"
        except Exception:
            return ""

    if isinstance(parsed, list):
        for index, item in enumerate(parsed, start=1):
            if isinstance(item, str):
                _append_character(item, item)
            elif isinstance(item, dict):
                name = item.get("name") or item.get("character") or item.get("title") or f"Character {index}"
                role = item.get("role") or item.get("job") or item.get("archetype") or ""
                cadence = item.get("cadence") or item.get("frequency") or item.get("cadence_rule") or "every_6_posts"
                parts = []
                for key in ("description", "backstory", "voice", "traits", "hook", "running_bit", "guardrails", "notes"):
                    value = item.get(key)
                    if value:
                        label = key.replace("_", " ").title()
                        parts.append(f"{label}: {value}")
                json_profile = _json_profile_text(item.get("json_profile"))
                if json_profile:
                    parts.append(json_profile)
                profile = "; ".join(parts) or json.dumps(item, ensure_ascii=True)
                _append_character(name, profile, role=role, cadence=cadence)

    if characters:
        return characters

    for line in raw.splitlines():
        cleaned = line.strip(" -\t")
        if not cleaned:
            continue
        if ":" in cleaned:
            name, details = cleaned.split(":", 1)
            _append_character(name, details)
        else:
            _append_character(cleaned, cleaned)
    return characters


def _select_facebook_recurring_character(brand, requested=""):
    characters = _parse_facebook_recurring_characters((brand or {}).get("facebook_recurring_characters"))
    if not characters:
        return None
    requested_clean = (requested or "").strip().lower()
    if not requested_clean:
        return None
    for character in characters:
        if requested_clean in {
            character["name"].strip().lower(),
            character["key"].strip().lower(),
        }:
            return character
    return None


def _format_facebook_storytelling_context(brand):
    strategy = ((brand or {}).get("facebook_storytelling_strategy") or "").strip()
    personality_key = _normalize_facebook_content_personality((brand or {}).get("facebook_content_personality"), default=None)
    cta_key = _normalize_facebook_cta_style((brand or {}).get("facebook_cta_style"), default=None)
    length_key = _normalize_facebook_post_length((brand or {}).get("facebook_post_length"), default=None)
    guardrails = ((brand or {}).get("facebook_storytelling_guardrails") or "").strip()
    characters = _parse_facebook_recurring_characters((brand or {}).get("facebook_recurring_characters"))
    personality = _FACEBOOK_CONTENT_PERSONALITY_MAP.get(personality_key)
    cta_style = _FACEBOOK_CTA_STYLE_MAP.get(cta_key)
    length_profile = _FACEBOOK_POST_LENGTH_MAP.get(length_key)

    context_lines = []
    if strategy:
        context_lines.append(f"Organic storytelling direction: {strategy}")
    if personality:
        context_lines.append(f"Personality mode: {personality['label']} - {personality['description']}")
        context_lines.append(f"Personality instruction: {personality['instruction']}")
    if cta_style:
        context_lines.append(f"CTA style: {cta_style['label']} - {cta_style['description']}")
        context_lines.append(f"CTA instruction: {cta_style['instruction']}")
    if length_profile:
        context_lines.append(f"Post length: {length_profile['label']} - {length_profile['description']}")
        context_lines.append(f"Length instruction: {length_profile['instruction']}")
    if guardrails:
        context_lines.append(f"Organic guardrails: {guardrails}")
    if characters:
        char_lines = []
        for character in characters:
            meta_parts = []
            if character.get("role"):
                meta_parts.append(character["role"])
            if character.get("cadence_label"):
                meta_parts.append(character["cadence_label"])
            meta = f" ({', '.join(meta_parts)})" if meta_parts else ""
            char_lines.append(f"- {character['name']}{meta}: {character.get('profile') or ''}".strip())
        context_lines.append("Recurring characters:\n" + "\n".join(char_lines))

    return {
        "strategy": strategy,
        "personality": personality,
        "cta_style": cta_style,
        "length_profile": length_profile,
        "guardrails": guardrails,
        "characters": characters,
        "prompt_block": "\n".join(context_lines).strip(),
    }


def _apply_recurring_characters_to_calendar_plan(calendar_plan, characters):
    if not calendar_plan or not characters:
        return [dict(slot) for slot in calendar_plan or []]

    eligible_types = {"behind_the_scenes", "team_intro", "business_growth", "community_spotlight", "character_spotlight", "value"}
    enriched = [dict(slot) for slot in calendar_plan]
    eligible_indices = [index for index, slot in enumerate(enriched) if slot.get("post_type") in eligible_types]
    if not eligible_indices:
        return enriched

    assignments = {}
    ordered_characters = []
    for position, character in enumerate(characters):
        cadence_key = _normalize_facebook_character_cadence(character.get("cadence"))
        cadence_option = _FACEBOOK_CHARACTER_CADENCE_MAP.get(cadence_key, _FACEBOOK_CHARACTER_CADENCE_OPTIONS[1])
        interval_posts = cadence_option.get("interval_posts")
        if interval_posts:
            target_count = max(1, (len(eligible_indices) + interval_posts - 1) // interval_posts)
        else:
            target_count = 1
        ordered_characters.append((0 - target_count, position, character, cadence_option, target_count))

    ordered_characters.sort()

    def _pick_even_slots(free_indices, count):
        if count <= 0 or not free_indices:
            return []
        if count >= len(free_indices):
            return list(free_indices)
        selected = []
        for offset in range(count):
            if count == 1:
                candidate_position = len(free_indices) // 2
            else:
                candidate_position = round(offset * (len(free_indices) - 1) / (count - 1))
            chosen = free_indices[candidate_position]
            if chosen in selected:
                for fallback in free_indices:
                    if fallback not in selected:
                        chosen = fallback
                        break
            selected.append(chosen)
        return selected

    for _negative_target, _position, character, cadence_option, target_count in ordered_characters:
        free_indices = [idx for idx in eligible_indices if idx not in assignments]
        selected_indices = _pick_even_slots(free_indices, min(target_count, len(free_indices)))
        for idx in selected_indices:
            assignments[idx] = {
                "character_name": character["name"],
                "character_profile": character.get("profile") or "",
                "character_cadence": cadence_option["label"],
            }

    if not assignments:
        fallback_index = eligible_indices[len(eligible_indices) // 2]
        fallback_character = characters[0]
        fallback_cadence = _FACEBOOK_CHARACTER_CADENCE_MAP.get(
            _normalize_facebook_character_cadence(fallback_character.get("cadence")),
            _FACEBOOK_CHARACTER_CADENCE_OPTIONS[1],
        )
        assignments[fallback_index] = {
            "character_name": fallback_character["name"],
            "character_profile": fallback_character.get("profile") or "",
            "character_cadence": fallback_cadence["label"],
        }

    for index, payload in assignments.items():
        enriched[index].update(payload)

    return enriched


def _build_facebook_storytelling_summary(brand):
    context = _format_facebook_storytelling_context(brand)
    return {
        "strategy": context["strategy"],
        "personality_label": (context.get("personality") or {}).get("label", ""),
        "cta_label": (context.get("cta_style") or {}).get("label", ""),
        "length_label": (context.get("length_profile") or {}).get("label", ""),
        "character_count": len(context.get("characters") or []),
        "has_profile": any(
            [
                context["strategy"],
                context.get("personality"),
                context.get("cta_style"),
                context.get("length_profile"),
                context["guardrails"],
                context.get("characters"),
            ]
        ),
        "characters": context.get("characters") or [],
    }


def _resolve_facebook_calendar_post_types(selected_types=None, content_mix=None):
    normalized_selected = _normalize_facebook_post_type_list(selected_types)
    mix_key = _normalize_facebook_content_mix(content_mix)
    mix = _FACEBOOK_CONTENT_MIX_MAP.get(mix_key, _FACEBOOK_CONTENT_MIXES[0])
    if not normalized_selected:
        return list(mix["post_types"]), mix

    ordered = [post_type for post_type in mix["post_types"] if post_type in normalized_selected]
    ordered.extend(post_type for post_type in normalized_selected if post_type not in ordered)
    return ordered, mix


def _build_facebook_calendar_slots(start_date, weeks, posts_per_week):
    offsets = _FACEBOOK_CALENDAR_DAY_OFFSETS.get(posts_per_week, _FACEBOOK_CALENDAR_DAY_OFFSETS[3])
    slots = []
    for week_index in range(weeks):
        base_date = start_date + timedelta(days=week_index * 7)
        for post_index, day_offset in enumerate(offsets):
            slot_date = base_date + timedelta(days=day_offset)
            slot_time = _FACEBOOK_CALENDAR_TIMES[post_index % len(_FACEBOOK_CALENDAR_TIMES)]
            scheduled_at = datetime.fromisoformat(f"{slot_date.isoformat()}T{slot_time}")
            slots.append(scheduled_at.strftime("%Y-%m-%dT%H:%M:%S"))
    return slots


def _build_facebook_calendar_plan(start_date, weeks, posts_per_week, selected_types=None, content_mix=None):
    post_types, mix = _resolve_facebook_calendar_post_types(selected_types, content_mix)
    slots = _build_facebook_calendar_slots(start_date, weeks, posts_per_week)
    plan = []
    for index, scheduled_at in enumerate(slots):
        post_type = post_types[index % len(post_types)]
        plan.append(
            {
                "index": index + 1,
                "scheduled_at": scheduled_at,
                "post_type": post_type,
                "post_type_label": _facebook_post_type_label(post_type),
                "description": _FACEBOOK_POST_TYPE_MAP[post_type]["description"],
                "prompt_focus": _FACEBOOK_POST_TYPE_MAP[post_type]["prompt_focus"],
                "content_mix": mix["key"],
            }
        )
    return plan


def _build_facebook_post_generation_messages(brand, post_type, brief="", character_name=""):
    post_type = _normalize_facebook_post_type(post_type)
    config = _FACEBOOK_POST_TYPE_MAP[post_type]
    brand_name = (brand.get("display_name") or brand.get("name") or "This business").strip()
    industry = (brand.get("industry") or "local services").strip()
    services = (brand.get("primary_services") or "").strip()
    area = (brand.get("service_area") or "").strip()
    voice = (brand.get("brand_voice") or "Clear, helpful, confident").strip()
    audience = (brand.get("target_audience") or "local customers").strip()
    offers = (brand.get("active_offers") or "").strip()
    story_ctx = _format_facebook_storytelling_context(brand)
    length_profile = story_ctx.get("length_profile")
    selected_character = _select_facebook_recurring_character(brand, character_name)
    storytelling_block = story_ctx.get("prompt_block") or "None provided"
    selected_character_block = (
        f"Requested recurring character: {selected_character['name']} - {selected_character.get('profile') or ''}"
        if selected_character
        else "Requested recurring character: None"
    )

    system_prompt = (
        "You write strong Facebook Page posts for local businesses. "
        "The posts should feel specific, human, and worth reading in-feed. "
        "Do not sound like a generic marketing assistant. Return only valid JSON."
    )
    user_prompt = f"""Write one Facebook Page post draft.

Business: {brand_name}
Industry: {industry}
Primary services: {services or 'Not provided'}
Service area: {area or 'Not provided'}
Target audience: {audience}
Brand voice: {voice}
Active offers: {offers or 'None provided'}
Post type: {config['label']}
Type goal: {config['description']}
Type focus: {config['prompt_focus']}
Organic storytelling controls:
{storytelling_block}
{selected_character_block}
Additional brief: {brief or 'None provided'}

Rules:
- Write like a competent local business owner or operator, not a social media guru.
- Keep the post {length_profile['single_post_words'] if length_profile else 'between 70 and 180 words'}.
- Make it specific to the service, audience, or local market when possible.
- Format the post for readability in-feed, not as one dense block of text.
- Use intentional line breaks between the hook, the body, and the CTA when it helps clarity.
- Prefer this flow when it fits the idea: opening hook, supporting body, closing CTA.
- Include a clear next step, but do not sound pushy.
- Avoid cliches, forced inspiration, and vague filler.
- Do NOT use em dashes.
- Use at most 2 hashtags, and only if they actually fit.
- Do not invent fake statistics, awards, or customer details.
- If a recurring character is assigned, center the post around that character and keep them consistent.
- If storytelling controls are provided, follow them even when they push the post away from generic service marketing.
- {_FACEBOOK_POST_TYPE_RULES[post_type]}

Return a JSON object with these exact keys:
- message: the Facebook post body
- image_hint: a short suggestion for what image or photo would fit this post
- link_url: either an empty string or a relevant website URL if a link would genuinely help this type of post
"""
    return system_prompt, user_prompt


def _build_facebook_calendar_generation_messages(brand, calendar_plan, brief="", content_mix=None):
    brand_name = (brand.get("display_name") or brand.get("name") or "This business").strip()
    industry = (brand.get("industry") or "local services").strip()
    services = (brand.get("primary_services") or "").strip()
    area = (brand.get("service_area") or "").strip()
    voice = (brand.get("brand_voice") or "Clear, helpful, confident").strip()
    audience = (brand.get("target_audience") or "local customers").strip()
    offers = (brand.get("active_offers") or "").strip()
    mix = _FACEBOOK_CONTENT_MIX_MAP.get(_normalize_facebook_content_mix(content_mix), _FACEBOOK_CONTENT_MIXES[0])
    story_ctx = _format_facebook_storytelling_context(brand)
    length_profile = story_ctx.get("length_profile")
    calendar_plan = _apply_recurring_characters_to_calendar_plan(calendar_plan, story_ctx.get("characters") or [])
    slot_lines = []
    for slot in calendar_plan:
        character_clause = ""
        if slot.get("character_name"):
            character_clause = f" - Feature character: {slot['character_name']} - {slot.get('character_profile') or ''}"
        slot_lines.append(
            f"{slot['index']}. {slot['scheduled_at']} - {slot['post_type_label']} - {slot['description']} - Focus: {slot['prompt_focus']}{character_clause}"
        )

    system_prompt = (
        "You create high-quality Facebook Page content calendars for local businesses. "
        "The writing should feel specific, human, and useful, not like a generic social media assistant. "
        "Return only valid JSON."
    )
    user_prompt = f"""Build a Facebook content calendar.

Business: {brand_name}
Industry: {industry}
Primary services: {services or 'Not provided'}
Service area: {area or 'Not provided'}
Target audience: {audience}
Brand voice: {voice}
Active offers: {offers or 'None provided'}
Content mix: {mix['label']}
Mix goal: {mix['description']}
Mix focus: {mix['prompt_focus']}
Organic storytelling controls:
{story_ctx.get('prompt_block') or 'None provided'}
Additional brief: {brief or 'None provided'}

Post plan:
{chr(10).join(slot_lines)}

Rules:
- Write one post for each planned slot and keep the same order.
- Each post should be {length_profile['calendar_words'] if length_profile else 'between 70 and 170 words'}.
- Make each post specific to the business, service mix, audience, or local market.
- Vary the opening lines so the calendar does not sound repetitive.
- Keep calls to action clear but not pushy.
- Avoid cliches, vague filler, and generic motivational language.
- Do NOT use em dashes.
- Use at most 2 hashtags per post, and only when they fit naturally.
- Do not invent fake reviews, fake names, fake awards, or fake metrics.
- Respect the assigned post type for each slot.
- If a slot includes a recurring character, build the post around that character and keep them consistent.
- If recurring characters are provided overall, weave them in periodically so the feed feels like an ongoing brand story.

Return a JSON object with this exact shape:
{{
    "posts": [
        {{
            "post_type": "value",
            "message": "...",
            "image_hint": "...",
            "link_url": ""
        }}
    ]
}}
"""
    return system_prompt, user_prompt


def _build_warren_lead_intelligence(db, brand):
    """Build a compact lead and pipeline snapshot for owner-facing Warren chat."""
    brand_id = brand["id"]
    from webapp.warren_pipeline import get_pipeline_metrics

    metrics = get_pipeline_metrics(db, brand_id)
    threads = db.get_lead_threads(brand_id, limit=40)
    stage_counts = metrics.get("stage_counts") or {}
    recent_threads = []
    unread_threads = 0

    for thread in threads:
        if len(recent_threads) < 5:
            recent_threads.append(
                {
                    "id": thread.get("id"),
                    "name": thread.get("lead_name") or thread.get("contact_name") or "Lead",
                    "stage": thread.get("status") or "new",
                    "channel": thread.get("channel") or "unknown",
                    "updated_at": thread.get("updated_at") or thread.get("last_message_at") or "",
                }
            )
        unread_threads += int(thread.get("unread_count") or 0)

    return {
        "total_leads": metrics.get("total_leads", 0),
        "active_leads": metrics.get("active_leads", 0),
        "conversion_rate": metrics.get("conversion_rate", 0),
        "avg_response_time_minutes": metrics.get("avg_response_time_minutes", 0),
        "channels": metrics.get("channels") or {},
        "stage_counts": stage_counts,
        "unread_threads": unread_threads,
        "won_leads": stage_counts.get("won", 0),
        "lost_leads": stage_counts.get("lost", 0),
        "recent_threads": recent_threads,
    }


# ── Feature gate (before_request on blueprint) ──


def _resolve_feature_state(db, feature_key, brand_id):
    flag = db.get_feature_flag(feature_key)
    if not flag or not flag.get("enabled"):
        return "off"

    level = flag.get("access_level") or "all"
    if level == "admin" and "user_id" not in session:
        return "off"
    if level == "beta" and not (brand_id and db.is_beta_brand(brand_id)):
        return "off"

    if brand_id:
        overrides = db.get_brand_feature_access(brand_id)
        return overrides.get(feature_key, "on")
    return "on"


def _feature_gate_error_response(feature_key):
    upgrade_url = url_for("client.client_feature_upgrade", feature_key=feature_key)
    wants_json = (
        request.path.startswith("/client/api/")
        or request.headers.get("X-Requested-With") in {"XMLHttpRequest", "PJAX"}
        or request.is_json
    )
    if wants_json or request.method != "GET":
        return jsonify({
            "ok": False,
            "error": "This feature requires an upgrade.",
            "feature_key": feature_key,
            "upgrade_url": upgrade_url,
        }), 403
    return redirect(upgrade_url)


def _build_feature_contact_message(brand, feature_flag):
    brand_name = (brand or {}).get("display_name") or "our brand"
    feature_name = (feature_flag or {}).get("label") or "this feature"
    description = ((feature_flag or {}).get("description") or "").strip()
    lines = [
        "Hi,",
        "",
        f"Please help us enable {feature_name} for {brand_name} in GroMore.",
    ]
    if description:
        lines.extend([
            "",
            f"What we want to turn on: {description}",
        ])
    lines.extend([
        "",
        "Please review the setup, any implementation work needed, and what it will take to get this live.",
        "",
        "Thanks,",
        session.get("client_name") or brand_name,
    ])
    return "\n".join(lines)


def _build_feature_upgrade_request(brand, feature_flag, note=""):
    brand_name = (brand or {}).get("display_name") or "Unknown Brand"
    feature_name = (feature_flag or {}).get("label") or "Feature Upgrade"
    description = ((feature_flag or {}).get("description") or "").strip()
    detail_lines = [
        f"Brand: {brand_name}",
        f"Feature: {feature_name}",
    ]
    if description:
        detail_lines.append(f"Feature detail: {description}")
    if note.strip():
        detail_lines.extend(["", "Client note:", note.strip()])
    return "\n".join(detail_lines)


def _build_getting_started_checklist(db, brand, brand_id, client_user_id):
    progress = db.get_client_onboarding_progress(brand_id, client_user_id) if client_user_id else {}
    connections = db.get_brand_connections(brand_id) or {}
    google_connected = (connections.get("google", {}).get("status") == "connected")
    meta_connected = (connections.get("meta", {}).get("status") == "connected")
    active_leads = db.get_active_lead_contacts(brand_id, limit=1)

    def _saved(key, field="is_completed"):
        return bool((progress.get(key) or {}).get(field))

    has_brand_profile = bool(
        ((brand or {}).get("website") or "").strip()
        and ((brand or {}).get("service_area") or "").strip()
        and ((brand or {}).get("primary_services") or "").strip()
    )

    items = [
        {
            "key": "connect_accounts",
            "title": "Connect Google or Meta",
            "description": "Start by linking at least one ad channel so Warren can read data and help you act on it.",
            "href": url_for("client.client_settings"),
            "cta_label": "Open Settings",
            "completed": google_connected or meta_connected,
            "auto": True,
        },
        {
            "key": "brand_profile",
            "title": "Fill out your business profile",
            "description": "Add your website, service area, and primary services so the rest of the app has real business context.",
            "href": url_for("client.client_settings"),
            "cta_label": "Finish Profile",
            "completed": has_brand_profile,
            "auto": True,
        },
        {
            "key": "quick_launch_visit",
            "title": "Open Quick Launch",
            "description": "Use the simplest campaign setup flow first. It is the fastest way to understand how the platform guides action.",
            "href": url_for("client.client_quick_launch"),
            "cta_label": "Go to Quick Launch",
            "completed": _saved("quick_launch_visit"),
            "auto": False,
        },
        {
            "key": "review_leads",
            "title": "Review Leads and profiles",
            "description": "Open Leads to see objections, blockers, and closeability so you know what Warren is tracking for each deal.",
            "href": url_for("client.client_inbox"),
            "cta_label": "Open Leads",
            "completed": bool(active_leads) or _saved("review_leads"),
            "auto": bool(active_leads),
        },
        {
            "key": "help_center",
            "title": "Open Help",
            "description": "Keep the Help Center as your fallback reference any time a page feels unfamiliar or you need the next step.",
            "href": url_for("client.client_help"),
            "cta_label": "Open Help",
            "completed": _saved("help_center"),
            "auto": False,
        },
    ]

    completed_count = sum(1 for item in items if item["completed"])
    dismissed = _saved("dashboard_checklist", "is_dismissed")
    return {
        "items": items,
        "completed_count": completed_count,
        "total_count": len(items),
        "all_done": completed_count >= len(items),
        "is_dismissed": dismissed,
        "progress_pct": int((completed_count / max(len(items), 1)) * 100),
    }


def _has_brand_profile_basics(brand):
    return bool(
        ((brand or {}).get("website") or "").strip()
        and ((brand or {}).get("service_area") or "").strip()
        and ((brand or {}).get("primary_services") or "").strip()
    )


def _build_warren_onboarding_context(db, brand, brand_id, client_user_id):
    progress = db.get_client_onboarding_progress(brand_id, client_user_id) if client_user_id else {}
    has_google, has_meta = _get_ad_connection_status(db, brand)
    has_brand_profile = _has_brand_profile_basics(brand)

    try:
        latest_dashboard_month = db.get_latest_dashboard_month(brand_id)
    except Exception:
        latest_dashboard_month = None

    first_run = not has_google and not has_meta and not latest_dashboard_month
    started = bool((progress.get("warren_onboarding_started") or {}).get("is_completed"))
    dismissed = bool((progress.get("warren_onboarding_v1") or {}).get("is_dismissed"))
    completed_override = bool((progress.get("warren_onboarding_v1") or {}).get("is_completed"))

    steps = [
        {
            "key": "connect_accounts",
            "title": "Connect your first ad channel",
            "description": "Link Google or Meta so Warren can see live account context instead of working blind.",
            "href": url_for("client.client_settings"),
            "cta_label": "Open Connections",
            "required": True,
            "completed": has_google or has_meta,
            "status_label": "Connected" if (has_google or has_meta) else "Needs attention",
        },
        {
            "key": "business_profile",
            "title": "Confirm your business basics",
            "description": "Add your website, service area, and primary services so Warren can speak from real business context.",
            "href": url_for("client.client_my_business"),
            "cta_label": "Open My Business",
            "required": True,
            "completed": has_brand_profile,
            "status_label": "Confirmed" if has_brand_profile else "Needs attention",
        },
        {
            "key": "help_center",
            "title": "Keep the Warren guide handy",
            "description": "Open the Warren guide for webhook, channel, and setup references while you are getting oriented.",
            "href": url_for("client.client_help", guide="warren"),
            "cta_label": "Open Warren Guide",
            "required": False,
            "completed": bool((progress.get("help_center") or {}).get("is_completed")),
            "status_label": "Opened" if bool((progress.get("help_center") or {}).get("is_completed")) else "Optional",
        },
    ]

    required_total = sum(1 for step in steps if step["required"])
    required_completed = sum(1 for step in steps if step["required"] and step["completed"])
    ready_for_workspace = bool(completed_override or required_completed >= required_total)

    return {
        "first_run": first_run,
        "is_started": started,
        "is_dismissed": dismissed,
        "is_completed": ready_for_workspace,
        "has_dashboard_data": bool(latest_dashboard_month),
        "steps": steps,
        "required_total": required_total,
        "required_completed": required_completed,
        "progress_pct": int((required_completed / max(required_total, 1)) * 100),
        "should_redirect": bool(first_run and not ready_for_workspace and not dismissed),
        "primary_prompt": (
            "I can walk you through setup, explain what each connection does, and help shape your business profile. "
            "Start with the two core steps below, then use the Warren chat on the right if you want help in plain English."
        ),
    }


def _build_warren_onboarding_chat_context(db, brand, brand_id, client_user_id):
    onboarding = _build_warren_onboarding_context(db, brand, brand_id, client_user_id)
    interview = _build_warren_onboarding_interview(db, brand, brand_id, client_user_id)
    missing_steps = [step["title"] for step in onboarding["steps"] if not step["completed"] and step["required"]]
    profile_gaps = []
    if not ((brand or {}).get("website") or "").strip():
        profile_gaps.append("website")
    if not ((brand or {}).get("service_area") or "").strip():
        profile_gaps.append("service area")
    if not ((brand or {}).get("primary_services") or "").strip():
        profile_gaps.append("primary services")
    if not ((brand or {}).get("target_audience") or "").strip():
        profile_gaps.append("target audience")
    if not ((brand or {}).get("brand_voice") or "").strip():
        profile_gaps.append("brand voice")
    if not ((brand or {}).get("goals") or "").strip() or str((brand or {}).get("goals")) == "[]":
        profile_gaps.append("goals")

    return {
        "progress_pct": onboarding["progress_pct"],
        "required_completed": onboarding["required_completed"],
        "required_total": onboarding["required_total"],
        "missing_required_steps": missing_steps,
        "profile_gaps": profile_gaps,
        "is_first_run": onboarding["first_run"],
        "is_completed": onboarding["is_completed"],
        "interview_progress_pct": interview["progress_pct"],
        "current_question": interview.get("current_question"),
        "owner_profile": interview.get("profile"),
        "guidance": (
            "Walk the owner through setup in plain English, ask one question at a time, avoid jargon, "
            "and use each answer to build a more useful operating profile for the business."
        ),
    }


WARREN_ONBOARDING_QUESTIONS = [
    {
        "key": "website",
        "stage": "identity",
        "title": "Business website",
        "prompt": "What website should Warren use as the primary source of truth for your business?",
        "help_text": "Paste the main live website URL, even if it still needs work.",
        "placeholder": "https://example.com",
        "target_field": "website",
        "multiline": False,
    },
    {
        "key": "service_area",
        "stage": "identity",
        "title": "Service area",
        "prompt": "What cities, neighborhoods, or radius do you actually want to serve?",
        "help_text": "Be specific enough that Warren can avoid recommending work outside your real coverage area.",
        "placeholder": "Phoenix, Scottsdale, Tempe",
        "target_field": "service_area",
        "multiline": False,
    },
    {
        "key": "primary_services",
        "stage": "offers",
        "title": "Primary services",
        "prompt": "What are the main services you want Warren to talk about first?",
        "help_text": "List the services that actually drive revenue, not every possible thing you can technically do.",
        "placeholder": "Google Ads management, landing pages, call tracking",
        "target_field": "primary_services",
        "multiline": True,
    },
    {
        "key": "target_audience",
        "stage": "offers",
        "title": "Best-fit customer",
        "prompt": "Who is your best-fit customer, and who is usually a bad fit?",
        "help_text": "This helps Warren tailor missions, advice, and messaging to the right buyer.",
        "placeholder": "Local service businesses doing at least 30k per month and willing to invest in growth.",
        "target_field": "target_audience",
        "multiline": True,
    },
    {
        "key": "brand_voice",
        "stage": "voice",
        "title": "Brand voice",
        "prompt": "How should Warren sound when helping you and when shaping messaging for the business?",
        "help_text": "Examples: direct, calm, premium, blunt, warm, high-trust, expert, simple.",
        "placeholder": "Direct, clear, premium, no fluff.",
        "target_field": "brand_voice",
        "multiline": True,
    },
    {
        "key": "active_offers",
        "stage": "offers",
        "title": "Current offers",
        "prompt": "What offers, packages, or positioning angles matter right now?",
        "help_text": "Warren should know what you are actively selling, not just what exists in theory.",
        "placeholder": "Free audit, first month discount, premium done-for-you management.",
        "target_field": "active_offers",
        "multiline": True,
    },
    {
        "key": "owner_goal",
        "stage": "goals",
        "title": "Primary business goal",
        "prompt": "What is the most important business outcome you want Warren helping you move right now?",
        "help_text": "Think about the next 90 days, not a vague forever goal.",
        "placeholder": "Get 12 qualified leads per month without wasting budget.",
        "target_field": None,
        "multiline": True,
    },
    {
        "key": "owner_skill_level",
        "stage": "guidance",
        "title": "Owner skill level",
        "prompt": "How hands-on and technical do you want Warren to assume you are?",
        "help_text": "Examples: beginner, operator, advanced, expert. This changes how much explanation Warren gives.",
        "placeholder": "Beginner, explain things plainly and tell me exactly what to click.",
        "target_field": None,
        "multiline": True,
    },
    {
        "key": "guidance_preferences",
        "stage": "guidance",
        "title": "How Warren should help",
        "prompt": "What kind of help is most useful for you: exact next steps, strategic advice, checklists, warnings, accountability, or something else?",
        "help_text": "This is where Warren's behavior becomes personal to the owner, not generic to the app.",
        "placeholder": "Give me one clear next step, explain why it matters, and warn me before I break something.",
        "target_field": None,
        "multiline": True,
    },
    {
        "key": "constraints",
        "stage": "guidance",
        "title": "Constraints and hard rules",
        "prompt": "What should Warren avoid, protect, or respect while helping this business?",
        "help_text": "Examples: budget ceilings, compliance concerns, tone limits, service exclusions, approval rules.",
        "placeholder": "Do not recommend anything that needs a long dev cycle. Keep language simple and avoid aggressive claims.",
        "target_field": None,
        "multiline": True,
    },
]


def _default_warren_onboarding_profile(brand):
    return {
        "website": ((brand or {}).get("website") or "").strip(),
        "service_area": ((brand or {}).get("service_area") or "").strip(),
        "primary_services": ((brand or {}).get("primary_services") or "").strip(),
        "target_audience": ((brand or {}).get("target_audience") or "").strip(),
        "brand_voice": ((brand or {}).get("brand_voice") or "").strip(),
        "active_offers": ((brand or {}).get("active_offers") or "").strip(),
        "owner_goal": "",
        "owner_skill_level": "",
        "guidance_preferences": "",
        "constraints": "",
    }


def _build_warren_onboarding_interview(db, brand, brand_id, client_user_id):
    session_state = db.get_client_onboarding_session(brand_id, client_user_id) if client_user_id else None
    profile = _default_warren_onboarding_profile(brand)
    if session_state and isinstance(session_state.get("profile"), dict):
        for key, value in session_state["profile"].items():
            if value is None:
                continue
            profile[str(key)] = str(value).strip()

    questions = []
    completed_count = 0
    current_question = None
    for question in WARREN_ONBOARDING_QUESTIONS:
        answer = str(profile.get(question["key"]) or "").strip()
        is_completed = bool(answer)
        if is_completed:
            completed_count += 1
        question_payload = dict(question)
        question_payload["answer"] = answer
        question_payload["completed"] = is_completed
        questions.append(question_payload)
        if current_question is None and not is_completed:
            current_question = question_payload

    if current_question is None and questions:
        current_question = questions[-1]

    stage_key = (session_state or {}).get("stage_key") or (current_question or {}).get("stage") or "identity"
    current_question_key = (session_state or {}).get("current_question_key") or (current_question or {}).get("key") or ""
    progress_pct = int((completed_count / max(len(questions), 1)) * 100)

    return {
        "session": session_state or {},
        "profile": profile,
        "questions": questions,
        "current_question": current_question,
        "current_question_key": current_question_key,
        "stage_key": stage_key,
        "completed_count": completed_count,
        "total_count": len(questions),
        "progress_pct": progress_pct,
        "is_complete": completed_count >= len(questions),
    }


def _update_warren_onboarding_brand_field(db, brand_id, question_key, answer):
    question = next((item for item in WARREN_ONBOARDING_QUESTIONS if item["key"] == question_key), None)
    if not question:
        return
    target_field = question.get("target_field")
    if not target_field:
        return
    db.update_brand_text_field(brand_id, target_field, answer)

# Map route function names → feature flag keys.
# Routes not listed here are ungated (login, logout, assistant, etc.).
_ENDPOINT_FEATURE_MAP = {
    "client_dashboard":            "dashboard",
    "client_dashboard_data":       "dashboard",
    "client_kpis":                 "kpis",
    "client_campaigns":            "campaigns",
    "client_campaign_detail":      "campaigns",
    "client_campaign_status":      "campaigns",
    "client_campaign_budget":      "campaigns",
    "client_add_negative_keyword": "campaigns",
    "client_campaign_create":      "campaigns",
    "client_campaign_generate":    "campaigns",
    "client_campaign_launch":      "campaigns",
    "client_campaign_upload_image":"campaigns",
    "client_campaign_save_draft":  "campaigns",
    "client_campaign_launch_draft":"campaigns",
    "client_campaign_delete_draft":"campaigns",
    "client_campaign_preflight":   "campaigns",
    "client_campaign_check_config":"campaigns",
    "client_quick_launch":         "quick_launch",
    "client_actions":              "missions",
    "client_actions_dismiss":      "missions",
    "client_actions_restore":      "missions",
    "client_actions_chat":         "missions",
    "client_coaching":             "coaching",
    "client_coaching_start":       "coaching",
    "client_ad_builder":           "ad_builder",
    "client_ad_builder_generate":  "ad_builder",
    "client_creative":             "creative",
    "client_creative_generate":    "creative",
    "client_creative_templates_list":  "creative",
    "client_creative_templates_save":  "creative",
    "client_creative_template_load":   "creative",
    "client_creative_template_update": "creative",
    "client_ai_copy_variants":     "creative",
    "client_blog":                 "blog",
    "client_blog_editor":          "blog",
    "client_blog_save":            "blog",
    "client_blog_delete":          "blog",
    "client_blog_import_csv":      "blog",
    "client_blog_test_connection": "blog",
    "client_blog_ai_generate":     "blog",
    "client_my_business":          "my_business",
    "client_upload_logo":          "my_business",
    "client_set_primary_logo":     "my_business",
    "client_rename_logo_variant":  "my_business",
    "client_delete_logo_variant":  "my_business",
    "client_crm":                  "crm",
    "client_crm_data":             "crm",
    "client_lead_assistant":       "crm",
    "client_save_lead_assistant_profile": "crm",
    "client_commercial_prospecting": "commercial",
    "client_commercial_search":    "commercial",
    "client_commercial_import":    "commercial",
    "client_commercial_thread":    "commercial",
    "client_commercial_thread_save_worksheet": "commercial",
    "client_commercial_thread_promote_research": "commercial",
    "client_commercial_thread_ai_worksheet": "commercial",
    "client_commercial_thread_qualification": "commercial",
    "client_commercial_thread_walkthrough": "commercial",
    "client_commercial_thread_refresh": "commercial",
    "client_commercial_thread_save_outreach": "commercial",
    "client_commercial_thread_rewrite_outreach": "commercial",
    "client_commercial_thread_reset_outreach": "commercial",
    "client_commercial_thread_send_email": "commercial",
    "client_commercial_thread_enroll_drip": "commercial",
    "client_commercial_thread_build_proposal": "commercial",
    "client_commercial_thread_service_visit": "commercial",
    "client_commercial_thread_delete": "commercial",
    "client_va_services":          "va_services",
    "client_va_request_create":    "va_services",
    "client_va_request_cancel":    "va_services",
    "client_inbox":                "warren_inbox",
    "client_inbox_thread":         "warren_inbox",
    "client_inbox_reply":          "warren_inbox",
    "client_inbox_stage":          "warren_inbox",
    "client_inbox_delete":         "warren_inbox",
    "client_inbox_warren_draft":   "warren_inbox",
    "client_gbp":                  "gbp",
    "client_gbp_audit":            "gbp",
    "client_post_scheduler":       "post_scheduler",
    "schedule_post":               "post_scheduler",
    "schedule_post_bulk":          "post_scheduler",
    "delete_scheduled_post":       "post_scheduler",
    "client_competitors":          "competitor_intel",
    "client_competitors_refresh_all": "competitor_intel",
    "client_competitor_refresh":   "competitor_intel",
    "client_add_competitor":       "competitor_intel",
    "client_delete_competitor":    "competitor_intel",
    "client_edit_competitor":      "competitor_intel",
    "client_settings":             "connections",
    "client_feedback":             "feedback",
    "client_feedback_submit":      "feedback",
    "client_help":                 "help",
    "client_team":                 "your_team",
    "client_team_data":            "your_team",
    "client_team_hire":            "your_team",
    "client_team_train":           "your_team",
    "client_team_findings":        "your_team",
    "client_dismiss_finding":      "your_team",
    "client_vote_finding":         "your_team",
    "client_finding_status":       "your_team",
    "client_run_team":             "your_team",
    "client_team_run_status":      "your_team",
    "client_staff":                "staff",
    "client_staff_invite":         "staff",
    "client_staff_update_role":    "staff",
    "client_staff_toggle_active":  "staff",
    "client_tasks":                "tasks",
    "client_task_create":          "tasks",
    "client_task_detail":          "tasks",
    "client_task_update":          "tasks",
    "client_task_delete":          "tasks",
    "client_task_from_finding":    "tasks",
}


@client_bp.before_request
def _check_feature_gate():
    """Block access to routes whose feature flag is not visible to this user."""
    endpoint = request.endpoint or ""
    # Strip blueprint prefix: "client.client_dashboard" → "client_dashboard"
    func_name = endpoint.split(".")[-1] if "." in endpoint else endpoint
    feature_key = _ENDPOINT_FEATURE_MAP.get(func_name)
    if not feature_key:
        return  # ungated route

    db = _get_db()
    brand_id = session.get("client_brand_id")
    state = _resolve_feature_state(db, feature_key, brand_id)
    if state == "off":
        abort(404)
    # Admin session sees everything
    if "user_id" in session:
        return
    if state == "on":
        return
    return _feature_gate_error_response(feature_key)


@client_bp.route("/upgrade/<feature_key>")
@client_login_required
def client_feature_upgrade(feature_key):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    feature_flag = db.get_feature_flag(feature_key)
    if not feature_flag:
        abort(404)

    state = _resolve_feature_state(db, feature_key, brand_id)
    if state == "off":
        abort(404)
    if state == "on":
        return redirect(url_for("client.client_dashboard"))

    return render_template(
        "client/client_feature_upgrade.html",
        brand=brand,
        feature_flag=feature_flag,
        contact_recipients=db.get_brand_upgrade_contacts(brand_id),
        default_contact_message=_build_feature_contact_message(brand, feature_flag),
        default_upgrade_request=_build_feature_upgrade_request(brand, feature_flag),
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/upgrade/<feature_key>/email-contacts", methods=["POST"])
@client_login_required
def client_feature_upgrade_email_contacts(feature_key):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    feature_flag = db.get_feature_flag(feature_key)
    if not feature_flag or _resolve_feature_state(db, feature_key, brand_id) != "upgrade":
        abort(404)

    recipients = db.get_brand_upgrade_contacts(brand_id)
    if not recipients:
        flash("Add a developer email or contact emails in Brand Settings first.", "warning")
        return redirect(url_for("client.client_feature_upgrade", feature_key=feature_key))

    subject = request.form.get("subject", "").strip() or f"{brand.get('display_name', 'Brand')} - {feature_flag.get('label', 'Feature')}"
    message = request.form.get("message", "").strip() or _build_feature_contact_message(brand, feature_flag)
    try:
        from webapp.email_sender import send_bulk_email

        send_bulk_email(current_app.config, recipients, subject, message)
        flash(f"Sent to {len(recipients)} contact(s).", "success")
    except Exception as exc:
        flash(f"Email failed: {exc}", "warning")

    return redirect(url_for("client.client_feature_upgrade", feature_key=feature_key))


@client_bp.route("/upgrade/<feature_key>/request", methods=["POST"])
@client_login_required
def client_feature_upgrade_request(feature_key):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    feature_flag = db.get_feature_flag(feature_key)
    if not feature_flag or _resolve_feature_state(db, feature_key, brand_id) != "upgrade":
        abort(404)

    subject = request.form.get("subject", "").strip() or f"Upgrade request: {feature_flag.get('label', 'Feature')}"
    message = request.form.get("message", "").strip() or _build_feature_upgrade_request(brand, feature_flag)

    db.create_upgrade_consideration({
        "title": f"{feature_flag.get('label', 'Feature')} for {brand.get('display_name', 'Brand')}",
        "description": _build_feature_upgrade_request(brand, feature_flag, message),
        "category": "feature",
        "request_count": 1,
    })

    admin_recipients = current_app.db.get_users_with_email()
    if admin_recipients:
        try:
            from webapp.email_sender import send_bulk_email

            send_bulk_email(current_app.config, admin_recipients, subject, message)
        except Exception as exc:
            flash(f"Upgrade request was logged, but admin email failed: {exc}", "warning")
            return redirect(url_for("client.client_feature_upgrade", feature_key=feature_key))

    flash("Upgrade request sent.", "success")
    return redirect(url_for("client.client_feature_upgrade", feature_key=feature_key))

@client_bp.route("/login", methods=["GET", "POST"])
def client_login():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        db = _get_db()
        user = db.authenticate_client(email, password)
        if user:
            session["client_user_id"] = user["id"]
            session["client_brand_id"] = user["brand_id"]
            session["client_name"] = user["display_name"]
            session["client_brand_name"] = user["brand_name"]
            session["client_role"] = user.get("role", "owner")

            # Warm caches in the background so navigation is snappy.
            current_month = datetime.now().strftime("%Y-%m")
            last_warm = session.get("warmup_started_month")
            if last_warm != current_month:
                session["warmup_started_month"] = current_month
                _warm_client_snapshots_async(brand_id=int(user["brand_id"]), month=current_month)

            db.update_client_user_login(user["id"])
            return redirect(url_for("client.client_dashboard"))
        flash("Invalid email or password", "error")
    return render_template("client_login.html")


def _consume_login_refresh_month(session_key: str, month: str) -> bool:
    """Return True once per login for the target month."""
    try:
        refresh_month = session.get(session_key)
        if refresh_month and refresh_month == month:
            session.pop(session_key, None)
            return True
    except Exception:
        pass
    return False


_CAMPAIGNS_CACHE_TTL_SECONDS = 6 * 60 * 60


def _campaigns_cache_key(brand_id: int, month: str) -> str:
    return f"campaigns_cache_{brand_id}_{month}"


def _get_campaigns_cached(db, brand: dict, month: str, *, force_sync: bool = False) -> dict:
    from webapp.campaign_manager import list_all_campaigns

    key = _campaigns_cache_key(int(brand.get("id") or 0), month)

    if not force_sync:
        try:
            raw = db.get_setting(key, "")
            if raw:
                cached = json.loads(raw)
                cached_at = float(cached.get("cached_at") or 0)
                campaigns = cached.get("campaigns")
                if (
                    isinstance(campaigns, dict)
                    and cached_at
                    and (time.time() - cached_at) < _CAMPAIGNS_CACHE_TTL_SECONDS
                ):
                    return campaigns
        except Exception:
            pass

    campaigns = list_all_campaigns(db, brand, month)
    try:
        db.save_setting(key, json.dumps({"cached_at": time.time(), "campaigns": campaigns}, default=str))
    except Exception:
        pass
    return campaigns


@client_bp.route("/forgot-password", methods=["GET", "POST"])
def client_forgot_password():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        db = _get_db()
        user = db.get_client_user_by_email(email)
        if user:
            import secrets as _secrets
            token = _secrets.token_urlsafe(32)
            expires = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            db.set_password_reset_token(user["id"], token, expires)
            try:
                from webapp.email_sender import send_password_reset_email
                reset_url = f"{_external_app_url()}{url_for('client.client_reset_password', token=token)}"
                send_password_reset_email(current_app.config, email, user["display_name"], reset_url)
            except Exception:
                current_app.logger.exception("Password reset email delivery failed for %s", email)
        # Always show success to prevent email enumeration
        flash("If that email is on file, you'll receive a reset link shortly.", "success")
        return render_template("client_forgot_password.html", sent=True)
    return render_template("client_forgot_password.html")


@client_bp.route("/reset-password/<token>", methods=["GET", "POST"])
def client_reset_password(token):
    db = _get_db()
    user = db.validate_password_reset_token(token)
    if not user:
        flash("This reset link is invalid or has expired.", "error")
        return redirect(url_for("client.client_forgot_password"))
    if request.method == "POST":
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template("client_reset_password.html", token=token)
        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("client_reset_password.html", token=token)
        db.update_client_user_password(user["id"], password)
        db.clear_password_reset_token(user["id"])
        flash("Password updated. You can now sign in.", "success")
        return redirect(url_for("client.client_login"))
    return render_template("client_reset_password.html", token=token)


@client_bp.route("/logout")
def client_logout():
    session.pop("client_user_id", None)
    session.pop("client_brand_id", None)
    session.pop("client_name", None)
    session.pop("client_brand_name", None)
    return redirect(url_for("client.client_login"))


# ── React SPA API endpoints ──

@client_bp.route("/api/me")
def api_me():
    """Return current authenticated user + brand for the React SPA."""
    uid = session.get("client_user_id")
    if not uid:
        return jsonify({"error": "not_authenticated"}), 401
    return jsonify({
        "user": {
            "id": uid,
            "display_name": session.get("client_name", ""),
            "role": session.get("client_role", "owner"),
        },
        "brand": {
            "id": session.get("client_brand_id"),
            "display_name": session.get("client_brand_name", ""),
        },
    })


@client_bp.route("/api/login", methods=["POST"])
def api_login():
    """JSON login for the React SPA."""
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return jsonify({"ok": False, "error": "Email and password are required."}), 400

    db = _get_db()
    user = db.authenticate_client(email, password)
    if not user:
        return jsonify({"ok": False, "error": "Invalid email or password."}), 401

    session["client_user_id"] = user["id"]
    session["client_brand_id"] = user["brand_id"]
    session["client_name"] = user["display_name"]
    session["client_brand_name"] = user["brand_name"]
    session["client_role"] = user.get("role", "owner")

    current_month = datetime.now().strftime("%Y-%m")
    last_warm = session.get("warmup_started_month")
    if last_warm != current_month:
        session["warmup_started_month"] = current_month
        _warm_client_snapshots_async(brand_id=int(user["brand_id"]), month=current_month)

    db.update_client_user_login(user["id"])
    return jsonify({
        "ok": True,
        "user": {
            "id": user["id"],
            "display_name": user["display_name"],
            "role": user.get("role", "owner"),
        },
        "brand": {
            "id": user["brand_id"],
            "display_name": user["brand_name"],
        },
    })


@client_bp.route("/api/logout", methods=["POST"])
def api_logout():
    """JSON logout for the React SPA."""
    session.pop("client_user_id", None)
    session.pop("client_brand_id", None)
    session.pop("client_name", None)
    session.pop("client_brand_name", None)
    return jsonify({"ok": True})


def _serialize_heatmap_scan(scan, include_results=False):
    from webapp.heatmap import summarize_competitor_landscape

    if not scan:
        return None
    payload = dict(scan)
    if include_results:
        try:
            payload["results"] = json.loads(payload.get("results_json") or "[]")
        except Exception:
            payload["results"] = []
        payload["competitor_summary"] = summarize_competitor_landscape(payload["results"])
    return payload


def _load_heatmap_state(db, brand_id, scan_limit=20):
    brand = db.get_brand(brand_id)
    if not brand:
        return None, [], None

    scans = db.get_heatmap_scans(brand_id, limit=scan_limit)
    active_scan = _serialize_heatmap_scan(scans[0], include_results=True) if scans else None
    return brand, scans, active_scan


@client_bp.route("/api/heatmap")
@client_login_required
def api_heatmap_state():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand, scans, active_scan = _load_heatmap_state(db, brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    return jsonify(
        ok=True,
        brand={
            "id": brand.get("id"),
            "display_name": brand.get("display_name", ""),
            "service_area": brand.get("service_area", ""),
            "business_lat": float(brand.get("business_lat") or 0),
            "business_lng": float(brand.get("business_lng") or 0),
            "google_maps_api_key": brand.get("google_maps_api_key", ""),
            "google_place_id": brand.get("google_place_id", ""),
        },
        scans=[_serialize_heatmap_scan(scan, include_results=False) for scan in scans],
        active_scan=active_scan,
    )


# ── Agent Activity Helper ──

def _log_agent(agent_key, action, detail="", status="completed"):
    """Log an agent activity for the current brand. Non-blocking, best-effort."""
    try:
        brand_id = session.get("client_brand_id")
        if brand_id:
            db = _get_db()
            db.log_agent_activity(brand_id, agent_key, action, detail, status)
    except Exception:
        pass


def _resolve_dashboard_month(db, brand_id, requested_month):
    explicit_request = bool((requested_month or "").strip())
    month = (requested_month or "").strip() or datetime.now().strftime("%Y-%m")
    try:
        fallback_month = db.get_latest_dashboard_month(brand_id)
    except Exception:
        fallback_month = None

    if explicit_request:
        try:
            available = db.get_available_dashboard_months(brand_id, limit=24)
        except Exception:
            available = []
        if fallback_month and month not in available and month >= fallback_month:
            return fallback_month, month, True
        return month, month, False
    if fallback_month and fallback_month != month:
        return fallback_month, month, True
    return month, month, False


# ── Dashboard ──

@client_bp.route("/")
@client_bp.route("/dashboard")
@client_login_required
def client_dashboard():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Your account is not linked to an active brand.", "error")
        return redirect(url_for("client.client_logout"))

    requested_month = request.args.get("month", "")
    month, requested_month, used_fallback = _resolve_dashboard_month(db, brand_id, requested_month)

    connections = db.get_brand_connections(brand_id) or {}
    google_connected = (connections.get("google", {}).get("status") == "connected")
    meta_connected = (connections.get("meta", {}).get("status") == "connected")

    has_google, has_meta = _get_ad_connection_status(db, brand)
    try:
        latest_dashboard_month = db.get_latest_dashboard_month(brand_id)
    except Exception:
        latest_dashboard_month = None
    first_run = not has_google and not has_meta and not latest_dashboard_month
    warren_onboarding = _build_warren_onboarding_context(db, brand, brand_id, session.get("client_user_id"))
    if warren_onboarding["should_redirect"]:
        return redirect(url_for("client.client_warren_onboarding"))

    onboarding = _build_getting_started_checklist(db, brand, brand_id, session.get("client_user_id"))

    return render_template(
        "client_dashboard.html",
        brand=brand,
        month=month,
        requested_month=requested_month,
        used_month_fallback=used_fallback,
        dashboard=None,
        error="",
        async_load=True,
        first_run=first_run,
        onboarding=onboarding,
        warren_onboarding=warren_onboarding,
        has_google=has_google,
        has_meta=has_meta,
        google_connected=google_connected,
        meta_connected=meta_connected,
        client_name=session.get("client_name", ""),
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/warren-onboarding")
@client_login_required
def client_warren_onboarding():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Your account is not linked to an active brand.", "error")
        return redirect(url_for("client.client_logout"))

    warren_onboarding = _build_warren_onboarding_context(db, brand, brand_id, session.get("client_user_id"))
    onboarding_interview = _build_warren_onboarding_interview(db, brand, brand_id, session.get("client_user_id"))
    onboarding = _build_getting_started_checklist(db, brand, brand_id, session.get("client_user_id"))

    return render_template(
        "client/client_warren_onboarding.html",
        brand=brand,
        onboarding=onboarding,
        warren_onboarding=warren_onboarding,
        onboarding_interview=onboarding_interview,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        client_name=session.get("client_name", ""),
    )


@client_bp.route("/warren-onboarding/status", methods=["POST"])
@client_login_required
def client_warren_onboarding_status():
    db = _get_db()
    brand_id = session["client_brand_id"]
    client_user_id = session["client_user_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    data = request.get_json(silent=True) or {}
    action = (data.get("action") or "start").strip().lower()

    if action == "start":
        db.save_client_onboarding_progress(brand_id, client_user_id, "warren_onboarding_started", is_completed=True)
    elif action == "dismiss":
        db.save_client_onboarding_progress(brand_id, client_user_id, "warren_onboarding_v1", is_dismissed=True)
    elif action == "restore":
        db.save_client_onboarding_progress(brand_id, client_user_id, "warren_onboarding_v1", is_dismissed=False)
    else:
        return jsonify({"error": "Invalid onboarding action."}), 400

    warren_onboarding = _build_warren_onboarding_context(db, brand, brand_id, client_user_id)
    redirect_url = url_for("client.client_dashboard") if (warren_onboarding["is_completed"] or warren_onboarding["is_dismissed"]) else url_for("client.client_warren_onboarding")
    return jsonify({"ok": True, "warren_onboarding": warren_onboarding, "redirect_url": redirect_url})


@client_bp.route("/warren-onboarding/interview", methods=["POST"])
@client_login_required
def client_warren_onboarding_interview_save():
    db = _get_db()
    brand_id = session["client_brand_id"]
    client_user_id = session["client_user_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found.", "error")
        return redirect(url_for("client.client_warren_onboarding"))

    question_key = (request.form.get("question_key") or "").strip()
    answer = (request.form.get("answer") or "").strip()
    question = next((item for item in WARREN_ONBOARDING_QUESTIONS if item["key"] == question_key), None)
    if not question:
        flash("Unknown onboarding question.", "error")
        return redirect(url_for("client.client_warren_onboarding"))
    if not answer:
        flash("Answer cannot be empty.", "warning")
        return redirect(url_for("client.client_warren_onboarding") + f"#question-{question_key}")

    current_interview = _build_warren_onboarding_interview(db, brand, brand_id, client_user_id)
    profile = dict(current_interview.get("profile") or {})
    profile[question_key] = answer

    ordered_keys = [item["key"] for item in WARREN_ONBOARDING_QUESTIONS]
    next_question_key = ""
    try:
        current_index = ordered_keys.index(question_key)
    except ValueError:
        current_index = -1
    for next_key in ordered_keys[current_index + 1:]:
        if not str(profile.get(next_key) or "").strip():
            next_question_key = next_key
            break
    if not next_question_key:
        for fallback_key in ordered_keys:
            if not str(profile.get(fallback_key) or "").strip():
                next_question_key = fallback_key
                break

    stage_key = question.get("stage") or "identity"
    status = "complete" if all(str(profile.get(key) or "").strip() for key in ordered_keys) else "active"
    db.save_client_onboarding_session(
        brand_id,
        client_user_id,
        status=status,
        stage_key=stage_key,
        current_question_key=next_question_key,
        profile=profile,
        notes={"last_saved_question": question_key},
    )
    _update_warren_onboarding_brand_field(db, brand_id, question_key, answer)
    db.save_client_onboarding_progress(brand_id, client_user_id, "warren_onboarding_started", is_completed=True)

    flash(f"Saved: {question['title']}", "success")
    anchor = f"#question-{next_question_key}" if next_question_key else "#onboarding-profile"
    return redirect(url_for("client.client_warren_onboarding") + anchor)


@client_bp.route("/dashboard/onboarding", methods=["POST"])
@client_login_required
def client_dashboard_onboarding_update():
    db = _get_db()
    brand_id = session["client_brand_id"]
    client_user_id = session["client_user_id"]
    data = request.get_json(silent=True) or {}
    item_key = (data.get("item_key") or "").strip()
    action = (data.get("action") or "complete").strip().lower()

    allowed_items = {
        "dashboard_checklist",
        "quick_launch_visit",
        "review_leads",
        "help_center",
    }
    if item_key not in allowed_items:
        return jsonify({"error": "Invalid onboarding item."}), 400

    if action == "dismiss":
        db.save_client_onboarding_progress(brand_id, client_user_id, item_key, is_dismissed=True)
    elif action == "restore":
        db.save_client_onboarding_progress(brand_id, client_user_id, item_key, is_dismissed=False)
    elif action == "complete":
        db.save_client_onboarding_progress(brand_id, client_user_id, item_key, is_completed=True)
    elif action == "reset":
        db.save_client_onboarding_progress(brand_id, client_user_id, item_key, is_completed=False, is_dismissed=False)
    else:
        return jsonify({"error": "Invalid onboarding action."}), 400

    brand = db.get_brand(brand_id) or {}
    onboarding = _build_getting_started_checklist(db, brand, brand_id, client_user_id)
    return jsonify({"ok": True, "onboarding": onboarding})


@client_bp.route("/dashboard/data")
@client_login_required
def client_dashboard_data():
    """JSON endpoint for async dashboard loading.

    Returns a cached snapshot when available (sub-second).
    Pass ?refresh=1 to force a live pull from ad platforms.
    """
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    requested_month = request.args.get("month", "")
    month, requested_month, used_fallback = _resolve_dashboard_month(db, brand_id, requested_month)

    force_refresh = (request.args.get("refresh") == "1")
    force_campaign_sync = (request.args.get("sync") == "1")

    # ── Fast path: serve from snapshot cache ──
    if not force_refresh and not force_campaign_sync:
        try:
            snapshot = db.get_dashboard_snapshot(brand_id, month)
            if snapshot:
                cached_data = json.loads(snapshot["snapshot_json"])
                cached_data["_cached"] = True
                cached_data["_cached_at"] = snapshot["created_at"]
                return jsonify({
                    "dashboard": cached_data,
                    "error": "",
                    "month": month,
                    "requested_month": requested_month,
                    "used_month_fallback": used_fallback,
                })
        except Exception:
            pass  # Fall through to live pull

    # ── Slow path: live pull ──
    try:
        from webapp.report_runner import get_analysis_and_suggestions_for_brand

        analysis, suggestions = get_analysis_and_suggestions_for_brand(
            db, brand, month, force_refresh=force_refresh
        )

        campaigns_data = {}
        try:
            campaigns_data = _get_campaigns_cached(db, brand, month, force_sync=force_campaign_sync)
        except Exception as exc:
            current_app.logger.exception("Campaign listing failed: %s", exc)

        if analysis:
            dashboard_data = _assemble_dashboard_payload(
                db, brand, brand_id, month, analysis, suggestions, campaigns_data
            )

            # Save snapshot for next visit
            try:
                source = "manual" if force_refresh else "auto"
                db.upsert_dashboard_snapshot(
                    brand_id, month,
                    json.dumps(dashboard_data, default=str),
                    source,
                )
            except Exception:
                pass

            # Include _cached_at so the UI shows "Synced just now" after refresh
            dashboard_data["_cached_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            _log_agent("scout", "Analyzed campaign performance", f"Scanned {len(campaigns_data.get('google', []))} Google + {len(campaigns_data.get('meta', []))} Meta campaigns")
            _log_agent("warren", "Built dashboard briefing", f"Month: {month}")
            return jsonify({
                "dashboard": dashboard_data,
                "error": "",
                "month": month,
                "requested_month": requested_month,
                "used_month_fallback": used_fallback,
            })
        else:
            # Live pull returned nothing - try stale snapshot before giving up
            stale = db.get_dashboard_snapshot(brand_id, month, max_age_hours=8760)
            if stale:
                stale_data = json.loads(stale["snapshot_json"])
                stale_data["_cached"] = True
                stale_data["_cached_at"] = stale["created_at"]
                return jsonify({
                    "dashboard": stale_data,
                    "error": "",
                    "month": month,
                    "requested_month": requested_month,
                    "used_month_fallback": used_fallback,
                })
            return jsonify({
                "dashboard": None,
                "error": "No data available for this month.",
                "month": month,
                "requested_month": requested_month,
                "used_month_fallback": used_fallback,
            })
    except Exception as e:
        refresh_error = str(e)
        current_app.logger.exception("Dashboard live pull failed for brand %s month %s: %s", brand_id, month, refresh_error)
        # Error during live pull - rebuild from snapshot analysis with fresh suggestions
        try:
            stale = db.get_dashboard_snapshot(brand_id, month, max_age_hours=8760)
            if stale and force_refresh:
                stale_data = json.loads(stale["snapshot_json"])
                # Try to regenerate from the snapshot's stored analysis data
                analysis_from_snap = stale_data.get("_analysis")
                if analysis_from_snap and isinstance(analysis_from_snap, dict):
                    try:
                        from src.suggestions import generate_suggestions
                        from webapp.client_advisor import build_client_dashboard
                        if any(analysis_from_snap.get(k) for k in ("google_analytics", "meta_business", "search_console", "google_ads")):
                            suggestions = generate_suggestions(analysis_from_snap)
                            campaigns_data = {}
                            try:
                                campaigns_data = _get_campaigns_cached(db, brand, month, force_sync=False)
                            except Exception:
                                pass
                            dashboard_data = _assemble_dashboard_payload(
                                db, brand, brand_id, month, analysis_from_snap, suggestions, campaigns_data
                            )
                            db.upsert_dashboard_snapshot(
                                brand_id, month,
                                json.dumps(dashboard_data, default=str),
                                source="regen_from_snapshot",
                            )
                            dashboard_data["_cached_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                            return jsonify({
                                "dashboard": dashboard_data,
                                "error": "",
                                "refresh_error": refresh_error,
                                "month": month,
                                "requested_month": requested_month,
                                "used_month_fallback": used_fallback,
                            })
                    except Exception:
                        current_app.logger.exception("Snapshot regen also failed")
                # Plain stale fallback
                stale_data["_cached"] = True
                stale_data["_cached_at"] = stale["created_at"]
                return jsonify({
                    "dashboard": stale_data,
                    "error": "",
                    "refresh_error": refresh_error,
                    "month": month,
                    "requested_month": requested_month,
                    "used_month_fallback": used_fallback,
                })
            elif stale:
                stale_data = json.loads(stale["snapshot_json"])
                stale_data["_cached"] = True
                stale_data["_cached_at"] = stale["created_at"]
                return jsonify({
                    "dashboard": stale_data,
                    "error": "",
                    "refresh_error": refresh_error,
                    "month": month,
                    "requested_month": requested_month,
                    "used_month_fallback": used_fallback,
                })
        except Exception:
            pass
        return jsonify({
            "dashboard": None,
            "error": refresh_error,
            "month": month,
            "requested_month": requested_month,
            "used_month_fallback": used_fallback,
        })


# ── KPI Deep Dive ──

@client_bp.route("/kpis")
@client_login_required
def client_kpis():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return redirect(url_for("client.client_login"))

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    kpi_data = None
    channels = {}
    error = None
    force_refresh = (request.args.get("refresh") == "1")

    # ── Fast path: serve from dashboard snapshot ──
    if not force_refresh:
        try:
            snapshot = db.get_dashboard_snapshot(brand_id, month)
            if snapshot:
                cached = json.loads(snapshot["snapshot_json"])
                kpi_data = cached.get("kpi_status")
                channels = cached.get("channels", {})
        except Exception:
            pass

    # ── Slow path: live pull (cache miss or forced refresh) ──
    if kpi_data is None:
        try:
            from webapp.report_runner import get_analysis_and_suggestions_for_brand
            from webapp.client_advisor import build_client_dashboard

            analysis, suggestions = get_analysis_and_suggestions_for_brand(
                db, brand, month, force_refresh=force_refresh
            )
            if analysis:
                dashboard = build_client_dashboard(analysis, suggestions, brand)
                kpi_data = dashboard.get("kpi_status", [])
                channels = dashboard.get("channels", {})
        except Exception as exc:
            current_app.logger.exception("KPI page data error: %s", exc)
            error = str(exc)

    return render_template(
        "client/client_kpis.html",
        month=month,
        kpi_data=kpi_data,
        channels=channels,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        error=error,
    )


# ── Actions Detail ──

@client_bp.route("/actions")
@client_login_required
def client_actions():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    requested_month = request.args.get("month") or ""
    month, requested_month, used_month_fallback = _resolve_dashboard_month(db, brand_id, requested_month)
    ai_model = _pick_ai_model(brand, "analysis", request.args.get("ai_model", ""))
    run_analysis = request.args.get("run_analysis") == "1"
    requested_skill_level = (request.args.get("skill_level") or "auto").strip().lower()

    actions = []
    ai_analysis = ""
    error = ""
    dismissed = db.get_dismissed_actions(brand_id, month) or []
    completed_count = len(dismissed)

    from webapp.client_advisor import MONTH_LEVELS, infer_mission_profile

    mission_profile = infer_mission_profile(
        completed_count=completed_count,
        requested_level=requested_skill_level,
    )
    force_refresh = (request.args.get("refresh") == "1") or run_analysis or mission_profile.get("source") == "manual"

    # ── Fast path: serve actions from dashboard snapshot ──
    if not force_refresh:
        try:
            snapshot = db.get_dashboard_snapshot(brand_id, month)
            if snapshot:
                cached = json.loads(snapshot["snapshot_json"])
                actions = cached.get("actions") or []
        except Exception:
            pass

    # ── Slow path: live pull (cache miss, forced refresh, or deep analysis) ──
    if not actions:
        try:
            from webapp.report_runner import get_analysis_and_suggestions_for_brand
            from webapp.client_advisor import build_client_dashboard

            analysis, suggestions = get_analysis_and_suggestions_for_brand(
                db, brand, month, force_refresh=force_refresh
            )
            if analysis:
                data = build_client_dashboard(
                    analysis,
                    suggestions,
                    brand,
                    ai_model=ai_model,
                    include_deep_analysis=run_analysis,
                    mission_profile=mission_profile,
                )
                actions = data.get("actions", [])
                ai_analysis = data.get("ai_analysis", "")

                # Save fresh snapshot so "last updated" timestamp reflects this regen
                if force_refresh and actions:
                    try:
                        snap_existing = db.get_dashboard_snapshot(brand_id, month, max_age_hours=8760)
                        snap_data = json.loads(snap_existing["snapshot_json"]) if snap_existing else {}
                        snap_data["actions"] = actions
                        if ai_analysis:
                            snap_data["ai_analysis"] = ai_analysis
                        db.upsert_dashboard_snapshot(
                            brand_id, month,
                            json.dumps(snap_data, default=str),
                            source="mission_regen",
                        )
                    except Exception:
                        pass
        except Exception as e:
            current_app.logger.exception("Mission Control load failed for brand %s month %s", brand_id, month)
            error = str(e)

    # Last resort: if no actions yet (regen failed, no data sources),
    # rebuild actions from the snapshot's own analysis data using fresh code
    if not actions and error:
        try:
            snapshot = db.get_dashboard_snapshot(brand_id, month, max_age_hours=8760)
            if snapshot:
                snap_data = json.loads(snapshot["snapshot_json"])
                # Try to extract analysis from the snapshot and regenerate
                analysis_from_snap = snap_data.get("_analysis")
                if analysis_from_snap and isinstance(analysis_from_snap, dict) and any(analysis_from_snap.get(k) for k in ("google_analytics", "meta_business", "search_console", "google_ads")):
                    from src.suggestions import generate_suggestions
                    from webapp.client_advisor import build_client_dashboard
                    suggestions = generate_suggestions(analysis_from_snap)
                    fresh = build_client_dashboard(
                        analysis_from_snap, suggestions, brand,
                        ai_model=ai_model, mission_profile=mission_profile,
                    )
                    actions = fresh.get("actions", [])
                    if actions:
                        snap_data["actions"] = actions
                        db.upsert_dashboard_snapshot(
                            brand_id, month,
                            json.dumps(snap_data, default=str),
                            source="mission_regen_from_snap",
                        )
                        error = ""
                if not actions:
                    # Plain fallback: serve old actions as-is
                    actions = snap_data.get("actions") or []
                    if actions:
                        error = ""
        except Exception:
            current_app.logger.exception("Mission snapshot regen also failed for brand %s", brand_id)

    actions = _normalize_client_actions(actions)
    for action in actions:
        steps = action.get("steps") or []
        action["step_count"] = len(steps)
        action["preview_steps"] = steps[: mission_profile.get("preview_steps", 3)]

    # ── Monthly cap: 20 total (completed + visible) ──
    # Already-completed items count toward the cap. The remaining visible
    # slots are filled from the generated pool so the user always sees
    # fresh work when they finish items.
    monthly_cap = 20
    visible_slots = max(0, monthly_cap - completed_count)

    profile_visible_cap = mission_profile.get("max_active", 4)
    if actions:
        visible_slots = max(1, min(profile_visible_cap, visible_slots or profile_visible_cap))
    else:
        visible_slots = 0

    # Split into active and completed
    active_actions = [a for a in actions if a["key"] not in dismissed][:visible_slots]
    done_actions = [a for a in actions if a["key"] in dismissed]
    featured_action = active_actions[0] if active_actions else None
    queued_actions = active_actions[1:] if len(active_actions) > 1 else []

    # ── XP & Level ──
    total_xp = sum(a.get("xp", 100) for a in done_actions)
    max_month_xp = monthly_cap * 150  # theoretical max if all high priority
    level_num = 1
    level_name = "Rookie"
    level_desc = "Just getting started"
    next_level_xp = 200
    for threshold, num, name, desc in MONTH_LEVELS:
        if total_xp >= threshold:
            level_num = num
            level_name = name
            level_desc = desc
    # Find next level threshold
    next_idx = level_num  # MONTH_LEVELS is 0-indexed, level_num is 1-based
    if next_idx < len(MONTH_LEVELS):
        next_level_xp = MONTH_LEVELS[next_idx][0]
    else:
        next_level_xp = total_xp

    return render_template(
        "client_actions.html",
        brand=brand,
        month=month,
        ai_model=ai_model,
        run_analysis=run_analysis,
        ai_analysis=ai_analysis,
        actions=actions,
        active_actions=active_actions,
        done_actions=done_actions,
        dismissed=dismissed,
        monthly_cap=monthly_cap,
        completed_count=completed_count,
        featured_action=featured_action,
        queued_actions=queued_actions,
        mission_profile=mission_profile,
        requested_skill_level=requested_skill_level,
        total_xp=total_xp,
        max_month_xp=max_month_xp,
        level_num=level_num,
        level_name=level_name,
        level_desc=level_desc,
        next_level_xp=next_level_xp,
        error=error,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/actions/dismiss", methods=["POST"])
@client_login_required
def client_actions_dismiss():
    db = _get_db()
    brand_id = session["client_brand_id"]
    data = request.get_json(silent=True) or {}
    action_key = (data.get("action_key") or "").strip()
    month = data.get("month") or datetime.now().strftime("%Y-%m")
    if not action_key:
        return jsonify({"error": "Missing action_key"}), 400
    db.dismiss_action(brand_id, month, action_key)
    return jsonify({"ok": True})


@client_bp.route("/actions/restore", methods=["POST"])
@client_login_required
def client_actions_restore():
    db = _get_db()
    brand_id = session["client_brand_id"]
    data = request.get_json(silent=True) or {}
    action_key = (data.get("action_key") or "").strip()
    month = data.get("month") or datetime.now().strftime("%Y-%m")
    if not action_key:
        return jsonify({"error": "Missing action_key"}), 400
    db.restore_action(brand_id, month, action_key)
    return jsonify({"ok": True})


@client_bp.route("/actions/chat", methods=["POST"])
@client_login_required
def client_actions_chat():
    return _client_assistant_chat_handler(request.form)


@client_bp.route("/assistant/chat", methods=["POST"])
@client_login_required
def client_assistant_chat():
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    return _client_assistant_chat_handler(payload)


@client_bp.route("/assistant/history")
@client_login_required
def client_assistant_history():
    db = _get_db()
    brand_id = session["client_brand_id"]
    month = _assistant_month()
    rows = db.get_ai_chat_messages(brand_id, month, limit=50)
    messages = [{"role": r.get("role"), "content": r.get("content", "")} for r in rows if r.get("content")]
    return jsonify({"messages": messages, "month": month})


@client_bp.route("/assistant/clear", methods=["POST"])
@client_login_required
def client_assistant_clear():
    db = _get_db()
    brand_id = session["client_brand_id"]
    month = _assistant_month()
    db.clear_ai_chat_messages(brand_id, month)
    return jsonify({"success": True})


@client_bp.route("/assistant/proactive", methods=["POST"])
@client_login_required
def client_assistant_proactive():
    """Generate a proactive check-in message from Warren on login/dashboard load."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"greeting": ""}), 200

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify({"greeting": ""}), 200

    month = _assistant_month()
    hint = (request.get_json(silent=True) or {}).get("hint", "")

    try:
        from webapp.report_runner import get_analysis_and_suggestions_for_brand
        from webapp.ai_assistant import chat_with_warren, summarize_analysis_for_ai, DEFAULT_CHAT_SYSTEM_PROMPT

        # Do not consume login refresh flags here. Keep this endpoint lightweight.
        analysis, suggestions = get_analysis_and_suggestions_for_brand(db, brand, month, force_refresh=False)
        summary = summarize_analysis_for_ai(analysis) if isinstance(analysis, dict) else None

        has_google, has_meta = _get_ad_connection_status(db, brand)

        proactive_prompt = (
            "You are greeting the user as they open their dashboard. "
            "Be brief (2-4 sentences). Do NOT repeat their name or say 'Welcome back'. "
            "Scan the data and lead with the single most important thing they need to know right now. "
            "If something is off-track, flag it directly. If things are going well, say so and suggest a next move. "
            "If no data is available, suggest connecting accounts or generating their first report. "
            "End with a question or nudge that invites a response."
        )
        if not has_google and not has_meta:
            proactive_prompt += " NOTE: No ad accounts are connected yet. Guide them to the Connections page."

        if hint:
            proactive_prompt += " Additional context: " + hint[:200]

        context = {
            "client_mode": True,
            "brand": {
                "name": brand.get("display_name"),
                "industry": brand.get("industry"),
                "service_area": brand.get("service_area"),
                "primary_services": brand.get("primary_services"),
                "monthly_budget": brand.get("monthly_budget"),
                "website": brand.get("website"),
                "goals": brand.get("goals"),
            },
            "month": month,
            "page_context": {"path": "/client/dashboard", "title": "Dashboard", "endpoint": "client.client_dashboard", "hint": "proactive greeting on load"},
            "analysis": summary,
            "suggestions": suggestions,
        }

        reply = chat_with_warren(
            api_key=api_key,
            model=_pick_ai_model(brand, "chat"),
            context=context,
            messages=[{"role": "user", "content": proactive_prompt}],
            admin_system_prompt=db.get_setting("ai_chat_system_prompt", "").strip() or DEFAULT_CHAT_SYSTEM_PROMPT,
            timeout=30,
            db=db,
            brand_id=brand_id,
        )
        reply = (reply or "").strip()
        if reply:
            db.add_ai_chat_message(brand_id, month, "assistant", reply)
        return jsonify({"greeting": reply})
    except Exception as e:
        log.warning("Proactive greeting failed: %s", e)
        return jsonify({"greeting": ""}), 200


@client_bp.route("/coaching")
@client_login_required
def client_coaching():
    """Warren Coaching - structured strategy session."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        flash("Brand not found.", "error")
        return redirect(url_for("client.client_dashboard"))

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")
    return render_template(
        "client/client_coaching.html",
        brand=brand,
        month=month,
    )


@client_bp.route("/coaching/start", methods=["POST"])
@client_login_required
def client_coaching_start():
    """Start a coaching session - Warren analyzes the account and opens the conversation."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify({"error": "No OpenAI API key configured. Add one in Connections."}), 400

    month = (request.get_json(silent=True) or {}).get("month") or _assistant_month()
    topic = (request.get_json(silent=True) or {}).get("topic", "general")

    try:
        from webapp.report_runner import get_analysis_and_suggestions_for_brand
        from webapp.ai_assistant import chat_with_warren, summarize_analysis_for_ai, DEFAULT_CHAT_SYSTEM_PROMPT

        # Do not consume login refresh flags here. Keep coaching start responsive.
        analysis, suggestions = get_analysis_and_suggestions_for_brand(db, brand, month, force_refresh=False)
        summary = summarize_analysis_for_ai(analysis) if isinstance(analysis, dict) else None

        topic_prompts = {
            "general": (
                "Run a coaching check-in. Scan every data point available. "
                "For each campaign or area you flag, use the structured format: STATUS (Winning/Underperforming/Neutral), "
                "WHY (root cause), ACTION (Cut/Scale/Fix/Test/Hold), PRIORITY (High/Medium/Low). "
                "Score each campaign: Kill/Fix/Scale/Test. "
                "Identify the top 2-3 things that need attention and explain why clearly. "
                "Connect every point to cost per lead or revenue impact. "
                "Then ask what the user is struggling with or what they want to focus on. "
                "Be direct, not generic."
            ),
            "budget": (
                "Focus on budget and spend efficiency. Look at CPA, ROAS, daily budget, "
                "and spend distribution across campaigns. Identify waste or underspend. "
                "Score each campaign: Kill (wasting money)/Fix (fixable)/Scale (earning)/Test (needs data). "
                "Frame every finding in dollar terms: how much is being wasted, how much could be saved. "
                "Then ask about their budget goals or constraints."
            ),
            "creative": (
                "Focus on ad creative and messaging. Look at CTR, engagement rates, "
                "top-performing ads vs underperformers. Identify patterns in what works. "
                "Then ask what messaging angles they want to explore."
            ),
            "growth": (
                "Focus on growth opportunities. Look at search terms, keyword opportunities, "
                "audience signals, and competitor gaps. Identify untapped potential. "
                "Then ask about their growth priorities for the next 30 days."
            ),
            "troubleshoot": (
                "The user needs help diagnosing a problem. Scan all metrics for red flags: "
                "declining trends, off-track KPIs, high CPAs, low CTR, wasted spend. "
                "For each problem, state: STATUS, WHY it's happening, ACTION to fix it, PRIORITY level. "
                "Estimate the dollar impact of each issue when possible. "
                "Present your findings clearly and ask what symptoms they are seeing."
            ),
        }

        coaching_prompt = topic_prompts.get(topic, topic_prompts["general"])
        coaching_prompt = (
            "You are starting a focused coaching session. "
            + coaching_prompt
            + " Keep your opening to 3-5 sentences. Use specific numbers from the data. "
            "End with 1-2 targeted questions to understand their situation better."
        )

        context = {
            "client_mode": True,
            "brand": {
                "name": brand.get("display_name"),
                "industry": brand.get("industry"),
                "service_area": brand.get("service_area"),
                "primary_services": brand.get("primary_services"),
                "monthly_budget": brand.get("monthly_budget"),
                "website": brand.get("website"),
                "goals": brand.get("goals"),
                "kpi_target_cpa": brand.get("kpi_target_cpa"),
                "kpi_target_leads": brand.get("kpi_target_leads"),
                "kpi_target_roas": brand.get("kpi_target_roas"),
                "competitors": brand.get("competitors"),
            },
            "month": month,
            "page_context": {"path": "/client/coaching", "title": "Coaching Session", "endpoint": "client.client_coaching", "hint": "coaching session, topic: " + topic},
            "analysis": summary,
            "suggestions": suggestions,
        }

        reply = chat_with_warren(
            api_key=api_key,
            model=_pick_ai_model(brand, "chat"),
            context=context,
            messages=[{"role": "user", "content": coaching_prompt}],
            admin_system_prompt=db.get_setting("ai_chat_system_prompt", "").strip() or DEFAULT_CHAT_SYSTEM_PROMPT,
            timeout=60,
            db=db,
            brand_id=brand_id,
        )
        reply = (reply or "").strip()
        if reply:
            db.add_ai_chat_message(brand_id, month, "assistant", reply)
            _log_agent("warren", f"Started {topic} coaching session", f"Month: {month}")
        return jsonify({"reply": reply})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _client_assistant_chat_handler(payload):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    onboarding_interview = _build_warren_onboarding_interview(db, brand, brand_id, session.get("client_user_id"))

    month = (payload.get("month") or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", month):
        month = datetime.now().strftime("%Y-%m")
    user_message = (payload.get("message") or "").strip()
    ai_model = _pick_ai_model(brand, "chat", payload.get("ai_model", ""))
    page_context = {
        "path": str(payload.get("page_path") or request.path),
        "title": str(payload.get("page_title") or ""),
        "endpoint": str(payload.get("page_endpoint") or request.endpoint or ""),
        "hint": str(payload.get("page_hint") or ""),
    }
    onboarding_mode = (
        page_context["endpoint"] == "client.client_warren_onboarding"
        or page_context["path"].startswith("/client/warren-onboarding")
        or page_context["hint"] == "warren_onboarding"
    )

    # Canvas screenshot from Creative Center (base64 data URI)
    canvas_image = (payload.get("canvas_image") or "").strip() or None

    # User-attached file (image or document)
    attached_file = payload.get("attached_file") or None
    attached_image = None
    attached_text = None
    if isinstance(attached_file, dict):
        file_data = (attached_file.get("data") or "").strip()
        file_type = (attached_file.get("type") or "").strip()
        file_name = (attached_file.get("name") or "file").strip()
        if file_data:
            if file_type.startswith("image/"):
                attached_image = file_data
            else:
                # Extract text content from base64 data URI for non-image files
                try:
                    import base64
                    raw = file_data
                    if "," in raw:
                        raw = raw.split(",", 1)[1]
                    decoded = base64.b64decode(raw)
                    attached_text = f"[Attached file: {file_name}]\n" + decoded.decode("utf-8", errors="replace")[:50000]
                except Exception:
                    attached_text = f"[Attached file: {file_name} - could not read contents]"

    # Use attached image OR canvas image (attached takes priority)
    vision_image = attached_image or canvas_image

    if not user_message and not attached_file:
        return jsonify({"error": "Message cannot be empty"}), 400

    # Build the stored message (include file reference if attached)
    stored_message = user_message
    if attached_text:
        stored_message = (user_message + "\n\n" + attached_text) if user_message else attached_text
    elif attached_image:
        stored_message = (user_message + "\n\n[Image attached]") if user_message else "[Image attached]"

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify({"error": "No OpenAI API key configured. Add one in Connections."}), 400

    db.add_ai_chat_message(brand_id, month, "user", stored_message)

    try:
        from webapp.report_runner import get_analysis_and_suggestions_for_brand
        from webapp.ai_assistant import chat_with_warren, summarize_analysis_for_ai

        analysis = None
        suggestions = None
        analysis_error = ""
        try:
            # Do not consume login refresh flags here. Keep chat responsive.
            analysis, suggestions = get_analysis_and_suggestions_for_brand(db, brand, month, force_refresh=False)
        except Exception as e:
            analysis_error = str(e)

        history = db.get_ai_chat_messages(brand_id, month, limit=50)
        trimmed = history[-25:] if len(history) > 25 else history
        messages = [{"role": m["role"], "content": m["content"]} for m in trimmed if m.get("content")]

        context = {
            "client_mode": True,
            "brand": {
                "name": brand.get("display_name"),
                "industry": brand.get("industry"),
                "service_area": brand.get("service_area"),
                "primary_services": brand.get("primary_services"),
                "monthly_budget": brand.get("monthly_budget"),
                "website": brand.get("website"),
                "goals": brand.get("goals"),
                "brand_voice": brand.get("brand_voice"),
                "active_offers": brand.get("active_offers"),
                "target_audience": brand.get("target_audience"),
                "competitors": brand.get("competitors"),
                "reporting_notes": brand.get("reporting_notes"),
                "kpi_target_cpa": brand.get("kpi_target_cpa"),
                "kpi_target_leads": brand.get("kpi_target_leads"),
                "kpi_target_roas": brand.get("kpi_target_roas"),
                "brand_colors": brand.get("brand_colors"),
                "call_tracking_number": brand.get("call_tracking_number"),
            },
            "month": month,
            "page_context": page_context,
            "analysis": summarize_analysis_for_ai(analysis) if isinstance(analysis, dict) else None,
            "suggestions": suggestions,
            "analysis_error": analysis_error,
            "lead_intelligence": _build_warren_lead_intelligence(db, brand),
            "onboarding_profile": onboarding_interview.get("profile"),
            "attached_text": attached_text,
            "_user_image_upload": bool(attached_image),
        }
        if onboarding_mode:
            context["onboarding_mode"] = True
            context["onboarding_context"] = _build_warren_onboarding_chat_context(db, brand, brand_id, session.get("client_user_id"))

        from webapp.ai_assistant import DEFAULT_CHAT_SYSTEM_PROMPT
        assistant_reply = chat_with_warren(
            api_key=api_key,
            model=ai_model,
            context=context,
            messages=messages,
            admin_system_prompt=(
                db.get_setting("ai_chat_system_prompt", "").strip()
                or DEFAULT_CHAT_SYSTEM_PROMPT
            ),
            timeout=90,
            db=db,
            brand_id=brand_id,
            canvas_image=vision_image,
        )
        assistant_reply = (assistant_reply or "").strip()
        if assistant_reply:
            db.add_ai_chat_message(brand_id, month, "assistant", assistant_reply)
            _log_agent("warren", "Responded to strategy question", user_message[:60])

        return jsonify({"reply": assistant_reply})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Ad Builder ──

@client_bp.route("/ad-builder")
@client_login_required
def client_ad_builder():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    has_data = False
    error = ""
    force_refresh = (request.args.get("refresh") == "1")

    # ── Fast path: snapshot exists means data is available ──
    if not force_refresh:
        try:
            snapshot = db.get_dashboard_snapshot(brand_id, month)
            if snapshot:
                has_data = True
        except Exception:
            pass

    # ── Slow path: live pull (cache miss or forced refresh) ──
    if not has_data:
        try:
            from webapp.report_runner import get_analysis_and_suggestions_for_brand
            analysis, _ = get_analysis_and_suggestions_for_brand(db, brand, month, force_refresh=force_refresh)
            has_data = bool(analysis)
        except Exception as e:
            error = str(e)

    return render_template(
        "client_ad_builder.html",
        brand=brand,
        month=month,
        has_data=has_data,
        google_ads=None,
        facebook_ads=None,
        error=error,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/ad-builder/generate", methods=["POST"])
@client_login_required
def client_ad_builder_generate():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.form.get("month") or datetime.now().strftime("%Y-%m")

    platform = request.form.get("platform", "")
    strategy = request.form.get("strategy", "")

    if platform not in ("google", "facebook"):
        flash("Select a platform.", "error")
        return redirect(url_for("client.client_ad_builder", month=month))

    analysis = None
    error = ""
    try:
        from webapp.report_runner import get_analysis_and_suggestions_for_brand
        analysis, _ = get_analysis_and_suggestions_for_brand(db, brand, month, force_refresh=False)
    except Exception as e:
        error = str(e)

    if not analysis:
        flash(error or "No data available for this month.", "error")
        return redirect(url_for("client.client_ad_builder", month=month))

    google_ads = None
    facebook_ads = None


    from webapp.ad_builder import generate_google_ads, generate_facebook_ads

    if platform == "google":
        google_ads = generate_google_ads(analysis, brand, strategy)
        if not google_ads:
            flash("AI generation failed. Check that your OpenAI key is configured in Settings.", "error")
            return redirect(url_for("client.client_ad_builder", month=month))
    else:
        facebook_ads = generate_facebook_ads(analysis, brand, strategy)
        if not facebook_ads:
            flash("AI generation failed. Check that your OpenAI key is configured in Settings.", "error")
            return redirect(url_for("client.client_ad_builder", month=month))

    _log_agent("ace", f"Generated {platform} ad copy", strategy or "default strategy")

    # Auto-save ad package to Drive
    try:
        import json as _json
        from webapp.google_drive import upload_file as drive_upload
        ad_data = google_ads if google_ads else facebook_ads
        if ad_data:
            ad_json = _json.dumps(ad_data, indent=2, default=str).encode("utf-8")
            label = "google" if google_ads else "facebook"
            fname = f"ad_package_{label}_{month}_{datetime.now().strftime('%H%M%S')}.json"
            drive_upload(db, brand_id, "Ads", fname, ad_json, "application/json")
    except Exception:
        pass  # Drive save is best-effort

    return render_template(
        "client_ad_builder.html",
        brand=brand,
        month=month,
        has_data=True,
        google_ads=google_ads,
        facebook_ads=facebook_ads,
        error="",
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


# ── Campaigns ──

@client_bp.route("/campaigns")
@client_login_required
def client_campaigns():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    from webapp.campaign_manager import get_campaign_recommendations

    force_campaign_sync = (request.args.get("sync") == "1")
    campaigns = None
    recommendations = []

    # ── Fast path: serve from dashboard snapshot ──
    if not force_campaign_sync:
        try:
            snapshot = db.get_dashboard_snapshot(brand_id, month)
            if snapshot:
                cached = json.loads(snapshot["snapshot_json"])
                snap_campaigns = cached.get("campaigns")
                if isinstance(snap_campaigns, dict):
                    campaigns = snap_campaigns
        except Exception:
            pass

    # ── Slow path: live pull (cache miss or forced sync) ──
    if campaigns is None:
        campaigns = _get_campaigns_cached(db, brand, month, force_sync=force_campaign_sync)

    if any(campaigns.values()):
        try:
            recommendations = get_campaign_recommendations(brand, campaigns)
        except Exception:
            pass

    changes = db.get_campaign_changes(brand_id, limit=20)
    drafts = db.get_campaign_drafts(brand_id)

    return render_template(
        "client_campaigns.html",
        brand=brand,
        month=month,
        campaigns=campaigns,
        recommendations=recommendations,
        changes=changes,
        drafts=drafts,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/campaigns/<platform>/<campaign_id>")
@client_login_required
def client_campaign_detail(platform, campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    if platform not in ("google", "meta"):
        abort(404)

    from webapp.campaign_manager import get_google_campaign_detail, get_meta_campaign_detail

    if platform == "google":
        campaign = get_google_campaign_detail(db, brand, campaign_id, month)
    else:
        campaign = get_meta_campaign_detail(db, brand, campaign_id, month)

    if not campaign:
        flash("Campaign not found or API error.", "error")
        return redirect(url_for("client.client_campaigns"))

    changes = db.get_campaign_changes(brand_id, limit=20)

    return render_template(
        "client_campaign_detail.html",
        brand=brand,
        campaign=campaign,
        platform=platform,
        month=month,
        changes=[c for c in changes if c.get("campaign_id") == campaign_id],
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/campaigns/<platform>/<campaign_id>/status", methods=["POST"])
@client_login_required
def client_campaign_status(platform, campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    new_status = request.form.get("status", "").upper()
    if platform == "google" and new_status not in ("PAUSED", "ENABLED"):
        flash("Invalid status.", "error")
        return redirect(url_for("client.client_campaigns"))
    if platform == "meta" and new_status not in ("PAUSED", "ACTIVE"):
        flash("Invalid status.", "error")
        return redirect(url_for("client.client_campaigns"))

    from webapp.campaign_manager import update_google_campaign_status, update_meta_campaign_status

    changed_by = session.get("client_name", "client")

    if platform == "google":
        result = update_google_campaign_status(db, brand, campaign_id, new_status, changed_by)
    else:
        result = update_meta_campaign_status(db, brand, campaign_id, new_status, changed_by)

    if result.get("success"):
        label = "paused" if new_status in ("PAUSED",) else "enabled"
        flash(f"Campaign {label} successfully.", "success")
    else:
        flash(f"Failed: {result.get('error', 'Unknown error')}", "error")

    return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))


@client_bp.route("/campaigns/<platform>/<campaign_id>/budget", methods=["POST"])
@client_login_required
def client_campaign_budget(platform, campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    try:
        new_budget = float(request.form.get("daily_budget", 0))
    except (ValueError, TypeError):
        flash("Invalid budget amount.", "error")
        return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))

    if new_budget < 1 or new_budget > 10000:
        flash("Budget must be between $1 and $10,000 per day.", "error")
        return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))

    from webapp.campaign_manager import update_google_budget, update_meta_budget

    changed_by = session.get("client_name", "client")

    if platform == "google":
        budget_resource = request.form.get("budget_resource", "")
        result = update_google_budget(db, brand, campaign_id, budget_resource, new_budget, changed_by)
    else:
        result = update_meta_budget(db, brand, campaign_id, new_budget, changed_by)

    if result.get("success"):
        flash(f"Daily budget updated to ${new_budget:.2f}.", "success")
    else:
        flash(f"Failed: {result.get('error', 'Unknown error')}", "error")

    return redirect(url_for("client.client_campaign_detail", platform=platform, campaign_id=campaign_id))


@client_bp.route("/campaigns/google/<campaign_id>/negative-keyword", methods=["POST"])
@client_login_required
def client_add_negative_keyword(campaign_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    keyword = request.form.get("keyword", "").strip()
    match_type = request.form.get("match_type", "BROAD").upper()

    if not keyword:
        flash("Keyword cannot be empty.", "error")
        return redirect(url_for("client.client_campaign_detail", platform="google", campaign_id=campaign_id))

    if match_type not in ("BROAD", "PHRASE", "EXACT"):
        match_type = "BROAD"

    from webapp.campaign_manager import add_google_negative_keyword

    changed_by = session.get("client_name", "client")
    result = add_google_negative_keyword(db, brand, campaign_id, keyword, match_type, changed_by)

    if result.get("success"):
        flash(f'Negative keyword "{keyword}" added.', "success")
    else:
        flash(f"Failed: {result.get('error', 'Unknown error')}", "error")

    return redirect(url_for("client.client_campaign_detail", platform="google", campaign_id=campaign_id))


# ── Quick Launch (simplified campaign creator for beginners) ──

@client_bp.route("/quick-launch")
@client_login_required
def client_quick_launch():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    has_google, has_meta = _get_ad_connection_status(db, brand)

    return render_template(
        "client_quick_launch.html",
        brand=brand,
        has_google=has_google,
        has_meta=has_meta,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


# ── Campaign Creator ──

@client_bp.route("/campaigns/new")
@client_login_required
def client_campaign_create():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    has_google, has_meta = _get_ad_connection_status(db, brand)

    from webapp.campaign_templates import get_active_strategies

    # Compute safe daily budget default in Python (avoids Jinja math on bad data)
    try:
        daily_budget_default = round(float(brand.get("monthly_budget") or 500) / 30)
    except (ValueError, TypeError):
        daily_budget_default = 17

    draft_id = request.args.get("draft_id", type=int)
    draft = db.get_campaign_draft(draft_id, brand_id) if draft_id else None

    # Load Meta Pixels if connected
    meta_pixels = []
    if has_meta:
        try:
            connections = db.get_brand_connections(brand_id)
            meta_conn = next((c for c in connections if c.get("platform") == "meta"), None)
            if meta_conn:
                from webapp.api_bridge import _get_meta_token
                token = _get_meta_token(db, brand_id, meta_conn)
                ad_account_id = meta_conn.get("meta_ad_account_id", "")
                if token and ad_account_id:
                    import requests as _req
                    act_id = ad_account_id if ad_account_id.startswith("act_") else f"act_{ad_account_id}"
                    px_resp = _req.get(
                        f"https://graph.facebook.com/v21.0/{act_id}/adspixels",
                        params={"access_token": token, "fields": "id,name"},
                        timeout=10,
                    )
                    if px_resp.status_code == 200:
                        meta_pixels = px_resp.json().get("data", [])
        except Exception as exc:
            current_app.logger.warning("Failed to load Meta Pixels: %s", exc)

    return render_template(
        "client_campaign_create.html",
        brand=brand,
        has_google=has_google,
        has_meta=has_meta,
        strategies=get_active_strategies(),
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        draft=draft,
        daily_budget_default=daily_budget_default,
        meta_pixels=meta_pixels,
    )


@client_bp.route("/campaigns/new/generate", methods=["POST"])
@client_login_required
def client_campaign_generate():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"})

    service = request.form.get("service", "").strip()
    location = request.form.get("location", "").strip()
    monthly_budget = request.form.get("monthly_budget", "0").strip()
    platform = request.form.get("platform", "").strip()
    strategy_type = request.form.get("strategy_type", "").strip()
    notes = request.form.get("notes", "").strip()

    if not service or not location or not monthly_budget or not platform:
        return jsonify({"success": False, "error": "All fields are required"})

    try:
        monthly_budget = float(monthly_budget)
        if monthly_budget < 100:
            return jsonify({"success": False, "error": "Minimum monthly budget is $100"})
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Invalid budget"})

    from webapp.campaign_manager import generate_campaign_plan

    try:
        result = generate_campaign_plan(
            brand, service, location, monthly_budget, platform, notes,
            strategy_type=strategy_type,
        )
    except Exception as exc:
        from flask import current_app
        current_app.logger.exception("Campaign plan generation failed")
        result = {"success": False, "error": f"Plan generation error: {exc}"}

    if result.get("success"):
        _log_agent("scout", f"Generated {platform} campaign plan", f"{service} in {location}, ${monthly_budget}/mo")
        _log_agent("penny", "Reviewed campaign budget", f"${monthly_budget}/mo for {platform}")
    return jsonify(result)


@client_bp.route("/campaigns/new/launch", methods=["POST"])
@client_login_required
def client_campaign_launch():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"})

    plan_json = request.form.get("plan", "")
    if not plan_json:
        return jsonify({"success": False, "error": "No campaign plan provided"})

    try:
        plan = json.loads(plan_json)
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "Invalid plan data"})

    platform = plan.get("platform", "")
    changed_by = session.get("client_name", "client")

    from webapp.campaign_manager import launch_google_campaign, launch_meta_campaign

    try:
        if platform == "google":
            result = launch_google_campaign(db, brand, plan, changed_by)
        elif platform == "meta":
            result = launch_meta_campaign(db, brand, plan, changed_by)
        else:
            return jsonify({"success": False, "error": "Invalid platform"})
    except Exception as exc:
        from flask import current_app
        current_app.logger.exception("Campaign launch failed")
        result = {"success": False, "error": f"Launch error: {exc}"}

    if result.get("success"):
        _log_agent("scout", f"Launched {platform} campaign", plan.get("campaign_name", ""))
    return jsonify(result)


@client_bp.route("/campaigns/upload-image", methods=["POST"])
@client_login_required
def client_campaign_upload_image():
    """Handle direct image upload for campaign ads."""
    import os
    import uuid
    from werkzeug.utils import secure_filename

    if "image" not in request.files:
        return jsonify({"success": False, "error": "No image file provided"})

    file = request.files["image"]
    if not file.filename:
        return jsonify({"success": False, "error": "Empty filename"})

    allowed_ext = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in allowed_ext:
        return jsonify({"success": False, "error": f"File type {ext} not allowed. Use JPG, PNG, GIF, or WebP."})

    # Limit file size (10MB)
    file.seek(0, 2)
    size = file.tell()
    file.seek(0)
    if size > 10 * 1024 * 1024:
        return jsonify({"success": False, "error": "Image too large. Max 10MB."})

    safe_name = secure_filename(f"{uuid.uuid4().hex}{ext}")
    upload_dir = os.path.join(current_app.static_folder or "static", "uploads", "campaign_images")
    os.makedirs(upload_dir, exist_ok=True)
    save_path = os.path.join(upload_dir, safe_name)
    file.save(save_path)

    url = url_for("static", filename=f"uploads/campaign_images/{safe_name}")
    return jsonify({"success": True, "url": url, "filename": safe_name})


@client_bp.route("/campaigns/new/save-draft", methods=["POST"])
@client_login_required
def client_campaign_save_draft():
    db = _get_db()
    brand_id = session["client_brand_id"]

    plan_json = request.form.get("plan", "")
    if not plan_json:
        return jsonify({"success": False, "error": "No campaign plan provided"})

    try:
        plan = json.loads(plan_json)
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "Invalid plan data"})

    platform = plan.get("platform", "")
    campaign_name = plan.get("campaign_name", "Untitled Campaign")
    created_by = session.get("client_name", "client")

    # Update existing draft or create new one
    existing_draft_id = request.form.get("draft_id", type=int)
    if existing_draft_id:
        draft = db.get_campaign_draft(existing_draft_id, brand_id)
        if draft:
            db.update_campaign_draft(existing_draft_id, brand_id, platform, campaign_name, plan_json)
            return jsonify({
                "success": True,
                "draft_id": existing_draft_id,
                "message": "Draft updated.",
            })

    draft_id = db.save_campaign_draft(
        brand_id, platform, campaign_name, plan_json, created_by,
    )

    return jsonify({
        "success": True,
        "draft_id": draft_id,
        "message": f"Campaign plan saved as draft. You can launch it later from the Campaigns page.",
    })


@client_bp.route("/campaigns/drafts/<int:draft_id>/launch", methods=["POST"])
@client_login_required
def client_campaign_launch_draft(draft_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"})

    draft = db.get_campaign_draft(draft_id, brand_id)
    if not draft:
        return jsonify({"success": False, "error": "Draft not found"})

    try:
        plan = json.loads(draft["plan_json"])
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "Invalid draft data"})

    platform = plan.get("platform", "")
    changed_by = session.get("client_name", "client")

    from webapp.campaign_manager import launch_google_campaign, launch_meta_campaign

    try:
        if platform == "google":
            result = launch_google_campaign(db, brand, plan, changed_by)
        elif platform == "meta":
            result = launch_meta_campaign(db, brand, plan, changed_by)
        else:
            return jsonify({"success": False, "error": "Invalid platform"})
    except Exception as exc:
        from flask import current_app
        current_app.logger.exception("Campaign draft launch failed")
        result = {"success": False, "error": f"Launch error: {exc}"}

    if result.get("success"):
        db.delete_campaign_draft(draft_id, brand_id)

    return jsonify(result)


@client_bp.route("/campaigns/drafts/<int:draft_id>/delete", methods=["POST"])
@client_login_required
def client_campaign_delete_draft(draft_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    db.delete_campaign_draft(draft_id, brand_id)
    return jsonify({"success": True, "message": "Draft deleted."})


@client_bp.route("/campaigns/new/preflight", methods=["POST"])
@client_login_required
def client_campaign_preflight():
    """Warren pre-flight check before campaign launch."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"})

    plan_json = request.form.get("plan", "")
    if not plan_json:
        return jsonify({"success": False, "error": "No campaign plan provided"})

    try:
        plan = json.loads(plan_json)
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "Invalid plan data"})

    checks = []
    platform = plan.get("platform", "")

    # Check 1: Platform config
    has_google, has_meta = _get_ad_connection_status(db, brand)
    if platform == "google" and not has_google:
        checks.append({"status": "fail", "label": "Google Ads Connection", "detail": "Google Ads account not connected or missing credentials."})
    elif platform == "meta" and not has_meta:
        checks.append({"status": "fail", "label": "Meta Ads Connection", "detail": "Meta Ads account not connected or missing credentials."})
    else:
        checks.append({"status": "pass", "label": f"{platform.title()} Connection", "detail": "Account connected and ready."})

    # Check 2: Campaign name
    cname = plan.get("campaign_name", "").strip()
    if not cname or cname == "Untitled Campaign":
        checks.append({"status": "warn", "label": "Campaign Name", "detail": "Using a generic name. Consider something descriptive."})
    else:
        checks.append({"status": "pass", "label": "Campaign Name", "detail": f'"{cname}"'})

    # Check 3: Budget
    daily_budget = plan.get("daily_budget", 0)
    try:
        daily_budget = float(daily_budget)
    except (ValueError, TypeError):
        daily_budget = 0
    if daily_budget < 3:
        checks.append({"status": "fail", "label": "Daily Budget", "detail": f"${daily_budget}/day is too low. Minimum $3/day."})
    elif daily_budget < 10:
        checks.append({"status": "warn", "label": "Daily Budget", "detail": f"${daily_budget}/day is low. Consider $10+ for better results."})
    else:
        checks.append({"status": "pass", "label": "Daily Budget", "detail": f"${daily_budget}/day (${round(daily_budget * 30)}/mo)"})

    # Check 4: Ad groups / ad sets
    groups_key = "ad_groups" if platform == "google" else "ad_sets"
    groups = plan.get(groups_key, [])
    if not groups:
        checks.append({"status": "fail", "label": "Ad Groups" if platform == "google" else "Ad Sets", "detail": "No ad groups defined. Add at least one."})
    else:
        checks.append({"status": "pass", "label": f"{len(groups)} {'Ad Group' if platform == 'google' else 'Ad Set'}{'s' if len(groups) != 1 else ''}", "detail": "Structure looks good."})

    # Check 5: Ads in each group
    total_ads = 0
    empty_groups = 0
    for g in groups:
        ads = g.get("ads", []) if platform == "google" else g.get("ad_copy", [])
        total_ads += len(ads)
        if not ads:
            empty_groups += 1
    if empty_groups > 0:
        checks.append({"status": "warn", "label": "Ad Coverage", "detail": f"{empty_groups} group(s) have no ads. Consider adding at least one ad per group."})
    elif total_ads > 0:
        checks.append({"status": "pass", "label": "Ad Coverage", "detail": f"{total_ads} ad(s) across all groups."})

    # Check 5b: Meta objective requirements
    if platform == "meta":
        objective = (plan.get("objective") or "OUTCOME_TRAFFIC").strip()
        pixel_id = (plan.get("pixel_id") or "").strip()
        if objective == "OUTCOME_LEADS" and not pixel_id:
            checks.append({
                "status": "warn",
                "label": "Meta Lead Tracking",
                "detail": "Lead Generation is selected without a Meta Pixel. Launch will fall back to Website Traffic so the ad set can be created.",
            })

    # Check 6: Location
    location = plan.get("location_targeting", "").strip()
    if not location:
        checks.append({"status": "warn", "label": "Location Targeting", "detail": "No location set. Campaign will target broadly."})
    else:
        checks.append({"status": "pass", "label": "Location Targeting", "detail": location})

    # Check 7: Destination URL for Meta ads
    if platform == "meta":
        website_url = ((brand.get("website_url") or brand.get("website") or "")).strip()
        if not website_url:
            checks.append({
                "status": "fail",
                "label": "Website URL",
                "detail": "No website URL is set on My Business. Add one before publishing Meta ads.",
            })
        else:
            checks.append({"status": "pass", "label": "Website URL", "detail": website_url})

    # Determine overall verdict
    statuses = [c["status"] for c in checks]
    if "fail" in statuses:
        verdict = "BLOCKED"
    elif "warn" in statuses:
        verdict = "WARNINGS"
    else:
        verdict = "READY"

    return jsonify({"success": True, "checks": checks, "verdict": verdict})


@client_bp.route("/campaigns/check-config")
@client_login_required
def client_campaign_check_config():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"})

    from webapp.campaign_manager import check_google_ads_config, check_meta_ads_config

    return jsonify({
        "google": check_google_ads_config(db, brand),
        "meta": check_meta_ads_config(db, brand),
    })


# ── Settings / Connections ──

@client_bp.route("/my-business", methods=["GET", "POST"])
@client_login_required
def client_my_business():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    if request.method == "POST":
        section = request.form.get("section", "")

        if section == "voice":
            # Guardrails: cap text fields to reasonable lengths
            brand_voice = request.form.get("brand_voice", "")[:2000].strip()
            active_offers = request.form.get("active_offers", "")[:1000].strip()
            target_audience = request.form.get("target_audience", "")[:2000].strip()
            reporting_notes = request.form.get("reporting_notes", "")[:1000].strip()
            website_url = request.form.get("website_url", "")[:500].strip()

            db.update_brand_text_field(brand_id, "brand_voice", brand_voice)
            db.update_brand_text_field(brand_id, "active_offers", active_offers)
            db.update_brand_text_field(brand_id, "target_audience", target_audience)
            db.update_brand_text_field(brand_id, "reporting_notes", reporting_notes)
            db.update_brand_text_field(brand_id, "website", website_url)
            flash("Brand profile updated.", "success")

        elif section == "targets":
            # Guardrails: clamp KPI targets to sane ranges
            cpa_raw = request.form.get("kpi_target_cpa", "0")
            leads_raw = request.form.get("kpi_target_leads", "0")
            roas_raw = request.form.get("kpi_target_roas", "0")
            call_num = request.form.get("call_tracking_number", "")[:30].strip()

            db.update_brand_number_field(brand_id, "kpi_target_cpa", cpa_raw)
            db.update_brand_number_field(brand_id, "kpi_target_leads", leads_raw)
            db.update_brand_number_field(brand_id, "kpi_target_roas", roas_raw)
            db.update_brand_text_field(brand_id, "call_tracking_number", call_num)
            flash("Performance targets saved.", "success")

        elif section == "identity":
            display_name = request.form.get("display_name", "")[:200].strip()
            industry = request.form.get("industry", "")[:200].strip()
            service_area = request.form.get("service_area", "")[:500].strip()
            primary_services = request.form.get("primary_services", "")[:500].strip()
            if display_name:
                db.update_brand_text_field(brand_id, "display_name", display_name)
                session["client_brand_name"] = display_name
            db.update_brand_text_field(brand_id, "industry", industry)
            db.update_brand_text_field(brand_id, "service_area", service_area)
            db.update_brand_text_field(brand_id, "primary_services", primary_services)
            flash("Business identity updated.", "success")

        elif section == "branding":
            brand_colors = request.form.get("brand_colors", "")[:200].strip()
            primary_color = request.form.get("primary_color", "")[:20].strip()
            accent_color = request.form.get("accent_color", "")[:20].strip()
            font_heading = normalize_google_font_family(request.form.get("font_heading", ""))
            font_body = normalize_google_font_family(request.form.get("font_body", ""))
            db.update_brand_text_field(brand_id, "brand_colors", brand_colors)
            db.update_brand_text_field(brand_id, "primary_color", primary_color)
            db.update_brand_text_field(brand_id, "accent_color", accent_color)
            db.update_brand_text_field(brand_id, "font_heading", font_heading)
            db.update_brand_text_field(brand_id, "font_body", font_body)
            flash("Brand design settings saved.", "success")

        return redirect(url_for("client.client_my_business"))

    # Reload latest
    brand = db.get_brand(brand_id)
    logo_variants = _parse_logo_variants(brand.get("logo_variants"))
    competitors = db.get_competitors(brand_id)

    # Calculate completion score for the profile
    profile_fields = [
        brand.get("brand_voice"),
        brand.get("active_offers"),
        brand.get("target_audience"),
        brand.get("website_url") or brand.get("website"),
        len(competitors) > 0,
    ]
    target_fields = [
        brand.get("kpi_target_cpa") and float(brand.get("kpi_target_cpa", 0)) > 0,
        brand.get("kpi_target_leads") and int(float(brand.get("kpi_target_leads", 0))) > 0,
    ]
    filled = sum(1 for f in profile_fields if f and str(f).strip()) + sum(1 for f in target_fields if f)
    profile_score = round(filled / (len(profile_fields) + len(target_fields)) * 100)

    return render_template(
        "client_my_business.html",
        brand=brand,
        logo_variants=logo_variants,
        competitors=competitors,
        profile_score=profile_score,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        google_font_choices=GOOGLE_FONT_CHOICES,
    )


def _sync_competitors_text(db, brand_id):
    """Keep the legacy brands.competitors text field in sync with the
    structured competitors table so the analytics pipeline keeps working."""
    comps = db.get_competitors(brand_id)
    names = ", ".join(c["name"] for c in comps)
    db.update_brand_text_field(brand_id, "competitors", names)


@client_bp.route("/competitors/add", methods=["POST"])
@client_login_required
def client_add_competitor():
    db = _get_db()
    brand_id = session["client_brand_id"]

    name = request.form.get("name", "").strip()[:200]
    if not name:
        flash("Competitor name is required.", "error")
        return redirect(url_for("client.client_my_business"))

    db.add_competitor(
        brand_id=brand_id,
        name=name,
        website=request.form.get("website", "").strip()[:500],
        facebook_url=request.form.get("facebook_url", "").strip()[:500],
        google_maps_url=request.form.get("google_maps_url", "").strip()[:500],
        gbp_cid=_clean_gbp_cid(request.form.get("gbp_cid", "")),
        yelp_url=request.form.get("yelp_url", "").strip()[:500],
        instagram_url=request.form.get("instagram_url", "").strip()[:500],
        notes=request.form.get("notes", "").strip()[:500],
    )
    _sync_competitors_text(db, brand_id)
    flash(f"Competitor '{name}' added.", "success")
    return redirect(url_for("client.client_my_business"))


@client_bp.route("/competitors/<int:competitor_id>/delete", methods=["POST"])
@client_login_required
def client_delete_competitor(competitor_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    comp = db.get_competitor(competitor_id, brand_id)
    if comp:
        db.delete_competitor(competitor_id, brand_id)
        _sync_competitors_text(db, brand_id)
        flash(f"Competitor '{comp['name']}' removed.", "success")
    return redirect(url_for("client.client_my_business"))


@client_bp.route("/competitors/<int:competitor_id>/edit", methods=["POST"])
@client_login_required
def client_edit_competitor(competitor_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    comp = db.get_competitor(competitor_id, brand_id)
    if not comp:
        abort(404)

    db.update_competitor(
        competitor_id,
        brand_id,
        name=request.form.get("name", "").strip()[:200] or comp["name"],
        website=request.form.get("website", "").strip()[:500],
        facebook_url=request.form.get("facebook_url", "").strip()[:500],
        google_maps_url=request.form.get("google_maps_url", "").strip()[:500],
        gbp_cid=_clean_gbp_cid(request.form.get("gbp_cid", "")),
        yelp_url=request.form.get("yelp_url", "").strip()[:500],
        instagram_url=request.form.get("instagram_url", "").strip()[:500],
        notes=request.form.get("notes", "").strip()[:500],
    )
    _sync_competitors_text(db, brand_id)
    flash(f"Competitor '{comp['name']}' updated.", "success")
    return redirect(url_for("client.client_my_business"))


# ── Competitor Intel ──

_COMPETITOR_INTEL_LABELS = {
    "google_places": "Google reputation",
    "meta_ads": "Active ads",
    "website": "Website",
    "pricing": "Public pricing",
    "research": "Counter moves",
}


def _parse_competitor_fetched_at(value):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _build_competitor_report_card(report):
    competitor = report.get("competitor") or {}
    available_sources = []
    missing_sources = []
    fetched_times = []

    for intel_type, label in _COMPETITOR_INTEL_LABELS.items():
        payload = report.get(intel_type)
        if payload:
            available_sources.append(label)
        else:
            missing_sources.append(label)
        fetched_at = _parse_competitor_fetched_at(report.get(f"{intel_type}_fetched"))
        if fetched_at:
            fetched_times.append(fetched_at)

    latest_scan_at = max(fetched_times) if fetched_times else None
    stale_before = datetime.now() - timedelta(days=7)
    if not available_sources:
        status_tone = "empty"
        status_label = "Needs first scan"
        status_summary = "No intel is saved yet. Run the first scan to pull website, pricing, ad, and review signals."
        scan_button_label = "Run first scan"
    elif latest_scan_at and latest_scan_at < stale_before:
        status_tone = "stale"
        status_label = "Needs rescan"
        status_summary = "This intel is older than 7 days. Rescan now before using it for positioning or pricing decisions."
        scan_button_label = "Rescan now"
    elif missing_sources:
        status_tone = "partial"
        status_label = "Partially scanned"
        status_summary = f"{len(available_sources)} of {len(_COMPETITOR_INTEL_LABELS)} intel sources are available. Rescan to fill in the missing gaps."
        scan_button_label = "Rescan now"
    else:
        status_tone = "fresh"
        status_label = "Ready"
        status_summary = "This competitor has a full recent scan, so Warren can use it for cleaner positioning and counter-moves."
        scan_button_label = "Rescan now"

    if latest_scan_at:
        last_scanned_label = f"Last scan {latest_scan_at.strftime('%Y-%m-%d')}"
    else:
        last_scanned_label = "No scan saved yet"

    competitor_name = (competitor.get("name") or "this competitor").strip()
    warren_prompt = (
        f"Use the competitor intel for {competitor_name} to tell me how we should position ourselves, "
        "what gap to attack, and what move matters most next."
    )

    sort_weight = {
        "stale": 0,
        "empty": 1,
        "partial": 2,
        "fresh": 3,
    }.get(status_tone, 4)

    google_places = report.get("google_places") or {}
    google_places_match_reasons = []
    for reason in google_places.get("match_reasons") or []:
        cleaned = str(reason or "").strip()
        if cleaned and cleaned not in google_places_match_reasons:
            google_places_match_reasons.append(cleaned)
    google_places_match_score = float(google_places.get("match_score") or 0)
    if google_places_match_score >= 10:
        google_places_match_confidence = "Exact"
    elif google_places_match_score >= 6:
        google_places_match_confidence = "High"
    elif google_places_match_score >= 3:
        google_places_match_confidence = "Medium"
    else:
        google_places_match_confidence = "Low"

    return {
        "competitor": competitor,
        "report": report,
        "available_sources": available_sources,
        "missing_sources": missing_sources,
        "available_source_count": len(available_sources),
        "missing_source_count": len(missing_sources),
        "latest_scan_at": latest_scan_at,
        "last_scanned_label": last_scanned_label,
        "status_tone": status_tone,
        "status_label": status_label,
        "status_summary": status_summary,
        "scan_button_label": scan_button_label,
        "warren_prompt": warren_prompt,
        "sort_weight": sort_weight,
        "google_places_match_reasons": google_places_match_reasons,
        "google_places_match_score": google_places_match_score,
        "google_places_match_confidence": google_places_match_confidence,
    }


def _build_competitor_page_summary(report_cards):
    summary = {
        "tracked": len(report_cards),
        "fresh": 0,
        "stale": 0,
        "partial": 0,
        "empty": 0,
    }
    for card in report_cards:
        tone = card.get("status_tone")
        if tone in summary:
            summary[tone] += 1
    return summary

@client_bp.route("/competitors")
@client_login_required
def client_competitors():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    competitors = db.get_competitors(brand_id)
    report_cards = []
    pricing_market_summary = {}
    warren_pricing_strategy = ""
    pricing_strategy_competitor = ""
    pricing_position = ""
    pricing_observations = []
    for comp in competitors:
        from webapp.competitor_intel import get_competitor_report
        report = get_competitor_report(db, brand, comp)
        report_cards.append(_build_competitor_report_card(report))

    report_cards.sort(key=lambda card: (card.get("sort_weight", 99), (card.get("competitor") or {}).get("name", "").lower()))
    reports = [card["report"] for card in report_cards]
    competitor_page_summary = _build_competitor_page_summary(report_cards)

    if reports:
        from webapp.competitor_intel import summarize_market_pricing
        pricing_market_summary = summarize_market_pricing(reports)
        ranked_reports = []
        for report in reports:
            pricing = (report.get("pricing") or {}).get("summary") or {}
            ranked_reports.append((
                pricing.get("billable_sample_count") or 0,
                (report.get("research") or {}).get("pricing_strategy", ""),
                report,
            ))
        ranked_reports.sort(key=lambda row: row[0], reverse=True)
        top_report = ranked_reports[0][2] if ranked_reports else None
        if top_report:
            research = top_report.get("research") or {}
            warren_pricing_strategy = (research.get("pricing_strategy") or "").strip()
            pricing_position = (research.get("pricing_position") or "").strip()
            pricing_observations = list((research.get("observed_pricing") or [])[:4])
            pricing_strategy_competitor = ((top_report.get("competitor") or {}).get("name") or "").strip()

    return render_template(
        "client_competitors.html",
        competitors=competitors,
        report_cards=report_cards,
        pricing_market_summary=pricing_market_summary,
        warren_pricing_strategy=warren_pricing_strategy,
        pricing_strategy_competitor=pricing_strategy_competitor,
        pricing_position=pricing_position,
        pricing_observations=pricing_observations,
        competitor_page_summary=competitor_page_summary,
    )


@client_bp.route("/competitors/refresh-all", methods=["POST"])
@client_login_required
def client_competitors_refresh_all():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    competitors = db.get_competitors(brand_id)
    if not competitors:
        flash("Add at least one competitor before running a scan.", "warning")
        return redirect(url_for("client.client_competitors"))

    from webapp.competitor_intel import refresh_competitor_intel

    issue_names = []
    for comp in competitors:
        result = refresh_competitor_intel(db, brand, comp, force=True)
        if result.get("_errors"):
            issue_names.append(comp.get("name") or "Unknown competitor")

    if issue_names:
        flash(
            f"Scanned {len(competitors)} competitors. Some sources need attention for: {', '.join(issue_names[:3])}.",
            "warning",
        )
    else:
        flash(f"Scanned {len(competitors)} competitors.", "success")
    _log_agent("hawk", "Refreshed all competitor intel", brand.get("display_name") or brand.get("name") or "")
    return redirect(url_for("client.client_competitors"))


@client_bp.route("/competitors/<int:competitor_id>/refresh", methods=["POST"])
@client_login_required
def client_competitor_refresh(competitor_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    comp = db.get_competitor(competitor_id, brand_id)
    if not comp or not brand:
        abort(404)

    from webapp.competitor_intel import refresh_competitor_intel
    result = refresh_competitor_intel(db, brand, comp, force=True)
    scan_errors = result.get("_errors") or []
    if scan_errors:
        flash(f"Intel refreshed for '{comp['name']}' with issues: {'; '.join(scan_errors[:3])}", "warning")
    else:
        flash(f"Intel refreshed for '{comp['name']}'.", "success")
    _log_agent("hawk", "Refreshed competitor intel", comp.get("name", ""))
    return redirect(url_for("client.client_competitors"))


# ── Logo Upload ──

@client_bp.route("/upload-logo", methods=["POST"])
@client_login_required
def client_upload_logo():
    from pathlib import Path
    from flask import current_app
    from werkzeug.utils import secure_filename

    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    f = request.files.get("logo")
    if not f or not f.filename:
        flash("No file selected.", "error")
        return redirect(url_for("client.client_my_business"))

    ALLOWED_EXT = {"png", "jpg", "jpeg", "svg", "webp"}
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED_EXT:
        flash("Invalid file type. Use PNG, JPG, SVG, or WebP.", "error")
        return redirect(url_for("client.client_my_business"))

    # 20MB limit (client-side resize handles large images before upload)
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    if size > 20 * 1024 * 1024:
        flash("File too large. Maximum 20MB.", "error")
        return redirect(url_for("client.client_my_business"))

    uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
    logo_dir = uploads_dir / "logos" / str(brand_id)
    logo_dir.mkdir(parents=True, exist_ok=True)

    variant_key = (request.form.get("variant_key", "primary") or "primary").strip().lower()
    custom_label = (request.form.get("variant_label", "") or "").strip()[:40]
    variant_key = re.sub(r"[^a-z0-9_\-]", "_", variant_key)[:32] or "primary"
    variant_label = custom_label or variant_key.replace("_", " ").title()

    filename = secure_filename(f"logo_{variant_key}_{int(time.time())}.{ext}")
    filepath = logo_dir / filename
    f.save(str(filepath))

    # Store relative path: logos/<brand_id>/logo_<id>.<ext>
    rel_path = f"logos/{brand_id}/{filename}"

    variants = _parse_logo_variants(brand.get("logo_variants"))
    updated = False
    for item in variants:
        if item.get("key") == variant_key:
            item["path"] = rel_path
            item["label"] = variant_label
            updated = True
            break
    if not updated:
        variants.append({"key": variant_key, "label": variant_label, "path": rel_path})

    db.update_brand_text_field(brand_id, "logo_variants", json.dumps(variants))

    # Keep logo_path as the primary/default logo
    if variant_key == "primary" or not (brand.get("logo_path") or "").strip():
        db.update_brand_text_field(brand_id, "logo_path", rel_path)

    flash(f"Logo uploaded to variant: {variant_label}.", "success")
    return redirect(url_for("client.client_my_business"))


@client_bp.route("/my-business/logo/primary", methods=["POST"])
@client_login_required
def client_set_primary_logo():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    target_path = (request.form.get("variant_path", "") or "").strip()
    if not target_path:
        flash("No logo variant selected.", "error")
        return redirect(url_for("client.client_my_business"))

    variants = _parse_logo_variants(brand.get("logo_variants"))
    match = next((v for v in variants if (v.get("path") or "") == target_path), None)
    if not match:
        flash("Logo variant not found.", "error")
        return redirect(url_for("client.client_my_business"))

    db.update_brand_text_field(brand_id, "logo_path", target_path)
    flash(f"Primary logo set to: {match.get('label') or 'selected variant'}.", "success")
    return redirect(url_for("client.client_my_business"))


@client_bp.route("/my-business/logo/rename", methods=["POST"])
@client_login_required
def client_rename_logo_variant():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    variant_key = (request.form.get("variant_key", "") or "").strip().lower()
    variant_label = (request.form.get("variant_label", "") or "").strip()[:40]
    if not variant_key or not variant_label:
        flash("Variant and label are required.", "error")
        return redirect(url_for("client.client_my_business"))

    variants = _parse_logo_variants(brand.get("logo_variants"))
    target = next((v for v in variants if (v.get("key") or "").strip().lower() == variant_key), None)
    if not target:
        flash("Logo variant not found.", "error")
        return redirect(url_for("client.client_my_business"))

    target["label"] = variant_label
    db.update_brand_text_field(brand_id, "logo_variants", json.dumps(variants))
    flash("Logo variant renamed.", "success")
    return redirect(url_for("client.client_my_business"))


@client_bp.route("/my-business/logo/delete", methods=["POST"])
@client_login_required
def client_delete_logo_variant():
    from pathlib import Path
    from flask import current_app

    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    variant_key = (request.form.get("variant_key", "") or "").strip().lower()
    if not variant_key:
        flash("No variant selected.", "error")
        return redirect(url_for("client.client_my_business"))

    variants = _parse_logo_variants(brand.get("logo_variants"))
    target = next((v for v in variants if (v.get("key") or "").strip().lower() == variant_key), None)
    if not target:
        flash("Logo variant not found.", "error")
        return redirect(url_for("client.client_my_business"))

    target_path = (target.get("path") or "").strip()
    kept = [v for v in variants if (v.get("key") or "").strip().lower() != variant_key]
    db.update_brand_text_field(brand_id, "logo_variants", json.dumps(kept))

    current_primary = (brand.get("logo_path") or "").strip()
    if current_primary == target_path:
        new_primary = (kept[0].get("path") if kept else "") or ""
        db.update_brand_text_field(brand_id, "logo_path", new_primary)

    # Attempt file cleanup (best effort)
    try:
        if target_path:
            uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
            file_path = uploads_dir / target_path
            if file_path.exists():
                file_path.unlink()
    except Exception:
        pass

    flash("Logo variant deleted.", "success")
    return redirect(url_for("client.client_my_business"))


@client_bp.route("/uploads/<path:filename>")
@client_login_required
def client_serve_upload(filename):
    from pathlib import Path
    from flask import current_app, send_from_directory

    uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
    return send_from_directory(str(uploads_dir), filename)


# ── Creative Center ──

@client_bp.route("/creative")
@client_login_required
def client_creative():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    logo_variants = _parse_logo_variants(brand.get("logo_variants"))

    return render_template(
        "client_creative.html",
        brand=brand,
        logo_variants=logo_variants,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


# ── Creative Templates API ──

@client_bp.route("/creative/templates", methods=["GET"])
@client_login_required
def client_creative_templates_list():
    db = _get_db()
    brand_id = session["client_brand_id"]
    templates = db.get_creative_templates(brand_id)
    return jsonify({"templates": templates})


@client_bp.route("/creative/templates", methods=["POST"])
@client_login_required
def client_creative_templates_save():
    db = _get_db()
    brand_id = session["client_brand_id"]
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "Untitled Template").strip()[:120]
    ad_format = (data.get("ad_format") or "facebook_feed").strip()
    canvas_json = data.get("canvas_json", "")
    thumbnail = (data.get("thumbnail") or "")[:200000]  # cap thumbnail data URL size
    canvas_width = int(data.get("canvas_width", 1200))
    canvas_height = int(data.get("canvas_height", 628))
    if not canvas_json:
        return jsonify({"error": "No canvas data"}), 400
    created_by = session.get("client_name", "client")
    tid = db.save_creative_template(
        brand_id, name, ad_format, canvas_json, thumbnail, canvas_width, canvas_height, created_by
    )
    return jsonify({"ok": True, "id": tid})


@client_bp.route("/creative/templates/<int:template_id>", methods=["GET"])
@client_login_required
def client_creative_template_load(template_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    tpl = db.get_creative_template(template_id, brand_id)
    if not tpl:
        return jsonify({"error": "Template not found"}), 404
    return jsonify({"template": tpl})


@client_bp.route("/creative/templates/<int:template_id>", methods=["PUT"])
@client_login_required
def client_creative_template_update(template_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "Untitled Template").strip()[:120]
    canvas_json = data.get("canvas_json", "")
    thumbnail = (data.get("thumbnail") or "")[:200000]
    canvas_width = int(data.get("canvas_width", 1200))
    canvas_height = int(data.get("canvas_height", 628))
    if not canvas_json:
        return jsonify({"error": "No canvas data"}), 400
    db.update_creative_template(template_id, brand_id, name, canvas_json, thumbnail, canvas_width, canvas_height)
    return jsonify({"ok": True})


@client_bp.route("/creative/templates/<int:template_id>", methods=["DELETE"])
@client_login_required
def client_creative_template_delete(template_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    db.delete_creative_template(template_id, brand_id)
    return jsonify({"ok": True})


@client_bp.route("/creative/generate", methods=["POST"])
@client_login_required
def client_creative_generate():
    try:
        from pathlib import Path
        from flask import current_app
        from PIL import Image, ImageDraw
        import uuid

        db = _get_db()
        brand_id = session["client_brand_id"]
        brand = db.get_brand(brand_id)
        if not brand:
            return jsonify({"error": "Brand not found"}), 404

        # Get inputs
        image_file = request.files.get("image")
        ad_copy_headline = request.form.get("headline", "").strip()[:90]
        ad_copy_body = request.form.get("body_text", "").strip()[:150]
        cta_text = request.form.get("cta_text", "").strip()[:30]
        ad_format = request.form.get("ad_format", "facebook_feed")
        overlay_template = request.form.get("overlay_template", "lower_third")
        background_treatment = request.form.get("background_treatment", "brand_gradient")
        shape_style = request.form.get("shape_style", "rounded")
        text_placement = request.form.get("text_placement", "left")
        headline_font_family = request.form.get("headline_font_family", "strong")
        headline_font_weight = request.form.get("headline_font_weight", "bold")
        headline_font_color = request.form.get("headline_font_color", "#ffffff")
        body_font_family = request.form.get("body_font_family", "modern")
        body_font_weight = request.form.get("body_font_weight", "normal")
        body_font_color = request.form.get("body_font_color", "#dcdcdc")
        cta_font_family = request.form.get("cta_font_family", "strong")
        cta_font_weight = request.form.get("cta_font_weight", "bold")
        cta_font_color = request.form.get("cta_font_color", "#ffffff")
        headline_scale = float(request.form.get("headline_scale", "100") or 100)
        body_scale = float(request.form.get("body_scale", "100") or 100)
        overlay_opacity = float(request.form.get("overlay_opacity", "65") or 65)
        logo_scale = float(request.form.get("logo_scale", "100") or 100)
        logo_variant = request.form.get("logo_variant", "")
        logo_position_mode = request.form.get("logo_position_mode", "corner")
        logo_pos_x = float(request.form.get("logo_pos_x", "50") or 50)
        logo_pos_y = float(request.form.get("logo_pos_y", "50") or 50)
        logo_corner = request.form.get("logo_corner", "top_left")
        include_phone = request.form.get("include_phone", "1") in ("1", "true", "True", "yes", "on")
        include_website = request.form.get("include_website", "0") in ("1", "true", "True", "yes", "on")
        creative_prompt = request.form.get("creative_prompt", "").strip()[:800]

        allowed_overlay_templates = {"lower_third", "full_lower_third", "upper_third", "full_overlay", "soft_box", "brand_bar", "diagonal_band", "bubbles", "boxes"}
        allowed_background_treatments = {"brand_gradient", "flat", "none"}
        allowed_shape_styles = {"rounded", "sharp", "pill"}
        allowed_text_placements = {"left", "center", "right"}
        allowed_font_families = {"modern", "classic", "clean", "elegant", "friendly", "strong", "mono", "playful", "geometric", "serif_alt"}
        allowed_weights = {"normal", "semibold", "bold"}
        allowed_logo_corners = {"top_left", "top_right", "bottom_left", "bottom_right"}
        allowed_logo_position_modes = {"corner", "custom"}
        if overlay_template not in allowed_overlay_templates:
            overlay_template = "lower_third"
        if background_treatment not in allowed_background_treatments:
            background_treatment = "brand_gradient"
        if shape_style not in allowed_shape_styles:
            shape_style = "rounded"
        if text_placement not in allowed_text_placements:
            text_placement = "left"
        if headline_font_family not in allowed_font_families:
            headline_font_family = "strong"
        if body_font_family not in allowed_font_families:
            body_font_family = "modern"
        if cta_font_family not in allowed_font_families:
            cta_font_family = "strong"
        if headline_font_weight not in allowed_weights:
            headline_font_weight = "bold"
        if body_font_weight not in allowed_weights:
            body_font_weight = "normal"
        if cta_font_weight not in allowed_weights:
            cta_font_weight = "bold"
        headline_scale = max(80.0, min(150.0, headline_scale))
        body_scale = max(80.0, min(150.0, body_scale))
        overlay_opacity = max(30.0, min(95.0, overlay_opacity))
        logo_scale = max(50.0, min(180.0, logo_scale))
        if logo_corner not in allowed_logo_corners:
            logo_corner = "top_left"
        if logo_position_mode not in allowed_logo_position_modes:
            logo_position_mode = "corner"
        logo_pos_x = max(0.0, min(100.0, logo_pos_x))
        logo_pos_y = max(0.0, min(100.0, logo_pos_y))

        if creative_prompt:
            ai_suggestion = _suggest_creative_style(brand, creative_prompt, ad_format)
            if ai_suggestion:
                overlay_template = ai_suggestion.get("overlay_template", overlay_template)
                shape_style = ai_suggestion.get("shape_style", shape_style)
                text_placement = ai_suggestion.get("text_placement", text_placement)
                headline_font_family = ai_suggestion.get("headline_font_family", headline_font_family)
                body_font_family = ai_suggestion.get("body_font_family", body_font_family)
                cta_font_family = ai_suggestion.get("cta_font_family", cta_font_family)

        if not image_file or not image_file.filename:
            return jsonify({"error": "Please upload a background image."}), 400

        if not ad_copy_headline:
            return jsonify({"error": "Headline is required."}), 400

        # Validate image
        ext = image_file.filename.rsplit(".", 1)[-1].lower() if "." in image_file.filename else ""
        if ext not in {"png", "jpg", "jpeg", "webp"}:
            return jsonify({"error": "Image must be PNG, JPG, or WebP."}), 400

        image_file.seek(0, 2)
        if image_file.tell() > 10 * 1024 * 1024:
            return jsonify({"error": "Image too large. Max 10MB."}), 400
        image_file.seek(0)

        # Format dimensions
        FORMAT_SIZES = {
            "facebook_feed": (1200, 628),
            "facebook_story": (1080, 1920),
            "instagram_feed": (1080, 1080),
            "instagram_story": (1080, 1920),
            "google_display_landscape": (1200, 628),
            "google_display_square": (1200, 1200),
        }
        target_size = FORMAT_SIZES.get(ad_format, (1200, 628))
        w, h = target_size

        # Open as RGB (not RGBA - saves 25% memory), resize immediately
        bg = Image.open(image_file)
        bg.thumbnail((max(w, h) * 2, max(w, h) * 2), Image.LANCZOS)  # cap source size
        bg = bg.convert("RGB")
        bg = _fit_cover_rgb(bg, target_size)
        brand_color = _pick_brand_color(brand)

        # Apply selected overlay template
        dark = Image.new("RGB", (w, h), brand_color)
        grad_mask = Image.new("L", (w, h), 0)

        if background_treatment != "none":
            if overlay_template == "full_overlay":
                alpha_full = int((110 if background_treatment == "brand_gradient" else 145) * (overlay_opacity / 65.0))
                alpha_full = max(20, min(240, alpha_full))
                for y in range(0, h):
                    grad_mask.paste(alpha_full, (0, y, w, y + 1))
            elif overlay_template == "upper_third":
                top_end = max(int(h * 0.45), 1)
                for y in range(0, top_end):
                    if background_treatment == "flat":
                        alpha = int(165 * (overlay_opacity / 65.0))
                    else:
                        alpha = int(200 * (1 - (y / top_end)) * (overlay_opacity / 65.0))
                    alpha = max(15, min(240, alpha))
                    grad_mask.paste(alpha, (0, y, w, y + 1))
            elif overlay_template in ("brand_bar", "full_lower_third"):
                start_y = int(h * 0.66 if overlay_template == "full_lower_third" else 0.72 * h)
                for y in range(start_y, h):
                    base_alpha = 210 if background_treatment == "flat" else 190
                    alpha = int(base_alpha * (overlay_opacity / 65.0))
                    alpha = max(15, min(240, alpha))
                    grad_mask.paste(alpha, (0, y, w, y + 1))
            elif overlay_template == "diagonal_band":
                start_y = int(h * 0.52)
                for y in range(start_y, h):
                    if background_treatment == "flat":
                        alpha = int(165 * (overlay_opacity / 65.0))
                    else:
                        alpha = int(190 * (y - start_y) / max(h - start_y, 1) * (overlay_opacity / 65.0))
                    alpha = max(15, min(240, alpha))
                    grad_mask.paste(alpha, (0, y, w, y + 1))
            elif overlay_template in ("bubbles", "boxes"):
                start_y = int(h * 0.52)
                for y in range(start_y, h):
                    if background_treatment == "flat":
                        alpha = int(150 * (overlay_opacity / 65.0))
                    else:
                        alpha = int(170 * (y - start_y) / max(h - start_y, 1) * (overlay_opacity / 65.0))
                    alpha = max(15, min(240, alpha))
                    grad_mask.paste(alpha, (0, y, w, y + 1))
            else:
                start_y = int(h * 0.55)
                for y in range(start_y, h):
                    if background_treatment == "flat":
                        alpha = int(165 * (overlay_opacity / 65.0))
                    else:
                        alpha = int(210 * (y - start_y) / max(h - start_y, 1) * (overlay_opacity / 65.0))
                    alpha = max(15, min(240, alpha))
                    grad_mask.paste(alpha, (0, y, w, y + 1))

        if background_treatment != "none":
            bg = Image.composite(dark, bg, grad_mask)
        del dark, grad_mask  # free memory

        # Draw text
        draw = ImageDraw.Draw(bg)
        margin = int(w * 0.06)
        safe_pad = 16

        headline_color = _parse_hex_color(headline_font_color, (255, 255, 255))
        body_color = _parse_hex_color(body_font_color, (220, 220, 220))
        cta_color = _parse_hex_color(cta_font_color, (255, 255, 255))

        font_headline = _get_font(int(h * 0.065 * (headline_scale / 100.0)), family=headline_font_family, weight=headline_font_weight)
        font_body = _get_font(int(h * 0.038 * (body_scale / 100.0)), family=body_font_family, weight=body_font_weight)
        font_cta = _get_font(int(h * 0.04 * (headline_scale / 100.0)), family=cta_font_family, weight=cta_font_weight)

        text_width = min(int(w * 0.84), max(w - (safe_pad * 2), 120))
        margin = int(w * 0.06)
        if text_placement == "center":
            text_x = max((w - text_width) // 2, safe_pad)
        elif text_placement == "right":
            text_x = max(w - margin - text_width, safe_pad)
        else:
            text_x = max(margin, safe_pad)

        if overlay_template == "upper_third":
            y_cursor = int(h * 0.12)
        elif overlay_template == "full_overlay":
            y_cursor = int(h * 0.35)
        elif overlay_template in ("brand_bar", "full_lower_third"):
            y_cursor = int(h * 0.76)
        else:
            y_cursor = int(h * 0.60)
        y_cursor = max(y_cursor, safe_pad)

        headline_lines = _count_lines(ad_copy_headline, text_width, font_headline)
        body_lines = _count_lines(ad_copy_body, text_width, font_body) if ad_copy_body else 0
        headline_h = int(headline_lines * _font_size(font_headline) * 1.3)
        body_h = int(body_lines * _font_size(font_body) * 1.3) if ad_copy_body else 0
        cta_h = 0
        if cta_text:
            cta_bbox = draw.textbbox((0, 0), cta_text, font=font_cta)
            cta_h = (cta_bbox[3] - cta_bbox[1]) + 20

        if overlay_template == "soft_box":
            box_top = max(y_cursor - 18, 0)
            box_bottom = min(y_cursor + headline_h + body_h + cta_h + 36, h)
            box_left = max(text_x - 20, 0)
            box_right = min(text_x + text_width + 20, w)
            box_radius = 0 if shape_style == "sharp" else (28 if shape_style == "pill" else 18)
            box_mask = Image.new("L", (w, h), 0)
            box_mask_draw = ImageDraw.Draw(box_mask)
            if box_radius > 0:
                box_mask_draw.rounded_rectangle([box_left, box_top, box_right, box_bottom], radius=box_radius, fill=165)
            else:
                box_mask_draw.rectangle([box_left, box_top, box_right, box_bottom], fill=165)
            bg = Image.composite(Image.new("RGB", (w, h), brand_color), bg, box_mask)
            draw = ImageDraw.Draw(bg)

        if overlay_template in ("brand_bar", "full_lower_third"):
            bar_top = int(h * (0.66 if overlay_template == "full_lower_third" else 0.72))
            bar_mask = Image.new("L", (w, h), 0)
            bar_mask_draw = ImageDraw.Draw(bar_mask)
            bar_mask_draw.rectangle([0, bar_top, w, h], fill=195)
            bg = Image.composite(Image.new("RGB", (w, h), brand_color), bg, bar_mask)
            draw = ImageDraw.Draw(bg)
            stripe_color = tuple(min(c + 120, 255) for c in brand_color)
            draw.rectangle([0, bar_top - 10, w, bar_top], fill=stripe_color)

        if overlay_template == "diagonal_band":
            poly = [
                (0, int(h * 0.66)),
                (w, int(h * 0.56)),
                (w, h),
                (0, h),
            ]
            band_mask = Image.new("L", (w, h), 0)
            band_mask_draw = ImageDraw.Draw(band_mask)
            band_mask_draw.polygon(poly, fill=185)
            bg = Image.composite(Image.new("RGB", (w, h), brand_color), bg, band_mask)
            draw = ImageDraw.Draw(bg)

        if overlay_template == "bubbles":
            bubble_color = tuple(min(c + 70, 255) for c in brand_color)
            bubble_alpha = 115
            bubbles_mask = Image.new("L", (w, h), 0)
            bubbles_draw = ImageDraw.Draw(bubbles_mask)
            for i in range(10):
                radius = int(min(w, h) * (0.03 + (i % 4) * 0.01))
                cx = int((i * 0.13 % 1) * w)
                cy = int(h * (0.55 + ((i * 0.07) % 0.35)))
                bubbles_draw.ellipse([cx - radius, cy - radius, cx + radius, cy + radius], fill=bubble_alpha)
            bg = Image.composite(Image.new("RGB", (w, h), bubble_color), bg, bubbles_mask)
            draw = ImageDraw.Draw(bg)

        if overlay_template == "boxes":
            box_color = tuple(min(c + 55, 255) for c in brand_color)
            boxes_mask = Image.new("L", (w, h), 0)
            boxes_draw = ImageDraw.Draw(boxes_mask)
            base_y = int(h * 0.58)
            for i in range(6):
                bw = int(w * (0.11 + (i % 3) * 0.04))
                bh = int(h * (0.06 + (i % 2) * 0.03))
                x = int((0.06 + i * 0.15) * w) % max(w - bw, 1)
                y = base_y + int((i % 3) * h * 0.07)
                boxes_draw.rectangle([x, y, x + bw, y + bh], fill=120)
            bg = Image.composite(Image.new("RGB", (w, h), box_color), bg, boxes_mask)
            draw = ImageDraw.Draw(bg)

        # Headline
        _draw_text_wrapped(draw, ad_copy_headline, text_x, y_cursor, text_width, font_headline, fill=headline_color)
        y_cursor += int(headline_lines * _font_size(font_headline) * 1.3) + 8

        # Body text
        if ad_copy_body:
            _draw_text_wrapped(draw, ad_copy_body, text_x, y_cursor, text_width, font_body, fill=body_color)
            y_cursor += int(body_lines * _font_size(font_body) * 1.3) + 12

        # CTA button
        if cta_text:
            cta_bbox = draw.textbbox((0, 0), cta_text, font=font_cta)
            cta_w = cta_bbox[2] - cta_bbox[0] + 36
            cta_h = cta_bbox[3] - cta_bbox[1] + 20
            if text_placement == "center":
                cta_x = text_x + max((text_width - cta_w) // 2, 0)
            elif text_placement == "right":
                cta_x = text_x + max(text_width - cta_w, 0)
            else:
                cta_x = text_x
            cta_x = max(min(cta_x, w - cta_w - safe_pad), safe_pad)
            cta_y = max(min(y_cursor, h - cta_h - safe_pad), safe_pad)
            cta_radius = 0 if shape_style == "sharp" else (24 if shape_style == "pill" else 8)
            if cta_radius > 0:
                draw.rounded_rectangle([cta_x, cta_y, cta_x + cta_w, cta_y + cta_h], radius=cta_radius, fill=brand_color)
            else:
                draw.rectangle([cta_x, cta_y, cta_x + cta_w, cta_y + cta_h], fill=brand_color)
            draw.text((cta_x + 18, cta_y + 10), cta_text, fill=cta_color, font=font_cta)

        # Place logo (top-left)
        selected_logo_path = _resolve_logo_variant_path(brand, logo_variant)
        if selected_logo_path:
            uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
            logo_file = uploads_dir / selected_logo_path
            if logo_file.exists():
                try:
                    logo = Image.open(str(logo_file)).convert("RGBA")
                    logo_w = int(w * 0.36 * (logo_scale / 100.0))
                    ratio = logo_w / logo.width
                    logo_h = int(logo.height * ratio)
                    logo = logo.resize((logo_w, logo_h), Image.LANCZOS)
                    logo_margin = max(int(w * 0.04), safe_pad)
                    if logo_position_mode == "custom":
                        lx = int((w - logo_w) * (logo_pos_x / 100.0))
                        ly = int((h - logo_h) * (logo_pos_y / 100.0))
                        lx = max(safe_pad, min(lx, w - logo_w - safe_pad))
                        ly = max(safe_pad, min(ly, h - logo_h - safe_pad))
                    else:
                        if logo_corner == "top_right":
                            lx = max(w - logo_w - logo_margin, 0)
                            ly = logo_margin
                        elif logo_corner == "bottom_left":
                            lx = logo_margin
                            ly = max(h - logo_h - logo_margin, 0)
                        elif logo_corner == "bottom_right":
                            lx = max(w - logo_w - logo_margin, 0)
                            ly = max(h - logo_h - logo_margin, 0)
                        else:
                            lx = logo_margin
                            ly = logo_margin
                    bg.paste(logo, (lx, ly), logo)
                    del logo
                except Exception:
                    pass

        footer_items = []
        if include_phone and (brand.get("call_tracking_number") or "").strip():
            footer_items.append((brand.get("call_tracking_number") or "").strip())
        if include_website and (brand.get("website") or "").strip():
            footer_items.append((brand.get("website") or "").strip())
        if footer_items:
            footer_text = "  |  ".join(footer_items)[:120]
            footer_font = _get_font(int(h * 0.026), family=body_font_family, weight=body_font_weight)
            fb = draw.textbbox((0, 0), footer_text, font=footer_font)
            fw = fb[2] - fb[0]
            fh = fb[3] - fb[1]
            fx = max((w - fw) // 2, safe_pad)
            fy = max(h - fh - int(h * 0.02), safe_pad)
            draw.rectangle([fx - 12, fy - 6, min(fx + fw + 12, w - safe_pad), min(fy + fh + 6, h - safe_pad)], fill=(0, 0, 0))
            draw.text((fx, fy), footer_text, fill="white", font=footer_font)

        # Save as JPEG (much smaller + faster than PNG)
        output_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads")) / "creatives" / str(brand_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_name = f"creative_{uuid.uuid4().hex[:8]}.jpg"
        output_path = output_dir / output_name
        bg.save(str(output_path), "JPEG", quality=90)

        # Auto-save to Google Drive if configured
        drive_link = None
        try:
            from webapp.google_drive import upload_file as drive_upload
            with open(str(output_path), "rb") as df:
                drive_result = drive_upload(db, brand_id, "Creatives", output_name, df.read(), "image/jpeg")
            if drive_result:
                drive_link = drive_result.get("webViewLink")
        except Exception:
            pass  # Drive save is best-effort

        del bg

        rel_path = f"creatives/{brand_id}/{output_name}"
        resp = {
            "image_url": url_for("client.client_serve_upload", filename=rel_path),
            "filename": output_name,
        }
        if drive_link:
            resp["drive_link"] = drive_link
        return jsonify(resp)

    except Exception as e:
        import traceback
        traceback.print_exc()

        # Fail-safe fallback: generate a simple version instead of hard failing
        try:
            from pathlib import Path
            from flask import current_app
            from PIL import Image, ImageDraw
            import uuid

            image_file = request.files.get("image")
            if not image_file or not image_file.filename:
                return jsonify({"error": f"Failed to generate creative: {str(e)}"}), 500

            ext = image_file.filename.rsplit(".", 1)[-1].lower() if "." in image_file.filename else ""
            if ext not in {"png", "jpg", "jpeg", "webp"}:
                return jsonify({"error": f"Failed to generate creative: {str(e)}"}), 500

            ad_copy_headline = request.form.get("headline", "").strip()[:90] or "Your Next Best Offer"
            ad_copy_body = request.form.get("body_text", "").strip()[:150]
            cta_text = request.form.get("cta_text", "").strip()[:30] or "Learn More"
            ad_format = request.form.get("ad_format", "facebook_feed")
            logo_corner = request.form.get("logo_corner", "top_left")
            logo_position_mode = request.form.get("logo_position_mode", "corner")
            logo_pos_x = float(request.form.get("logo_pos_x", "50") or 50)
            logo_pos_y = float(request.form.get("logo_pos_y", "50") or 50)
            include_phone = request.form.get("include_phone", "1") in ("1", "true", "True", "yes", "on")
            include_website = request.form.get("include_website", "0") in ("1", "true", "True", "yes", "on")
            db = _get_db()
            fallback_brand = db.get_brand(session.get("client_brand_id")) if session.get("client_brand_id") else None
            brand_color = _pick_brand_color(fallback_brand or {})

            format_sizes = {
                "facebook_feed": (1200, 628),
                "facebook_story": (1080, 1920),
                "instagram_feed": (1080, 1080),
                "instagram_story": (1080, 1920),
                "google_display_landscape": (1200, 628),
                "google_display_square": (1200, 1200),
            }
            target_size = format_sizes.get(ad_format, (1200, 628))
            w, h = target_size

            image_file.seek(0)
            bg = Image.open(image_file)
            bg.thumbnail((max(w, h) * 2, max(w, h) * 2), Image.LANCZOS)
            bg = bg.convert("RGB")
            bg = _fit_cover_rgb(bg, target_size)

            # Basic lower-third semi-transparent brand overlay
            dark = Image.new("RGB", (w, h), brand_color)
            grad_mask = Image.new("L", (w, h), 0)
            start_y = int(h * 0.55)
            for y in range(start_y, h):
                alpha = int(185 * (y - start_y) / max(h - start_y, 1))
                grad_mask.paste(alpha, (0, y, w, y + 1))
            bg = Image.composite(dark, bg, grad_mask)

            draw = ImageDraw.Draw(bg)
            margin = int(w * 0.06)
            text_width = int(w * 0.84)
            y_cursor = int(h * 0.60)

            font_headline = _get_font(int(h * 0.065), bold=True, family="modern")
            font_body = _get_font(int(h * 0.038), family="modern")
            font_cta = _get_font(int(h * 0.04), bold=True, family="modern")

            _draw_text_wrapped(draw, ad_copy_headline, margin, y_cursor, text_width, font_headline, fill="white")
            headline_lines = _count_lines(ad_copy_headline, text_width, font_headline)
            y_cursor += int(headline_lines * _font_size(font_headline) * 1.3) + 8

            if ad_copy_body:
                _draw_text_wrapped(draw, ad_copy_body, margin, y_cursor, text_width, font_body, fill=(220, 220, 220))
                body_lines = _count_lines(ad_copy_body, text_width, font_body)
                y_cursor += int(body_lines * _font_size(font_body) * 1.3) + 12

            cta_bbox = draw.textbbox((0, 0), cta_text, font=font_cta)
            cta_w = cta_bbox[2] - cta_bbox[0] + 36
            cta_h = cta_bbox[3] - cta_bbox[1] + 20
            cta_x = margin
            cta_y = y_cursor
            try:
                draw.rounded_rectangle([cta_x, cta_y, cta_x + cta_w, cta_y + cta_h], radius=8, fill=brand_color)
            except Exception:
                draw.rectangle([cta_x, cta_y, cta_x + cta_w, cta_y + cta_h], fill=brand_color)
            draw.text((cta_x + 18, cta_y + 10), cta_text, fill="white", font=font_cta)

            fallback_logo_variant = request.form.get("logo_variant", "")
            selected_logo_path = _resolve_logo_variant_path(fallback_brand or {}, fallback_logo_variant)
            if selected_logo_path:
                uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
                logo_file = uploads_dir / selected_logo_path
                if logo_file.exists():
                    try:
                        logo = Image.open(str(logo_file)).convert("RGBA")
                        logo_w = int(w * 0.36 * (logo_scale / 100.0))
                        ratio = logo_w / logo.width
                        logo_h = int(logo.height * ratio)
                        logo = logo.resize((logo_w, logo_h), Image.LANCZOS)
                        logo_margin = max(int(w * 0.04), 16)
                        if logo_position_mode == "custom":
                            lx = int((w - logo_w) * (max(0.0, min(100.0, logo_pos_x)) / 100.0))
                            ly = int((h - logo_h) * (max(0.0, min(100.0, logo_pos_y)) / 100.0))
                            lx = max(16, min(lx, w - logo_w - 16))
                            ly = max(16, min(ly, h - logo_h - 16))
                        else:
                            if logo_corner == "top_right":
                                lx = max(w - logo_w - logo_margin, 0)
                                ly = logo_margin
                            elif logo_corner == "bottom_left":
                                lx = logo_margin
                                ly = max(h - logo_h - logo_margin, 0)
                            elif logo_corner == "bottom_right":
                                lx = max(w - logo_w - logo_margin, 0)
                                ly = max(h - logo_h - logo_margin, 0)
                            else:
                                lx = logo_margin
                                ly = logo_margin
                        bg.paste(logo, (lx, ly), logo)
                    except Exception:
                        pass

            footer_items = []
            if include_phone and fallback_brand and (fallback_brand.get("call_tracking_number") or "").strip():
                footer_items.append((fallback_brand.get("call_tracking_number") or "").strip())
            if include_website and fallback_brand and (fallback_brand.get("website") or "").strip():
                footer_items.append((fallback_brand.get("website") or "").strip())
            if footer_items:
                footer_text = "  |  ".join(footer_items)[:120]
                footer_font = _get_font(int(h * 0.026), family="modern")
                fb = draw.textbbox((0, 0), footer_text, font=footer_font)
                fw = fb[2] - fb[0]
                fh = fb[3] - fb[1]
                fx = max((w - fw) // 2, 12)
                fy = max(h - fh - int(h * 0.02), 8)
                draw.rectangle([fx - 12, fy - 6, min(fx + fw + 12, w - 4), min(fy + fh + 6, h - 4)], fill=(0, 0, 0))
                draw.text((fx, fy), footer_text, fill="white", font=footer_font)

            output_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads")) / "creatives" / str(session.get("client_brand_id"))
            output_dir.mkdir(parents=True, exist_ok=True)
            output_name = f"creative_{uuid.uuid4().hex[:8]}.jpg"
            output_path = output_dir / output_name
            bg.save(str(output_path), "JPEG", quality=90)

            rel_path = f"creatives/{session.get('client_brand_id')}/{output_name}"
            return jsonify({
                "image_url": url_for("client.client_serve_upload", filename=rel_path),
                "filename": output_name,
                "warning": "Used simplified template fallback",
            })
        except Exception:
            return jsonify({"error": f"Failed to generate creative: {str(e)}"}), 500


@client_bp.route("/creative/ai-copy", methods=["POST"])
@client_login_required
def client_creative_ai_copy():
    """Use AI to generate ad copy from an image description."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"error": "Brand not found"}), 404

    description = request.form.get("description", "").strip()
    ad_format = request.form.get("ad_format", "facebook_feed")
    if not description:
        return jsonify({"error": "Please describe the image."}), 400

    # Get API key - brand's own key first, then system key
    api_key = (brand.get("openai_api_key") or "").strip()
    if not api_key:
        from flask import current_app
        api_key = current_app.config.get("OPENAI_API_KEY", "")
    if not api_key:
        return jsonify({"error": "No OpenAI API key configured. Add one in Connections."}), 400

    model = _pick_ai_model(brand, "images")

    prompt = f"""Generate ad copy for a {ad_format.replace('_', ' ')} ad creative.

Brand: {brand.get('display_name', '')}
Industry: {brand.get('industry', '')}
Brand Voice: {brand.get('brand_voice', 'professional and friendly')}
Active Offers: {brand.get('active_offers', 'none specified')}
Image Description: {description}

Return JSON only with these fields:
- headline: max 40 characters, punchy and attention-grabbing
- body_text: max 125 characters, supports the headline, includes value proposition
- cta_text: max 20 characters, action-oriented button text (e.g. "Get Your Quote", "Book Now", "Learn More")

JSON only, no markdown."""

    import requests as req
    try:
        resp = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.7},
            timeout=30,
        )
        if resp.status_code != 200:
            return jsonify({"error": "AI request failed. Check your API key."}), 500
        content = resp.json()["choices"][0]["message"]["content"]
        # Strip markdown fences if present
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3]
        import json as _json
        data = _json.loads(content)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"AI generation failed: {str(e)}"}), 500


# ── Creative helpers ──

def _fit_cover(img, target_size):
    """Resize and crop image to cover target_size (center crop)."""
    from PIL import Image
    tw, th = target_size
    iw, ih = img.size
    scale = max(tw / iw, th / ih)
    new_w, new_h = int(iw * scale), int(ih * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    left = (new_w - tw) // 2
    top = (new_h - th) // 2
    return img.crop((left, top, left + tw, top + th))


def _fit_cover_rgb(img, target_size):
    """Memory-efficient resize and center crop for RGB images."""
    from PIL import Image
    tw, th = target_size
    iw, ih = img.size
    scale = max(tw / iw, th / ih)
    new_w, new_h = int(iw * scale), int(ih * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    left = (new_w - tw) // 2
    top = (new_h - th) // 2
    return img.crop((left, top, left + tw, top + th))


def _get_font(size, bold=False, family="modern", weight=None):
    """Try to load a system font, fall back to Pillow default."""
    from PIL import ImageFont
    family = (family or "modern").lower()
    if weight is None:
        weight = "bold" if bold else "normal"
    weight = (weight or "normal").lower()
    if weight not in ("normal", "semibold", "bold"):
        weight = "normal"

    font_sets = {
        "modern": {
            "bold": [
                "arialbd.ttf", "Arial Bold.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                "DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf",
            ],
            "regular": [
                "arial.ttf", "Arial.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/TTF/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                "DejaVuSans.ttf", "LiberationSans-Regular.ttf",
            ],
        },
        "classic": {
            "bold": [
                "timesbd.ttf", "Times New Roman Bold.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
                "DejaVuSerif-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf",
            ],
            "regular": [
                "times.ttf", "Times New Roman.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                "DejaVuSerif.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
            ],
        },
        "clean": {
            "bold": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed-Bold.ttf",
                "DejaVuSansCondensed-Bold.ttf",
                "/usr/share/fonts/TTF/DejaVuSansCondensed-Bold.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
                "DejaVuSansCondensed.ttf",
                "/usr/share/fonts/TTF/DejaVuSansCondensed.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            ],
        },
        "elegant": {
            "bold": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf",
                "DejaVuSerif-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
                "DejaVuSerif.ttf",
            ],
        },
        "friendly": {
            "bold": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
                "DejaVuSans-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                "DejaVuSans.ttf",
            ],
        },
        "strong": {
            "bold": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed-Bold.ttf",
                "/usr/share/fonts/TTF/DejaVuSansCondensed-Bold.ttf",
                "DejaVuSansCondensed-Bold.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
                "/usr/share/fonts/TTF/DejaVuSansCondensed.ttf",
                "DejaVuSansCondensed.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            ],
        },
        "mono": {
            "bold": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationMono-Bold.ttf",
                "DejaVuSansMono-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
                "DejaVuSansMono.ttf",
            ],
        },
        "playful": {
            "bold": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            ],
        },
        "geometric": {
            "bold": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed-Bold.ttf",
                "DejaVuSansCondensed-Bold.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
                "DejaVuSansCondensed.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            ],
        },
        "serif_alt": {
            "bold": [
                "/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
                "DejaVuSerif-Bold.ttf",
            ],
            "regular": [
                "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                "DejaVuSerif.ttf",
            ],
        },
    }

    chosen = font_sets.get(family, font_sets["modern"])
    if weight == "bold":
        candidates = chosen["bold"]
    elif weight == "semibold":
        candidates = chosen["bold"] + chosen["regular"]
    else:
        candidates = chosen["regular"]
    for name in candidates:
        try:
            f = ImageFont.truetype(name, size)
            f._fallback_size = size  # stash size for our helpers
            return f
        except (OSError, IOError):
            continue
    # Last resort: default bitmap font
    try:
        f = ImageFont.load_default(size=size)
    except TypeError:
        f = ImageFont.load_default()
    f._fallback_size = size
    return f


def _font_size(font):
    """Get the effective font size, works with both truetype and default fonts."""
    if hasattr(font, 'size') and font.size:
        return font.size
    return getattr(font, '_fallback_size', 16)


def _draw_text_wrapped(draw, text, x, y, max_width, font, fill="white"):
    """Draw text wrapping at max_width."""
    words = text.split()
    lines = []
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)

    for line in lines:
        draw.text((x, y), line, fill=fill, font=font)
        y += int(_font_size(font) * 1.3)


def _count_lines(text, max_width, font):
    """Estimate number of wrapped lines."""
    from PIL import ImageDraw, Image
    tmp = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(tmp)
    words = text.split()
    lines = 1
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            lines += 1
            current = word
    return lines


def _pick_brand_color(brand):
    raw = (brand.get("brand_colors") or "").strip()
    if not raw:
        return (99, 102, 241)

    parts = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    for part in parts:
        value = part.lstrip("#")
        if len(value) == 3:
            value = "".join(ch * 2 for ch in value)
        if len(value) == 6:
            try:
                return tuple(int(value[i:i+2], 16) for i in (0, 2, 4))
            except ValueError:
                continue
    return (99, 102, 241)


def _parse_logo_variants(raw):
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []

    cleaned = []
    for item in data:
        if not isinstance(item, dict):
            continue
        path = (item.get("path") or "").strip()
        if not path:
            continue
        key = (item.get("key") or "custom").strip()
        label = (item.get("label") or key.replace("_", " ").title()).strip()
        cleaned.append({"key": key, "label": label, "path": path})
    return cleaned


def _resolve_logo_variant_path(brand, requested_variant_key):
    if not brand:
        return ""

    variants = _parse_logo_variants(brand.get("logo_variants"))
    req = (requested_variant_key or "").strip().lower()
    if req and variants:
        match = next((v for v in variants if (v.get("key") or "").strip().lower() == req), None)
        if match and (match.get("path") or "").strip():
            return match.get("path").strip()

    return (brand.get("logo_path") or "").strip()


def _parse_hex_color(value, fallback=(255, 255, 255)):
    raw = (value or "").strip()
    if not raw:
        return fallback
    raw = raw.lstrip("#")
    if len(raw) == 3:
        raw = "".join(ch * 2 for ch in raw)
    if len(raw) != 6:
        return fallback
    try:
        return tuple(int(raw[i:i+2], 16) for i in (0, 2, 4))
    except ValueError:
        return fallback


def _normalize_hex_color_text(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("#"):
        raw = raw[1:]
    if len(raw) == 3 and re.fullmatch(r"[0-9a-fA-F]{3}", raw):
        raw = "".join(ch * 2 for ch in raw)
    if not re.fullmatch(r"[0-9a-fA-F]{6}", raw):
        return ""
    return f"#{raw.lower()}"


def _site_builder_brand_palette(brand, limit=4):
    colors = []
    seen = set()

    def add_color(value):
        color = _normalize_hex_color_text(value)
        if not color or color in seen:
            return
        seen.add(color)
        colors.append(color)

    add_color((brand or {}).get("primary_color"))
    add_color((brand or {}).get("accent_color"))

    raw = ((brand or {}).get("brand_colors") or "").strip()
    for match in re.finditer(r"#?[0-9a-fA-F]{3}(?:[0-9a-fA-F]{3})?", raw):
        add_color(match.group(0))
        if len(colors) >= limit:
            break

    return colors[:limit]


def _site_builder_brand_logo_url(brand):
    logo_path = ((brand or {}).get("logo_path") or "").strip()
    if not logo_path:
        return ""
    return url_for("client.client_serve_upload", filename=logo_path)


def _site_builder_wp_admin_url(brand):
    site_url = ((brand or {}).get("wp_site_url") or "").strip().rstrip("/")
    if not site_url:
        return ""
    return f"{site_url}/wp-admin/"


def _site_builder_stock_image_url(slot_key, industry, note=""):
    query_bits = [industry or "local service"]
    slot_label = slot_key.replace("_", " ").strip()
    if slot_label:
        query_bits.append(slot_label)
    if note:
        query_bits.append(note)
    query = ",".join(
        quote_plus(part.strip())
        for part in query_bits
        if str(part or "").strip()
    )
    return f"https://source.unsplash.com/1600x900/?{query}" if query else ""


def _site_builder_collect_image_slots(brand_id, industry):
    from werkzeug.utils import secure_filename

    slots = {}
    uploads_dir = Path(current_app.config.get("UPLOADS_DIR", "data/uploads"))
    target_dir = uploads_dir / "site_builder_intake" / str(brand_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    allowed_ext = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}
    use_stock_for_all = bool(request.form.get("image_slots_use_stock_all"))

    for slot in _SITE_BUILDER_INTAKE_IMAGE_SLOTS:
        key = slot["key"]
        note = (request.form.get(f"{key}_note") or "").strip()[:240]
        use_stock = bool(request.form.get(f"{key}_use_stock")) or use_stock_for_all
        field_name = slot["field_name"]

        files = request.files.getlist(field_name) if slot.get("multiple") else [request.files.get(field_name)]
        assets = []
        for upload in files:
            if not upload or not getattr(upload, "filename", ""):
                continue
            ext = os.path.splitext(upload.filename)[1].lower()
            if ext not in allowed_ext:
                continue
            upload.seek(0, 2)
            size = upload.tell()
            upload.seek(0)
            if size > 10 * 1024 * 1024:
                continue
            filename = secure_filename(f"{key}_{uuid.uuid4().hex}{ext}")
            if not filename:
                continue
            save_path = target_dir / filename
            upload.save(str(save_path))
            rel_path = f"site_builder_intake/{brand_id}/{filename}"
            assets.append({
                "path": rel_path,
                "url": url_for("client.client_serve_upload", filename=rel_path),
                "original_name": upload.filename,
            })

        entry = {
            "label": slot["label"],
            "note": note,
            "use_stock": use_stock,
            "stock_query": slot.get("stock_query") or "",
            "assets": assets,
        }
        if assets:
            entry["mode"] = "upload"
        elif use_stock:
            entry["mode"] = "stock"
            stock_url = _site_builder_stock_image_url(key, industry, note or slot.get("stock_query") or "")
            if stock_url:
                entry["stock_url"] = stock_url
        else:
            entry["mode"] = "empty"
        slots[key] = entry
    return slots


def _site_builder_theme_snapshot(theme):
    if not isinstance(theme, dict):
        return {}
    snapshot = {}
    for key in (
        "id",
        "name",
        "description",
        "primary_color",
        "secondary_color",
        "accent_color",
        "text_color",
        "bg_color",
        "font_heading",
        "font_body",
        "button_style",
        "layout_style",
        "custom_css",
        "preview_image",
    ):
        snapshot[key] = theme.get(key)
    return snapshot


def _site_builder_template_snapshots(templates):
    snapshots = []
    for template in templates or []:
        if not isinstance(template, dict):
            continue
        if not template.get("is_active", 1):
            continue
        snapshots.append({
            "id": template.get("id"),
            "name": template.get("name") or "",
            "category": template.get("category") or "section",
            "page_types": template.get("page_types") or "",
            "html_content": template.get("html_content") or "",
            "css_content": template.get("css_content") or "",
            "description": template.get("description") or "",
            "sort_order": template.get("sort_order") or 0,
        })
    return snapshots


def _site_builder_site_template_snapshot(site_template, theme=None, templates=None):
    if not isinstance(site_template, dict):
        return {}
    snapshot = {
        "id": site_template.get("id"),
        "name": site_template.get("name") or "",
        "slug": site_template.get("slug") or "",
        "description": site_template.get("description") or "",
        "preview_image": site_template.get("preview_image") or "",
        "prompt_notes": site_template.get("prompt_notes") or "",
        "theme_id": site_template.get("theme_id") or 0,
        "theme_name": site_template.get("theme_name") or "",
        "template_ids": list(site_template.get("template_ids") or []),
        "template_count": int(site_template.get("template_count") or len(site_template.get("template_ids") or [])),
    }
    if isinstance(theme, dict) and theme:
        snapshot["theme_name"] = theme.get("name") or snapshot.get("theme_name") or ""
        snapshot["builder_theme"] = _site_builder_theme_snapshot(theme)
    if templates:
        snapshot["builder_templates"] = _site_builder_template_snapshots(templates)
    return snapshot


def _site_builder_prompt_override_snapshots(overrides):
    snapshots = []
    for override in overrides or []:
        if not isinstance(override, dict):
            continue
        if not override.get("is_active", 1):
            continue
        content = str(override.get("content") or "").strip()
        if not content:
            continue
        snapshots.append({
            "page_type": str(override.get("page_type") or "").strip(),
            "section": str(override.get("section") or "user_prompt").strip(),
            "content": content,
        })
    return snapshots


def _site_builder_reference_mode(value):
    mode = str(value or "").strip().lower()
    if mode in {"layout", "sections", "vibe"}:
        return mode
    return "vibe"


def _merge_reference_texts(primary, secondary, limit=8, item_limit=180):
    merged = []
    seen = set()
    for value in list(primary or []) + list(secondary or []):
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(text[:item_limit])
        if len(merged) >= limit:
            break
    return merged


def _merge_reference_section_patterns(primary, secondary, limit=10):
    merged = []
    seen = set()
    for item in list(primary or []) + list(secondary or []):
        if not isinstance(item, dict):
            continue
        category = str(item.get("category") or "content").strip()[:60]
        heading = str(item.get("heading") or "").strip()[:140]
        summary = str(item.get("summary") or "").strip()[:180]
        layout_hint = str(item.get("layout_hint") or "stacked").strip()[:60]
        cta_texts = []
        for cta in item.get("cta_texts") or []:
            text = re.sub(r"\s+", " ", str(cta or "")).strip()
            if text:
                cta_texts.append(text[:80])
        try:
            item_count = int(item.get("item_count") or 0)
        except Exception:
            item_count = 0
        key = (category.lower(), (heading or summary).lower(), layout_hint.lower())
        if key in seen or not (heading or summary):
            continue
        seen.add(key)
        merged.append({
            "category": category,
            "heading": heading,
            "summary": summary,
            "layout_hint": layout_hint,
            "item_count": item_count,
            "cta_texts": cta_texts[:3],
        })
        if len(merged) >= limit:
            break
    return merged


def _extract_rendered_reference_design(reference_url):
    target_url = str(reference_url or "").strip()
    if not target_url:
        return {}

    try:
        import base64
        import importlib

        sync_playwright = importlib.import_module("playwright.sync_api").sync_playwright
    except Exception:
        return {}

    extractor_js = r"""
() => {
  const clean = (value, limit = 160) => String(value || '').replace(/\s+/g, ' ').trim().slice(0, limit);
  const visible = (el) => {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || '1') > 0 && rect.width >= 140 && rect.height >= 70;
  };
  const unique = (values, limit = 6) => {
    const seen = new Set();
    const items = [];
    for (const raw of values || []) {
      const text = clean(raw, 120);
      const key = text.toLowerCase();
      if (!text || seen.has(key)) continue;
      seen.add(key);
      items.push(text);
      if (items.length >= limit) break;
    }
    return items;
  };
  const inferCategory = (node, index) => {
    const heading = clean(node.querySelector('h1,h2,h3')?.innerText || '', 120).toLowerCase();
    const identity = clean(`${node.id || ''} ${node.className || ''} ${heading}`, 240).toLowerCase();
    const text = clean(node.innerText || '', 360).toLowerCase();
    const ctas = unique(Array.from(node.querySelectorAll('a,button')).map((el) => el.innerText), 3);
    if (node.querySelector('h1') || (index === 0 && ctas.length)) return 'hero';
    if (/(testimonial|review|what customers say|happy clients)/.test(identity + ' ' + text)) return 'testimonials';
    if (/(services|solutions|what we do|service)/.test(identity + ' ' + text)) return 'services';
    if (/(faq|frequently asked|questions)/.test(identity + ' ' + text)) return 'faq';
    if (/(about|our team|who we are|company)/.test(identity + ' ' + text)) return 'about';
    if (/(contact|get in touch|location|call us)/.test(identity + ' ' + text)) return 'contact';
    if (/(gallery|projects|portfolio|recent work|before and after)/.test(identity + ' ' + text)) return 'gallery';
    if (/(pricing|plans|cost|quote)/.test(identity + ' ' + text)) return 'pricing';
    if (ctas.length && text.length < 220) return 'cta';
    return 'content';
  };
  const inferLayout = (node) => {
    const style = window.getComputedStyle(node);
    const rect = node.getBoundingClientRect();
    const children = Array.from(node.children || []).filter(visible);
    const wideChildren = children.filter((child) => child.getBoundingClientRect().width >= rect.width * 0.28);
    if ((style.backgroundImage || 'none') !== 'none') return 'immersive';
    if ((style.display || '').includes('grid')) return 'grid';
    if ((style.display || '').includes('flex') && !String(style.flexDirection || '').includes('column') && wideChildren.length >= 2) return 'split';
    if (wideChildren.length >= 3) return 'grid';
    if (wideChildren.length >= 2) return 'split';
    return 'stacked';
  };
  const candidates = [];
  const seen = new Set();
  ['main section', 'main > div', 'main article', 'header', '[class*="hero"]'].forEach((selector) => {
    document.querySelectorAll(selector).forEach((node) => {
      if (!visible(node) || seen.has(node)) return;
      seen.add(node);
      candidates.push(node);
    });
  });
  const filtered = candidates.slice(0, 12);
  const sectionPatterns = filtered.map((node, index) => {
    const style = window.getComputedStyle(node);
    const heading = clean(node.querySelector('h1,h2,h3')?.innerText || '', 120);
    const text = clean(node.innerText || '', 180);
    const ctas = unique(Array.from(node.querySelectorAll('a,button')).map((el) => el.innerText), 3);
    const directItems = Array.from(node.children || []).filter(visible).length;
    return {
      category: inferCategory(node, index),
      heading,
      summary: heading || text,
      layout_hint: inferLayout(node),
      item_count: directItems,
      cta_texts: ctas,
      background: clean(style.backgroundColor || '', 40),
      text_align: clean(style.textAlign || '', 20),
      has_background_image: (style.backgroundImage || 'none') !== 'none',
      image_count: node.querySelectorAll('img,picture').length,
      padding_y: Math.round((parseFloat(style.paddingTop || '0') + parseFloat(style.paddingBottom || '0')) || 0),
    };
  }).filter((item) => item.summary);
  const colorHints = unique(
    filtered.flatMap((node) => {
      const style = window.getComputedStyle(node);
      return [style.backgroundColor, style.color, style.borderColor];
    }).filter((value) => value && value !== 'rgba(0, 0, 0, 0)' && value !== 'rgb(0, 0, 0)' && value !== 'rgb(255, 255, 255)'),
    6,
  );
  const buttons = Array.from(document.querySelectorAll('a,button')).filter((node) => visible(node) && clean(node.innerText || '', 40));
  const primaryButton = buttons[0] || null;
  const buttonStyle = primaryButton ? (() => {
    const style = window.getComputedStyle(primaryButton);
    const radius = parseFloat(style.borderRadius || '0') || 0;
    const fill = (style.backgroundColor || '').includes('rgba(0, 0, 0, 0)') ? 'outline' : 'solid';
    if (radius >= 20) return `${fill} pill`;
    if (radius >= 8) return `${fill} rounded`;
    return `${fill} square`;
  })() : '';
  const hero = sectionPatterns.find((item) => item.category === 'hero') || sectionPatterns[0] || null;
  const headingFont = clean(window.getComputedStyle(document.querySelector('h1,h2') || document.body).fontFamily || '', 80);
  const bodyFont = clean(window.getComputedStyle(document.body).fontFamily || '', 80);
  return {
    resolved_url: window.location.href,
    section_count: sectionPatterns.length,
    section_patterns: sectionPatterns,
    color_hints: colorHints,
    heading_font_hint: headingFont,
    body_font_hint: bodyFont,
    button_style_hint: buttonStyle,
    hero_layout_hint: hero ? hero.layout_hint : '',
    has_immersive_hero: !!(hero && hero.has_background_image),
    has_multiple_grids: sectionPatterns.filter((item) => item.layout_hint === 'grid').length >= 2,
  };
}
"""

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(viewport={"width": 1440, "height": 1800}, device_scale_factor=1)
            page = context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=25000)
            try:
                page.wait_for_load_state("networkidle", timeout=7000)
            except Exception:
                pass
            rendered = page.evaluate(extractor_js) or {}
            try:
                shot = page.screenshot(type="jpeg", quality=45, full_page=False)
            except Exception:
                shot = b""
            browser.close()
    except Exception:
        return {}

    if not isinstance(rendered, dict):
        return {}

    section_patterns = _merge_reference_section_patterns([], rendered.get("section_patterns") or [], limit=10)
    color_hints = _merge_reference_texts([], rendered.get("color_hints") or [], limit=6, item_limit=60)
    layout_style_hint = "classic-stacked"
    if rendered.get("has_immersive_hero"):
        layout_style_hint = "hero-driven"
    elif rendered.get("has_multiple_grids"):
        layout_style_hint = "card-grid"
    elif len(section_patterns) >= 4:
        layout_style_hint = "modern-sections"

    style_preset_hint = "bold-modern"
    joined_colors = " ".join(color_hints).lower()
    if any(token.startswith("rgb(0") or token.startswith("rgb(1") for token in color_hints):
        style_preset_hint = "dark-premium"
    elif any(token.startswith("rgb(24") or token.startswith("rgb(25") for token in color_hints):
        style_preset_hint = "dark-premium"
    elif "245" in joined_colors or "250" in joined_colors:
        style_preset_hint = "clean-minimal"

    design_traits = []
    hero_pattern = next((item for item in section_patterns if item.get("category") == "hero"), None)
    if hero_pattern:
        if hero_pattern.get("layout_hint") == "immersive":
            design_traits.append("Hero is image-led with overlaid copy instead of a plain text block.")
        elif hero_pattern.get("layout_hint") == "split":
            design_traits.append("Hero uses a split layout with copy paired beside media instead of stacked content.")
    if any(item.get("layout_hint") == "grid" for item in section_patterns):
        design_traits.append("Mid-page sections rely on card or grid groupings to keep services and proof scannable.")
    if any(item.get("padding_y", 0) >= 120 for item in rendered.get("section_patterns") or []):
        design_traits.append("Sections breathe with generous vertical spacing rather than tight stacked bands.")
    button_style_hint = str(rendered.get("button_style_hint") or "").strip()
    if button_style_hint:
        design_traits.append(f"Primary CTAs read as {button_style_hint} buttons.")

    result = {
        "resolved_url": str(rendered.get("resolved_url") or target_url).strip(),
        "section_count": int(rendered.get("section_count") or len(section_patterns) or 0),
        "section_patterns": section_patterns,
        "color_hints": color_hints,
        "layout_style_hint": layout_style_hint,
        "style_preset_hint": style_preset_hint,
        "design_traits": _merge_reference_texts([], design_traits, limit=6),
        "heading_font_hint": str(rendered.get("heading_font_hint") or "").strip()[:80],
        "body_font_hint": str(rendered.get("body_font_hint") or "").strip()[:80],
        "button_style_hint": button_style_hint[:60],
        "hero_layout_hint": str(rendered.get("hero_layout_hint") or "").strip()[:60],
    }
    if shot:
        data_url = "data:image/jpeg;base64," + base64.b64encode(shot).decode("ascii")
        if len(data_url) <= 1_800_000:
            result["screenshot_data_url"] = data_url
    return result


def _reference_design_vision_summary(brand, rendered_reference):
    rendered = rendered_reference if isinstance(rendered_reference, dict) else {}
    screenshot_data_url = str(rendered.get("screenshot_data_url") or "").strip()
    if not screenshot_data_url:
        return []

    api_key = _get_openai_api_key(brand or {})
    if not api_key:
        return []

    try:
        import requests as req
    except Exception:
        return []

    model = _pick_ai_model(brand or {}, "analysis") or "gpt-4o-mini"
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {
                "role": "system",
                "content": "You analyze website screenshots for layout and styling cues. Return valid JSON only. Do not use em dashes.",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Analyze this website screenshot and return JSON with a single key `notes` containing 3 to 5 short strings. "
                            "Focus on layout composition, spacing, hero treatment, card density, contrast, and CTA styling. "
                            "Only mention traits that would help rebuild the design language, not the copy."
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": screenshot_data_url, "detail": "low"},
                    },
                ],
            },
        ],
    }

    try:
        response = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=60,
        )
        if response.status_code >= 400:
            return []
        body = response.json() if hasattr(response, "json") else {}
        content = ((((body or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or "")
        parsed = _extract_json_object_from_ai_text(content)
    except Exception:
        return []

    notes = []
    for item in (parsed.get("notes") or parsed.get("design_notes") or []):
        text = re.sub(r"\s+", " ", str(item or "")).strip()
        if text:
            notes.append(text[:180])
    return _merge_reference_texts([], notes, limit=5)


def _site_builder_reference_style_brief(reference_url, reference_mode="vibe", industry="", business_name="", brand=None):
    raw_url = str(reference_url or "").strip()
    if not raw_url:
        return {}

    normalized_url = raw_url
    if not normalized_url.startswith(("http://", "https://")):
        normalized_url = "https://" + normalized_url

    try:
        from bs4 import BeautifulSoup
        import requests
    except Exception:
        return {
            "requested_url": raw_url,
            "resolved_url": normalized_url,
            "mode": _site_builder_reference_mode(reference_mode),
            "error": "reference-site dependencies unavailable",
        }

    def _clean_text(value, limit=160):
        text = re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()
        return text[:limit]

    def _unique_texts(values, limit, min_length=2):
        seen = set()
        items = []
        for value in values:
            text = _clean_text(value, 120)
            key = text.lower()
            if len(text) < min_length or key in seen:
                continue
            seen.add(key)
            items.append(text)
            if len(items) >= limit:
                break
        return items

    def _extract_color_hints(source_html):
        counts = {}
        for match in re.finditer(r"#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3}", source_html or ""):
            color = match.group(0).lower()
            if len(color) == 4:
                color = "#" + "".join(ch * 2 for ch in color[1:])
            if color in {"#ffffff", "#000000", "#f8fafc", "#f9fafb", "#111111"}:
                continue
            counts[color] = counts.get(color, 0) + 1
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return [color for color, _ in ranked[:4]]

    def _infer_layout_style(soup, source_html):
        text = (source_html or "").lower()
        if "sidebar" in text:
            return "sidebar-content"
        if text.count("card") >= 3 or text.count("grid") >= 3:
            return "card-grid"
        if "hero" in text or "banner" in text:
            return "hero-driven"
        if len(soup.find_all("section")) >= 4:
            return "modern-sections"
        return "classic-stacked"

    def _infer_style_preset(color_hints):
        joined = " ".join(color_hints or [])
        if any(color.startswith("#0") or color.startswith("#1") for color in color_hints or []):
            return "dark-premium"
        if any(color.startswith("#f9") or color.startswith("#fa") or color.startswith("#fb") for color in color_hints or []):
            return "clean-minimal"
        if any(color.startswith("#d9") or color.startswith("#c2") or color.startswith("#b9") for color in color_hints or []) and "#f" in joined:
            return "warm-traditional"
        return "bold-modern"

    def _section_layout_hint(section):
        classes = " ".join(section.get("class") or [])
        text = f"{section.get('id') or ''} {classes}".lower()
        if any(token in text for token in ("grid", "cards", "card-grid", "columns")):
            return "grid"
        if any(token in text for token in ("split", "two-col", "two-column", "columns-2")):
            return "split"
        items = section.find_all(["article", "li", "div"], recursive=False)
        if len(items) >= 3:
            return "grid"
        return "stacked"

    def _classify_reference_section(section, index):
        heading = _clean_text(
            (section.find(["h1", "h2", "h3"]).get_text(" ", strip=True) if section.find(["h1", "h2", "h3"]) else ""),
            120,
        )
        classes = " ".join(section.get("class") or [])
        identity = f"{section.get('id') or ''} {classes} {heading}".lower()
        section_text = _clean_text(section.get_text(" ", strip=True), 500).lower()
        ctas = _unique_texts(
            (tag.get_text(" ", strip=True) for tag in section.find_all(["a", "button"])),
            4,
            min_length=3,
        )

        if section.find("h1") or (index == 0 and ctas):
            return "hero"
        if any(word in identity or word in section_text for word in ("testimonial", "reviews", "review", "what customers say", "happy clients")):
            return "testimonials"
        if any(word in identity or word in section_text for word in ("faq", "frequently asked", "questions")):
            return "faq"
        if any(word in identity or word in section_text for word in ("process", "how it works", "our process", "steps")):
            return "process"
        if any(word in identity or word in section_text for word in ("services", "what we do", "solutions", "service")):
            return "services"
        if any(word in identity or word in section_text for word in ("why choose", "why us", "trusted", "licensed", "insured", "guarantee", "proof")):
            return "proof"
        if any(word in identity or word in section_text for word in ("pricing", "plans", "cost", "quote options")):
            return "pricing"
        if any(word in identity or word in section_text for word in ("gallery", "projects", "recent work", "portfolio", "before and after")):
            return "gallery"
        if any(word in identity or word in section_text for word in ("about", "our team", "who we are", "company")):
            return "about"
        if any(word in identity or word in section_text for word in ("contact", "get in touch", "location", "call us")):
            return "contact"
        if ctas and len(_clean_text(section.get_text(" ", strip=True), 220)) < 180:
            return "cta"
        return "content"

    def _extract_section_patterns(soup):
        container = soup.find("main") or soup.body or soup
        candidates = []
        seen_nodes = set()

        for section in container.find_all("section"):
            candidates.append(section)
            seen_nodes.add(id(section))
            if len(candidates) >= 12:
                break

        if len(candidates) < 4:
            for child in container.find_all(["div", "article"], recursive=False):
                if id(child) in seen_nodes:
                    continue
                text = _clean_text(child.get_text(" ", strip=True), 320)
                has_heading = child.find(["h1", "h2", "h3"]) is not None
                if len(text) < 80 and not has_heading:
                    continue
                candidates.append(child)
                seen_nodes.add(id(child))
                if len(candidates) >= 12:
                    break

        patterns = []
        for index, section in enumerate(candidates):
            heading_tag = section.find(["h1", "h2", "h3"])
            heading = _clean_text(heading_tag.get_text(" ", strip=True) if heading_tag else "", 120)
            snippet_parts = []
            for tag in section.find_all(["p", "li"], limit=4):
                text = _clean_text(tag.get_text(" ", strip=True), 90)
                if text:
                    snippet_parts.append(text)
            snippet = " ".join(snippet_parts[:2]).strip()
            category = _classify_reference_section(section, index)
            cta_texts = _unique_texts(
                (tag.get_text(" ", strip=True) for tag in section.find_all(["a", "button"])),
                3,
                min_length=3,
            )
            item_count = len(section.find_all(["article", "li", "div"], recursive=False))
            layout_hint = _section_layout_hint(section)
            summary_source = heading or snippet or _clean_text(section.get_text(" ", strip=True), 120)
            if not summary_source:
                continue
            patterns.append({
                "category": category,
                "heading": heading,
                "summary": summary_source[:160],
                "layout_hint": layout_hint,
                "item_count": item_count,
                "cta_texts": cta_texts,
            })
            if len(patterns) >= 10:
                break

        return patterns

    def _industry_image_queries(industry_name):
        text = str(industry_name or "").strip().lower()
        base = {
            "hero": "local service business team helping a homeowner",
            "services": "home service technician working on site",
            "about": "service business team portrait",
            "gallery": "completed home service project",
            "testimonials": "happy homeowner with service technician",
            "contact": "service truck parked outside a home",
            "cta": "homeowner calling a local service company",
        }
        if "plumb" in text:
            return {
                "hero": "plumber service van in driveway",
                "services": "plumber repairing sink or drain",
                "about": "plumbing team in uniform",
                "gallery": "clean plumbing repair installation",
                "testimonials": "happy homeowner with plumber",
                "contact": "plumber at front door ready to help",
                "cta": "plumber answering emergency service call",
            }
        if "hvac" in text or text in {"ac", "heating"}:
            return {
                "hero": "hvac technician at residential unit",
                "services": "hvac technician servicing air conditioner",
                "about": "hvac service team in branded uniforms",
                "gallery": "clean hvac install in home",
                "testimonials": "happy family with hvac technician",
                "contact": "hvac van outside suburban home",
                "cta": "homeowner scheduling hvac repair",
            }
        if "roof" in text:
            return {
                "hero": "roofing crew on residential roof",
                "services": "roofer inspecting shingles",
                "about": "roofing team with equipment",
                "gallery": "completed roof replacement exterior",
                "testimonials": "happy homeowner after roof repair",
                "contact": "roofing truck and crew onsite",
                "cta": "homeowner meeting roofing contractor",
            }
        if "landscap" in text or "lawn" in text:
            return {
                "hero": "landscaping crew at front yard",
                "services": "landscaper trimming and planting",
                "about": "landscaping team with equipment trailer",
                "gallery": "finished landscaped backyard",
                "testimonials": "happy homeowner in landscaped yard",
                "contact": "landscaping truck outside home",
                "cta": "homeowner planning yard project",
            }
        if "clean" in text or "janitor" in text:
            return {
                "hero": "professional cleaning team in modern home",
                "services": "cleaner working in kitchen",
                "about": "cleaning crew portrait",
                "gallery": "freshly cleaned living room",
                "testimonials": "happy homeowner after house cleaning",
                "contact": "cleaning team arriving for appointment",
                "cta": "customer booking cleaning service",
            }
        if "electric" in text:
            return {
                "hero": "electrician at residential panel",
                "services": "electrician installing fixture",
                "about": "electrician team portrait",
                "gallery": "modern lighting installation",
                "testimonials": "happy homeowner with electrician",
                "contact": "electric service van outside house",
                "cta": "electrician responding to service call",
            }
        return base

    def _classify_reference_image(img, index):
        section = img.find_parent(["section", "article", "header", "div"])
        section_role = _classify_reference_section(section, index) if section else "gallery"
        src = " ".join(
            str(img.get(attr) or "")
            for attr in ("src", "data-src", "data-lazy-src", "class", "id")
        ).lower()
        alt = _clean_text(img.get("alt") or img.get("title") or "", 160).lower()
        text = f"{src} {alt}".strip()
        if any(token in text for token in ("logo", "icon", "avatar", "badge", "favicon", "sprite")):
            return ""
        if section_role == "hero" or index == 0 or any(token in text for token in ("hero", "banner", "header")):
            return "hero"
        if "team" in text or "staff" in text or section_role == "about":
            return "about"
        if section_role == "testimonials" or any(token in text for token in ("testimonial", "review", "customer")):
            return "testimonials"
        if section_role == "services" or any(token in text for token in ("service", "repair", "install", "project")):
            return "services"
        if section_role == "contact" or any(token in text for token in ("contact", "truck", "van", "location")):
            return "contact"
        if section_role == "cta":
            return "cta"
        return "gallery"

    def _stock_query_for_image(role, alt_text, industry_name, business_label):
        queries = _industry_image_queries(industry_name)
        descriptor = _clean_text(alt_text or "", 90).lower()
        for token in re.split(r"[^a-z0-9]+", str(business_label or "").lower()):
            if len(token) >= 3:
                descriptor = re.sub(rf"\b{re.escape(token)}\b", " ", descriptor)
        descriptor = re.sub(r"\s+", " ", descriptor).strip(" ,.-")
        if len(descriptor.split()) >= 3 and not any(token in descriptor for token in ("logo", "icon", "banner")):
            return descriptor
        return queries.get(role) or queries["hero"]

    def _stock_image_url(query, role, index):
        size = "1600x900" if role in {"hero", "services", "about", "contact", "cta"} else "1200x900"
        return f"https://source.unsplash.com/featured/{size}/?{quote_plus(query)}&sig={index + 1}"

    def _extract_reference_image_assets(soup, base_url):
        container = soup.find("main") or soup.body or soup
        assets = []
        seen = set()
        for index, img in enumerate(container.find_all("img", limit=18)):
            src = (
                img.get("src")
                or img.get("data-src")
                or img.get("data-lazy-src")
                or ((img.get("srcset") or "").split(",")[0].strip().split(" ")[0] if img.get("srcset") else "")
            )
            if not src:
                continue
            src = str(src).strip()
            if not src or src.startswith("data:"):
                continue
            role = _classify_reference_image(img, index)
            if not role:
                continue
            alt_text = _clean_text(img.get("alt") or img.get("title") or "", 140) or f"{role.title()} image"
            query = _stock_query_for_image(role, alt_text, industry, business_name)
            dedupe_key = (role, query.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            assets.append({
                "role": role,
                "alt": alt_text,
                "reference_url": urljoin(base_url, src),
                "asset_url": _stock_image_url(query, role, len(assets)),
                "query": query[:120],
            })
            if len(assets) >= 6:
                break

        if assets:
            return assets

        fallback_roles = ("hero", "services", "about")
        queries = _industry_image_queries(industry)
        fallback = []
        for index, role in enumerate(fallback_roles):
            query = queries.get(role) or queries["hero"]
            fallback.append({
                "role": role,
                "alt": f"{role.title()} stock image",
                "reference_url": "",
                "asset_url": _stock_image_url(query, role, index),
                "query": query[:120],
            })
        return fallback

    try:
        resp = requests.get(
            normalized_url,
            timeout=15,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; GroMoreBot/1.0)"},
        )
        resp.raise_for_status()
    except Exception as exc:
        return {
            "requested_url": raw_url,
            "resolved_url": normalized_url,
            "mode": _site_builder_reference_mode(reference_mode),
            "error": str(exc)[:200],
        }

    html_text = resp.text[:180000]
    soup = BeautifulSoup(html_text, "html.parser")

    title = _clean_text(soup.title.string if soup.title and soup.title.string else "", 200)
    meta_description = ""
    meta_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    if meta_tag and meta_tag.get("content"):
        meta_description = _clean_text(meta_tag.get("content"), 320)

    nav_items = []
    nav = soup.find("nav") or soup.find("header")
    if nav:
        nav_items = _unique_texts((link.get_text(" ", strip=True) for link in nav.find_all("a")), 6)

    headings = _unique_texts((tag.get_text(" ", strip=True) for tag in soup.find_all(["h1", "h2", "h3"])), 8, min_length=4)
    cta_candidates = []
    for tag in soup.find_all(["a", "button"]):
        text = _clean_text(tag.get_text(" ", strip=True), 60)
        lowered = text.lower()
        if any(word in lowered for word in ("quote", "estimate", "call", "book", "schedule", "start", "contact", "get ", "free")):
            cta_candidates.append(text)
    cta_texts = _unique_texts(cta_candidates, 5, min_length=3)

    color_hints = _extract_color_hints(html_text)
    layout_style_hint = _infer_layout_style(soup, html_text)
    style_preset_hint = _infer_style_preset(color_hints)
    section_count = len(soup.find_all("section"))
    section_patterns = _extract_section_patterns(soup)
    image_assets = _extract_reference_image_assets(soup, resp.url or normalized_url)
    rendered_reference = _extract_rendered_reference_design(resp.url or normalized_url)
    color_hints = _merge_reference_texts(color_hints, rendered_reference.get("color_hints") or [], limit=6, item_limit=60)
    section_patterns = _merge_reference_section_patterns(section_patterns, rendered_reference.get("section_patterns") or [], limit=10)
    section_count = max(section_count, int(rendered_reference.get("section_count") or 0))
    layout_style_hint = str(rendered_reference.get("layout_style_hint") or layout_style_hint).strip()
    style_preset_hint = str(rendered_reference.get("style_preset_hint") or "").strip() or _infer_style_preset(color_hints)
    design_traits = _merge_reference_texts([], rendered_reference.get("design_traits") or [], limit=6)
    vision_notes = _reference_design_vision_summary(brand, rendered_reference)
    heading_font_hint = str(rendered_reference.get("heading_font_hint") or "").strip()
    body_font_hint = str(rendered_reference.get("body_font_hint") or "").strip()
    button_style_hint = str(rendered_reference.get("button_style_hint") or "").strip()
    hero_layout_hint = str(rendered_reference.get("hero_layout_hint") or "").strip()

    notes = []
    if nav_items:
        notes.append(f"Top navigation uses {len(nav_items)} primary items and a clear header CTA pattern.")
    if headings:
        notes.append(f"Section rhythm is driven by headings like: {', '.join(headings[:4])}.")
    if cta_texts:
        notes.append(f"CTA language repeats actions like: {', '.join(cta_texts[:3])}.")
    if section_count:
        notes.append(f"The page appears to use about {section_count} distinct section blocks.")
    if section_patterns:
        labels = [pattern.get("category") for pattern in section_patterns[:6] if pattern.get("category")]
        if labels:
            notes.append(f"Visible section sequence includes: {', '.join(labels)}.")
    if image_assets:
        image_roles = [asset.get("role") for asset in image_assets[:4] if asset.get("role")]
        if image_roles:
            notes.append(f"Visual rhythm uses {', '.join(image_roles)} imagery, recreated with stock replacements for this build.")
    if button_style_hint:
        notes.append(f"Primary CTA styling reads as {button_style_hint} buttons in the rendered page.")
    if hero_layout_hint:
        notes.append(f"Hero treatment feels {hero_layout_hint} in the rendered layout.")
    notes.extend(design_traits[:2])
    notes.extend(vision_notes[:2])
    resolved_url = str(rendered_reference.get("resolved_url") or resp.url or normalized_url).strip()
    try:
        parsed_resolved = urlparse(resolved_url)
        if parsed_resolved.scheme and parsed_resolved.netloc and (parsed_resolved.path or "") == "/" and not parsed_resolved.query and not parsed_resolved.fragment:
            resolved_url = f"{parsed_resolved.scheme}://{parsed_resolved.netloc}"
    except Exception:
        pass

    return {
        "requested_url": raw_url,
        "resolved_url": resolved_url,
        "mode": _site_builder_reference_mode(reference_mode),
        "title": title,
        "description": meta_description,
        "nav_items": nav_items,
        "headings": headings,
        "cta_texts": cta_texts,
        "color_hints": color_hints,
        "section_count": section_count,
        "section_patterns": section_patterns,
        "image_assets": image_assets,
        "layout_style_hint": layout_style_hint,
        "style_preset_hint": style_preset_hint,
        "design_traits": design_traits,
        "vision_notes": vision_notes,
        "heading_font_hint": heading_font_hint,
        "body_font_hint": body_font_hint,
        "button_style_hint": button_style_hint,
        "hero_layout_hint": hero_layout_hint,
        "notes": notes,
    }


def _suggest_creative_style(brand, prompt, ad_format):
    api_key = (brand.get("openai_api_key") or "").strip()
    if not api_key:
        from flask import current_app
        api_key = (current_app.config.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None

    model = _pick_ai_model(brand, "images")

    ask = f"""You are selecting visual style settings for an ad creative.

Brand voice: {brand.get('brand_voice', '')}
Industry: {brand.get('industry', '')}
Ad format: {ad_format}
User direction prompt: {prompt}

Return JSON only with:
- overlay_template: one of [lower_third, full_lower_third, upper_third, full_overlay, soft_box, brand_bar, diagonal_band, bubbles, boxes]
- shape_style: one of [rounded, sharp, pill]
- text_placement: one of [left, center, right]
- headline_font_family: one of [modern, classic, clean, elegant, friendly, strong, mono, playful, geometric, serif_alt]
- body_font_family: one of [modern, classic, clean, elegant, friendly, strong, mono, playful, geometric, serif_alt]
- cta_font_family: one of [modern, classic, clean, elegant, friendly, strong, mono, playful, geometric, serif_alt]

JSON only, no markdown."""

    import requests as req
    try:
        resp = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "messages": [{"role": "user", "content": ask}], "temperature": 0.5},
            timeout=20,
        )
        if resp.status_code != 200:
            return None
        content = resp.json()["choices"][0]["message"]["content"].strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3]
        data = json.loads(content)
        if not isinstance(data, dict):
            return None
        return {
            "overlay_template": data.get("overlay_template"),
            "shape_style": data.get("shape_style"),
            "text_placement": data.get("text_placement"),
            "headline_font_family": data.get("headline_font_family"),
            "body_font_family": data.get("body_font_family"),
            "cta_font_family": data.get("cta_font_family"),
        }
    except Exception:
        return None


@client_bp.route("/settings")
@client_login_required
def client_settings():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    connections = db.get_brand_connections(brand_id)
    google_conn = connections.get("google", {})
    meta_conn = connections.get("meta", {})
    scopes = (google_conn.get("scopes") or "").lower()
    # Check for full 'auth/drive' scope (not just drive.file)
    has_full_drive = "auth/drive " in (scopes + " ") or scopes.endswith("auth/drive")
    drive_scoped = has_full_drive or ("spreadsheets" in scopes)
    try:
        chatbot_channels = set(json.loads(brand.get("sales_bot_channels") or "[]"))
    except Exception:
        chatbot_channels = set()

    def _brand_has_value(field_name):
        return bool((brand.get(field_name) or "").strip())

    warren_channel_readiness = {
        "sms": (
            _brand_has_value("quo_api_key")
            and _brand_has_value("quo_phone_number")
            and _brand_has_value("sales_bot_quo_webhook_secret")
        ),
        "lead_forms": _brand_has_value("sales_bot_incoming_webhook_secret"),
        "messenger": bool(meta_conn.get("status") == "connected" and _brand_has_value("facebook_page_id")),
    }
    warren_ready_count = sum(1 for is_ready in warren_channel_readiness.values() if is_ready)
    warren_total_count = len(warren_channel_readiness)
    if warren_ready_count == warren_total_count and warren_total_count:
        warren_status_state = "ready"
    elif warren_ready_count:
        warren_status_state = "partial"
    else:
        warren_status_state = "not_configured"

    try:
        sng_secret = db.ensure_brand_sng_webhook_secret(brand_id)
        sng_webhook_url = f"{_external_app_url()}{url_for('webhooks.sng_webhook', brand_slug=brand['slug'], secret=sng_secret)}"

        sng_webhook_events = db.get_sng_webhook_events(brand_id, limit=10)
        for event in sng_webhook_events:
            event["summary"] = _safe_json_object(event.get("summary_json"))
            event["status_badge"] = {
                "received": "success",
                "processed": "primary",
                "ignored": "secondary",
                "failed": "danger",
            }.get((event.get("status") or "").strip().lower(), "secondary")

        return render_template(
            "client_connections.html",
            brand=brand,
            google_connected=(google_conn.get("status") == "connected"),
            meta_connected=(meta_conn.get("status") == "connected"),
            drive_scoped=drive_scoped,
            chatbot_channels=chatbot_channels,
            google_conn=google_conn,
            meta_conn=meta_conn,
            warren_channel_readiness=warren_channel_readiness,
            warren_ready_count=warren_ready_count,
            warren_total_count=warren_total_count,
            warren_status_state=warren_status_state,
            sng_webhook_url=sng_webhook_url,
            sng_webhook_events=sng_webhook_events,
            brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        )
    except Exception:
        current_app.logger.exception("client_settings render error for brand %s", brand_id)
        flash("Settings page failed to load. The error has been logged.", "error")
        return redirect(url_for("client.client_dashboard"))


def _load_client_automation_context(db, brand_id):
    chatbot_channels = set()
    brand = db.get_brand(brand_id) or {}
    from webapp.warren_crm_events import RULE_DEFINITIONS, load_crm_event_rules

    try:
        chatbot_channels = set(json.loads(brand.get("sales_bot_channels") or "[]"))
    except Exception:
        chatbot_channels = set()

    appointment_runs = db.get_appointment_reminder_runs(brand_id, limit=8)
    for run in appointment_runs:
        summary = _safe_json_object(run.get("summary_json"))
        run["summary"] = summary
        run["status_badge"] = {
            "completed": "success",
            "partial": "warning",
            "failed": "danger",
            "waiting": "secondary",
            "config_error": "danger",
        }.get((run.get("status") or "").strip().lower(), "secondary")

    appointment_attempts = db.get_brand_client_billing_reminders(
        brand_id,
        reminder_type="appointment_day_ahead",
        limit=20,
    )
    for attempt in appointment_attempts:
        attempt["detail_data"] = _safe_json_object(attempt.get("detail"))
        attempt["detail_summary"] = _summarize_delivery_detail(attempt.get("detail"))

    billing_attempts = db.get_brand_client_billing_reminders(
        brand_id,
        reminder_type="payment_due",
        limit=12,
    )
    for attempt in billing_attempts:
        attempt["detail_data"] = _safe_json_object(attempt.get("detail"))
        attempt["detail_summary"] = _summarize_delivery_detail(attempt.get("detail"))

    sng_webhook_events = db.get_sng_webhook_events(brand_id, limit=10)
    for event in sng_webhook_events:
        event["summary"] = _safe_json_object(event.get("summary_json"))
        event["status_badge"] = {
            "received": "success",
            "processed": "primary",
            "ignored": "secondary",
            "failed": "danger",
        }.get((event.get("status") or "").strip().lower(), "secondary")

    crm_event_rules = load_crm_event_rules(brand)
    crm_event_actions = db.get_crm_event_actions(brand_id, limit=20)
    for action in crm_event_actions:
        action["status_badge"] = {
            "queued": "secondary",
            "sent": "success",
            "failed": "danger",
            "resolved": "primary",
        }.get((action.get("status") or "").strip().lower(), "secondary")
        action["rule_label"] = RULE_DEFINITIONS.get(action.get("rule_key"), {}).get("label", (action.get("rule_key") or "").replace("_", " ").title())

    return {
        "brand": brand,
        "chatbot_channels": chatbot_channels,
        "appointment_reminder_runs": appointment_runs,
        "appointment_reminder_attempts": appointment_attempts,
        "billing_reminder_attempts": billing_attempts,
        "sng_webhook_events": sng_webhook_events,
        "crm_event_rules": crm_event_rules,
        "crm_event_actions": crm_event_actions,
        "crm_event_rule_definitions": RULE_DEFINITIONS,
    }


@client_bp.route("/automations")
@client_login_required
def client_automations():
    db = _get_db()
    brand_id = session["client_brand_id"]
    automation_context = _load_client_automation_context(db, brand_id)
    brand = automation_context.get("brand")
    if not brand:
        abort(404)

    return render_template(
        "client_automations.html",
        brand=brand,
        chatbot_channels=automation_context["chatbot_channels"],
        appointment_reminder_runs=automation_context["appointment_reminder_runs"],
        appointment_reminder_attempts=automation_context["appointment_reminder_attempts"],
        billing_reminder_attempts=automation_context["billing_reminder_attempts"],
        sng_webhook_events=automation_context["sng_webhook_events"],
        crm_event_rules=automation_context["crm_event_rules"],
        crm_event_actions=automation_context["crm_event_actions"],
        crm_event_rule_definitions=automation_context["crm_event_rule_definitions"],
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/settings/warren-channels", methods=["POST"])
@client_login_required
def client_save_warren_channels_settings():
    db = _get_db()
    brand_id = session["client_brand_id"]

    quo_api_key = (request.form.get("quo_api_key") or "").strip()
    if quo_api_key:
        db.update_brand_text_field(brand_id, "quo_api_key", quo_api_key[:500])

    db.update_brand_text_field(
        brand_id,
        "quo_phone_number",
        (request.form.get("quo_phone_number") or "").strip()[:100],
    )

    quo_secret = (request.form.get("sales_bot_quo_webhook_secret") or "").strip()
    if quo_secret:
        db.update_brand_text_field(brand_id, "sales_bot_quo_webhook_secret", quo_secret[:255])

    incoming_secret = (request.form.get("sales_bot_incoming_webhook_secret") or "").strip()
    if incoming_secret:
        db.update_brand_text_field(brand_id, "sales_bot_incoming_webhook_secret", incoming_secret[:255])

    flash("Warren channel settings saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/automations/save", methods=["POST"])
@client_login_required
def client_save_automations():
    db = _get_db()
    brand_id = session["client_brand_id"]
    from webapp.warren_crm_events import serialize_crm_event_rules

    valid_channels = {"sms", "messenger", "lead_forms", "calls"}
    valid_payment_channels = {"email", "sms"}
    valid_appointment_channels = {"email", "sms"}

    selected_channels = [c for c in request.form.getlist("sales_bot_channels") if c in valid_channels]
    selected_payment_channels = [c for c in request.form.getlist("sales_bot_payment_reminder_channels") if c in valid_payment_channels]
    selected_appointment_channels = [c for c in request.form.getlist("sales_bot_appointment_reminder_channels") if c in valid_appointment_channels]
    if not selected_payment_channels:
        selected_payment_channels = ["email"]
    if not selected_appointment_channels:
        selected_appointment_channels = ["sms"]

    quote_mode = (request.form.get("sales_bot_quote_mode") or "hybrid").strip().lower()
    if quote_mode not in {"simple", "hybrid", "structured"}:
        quote_mode = "hybrid"

    db.update_brand_number_field(brand_id, "sales_bot_enabled", 1 if request.form.get("sales_bot_enabled") else 0)
    db.update_brand_text_field(brand_id, "sales_bot_channels", json.dumps(selected_channels))
    db.update_brand_text_field(brand_id, "sales_bot_quote_mode", quote_mode)
    db.update_brand_text_field(
        brand_id,
        "sales_bot_business_hours",
        (request.form.get("sales_bot_business_hours") or "").strip()[:1000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_reply_tone",
        (request.form.get("sales_bot_reply_tone") or "").strip()[:500],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_handoff_alert_phones",
        (request.form.get("sales_bot_handoff_alert_phones") or "").strip()[:500],
    )
    try:
        reply_delay_seconds = max(0, min(300, float(request.form.get("sales_bot_reply_delay_seconds") or 0)))
    except (ValueError, TypeError):
        reply_delay_seconds = 0
    db.update_brand_number_field(brand_id, "sales_bot_reply_delay_seconds", reply_delay_seconds)

    db.update_brand_number_field(brand_id, "sales_bot_payment_reminders_enabled", 1 if request.form.get("sales_bot_payment_reminders_enabled") else 0)
    try:
        payment_days_before = max(0, min(21, int(float(request.form.get("sales_bot_payment_reminder_days_before") or 3))))
    except (ValueError, TypeError):
        payment_days_before = 3
    db.update_brand_number_field(brand_id, "sales_bot_payment_reminder_days_before", payment_days_before)
    try:
        payment_billing_day = max(1, min(31, int(float(request.form.get("sales_bot_payment_reminder_billing_day") or 1))))
    except (ValueError, TypeError):
        payment_billing_day = 1
    db.update_brand_number_field(brand_id, "sales_bot_payment_reminder_billing_day", payment_billing_day)
    db.update_brand_text_field(brand_id, "sales_bot_payment_reminder_channels", json.dumps(selected_payment_channels))
    db.update_brand_text_field(
        brand_id,
        "sales_bot_payment_reminder_template",
        (request.form.get("sales_bot_payment_reminder_template") or "").strip()[:2000],
    )

    db.update_brand_number_field(brand_id, "sales_bot_appointment_reminders_enabled", 1 if request.form.get("sales_bot_appointment_reminders_enabled") else 0)
    db.update_brand_text_field(
        brand_id,
        "sales_bot_appointment_reminder_send_time",
        (request.form.get("sales_bot_appointment_reminder_send_time") or "17:00").strip()[:10],
    )
    appointment_timezone = (request.form.get("sales_bot_appointment_reminder_timezone") or "America/New_York").strip()
    if appointment_timezone not in {"America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu"}:
        appointment_timezone = "America/New_York"
    db.update_brand_text_field(brand_id, "sales_bot_appointment_reminder_timezone", appointment_timezone)
    db.update_brand_text_field(brand_id, "sales_bot_appointment_reminder_channels", json.dumps(selected_appointment_channels))
    db.update_brand_text_field(
        brand_id,
        "sales_bot_appointment_reminder_template",
        (request.form.get("sales_bot_appointment_reminder_template") or "").strip()[:2000],
    )
    db.update_brand_number_field(
        brand_id,
        "sales_bot_appointment_reminder_respect_client_channel",
        1 if request.form.get("sales_bot_appointment_reminder_respect_client_channel") else 0,
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_crm_event_alert_emails",
        (request.form.get("sales_bot_crm_event_alert_emails") or "").strip()[:1000],
    )

    db.update_brand_number_field(brand_id, "sales_bot_transcript_export", 1 if request.form.get("sales_bot_transcript_export") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_meta_lead_forms", 1 if request.form.get("sales_bot_meta_lead_forms") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_messenger_enabled", 1 if request.form.get("sales_bot_messenger_enabled") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_call_logging", 1 if request.form.get("sales_bot_call_logging") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_auto_push_crm", 1 if request.form.get("sales_bot_auto_push_crm") else 0)

    db.update_brand_number_field(brand_id, "sales_bot_nurture_enabled", 1 if request.form.get("sales_bot_nurture_enabled") else 0)
    for tier in ("hot", "warm", "cold"):
        hours_key = f"sales_bot_nurture_{tier}_hours"
        max_key = f"sales_bot_nurture_{tier}_max"
        try:
            hours_val = max(0.5, min(720, float(request.form.get(hours_key) or 0)))
        except (ValueError, TypeError):
            hours_val = {"hot": 2, "warm": 24, "cold": 48}[tier]
        try:
            max_val = max(1, min(10, int(request.form.get(max_key) or 0)))
        except (ValueError, TypeError):
            max_val = {"hot": 3, "warm": 2, "cold": 2}[tier]
        db.update_brand_number_field(brand_id, hours_key, hours_val)
        db.update_brand_number_field(brand_id, max_key, max_val)

    try:
        ghost_hours = max(24, min(720, float(request.form.get("sales_bot_nurture_ghost_hours") or 72)))
    except (ValueError, TypeError):
        ghost_hours = 72
    db.update_brand_number_field(brand_id, "sales_bot_nurture_ghost_hours", ghost_hours)

    db.update_brand_number_field(brand_id, "sales_bot_dnd_enabled", 1 if request.form.get("sales_bot_dnd_enabled") else 0)
    db.update_brand_text_field(brand_id, "sales_bot_dnd_start", (request.form.get("sales_bot_dnd_start") or "21:00").strip()[:5])
    db.update_brand_text_field(brand_id, "sales_bot_dnd_end", (request.form.get("sales_bot_dnd_end") or "08:00").strip()[:5])
    db.update_brand_number_field(brand_id, "sales_bot_dnd_weekends", 1 if request.form.get("sales_bot_dnd_weekends") else 0)
    valid_tz = {"America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu"}
    tz = (request.form.get("sales_bot_dnd_timezone") or "America/New_York").strip()
    if tz not in valid_tz:
        tz = "America/New_York"
    db.update_brand_text_field(brand_id, "sales_bot_dnd_timezone", tz)

    db.update_brand_text_field(
        brand_id,
        "sales_bot_sms_opt_out_footer",
        (request.form.get("sales_bot_sms_opt_out_footer") or "").strip()[:200],
    )

    crm_event_rules = serialize_crm_event_rules(request.form)
    db.update_brand_text_field(
        brand_id,
        "sales_bot_crm_event_rules",
        json.dumps(crm_event_rules.get("rules") or {}, separators=(",", ":")),
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_crm_event_alert_emails",
        ", ".join(crm_event_rules.get("alert_emails") or [])[:1000],
    )

    flash("Automations saved.", "success")
    return redirect(url_for("client.client_automations"))


@client_bp.route("/settings/ads-id", methods=["POST"])
@client_login_required
def client_save_ads_id():
    db = _get_db()
    brand_id = session["client_brand_id"]

    raw = request.form.get("google_ads_customer_id", "").strip()
    # Keep only digits and dashes
    cleaned = "".join(c for c in raw if c.isdigit() or c == "-")
    db.update_brand_api_field(brand_id, "google_ads_customer_id", cleaned)
    flash("Google Ads Customer ID saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/settings/facebook-page", methods=["POST"])
@client_login_required
def client_save_facebook_page_id():
    db = _get_db()
    brand_id = session["client_brand_id"]

    raw = (request.form.get("facebook_page_id") or "").strip()
    db.update_brand_api_field(brand_id, "facebook_page_id", raw)
    flash("Facebook Page reference saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/settings/openai", methods=["POST"])
@client_login_required
def client_save_openai():
    db = _get_db()
    brand_id = session["client_brand_id"]

    api_key = request.form.get("openai_api_key", "").strip()
    quality_tier = request.form.get("ai_quality_tier", "").strip().lower()

    # Save quality tier
    if quality_tier in ("efficient", "balanced", "premium"):
        db.update_brand_text_field(brand_id, "ai_quality_tier", quality_tier)

        # Also update the per-purpose model fields so other features (chat, blog, etc.) pick them up
        _tier_map = {
            "efficient": {"openai_model": "gpt-4o-mini", "openai_model_chat": "gpt-4o-mini", "openai_model_images": "gpt-4o-mini", "openai_model_analysis": "gpt-4o-mini", "openai_model_ads": "gpt-4o-mini"},
            "balanced":  {"openai_model": "gpt-4o-mini", "openai_model_chat": "gpt-4o-mini", "openai_model_images": "gpt-4o",      "openai_model_analysis": "gpt-4o-mini", "openai_model_ads": "gpt-4o"},
            "premium":   {"openai_model": "gpt-4.1",     "openai_model_chat": "gpt-4.1",     "openai_model_images": "gpt-4o",      "openai_model_analysis": "gpt-4.1",     "openai_model_ads": "gpt-4.1"},
        }
        for field, model_val in _tier_map[quality_tier].items():
            db.update_brand_text_field(brand_id, field, model_val)

    # Only update key if user actually entered something (don't blank it on empty submit)
    if api_key:
        if not api_key.startswith("sk-"):
            flash("Invalid API key format. OpenAI keys start with sk-", "error")
            return redirect(url_for("client.client_settings"))
        db.update_brand_text_field(brand_id, "openai_api_key", api_key)

    flash("AI settings saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/settings/agent-context", methods=["POST"])
@client_login_required
def client_save_agent_context():
    """Save per-agent custom instructions."""
    db = _get_db()
    brand_id = session["client_brand_id"]

    valid_agents = {"scout", "penny", "ace", "radar", "hawk", "pulse", "spark", "bridge"}
    context = {}
    for agent_key in valid_agents:
        val = (request.form.get(f"agent_ctx_{agent_key}") or "").strip()[:1000]
        if val:
            context[agent_key] = val

    db.update_brand_text_field(brand_id, "agent_context", json.dumps(context))
    flash("Team instructions saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/settings/google-drive", methods=["POST"])
@client_login_required
def client_save_google_drive():
    db = _get_db()
    brand_id = session["client_brand_id"]

    folder_id = (request.form.get("google_drive_folder_id") or "").strip()[:500]
    sheet_id = (request.form.get("google_drive_sheet_id") or "").strip()[:500]

    # Extract folder ID from full Drive URL if user pasted one
    import re
    drive_url_match = re.search(r'folders/([a-zA-Z0-9_-]+)', folder_id)
    if drive_url_match:
        folder_id = drive_url_match.group(1)
    # Extract sheet ID from full Sheets URL if user pasted one
    sheet_url_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', sheet_id)
    if sheet_url_match:
        sheet_id = sheet_url_match.group(1)

    db.update_brand_text_field(brand_id, "google_drive_folder_id", folder_id)
    db.update_brand_text_field(brand_id, "google_drive_sheet_id", sheet_id)

    # Auto-create subfolder structure if folder ID was provided
    if folder_id:
        # Check if Drive scope exists before attempting setup
        connections = db.get_brand_connections(brand_id)
        google_conn = connections.get("google", {})
        scopes = (google_conn.get("scopes") or "").lower()
        if "drive" not in scopes:
            flash("Folder ID saved. To complete setup, click 'Reconnect Google With Drive Access' above to grant Drive permissions, then save again.", "warning")
        else:
            from webapp.google_drive import setup_brand_drive
            result = setup_brand_drive(db, brand_id)
            if result.get("ok"):
                flash("Google Drive connected and folders created.", "success")
            else:
                flash(f"Folder ID saved but auto-setup failed: {result.get('error', 'Unknown error')}", "warning")
    else:
        flash("Google Drive sync settings saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/settings/leads-assistant", methods=["POST"])
@client_login_required
def client_save_leads_assistant_settings():
    db = _get_db()
    brand_id = session["client_brand_id"]

    valid_channels = {"sms", "messenger", "lead_forms", "calls"}
    valid_payment_channels = {"email", "sms"}
    valid_appointment_channels = {"email", "sms"}
    selected_channels = [c for c in request.form.getlist("sales_bot_channels") if c in valid_channels]
    selected_payment_channels = [c for c in request.form.getlist("sales_bot_payment_reminder_channels") if c in valid_payment_channels]
    selected_appointment_channels = [c for c in request.form.getlist("sales_bot_appointment_reminder_channels") if c in valid_appointment_channels]
    if not selected_payment_channels:
        selected_payment_channels = ["email"]
    if not selected_appointment_channels:
        selected_appointment_channels = ["sms"]
    quote_mode = (request.form.get("sales_bot_quote_mode") or "hybrid").strip().lower()
    if quote_mode not in {"simple", "hybrid", "structured"}:
        quote_mode = "hybrid"

    db.update_brand_number_field(brand_id, "sales_bot_enabled", 1 if request.form.get("sales_bot_enabled") else 0)
    db.update_brand_text_field(brand_id, "sales_bot_channels", json.dumps(selected_channels))
    db.update_brand_text_field(brand_id, "sales_bot_quote_mode", quote_mode)
    db.update_brand_text_field(
        brand_id,
        "sales_bot_business_hours",
        (request.form.get("sales_bot_business_hours") or "").strip()[:1000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_reply_tone",
        (request.form.get("sales_bot_reply_tone") or "").strip()[:500],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_handoff_alert_phones",
        (request.form.get("sales_bot_handoff_alert_phones") or "").strip()[:500],
    )
    try:
        reply_delay_seconds = max(0, min(300, float(request.form.get("sales_bot_reply_delay_seconds") or 0)))
    except (ValueError, TypeError):
        reply_delay_seconds = 0
    db.update_brand_number_field(brand_id, "sales_bot_reply_delay_seconds", reply_delay_seconds)
    db.update_brand_number_field(brand_id, "sales_bot_payment_reminders_enabled", 1 if request.form.get("sales_bot_payment_reminders_enabled") else 0)
    try:
        payment_days_before = max(0, min(21, int(float(request.form.get("sales_bot_payment_reminder_days_before") or 3))))
    except (ValueError, TypeError):
        payment_days_before = 3
    db.update_brand_number_field(brand_id, "sales_bot_payment_reminder_days_before", payment_days_before)
    try:
        payment_billing_day = max(1, min(31, int(float(request.form.get("sales_bot_payment_reminder_billing_day") or 1))))
    except (ValueError, TypeError):
        payment_billing_day = 1
    db.update_brand_number_field(brand_id, "sales_bot_payment_reminder_billing_day", payment_billing_day)
    db.update_brand_text_field(
        brand_id,
        "sales_bot_payment_reminder_channels",
        json.dumps(selected_payment_channels),
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_payment_reminder_template",
        (request.form.get("sales_bot_payment_reminder_template") or "").strip()[:2000],
    )
    db.update_brand_number_field(brand_id, "sales_bot_appointment_reminders_enabled", 1 if request.form.get("sales_bot_appointment_reminders_enabled") else 0)
    db.update_brand_text_field(
        brand_id,
        "sales_bot_appointment_reminder_send_time",
        (request.form.get("sales_bot_appointment_reminder_send_time") or "17:00").strip()[:10],
    )
    appointment_timezone = (request.form.get("sales_bot_appointment_reminder_timezone") or "America/New_York").strip()
    if appointment_timezone not in {"America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu"}:
        appointment_timezone = "America/New_York"
    db.update_brand_text_field(brand_id, "sales_bot_appointment_reminder_timezone", appointment_timezone)
    db.update_brand_text_field(
        brand_id,
        "sales_bot_appointment_reminder_channels",
        json.dumps(selected_appointment_channels),
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_appointment_reminder_template",
        (request.form.get("sales_bot_appointment_reminder_template") or "").strip()[:2000],
    )
    db.update_brand_number_field(
        brand_id,
        "sales_bot_appointment_reminder_respect_client_channel",
        1 if request.form.get("sales_bot_appointment_reminder_respect_client_channel") else 0,
    )
    db.update_brand_number_field(brand_id, "sales_bot_transcript_export", 1 if request.form.get("sales_bot_transcript_export") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_meta_lead_forms", 1 if request.form.get("sales_bot_meta_lead_forms") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_messenger_enabled", 1 if request.form.get("sales_bot_messenger_enabled") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_call_logging", 1 if request.form.get("sales_bot_call_logging") else 0)
    db.update_brand_number_field(brand_id, "sales_bot_auto_push_crm", 1 if request.form.get("sales_bot_auto_push_crm") else 0)

    quo_api_key = (request.form.get("quo_api_key") or "").strip()
    if quo_api_key:
        db.update_brand_text_field(brand_id, "quo_api_key", quo_api_key[:500])
    db.update_brand_text_field(
        brand_id,
        "quo_phone_number",
        (request.form.get("quo_phone_number") or "").strip()[:100],
    )

    quo_secret = (request.form.get("sales_bot_quo_webhook_secret") or "").strip()
    if quo_secret:
        db.update_brand_text_field(brand_id, "sales_bot_quo_webhook_secret", quo_secret[:255])

    incoming_secret = (request.form.get("sales_bot_incoming_webhook_secret") or "").strip()
    if incoming_secret:
        db.update_brand_text_field(brand_id, "sales_bot_incoming_webhook_secret", incoming_secret[:255])

    # ── Nurture cadence ──
    db.update_brand_number_field(brand_id, "sales_bot_nurture_enabled", 1 if request.form.get("sales_bot_nurture_enabled") else 0)

    for tier in ("hot", "warm", "cold"):
        hours_key = f"sales_bot_nurture_{tier}_hours"
        max_key = f"sales_bot_nurture_{tier}_max"
        try:
            hours_val = max(0.5, min(720, float(request.form.get(hours_key) or 0)))
        except (ValueError, TypeError):
            hours_val = {"hot": 2, "warm": 24, "cold": 48}[tier]
        try:
            max_val = max(1, min(10, int(request.form.get(max_key) or 0)))
        except (ValueError, TypeError):
            max_val = {"hot": 3, "warm": 2, "cold": 2}[tier]
        db.update_brand_number_field(brand_id, hours_key, hours_val)
        db.update_brand_number_field(brand_id, max_key, max_val)

    try:
        ghost_hours = max(24, min(720, float(request.form.get("sales_bot_nurture_ghost_hours") or 72)))
    except (ValueError, TypeError):
        ghost_hours = 72
    db.update_brand_number_field(brand_id, "sales_bot_nurture_ghost_hours", ghost_hours)

    # ── DND ──
    db.update_brand_number_field(brand_id, "sales_bot_dnd_enabled", 1 if request.form.get("sales_bot_dnd_enabled") else 0)
    db.update_brand_text_field(brand_id, "sales_bot_dnd_start", (request.form.get("sales_bot_dnd_start") or "21:00").strip()[:5])
    db.update_brand_text_field(brand_id, "sales_bot_dnd_end", (request.form.get("sales_bot_dnd_end") or "08:00").strip()[:5])
    db.update_brand_number_field(brand_id, "sales_bot_dnd_weekends", 1 if request.form.get("sales_bot_dnd_weekends") else 0)
    valid_tz = {"America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu"}
    tz = (request.form.get("sales_bot_dnd_timezone") or "America/New_York").strip()
    if tz not in valid_tz:
        tz = "America/New_York"
    db.update_brand_text_field(brand_id, "sales_bot_dnd_timezone", tz)

    # ── A2P / SMS Compliance ──
    db.update_brand_text_field(
        brand_id, "sales_bot_sms_opt_out_footer",
        (request.form.get("sales_bot_sms_opt_out_footer") or "").strip()[:200],
    )

    flash("Lead assistant settings saved.", "success")
    return redirect(url_for("client.client_settings"))


# ── Warren Connection Test Endpoints ──


@client_bp.route("/api/warren/test-openphone", methods=["POST"])
@client_login_required
def client_test_openphone_connection():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    api_key = (brand.get("quo_api_key") or "").strip() if brand else ""
    if not api_key:
        return jsonify({"ok": False, "error": "No OpenPhone / Quo API key configured."}), 400

    from webapp.quo_sms import get_phone_numbers

    numbers, err = get_phone_numbers(api_key)
    if err:
        return jsonify({"ok": False, "error": err, "phone_numbers": []})

    phone_numbers = []
    for item in numbers or []:
        if isinstance(item, dict):
            number = (
                item.get("phoneNumber")
                or item.get("number")
                or item.get("formattedPhoneNumber")
                or item.get("e164")
                or ""
            )
        else:
            number = str(item or "")
        number = _normalize_client_phone_number(number)
        if number and number not in phone_numbers:
            phone_numbers.append(number)

    return jsonify({"ok": True, "phone_numbers": phone_numbers, "count": len(phone_numbers)})


@client_bp.route("/api/warren/send-test-sms", methods=["POST"])
@client_login_required
def client_send_test_sms():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"ok": False, "error": "Brand not found."}), 404

    api_key = (brand.get("quo_api_key") or "").strip()
    from_number = _normalize_client_phone_number(brand.get("quo_phone_number") or "")
    payload = request.get_json(silent=True) or {}
    to_phone = _normalize_client_phone_number(payload.get("to_phone") or "")

    if not api_key:
        return jsonify({"ok": False, "error": "No OpenPhone / Quo API key configured."}), 400
    if not from_number:
        return jsonify({"ok": False, "error": "No OpenPhone / Quo sending number configured."}), 400
    if not to_phone:
        return jsonify({"ok": False, "error": "Enter a valid destination phone number."}), 400

    from webapp.quo_sms import send_test_sms

    result = send_test_sms(api_key, from_number, to_phone)
    status_code = 200 if result.get("ok") else 502
    return jsonify(result), status_code


@client_bp.route("/api/warren/run-appointment-reminders", methods=["POST"])
@client_login_required
def client_run_appointment_reminders_now():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"ok": False, "error": "Brand not found."}), 404

    from webapp.warren_appointments import process_appointment_reminders

    stats = process_appointment_reminders(
        db,
        current_app.config,
        brand_ids=[brand_id],
        ignore_send_time=True,
        include_disabled=True,
    )
    latest_runs = db.get_appointment_reminder_runs(brand_id, limit=1)
    latest_run = latest_runs[0] if latest_runs else None

    if latest_run and (latest_run.get("status") or "").strip().lower() == "config_error":
        return jsonify({
            "ok": False,
            "error": latest_run.get("reason") or "Appointment reminders are not fully configured.",
            "run": latest_run,
            "stats": stats,
        }), 400

    message = "Appointment reminder check finished."
    if latest_run and latest_run.get("reason"):
        message = latest_run["reason"]
    return jsonify({
        "ok": True,
        "message": message,
        "run": latest_run,
        "stats": stats,
    })


@client_bp.route("/api/drive/diagnose")
@client_login_required
def client_drive_diagnose():
    """Diagnostic endpoint: test every step of Drive access and report results."""
    import requests as _req
    from webapp.google_drive import (
        get_valid_access_token, _extract_folder_id, _drive_headers,
        _find_subfolder, DRIVE_API
    )
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    steps = []

    # Step 1: folder ID
    raw_folder = brand.get("google_drive_folder_id") or ""
    folder_id = _extract_folder_id(raw_folder)
    steps.append({"step": "folder_id", "raw": raw_folder[:60], "extracted": folder_id[:40] if folder_id else "EMPTY"})
    if not folder_id:
        return jsonify({"steps": steps, "error": "No folder ID"})

    # Step 2: connection + scopes
    conns = db.get_brand_connections(brand_id)
    google = conns.get("google", {})
    scopes = google.get("scopes") or ""
    has_token = bool(google.get("access_token"))
    steps.append({"step": "connection", "has_token": has_token, "scopes": scopes[:200]})

    # Step 3: get valid token
    token = get_valid_access_token(db, brand_id)
    steps.append({"step": "token_refresh", "ok": bool(token)})
    if not token:
        return jsonify({"steps": steps, "error": "Cannot get valid token"})

    # Step 4: list ALL items in root folder (files + folders)
    q = f"'{folder_id}' in parents and trashed = false"
    resp = _req.get(f"{DRIVE_API}/files", params={
        "q": q,
        "fields": "files(id,name,mimeType,size)",
        "pageSize": 30,
    }, headers=_drive_headers(token), timeout=15)
    steps.append({
        "step": "list_root",
        "status": resp.status_code,
        "items": resp.json().get("files", []) if resp.status_code == 200 else [],
        "error_body": resp.text[:300] if resp.status_code != 200 else None,
    })

    # Step 5: try to find "Creatives" subfolder
    creatives_id = _find_subfolder(token, folder_id, "Creatives")
    steps.append({"step": "find_creatives_subfolder", "found_id": creatives_id or "NOT FOUND"})

    # Step 6: check folder metadata (verify it's accessible)
    meta_resp = _req.get(f"{DRIVE_API}/files/{folder_id}",
                         params={"fields": "id,name,mimeType,ownedByMe,capabilities"},
                         headers=_drive_headers(token), timeout=15)
    if meta_resp.status_code == 200:
        meta = meta_resp.json()
        steps.append({"step": "folder_meta", "name": meta.get("name"), "mimeType": meta.get("mimeType"),
                       "ownedByMe": meta.get("ownedByMe"), "capabilities": meta.get("capabilities", {})})
    else:
        steps.append({"step": "folder_meta", "status": meta_resp.status_code, "error": meta_resp.text[:300]})

    return jsonify({"steps": steps})


@client_bp.route("/api/drive/all-images")
@client_login_required
def client_drive_all_images():
    """API: list image files from root folder and ALL subfolders."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    from webapp.google_drive import list_all_images
    files = list_all_images(db, brand_id)
    return jsonify({"files": files})


@client_bp.route("/api/drive/browse")
@client_login_required
def client_drive_browse():
    """API: browse a Drive folder - returns subfolders and image files."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    folder_id = request.args.get("folder_id") or None
    # Validate folder_id format (alphanumeric + dashes/underscores only)
    if folder_id and not all(c.isalnum() or c in "-_" for c in folder_id):
        return jsonify({"error": "Invalid folder ID"}), 400
    from webapp.google_drive import browse_folder
    result = browse_folder(db, brand_id, folder_id)
    # Include granted scopes for debugging
    conns = db.get_brand_connections(brand_id)
    result["scopes"] = (conns.get("google", {}).get("scopes") or "")[-80:]
    return jsonify(result)


@client_bp.route("/api/drive/files/<subfolder>")
@client_login_required
def client_drive_list_files(subfolder):
    """API: list files in a Drive subfolder (or root folder with 'Root')."""
    allowed = {"Creatives", "Ads", "Images", "Reports", "Root"}
    if subfolder not in allowed:
        return jsonify({"error": "Invalid subfolder"}), 400
    db = _get_db()
    brand_id = session["client_brand_id"]

    # Pre-flight checks with diagnostics
    from webapp.google_drive import list_files, get_valid_access_token, _extract_folder_id
    brand = db.get_brand(brand_id)
    folder_id = _extract_folder_id(brand.get("google_drive_folder_id") or "")
    if not folder_id:
        return jsonify({"files": [], "debug": "No Drive folder ID configured. Go to Settings and enter your Google Drive folder URL."})

    conns = db.get_brand_connections(brand_id)
    google = conns.get("google", {})
    scopes = google.get("scopes") or ""
    if not google.get("access_token"):
        return jsonify({"files": [], "debug": "No Google access token. Reconnect Google in Settings."})
    if "drive" not in scopes.lower():
        return jsonify({"files": [], "debug": f"Missing Drive scope. Current scopes: {scopes[:120]}. Reconnect Google With Drive Access in Settings."})

    token = get_valid_access_token(db, brand_id)
    if not token:
        return jsonify({"files": [], "debug": "Could not refresh Google access token. Try reconnecting Google."})

    files = list_files(db, brand_id, None if subfolder == "Root" else subfolder)
    return jsonify({"files": files, "debug": f"OK. folder_id={folder_id[:20]}, subfolder={subfolder}, found={len(files)} files"})


@client_bp.route("/api/drive/upload", methods=["POST"])
@client_login_required
def client_drive_upload():
    """API: upload a file to a Drive subfolder."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    subfolder = request.form.get("subfolder", "Images")
    allowed = {"Creatives", "Ads", "Images", "Reports"}
    if subfolder not in allowed:
        return jsonify({"error": "Invalid subfolder"}), 400

    # Pre-flight checks
    from webapp.google_drive import get_valid_access_token, _extract_folder_id
    brand = db.get_brand(brand_id)
    folder_id = _extract_folder_id(brand.get("google_drive_folder_id") or "")
    if not folder_id:
        return jsonify({"error": "No Drive folder configured. Go to Settings and enter your Google Drive folder URL."}), 400

    conns = db.get_brand_connections(brand_id)
    google = conns.get("google", {})
    scopes = google.get("scopes") or ""
    if "drive" not in scopes.lower():
        return jsonify({"error": f"Missing Drive scope. Reconnect Google With Drive Access in Settings. Current scopes: {scopes[:80]}"}), 403

    token = get_valid_access_token(db, brand_id)
    if not token:
        return jsonify({"error": "Could not get a valid Google token. Try reconnecting Google."}), 401

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file provided"}), 400

    # Basic safety: limit upload size (10MB)
    f.seek(0, 2)
    if f.tell() > 10 * 1024 * 1024:
        return jsonify({"error": "File too large (max 10MB)"}), 400
    f.seek(0)

    from webapp.google_drive import upload_file as drive_upload
    result = drive_upload(db, brand_id, subfolder, f.filename, f.read(), f.content_type or "application/octet-stream")
    if result:
        file_payload = dict(result)
        file_id = str(file_payload.get("id") or "").strip()
        if file_id:
            file_payload["download_url"] = url_for("client.client_drive_download", file_id=file_id)
            file_payload["thumbnail_url"] = url_for("client.client_drive_thumbnail", file_id=file_id)
        return jsonify({"ok": True, "file": file_payload})
    return jsonify({"error": "Upload failed. The Google token may lack permission for this folder. Try reconnecting Google With Drive Access."}), 500


@client_bp.route("/api/drive/download/<file_id>")
@client_login_required
def client_drive_download(file_id):
    """Proxy: download a file from Drive by ID. Returns the raw image bytes."""
    if not file_id or not all(c.isalnum() or c in "-_" for c in file_id):
        abort(400)
    db = _get_db()
    brand_id = session["client_brand_id"]
    from webapp.google_drive import download_file
    data, mime = download_file(db, brand_id, file_id)
    if data is None:
        abort(404)
    from flask import make_response
    resp = make_response(data)
    resp.headers["Content-Type"] = mime
    resp.headers["Cache-Control"] = "private, max-age=3600"
    return resp


@client_bp.route("/api/drive/thumbnail/<file_id>")
@client_login_required
def client_drive_thumbnail(file_id):
    """Return a Drive thumbnail URL as a redirect (or proxy small images)."""
    if not file_id or not all(c.isalnum() or c in "-_" for c in file_id):
        abort(400)
    db = _get_db()
    brand_id = session["client_brand_id"]
    from webapp.google_drive import get_valid_access_token
    import requests as _req
    token = get_valid_access_token(db, brand_id)
    if not token:
        abort(401)
    # Use Drive API to get thumbnailLink
    resp = _req.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}",
        params={"fields": "thumbnailLink,webContentLink"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if resp.status_code != 200:
        abort(404)
    info = resp.json()
    thumb = info.get("thumbnailLink") or ""
    if thumb:
        return redirect(thumb)
    # Fallback: serve the file directly
    return redirect(url_for("client.client_drive_download", file_id=file_id))


@client_bp.route("/settings/maps-api", methods=["POST"])
@client_login_required
def client_save_maps_api():
    db = _get_db()
    brand_id = session["client_brand_id"]
    api_key = (request.form.get("google_maps_api_key") or "").strip()[:200]
    if api_key:
        db.update_brand_text_field(brand_id, "google_maps_api_key", api_key)
    flash("Google Maps API key saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/settings/search-place", methods=["POST"])
@client_login_required
def client_search_place():
    """Search for a business via Places API so the user can pick their Place ID."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    api_key = (brand.get("google_maps_api_key") or "").strip()
    if not api_key:
        return jsonify(ok=False, error="Save your Google Maps API key first."), 400

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify(ok=False, error="Enter a business name to search."), 400

    import requests as _req

    # ── Detect if user pasted a Place ID directly (starts with "ChIJ") ──
    if query.startswith("ChIJ") and " " not in query:
        from webapp.heatmap import verify_place_id
        result = verify_place_id(api_key, query)
        if result and not result.get("error"):
            return jsonify(ok=True, results=[{
                "place_id": query,
                "name": result.get("name", "Unknown"),
                "address": result.get("address", ""),
            }])
        err = result.get("error", "Unknown error") if result else "Lookup failed"
        msg = result.get("message", "") if result else ""
        return jsonify(ok=False, error=f"Place ID lookup failed: {err}. {msg}".strip())

    lat = float(brand.get("business_lat") or 0)
    lng = float(brand.get("business_lng") or 0)
    api_errors = []

    # Try Places API (New) first
    url_new = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.displayName,places.id,places.formattedAddress",
        "Content-Type": "application/json",
    }
    body = {"textQuery": query, "maxResultCount": 5}
    if lat != 0 or lng != 0:
        body["locationBias"] = {
            "circle": {"center": {"latitude": lat, "longitude": lng}, "radius": 50000.0}
        }

    places = []
    try:
        resp = _req.post(url_new, json=body, headers=headers, timeout=15)
        if resp.status_code == 200:
            raw = resp.json().get("places", [])
            for p in raw:
                places.append({
                    "place_id": p.get("id", ""),
                    "name": (p.get("displayName", {}).get("text", "") or ""),
                    "address": p.get("formattedAddress", ""),
                })
        else:
            api_errors.append(f"Places API (New): {resp.status_code} - {resp.text[:200]}")
    except Exception as exc:
        api_errors.append(f"Places API (New): {exc}")

    # Fallback: legacy Places Text Search
    if not places:
        url_leg = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {"query": query, "key": api_key}
        if lat != 0 or lng != 0:
            params["location"] = f"{lat},{lng}"
            params["radius"] = 50000
        try:
            resp = _req.get(url_leg, params=params, timeout=15)
            data = resp.json()
            status = data.get("status", "")
            if status == "OK":
                for r in data.get("results", [])[:5]:
                    places.append({
                        "place_id": r.get("place_id", ""),
                        "name": r.get("name", ""),
                        "address": r.get("formatted_address", ""),
                    })
            else:
                err_msg = data.get("error_message", status)
                api_errors.append(f"Places API (Legacy): {err_msg}")
        except Exception as exc:
            api_errors.append(f"Places API (Legacy): {exc}")

    # Fallback: Find Place from text (different endpoint, sometimes enabled separately)
    if not places:
        url_find = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        params = {
            "input": query,
            "inputtype": "textquery",
            "fields": "place_id,name,formatted_address",
            "key": api_key,
        }
        if lat != 0 or lng != 0:
            params["locationbias"] = f"circle:50000@{lat},{lng}"
        try:
            resp = _req.get(url_find, params=params, timeout=15)
            data = resp.json()
            if data.get("status") == "OK":
                for c in data.get("candidates", [])[:5]:
                    places.append({
                        "place_id": c.get("place_id", ""),
                        "name": c.get("name", ""),
                        "address": c.get("formatted_address", ""),
                    })
            else:
                api_errors.append(f"Find Place: {data.get('error_message', data.get('status'))}")
        except Exception as exc:
            api_errors.append(f"Find Place: {exc}")

    if not places and api_errors:
        return jsonify(ok=False, error="No results. API errors: " + " | ".join(api_errors))

    if not places:
        return jsonify(ok=False, error="No results found. This usually means your API key has HTTP referrer restrictions that block server-side requests. In Google Cloud Console, edit your API key and either remove restrictions or add your server's IP to the allowed list.")

    return jsonify(ok=True, results=places)


@client_bp.route("/settings/save-place-id", methods=["POST"])
@client_login_required
def client_save_place_id():
    """Save the selected Google Place ID."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    data = request.get_json(silent=True) or {}
    place_id = (data.get("place_id") or "").strip()[:200]
    if not place_id:
        return jsonify(ok=False, error="No Place ID provided."), 400
    db.update_brand_text_field(brand_id, "google_place_id", place_id)
    return jsonify(ok=True)


# ── Context processor ──

@client_bp.context_processor
def inject_client_globals():
    assistant_month = _assistant_month()
    assistant_enabled = False
    assistant_messages = []
    assistant_model_chat = "gpt-4o-mini"

    brand_id = session.get("client_brand_id")
    if brand_id:
        try:
            db = _get_db()
            brand = db.get_brand(brand_id) or {}
            assistant_model_chat = _pick_ai_model(brand, "chat")
            assistant_enabled = bool(_get_openai_api_key(brand))
            rows = db.get_ai_chat_messages(brand_id, assistant_month, limit=30)
            assistant_messages = [{"role": r.get("role"), "content": r.get("content", "")} for r in rows if r.get("content")]
        except Exception:
            assistant_messages = []

    return {
        "client_user": session.get("client_name"),
        "client_brand": session.get("client_brand_name"),
        "now": datetime.now(),
        "assistant_enabled": assistant_enabled,
        "assistant_messages": assistant_messages,
        "assistant_month": assistant_month,
        "assistant_model_chat": assistant_model_chat,
        "assistant_models": [m for m in ALLOWED_AI_MODELS if m],
    }


# ── Google Business Profile ──

@client_bp.route("/google-business-profile")
@client_login_required
def client_gbp():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    gbp = None
    guidance = {}
    try:
        from webapp.google_business import build_gbp_context, VERIFICATION_GUIDANCE
        gbp = build_gbp_context(db, brand_id)
        guidance = VERIFICATION_GUIDANCE
    except Exception:
        pass

    return render_template(
        "client_gbp.html",
        brand=brand,
        gbp=gbp,
        guidance=guidance,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/google-business-profile/audit")
@client_login_required
def client_gbp_audit():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    gbp = None
    audit = None
    ai_tips = None
    verification_issues = []

    try:
        from webapp.google_business import (
            build_gbp_context, run_gbp_audit, run_ai_audit,
            VERIFICATION_ISSUES,
        )
        gbp = build_gbp_context(db, brand_id)
        verification_issues = VERIFICATION_ISSUES

        if gbp and not gbp.get("error"):
            audit = run_gbp_audit(gbp)

            # AI-powered recommendations
            api_key = _get_openai_api_key(brand)
            model = _pick_ai_model(brand, "analysis")
            if api_key:
                ai_tips = run_ai_audit(gbp, audit, brand, api_key, model)
    except Exception:
        current_app.logger.exception("GBP audit error")

    if audit:
        _log_agent("radar", "Completed GBP audit", brand.get("display_name", ""))

    return render_template(
        "client_gbp_audit.html",
        brand=brand,
        gbp=gbp,
        audit=audit,
        ai_tips=ai_tips,
        verification_issues=verification_issues,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


# ── Local Rank Heatmap ──

@client_bp.route("/heatmap")
@client_login_required
def client_heatmap():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand, scans, active_scan = _load_heatmap_state(db, brand_id)
    if not brand:
        abort(404)
    return render_template(
        "client_heatmap.html",
        brand=brand,
        scans=scans,
        active_scan=active_scan,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/heatmap/scan", methods=["POST"])
@client_login_required
def client_heatmap_scan():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    data = request.get_json(silent=True) or {}
    keyword = (data.get("keyword") or "").strip()
    radius = float(data.get("radius_miles") or 5)
    grid_size = int(data.get("grid_size") or 6)
    if grid_size < 3:
        grid_size = 3
    if grid_size > 10:
        grid_size = 10

    if not keyword:
        return jsonify(ok=False, error="Keyword is required"), 400

    api_key = (brand.get("google_maps_api_key") or "").strip()
    if not api_key:
        return jsonify(ok=False, error="Google Maps API key not configured. Add it in Connections."), 400

    business_lat = float(brand.get("business_lat") or 0)
    business_lng = float(brand.get("business_lng") or 0)
    if business_lat == 0 and business_lng == 0:
        return jsonify(ok=False, error="Business location not set. Set your address on the heatmap page first."), 400

    center_lat_raw = data.get("center_lat")
    center_lng_raw = data.get("center_lng")
    try:
        scan_center_lat = float(center_lat_raw) if center_lat_raw is not None else business_lat
        scan_center_lng = float(center_lng_raw) if center_lng_raw is not None else business_lng
    except (TypeError, ValueError):
        return jsonify(ok=False, error="Scan center coordinates are invalid."), 400

    if not (-90 <= scan_center_lat <= 90) or not (-180 <= scan_center_lng <= 180):
        return jsonify(ok=False, error="Scan center coordinates are out of range."), 400

    from webapp.heatmap import generate_grid, scan_grid, calc_search_radius_m, verify_place_id, clean_keyword
    grid_points = generate_grid(scan_center_lat, scan_center_lng, radius, grid_size)
    search_radius = calc_search_radius_m(radius, grid_size)
    business_name = brand.get("display_name", "")
    place_id = brand.get("google_place_id") or None

    # Strip "near me" / "nearby" etc - the API already gets lat/lng + radius
    keyword, keyword_was_cleaned = clean_keyword(keyword)

    # Verify the Place ID resolves correctly
    place_verification = None
    if place_id:
        place_verification = verify_place_id(api_key, place_id)

    alternate_names = []
    verified_name = ((place_verification or {}).get("name") or "").strip()
    if verified_name and verified_name.lower() != business_name.lower().strip():
        alternate_names.append(verified_name)
    if not business_name and verified_name:
        business_name = verified_name

    listing_names = [business_name, *alternate_names]
    keyword_brand_query = False
    kw_lower = keyword.lower().strip()
    for listing_name in listing_names:
        normalized_listing_name = (listing_name or "").lower().strip()
        if normalized_listing_name and (kw_lower == normalized_listing_name or kw_lower in normalized_listing_name or normalized_listing_name in kw_lower):
            keyword_brand_query = True
            break

    try:
        results, debug_info = scan_grid(api_key, keyword, business_name, grid_points,
                                        place_id=place_id, search_radius_m=search_radius,
                                        alternate_names=alternate_names,
                                        brand_query=keyword_brand_query)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return jsonify(ok=False, error=f"Scan failed: {exc}"), 500

    if debug_info and place_verification:
        debug_info["place_id_verification"] = place_verification
        # Check if Place ID location matches stored business location
        pv_lat = place_verification.get("lat")
        pv_lng = place_verification.get("lng")
        if pv_lat is not None and pv_lng is not None:
            import math as _math
            dlat = abs(pv_lat - business_lat)
            dlng = abs(pv_lng - business_lng)
            dist_km = _math.sqrt(dlat**2 + dlng**2) * 111.32
            place_verification["distance_from_center_km"] = round(dist_km, 1)
            if dist_km > 50:
                place_verification["location_warning"] = (
                    f"Place ID location is {round(dist_km)}km from your stored business location. "
                    "This may be the wrong listing. Try re-searching your Place ID."
                )

    # Detect if keyword looks like the business name (common user mistake)
    keyword_warning = None
    bn_lower = business_name.lower().strip()
    if bn_lower and (kw_lower == bn_lower or kw_lower in bn_lower or bn_lower in kw_lower):
        keyword_warning = (
            "You searched your business name. The heatmap is designed for "
            "service keywords, the terms customers use to find businesses like yours. "
            "Try keywords like \"pooper scooper\", \"dog poop cleanup\", etc. "
            "That shows where you rank vs. competitors when people search for your service."
        )
    if debug_info and keyword_warning:
        debug_info["keyword_warning"] = keyword_warning
    # Warn (but still scan) if we stripped "near me" from the keyword
    if keyword_was_cleaned and debug_info:
        near_me_note = (
            'Stripped "near me" from your keyword (the API already receives your '
            'exact coordinates and search radius, so "near me" is redundant and '
            'can reduce result count).'
        )
        if debug_info.get("keyword_warning"):
            debug_info["keyword_warning"] += " " + near_me_note
        else:
            debug_info["keyword_warning"] = near_me_note

    from webapp.heatmap import summarize_competitor_landscape

    ranked = [r for r in results if r["rank"] > 0]
    avg_rank = round(sum(r["rank"] for r in ranked) / len(ranked), 1) if ranked else 0
    competitor_summary = summarize_competitor_landscape(results)

    import json as _json
    scan_id = db.save_heatmap_scan(brand_id, keyword, grid_size, radius, scan_center_lat, scan_center_lng,
                                   _json.dumps(results), avg_rank)

    return jsonify(ok=True, results=results, avg_rank=avg_rank,
                   found=len(ranked), total=len(results),
                   competitor_summary=competitor_summary,
                   scan_id=scan_id,
                   center_lat=scan_center_lat, center_lng=scan_center_lng,
                   debug=debug_info)


@client_bp.route("/heatmap/test-api", methods=["POST"])
@client_login_required
def client_heatmap_test_api():
    """Quick API key validation: tries a simple Places API call from the server."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    api_key = (brand.get("google_maps_api_key") or "").strip()
    if not api_key:
        return jsonify(ok=False, error="No API key configured."), 400

    import requests as _req
    checks = {}

    # Test 1: Places API (New) Text Search
    try:
        resp = _req.post(
            "https://places.googleapis.com/v1/places:searchText",
            json={"textQuery": "coffee", "maxResultCount": 1},
            headers={
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": "places.displayName",
                "Content-Type": "application/json",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            checks["places_new"] = {"ok": True, "detail": "Working"}
        else:
            body = resp.text[:300]
            checks["places_new"] = {"ok": False, "detail": f"HTTP {resp.status_code}: {body}"}
    except Exception as exc:
        checks["places_new"] = {"ok": False, "detail": str(exc)}

    # Test 2: Geocoding API
    try:
        resp = _req.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": "New York", "key": api_key},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "OK":
            checks["geocoding"] = {"ok": True, "detail": "Working"}
        else:
            checks["geocoding"] = {"ok": False, "detail": data.get("error_message") or data.get("status", "Unknown")}
    except Exception as exc:
        checks["geocoding"] = {"ok": False, "detail": str(exc)}

    # Test 3: Legacy Places Text Search
    try:
        resp = _req.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": "coffee", "key": api_key},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "OK":
            checks["places_legacy"] = {"ok": True, "detail": "Working"}
        else:
            checks["places_legacy"] = {"ok": False, "detail": data.get("error_message") or data.get("status", "Unknown")}
    except Exception as exc:
        checks["places_legacy"] = {"ok": False, "detail": str(exc)}

    all_ok = all(c["ok"] for c in checks.values())
    any_places = checks.get("places_new", {}).get("ok") or checks.get("places_legacy", {}).get("ok")
    return jsonify(ok=True, all_ok=all_ok, any_places=any_places, checks=checks)


@client_bp.route("/heatmap/save-location", methods=["POST"])
@client_login_required
def client_heatmap_save_location():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    data = request.get_json(silent=True) or {}
    address = (data.get("address") or "").strip()

    if not address:
        return jsonify(ok=False, error="Address is required"), 400

    api_key = (brand.get("google_maps_api_key") or "").strip()
    if not api_key:
        return jsonify(ok=False, error="Google Maps API key not configured. Add it in Connections."), 400

    from webapp.heatmap import geocode_address
    try:
        result = geocode_address(api_key, address)
    except Exception as exc:
        return jsonify(ok=False, error="Geocoding API error: " + str(exc)), 500
    if not result:
        return jsonify(ok=False, error="Could not geocode that address. Check spelling and try again."), 400

    db.update_brand_number_field(brand_id, "business_lat", result["lat"])
    db.update_brand_number_field(brand_id, "business_lng", result["lng"])

    return jsonify(ok=True, lat=result["lat"], lng=result["lng"],
                   formatted=result["formatted"])


@client_bp.route("/heatmap/scan/<int:scan_id>")
@client_login_required
def client_heatmap_view_scan(scan_id):
    db = _get_db()
    scan = db.get_heatmap_scan(scan_id)
    if not scan or scan["brand_id"] != session["client_brand_id"]:
        return jsonify(ok=False, error="Scan not found"), 404
    return jsonify(ok=True, scan=_serialize_heatmap_scan(scan, include_results=True))


@client_bp.route("/heatmap/scan/<int:scan_id>", methods=["DELETE"])
@client_login_required
def client_heatmap_delete_scan(scan_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    scan = db.get_heatmap_scan(scan_id)
    if not scan or scan["brand_id"] != brand_id:
        return jsonify(ok=False, error="Scan not found"), 404
    db.delete_heatmap_scan(scan_id, brand_id)
    return jsonify(ok=True)


@client_bp.route("/heatmap/scans", methods=["DELETE"])
@client_login_required
def client_heatmap_clear_scans():
    db = _get_db()
    brand_id = session["client_brand_id"]
    db.delete_all_heatmap_scans(brand_id)
    return jsonify(ok=True)


# ── Blog ──

def _wp_connected(brand):
    return bool(
        (brand.get("wp_site_url") or "").strip()
        and (brand.get("wp_username") or "").strip()
        and (brand.get("wp_app_password") or "").strip()
    )


def _wp_auth_headers(brand, user_agent):
    import base64

    wp_user = (brand.get("wp_username") or "").strip()
    wp_pass = (brand.get("wp_app_password") or "").strip()
    token = base64.b64encode(f"{wp_user}:{wp_pass}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "X-GM-Auth": f"Basic {token}",
        "User-Agent": user_agent,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _delete_wp_page(brand, wp_page_id, force=True):
    """Delete a published site-builder page from WordPress."""
    import requests as req_lib

    try:
        page_id = int(wp_page_id or 0)
    except (TypeError, ValueError):
        page_id = 0
    if not page_id:
        return {"ok": True, "deleted": False}

    wp_url = (brand.get("wp_site_url") or "").strip().rstrip("/")
    api_url = f"{wp_url}/wp-json/wp/v2/pages/{page_id}"
    headers = _wp_auth_headers(brand, "GroMore/1.0 (WordPress Site Builder Delete; +https://gromore.com)")
    params = {"force": "true"} if force else None

    try:
        resp = req_lib.delete(api_url, headers=headers, params=params, timeout=30)
        if resp.status_code in (200, 202, 204):
            return {"ok": True, "deleted": True}
        if resp.status_code in (404, 410):
            return {"ok": True, "deleted": False}
        return {"ok": False, "error": f"WordPress API error {resp.status_code}: {resp.text[:200]}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:200]}


def _publish_to_wp(brand, title, content, excerpt="", slug="",
                    seo_title="", seo_description="", categories="",
                    tags="", featured_image_url="", status="publish"):
    """Publish or update a post on WordPress via REST API. Returns dict."""
    import requests as req_lib
    import base64

    wp_url = brand["wp_site_url"].strip().rstrip("/")
    wp_user = brand["wp_username"].strip()
    wp_pass = brand["wp_app_password"].strip()

    api_url = f"{wp_url}/wp-json/wp/v2/posts"
    token = base64.b64encode(f"{wp_user}:{wp_pass}".encode()).decode()
    headers = {
        "Authorization": f"Basic {token}",
        "X-GM-Auth": f"Basic {token}",
        "User-Agent": "GroMore/1.0 (WordPress Blog Publisher; +https://gromore.com)",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    post_data = {
        "title": seo_title or title,
        "content": content,
        "excerpt": excerpt,
        "status": status,
    }
    if slug:
        post_data["slug"] = slug
    # Yoast SEO fields (if plugin installed)
    meta = {}
    if seo_title:
        meta["_yoast_wpseo_title"] = seo_title
    if seo_description:
        meta["_yoast_wpseo_metadesc"] = seo_description
    if meta:
        post_data["meta"] = meta

    def _describe_wp_error(status_code, response_text):
        body = (response_text or "")[:500]
        body_lower = body.lower()
        if "sgcaptcha" in body_lower or "/.well-known/sgcaptcha" in body_lower or (status_code == 202 and "captcha" in body_lower):
            return (
                "Publish failed: SiteGround's server-level bot protection returned a CAPTCHA challenge (HTTP 202) "
                "instead of allowing the REST API request through. This is not a WordPress plugin - it's a "
                "SiteGround hosting setting. Fix: Go to SiteGround Site Tools > Security > Bot Protection and "
                "either lower the protection level or whitelist the GroMore server. Alternatively, go to "
                "Security > Blocked IPs and make sure the server IP is not blocked."
            )
        if status_code == 202:
            return (
                "Publish failed: the site returned HTTP 202 instead of creating the post. This usually means "
                "a server-level security layer (WAF, bot protection, or firewall) intercepted the request before "
                "WordPress could process it. On SiteGround: go to Site Tools > Security > Bot Protection and "
                "lower the protection level, or whitelist the GroMore server IP."
            )
        if status_code == 401:
            return "Publish failed: WordPress returned 401. The application password may be expired or the user lacks permission to create posts. Re-enter your app password in Settings, or check that the WordPress user has an Editor/Administrator role."
        if status_code == 403:
            return "Publish failed: WordPress returned 403 Forbidden. A security plugin may be blocking REST API access."
        return f"WordPress API error {status_code}: {body[:200]}"

    try:
        import logging
        _wp_log = logging.getLogger(__name__)
        resp = req_lib.post(
            api_url,
            json=post_data,
            headers=headers,
            timeout=30,
        )
        if resp.status_code not in (200, 201):
            resp_headers = getattr(resp, "headers", {}) or {}
            if not isinstance(resp_headers, dict):
                resp_headers = {}
            _wp_log.warning("[WP-PUBLISH] status=%d headers=%s body=%s",
                            resp.status_code, dict(resp_headers), resp.text[:500])
        if resp.status_code in (200, 201):
            wp_post = resp.json()
            return {
                "ok": True,
                "wp_post_id": wp_post.get("id", 0),
                "wp_post_url": wp_post.get("link", ""),
            }
        else:
            return {"ok": False, "error": _describe_wp_error(resp.status_code, resp.text)}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@client_bp.route("/blog")
@client_login_required
def client_blog():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    wp_ok = _wp_connected(brand)
    posts = db.get_blog_posts(brand_id) if wp_ok else []

    # Check for due scheduled posts and publish them
    if wp_ok:
        due = db.get_due_blog_posts(brand_id)
        for bp in due:
            result = _publish_to_wp(
                brand, bp["title"], bp["content"],
                excerpt=bp.get("excerpt", ""),
                slug=bp.get("slug", ""),
                seo_title=bp.get("seo_title", ""),
                seo_description=bp.get("seo_description", ""),
            )
            if result["ok"]:
                db.update_blog_post(
                    bp["id"], status="published",
                    wp_post_id=result["wp_post_id"],
                    wp_post_url=result["wp_post_url"],
                    published_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                # Auto-create Facebook promo for the just-published scheduled post
                _create_fb_promo_for_blog(
                    db, brand, brand_id, bp["title"], result["wp_post_url"],
                    excerpt=bp.get("excerpt", ""),
                    featured_image_url=bp.get("featured_image_url", ""),
                )
            else:
                db.update_blog_post(bp["id"], status="failed")
        # Refresh list after publishing
        if due:
            posts = db.get_blog_posts(brand_id)

    return render_template(
        "client/client_blog.html",
        brand=brand,
        posts=posts,
        wp_connected=wp_ok,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/blog/new")
@client_bp.route("/blog/<int:post_id>/edit")
@client_login_required
def client_blog_editor(post_id=None):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)
    if not _wp_connected(brand):
        flash("Connect WordPress in Settings first.", "warning")
        return redirect(url_for("client.client_settings"))

    post = None
    if post_id:
        post = db.get_blog_post(post_id)
        if not post or post["brand_id"] != brand_id:
            abort(404)

    return render_template(
        "client/client_blog_editor.html",
        brand=brand,
        post=post,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/blog/save", methods=["POST"])
@client_login_required
def client_blog_save():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    post_id = request.form.get("post_id", "").strip()
    title = request.form.get("title", "").strip() or "Untitled Post"
    content = request.form.get("content", "")
    excerpt = request.form.get("excerpt", "").strip()
    categories = request.form.get("categories", "").strip()
    tags = request.form.get("tags", "").strip()
    seo_title = request.form.get("seo_title", "").strip()
    seo_description = request.form.get("seo_description", "").strip()
    featured_image_url = request.form.get("featured_image_url", "").strip()
    raw_scheduled_at = request.form.get("scheduled_at", "").strip()
    scheduled_at = _normalize_scheduled_datetime(raw_scheduled_at)
    action = (request.form.get("action", "draft") or "draft").strip().lower()
    auto_facebook = request.form.get("auto_facebook") == "1"

    fields = dict(
        title=title, content=content, excerpt=excerpt,
        categories=categories, tags=tags, seo_title=seo_title,
        seo_description=seo_description, featured_image_url=featured_image_url,
    )

    if action == "publish":
        if not _wp_connected(brand):
            flash("Connect WordPress in Settings first.", "error")
            return redirect(url_for("client.client_blog"))

        result = _publish_to_wp(
            brand, title, content,
            excerpt=excerpt, slug=title.lower().replace(' ', '-')[:80],
            seo_title=seo_title, seo_description=seo_description,
            categories=categories, tags=tags,
            featured_image_url=featured_image_url,
        )
        if result["ok"]:
            fields["status"] = "published"
            fields["wp_post_id"] = result["wp_post_id"]
            fields["wp_post_url"] = result["wp_post_url"]
            fields["published_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            flash("Post published to WordPress!", "success")

            # Auto-create Facebook promo post
            if auto_facebook:
                fb_result = _create_fb_promo_for_blog(
                    db, brand, brand_id, title, result["wp_post_url"],
                    excerpt=excerpt, featured_image_url=featured_image_url,
                )
                if fb_result.get("ok"):
                    flash("Facebook promo post scheduled.", "success")
                else:
                    fb_err = fb_result.get("error", "")
                    if fb_err:
                        flash(f"Facebook post skipped: {fb_err[:80]}", "warning")
        else:
            flash(f"Publish failed: {result['error']}", "error")
            fields["status"] = "draft"
    elif action == "schedule":
        if not scheduled_at:
            flash("Pick a date and time to schedule.", "error")
            fields["status"] = "draft"
        else:
            scheduled_dt = datetime.strptime(scheduled_at, "%Y-%m-%d %H:%M:%S")
            if scheduled_dt <= datetime.now() + timedelta(minutes=1):
                flash("Scheduled publish time must be at least 1 minute in the future.", "error")
                fields["status"] = "draft"
            else:
                fields["status"] = "scheduled"
                fields["scheduled_at"] = scheduled_at
                flash("Post scheduled.", "success")

                # Auto-create Facebook promo post (scheduled 15 min after blog)
                if auto_facebook:
                    site_url = (brand.get("wp_site_url") or "").rstrip("/")
                    slug = re.sub(r'[^a-z0-9]+', '-', title.lower())[:80].strip('-')
                    estimated_url = f"{site_url}/{slug}/" if site_url else ""
                    if estimated_url:
                        fb_result = _create_fb_promo_for_blog(
                            db, brand, brand_id, title, estimated_url,
                            excerpt=excerpt, featured_image_url=featured_image_url,
                            scheduled_at=scheduled_at,
                        )
                        if fb_result.get("ok"):
                            flash("Facebook promo post scheduled 15 min after blog publish.", "success")
                        else:
                            fb_err = fb_result.get("error", "")
                            if fb_err:
                                flash(f"Facebook post skipped: {fb_err[:80]}", "warning")
    else:
        fields["status"] = "draft"
        flash("Draft saved.", "success")

    if post_id:
        db.update_blog_post(int(post_id), **fields)
        return redirect(url_for("client.client_blog_editor", post_id=int(post_id)))
    else:
        new_id = db.save_blog_post(
            brand_id, title, content, excerpt=excerpt,
            categories=categories, tags=tags,
            seo_title=seo_title, seo_description=seo_description,
            featured_image_url=featured_image_url,
            status=fields.get("status", "draft"),
            scheduled_at=fields.get("scheduled_at"),
            created_by=session.get("client_user_id", 0),
        )
        if fields.get("wp_post_id"):
            db.update_blog_post(
                new_id,
                wp_post_id=fields["wp_post_id"],
                wp_post_url=fields["wp_post_url"],
                published_at=fields.get("published_at"),
            )
        return redirect(url_for("client.client_blog_editor", post_id=new_id))


@client_bp.route("/blog/<int:post_id>/delete", methods=["POST"])
@client_login_required
def client_blog_delete(post_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    post = db.get_blog_post(post_id)
    if not post or post["brand_id"] != brand_id:
        return jsonify(ok=False, error="Post not found"), 404
    db.delete_blog_post(post_id)
    return jsonify(ok=True)


def _create_fb_promo_for_blog(db, brand, brand_id, title, wp_post_url, excerpt="",
                               featured_image_url="", scheduled_at=None):
    """Create a Facebook Page post promoting a blog post.
    If scheduled_at is provided, schedule the FB post 15 minutes after that time.
    Otherwise post 10 minutes from now."""
    page_id = brand.get("facebook_page_id", "")
    if not page_id:
        return {"ok": False, "error": "No Facebook page connected."}

    connections = db.get_brand_connections(brand_id)
    meta_conn = connections.get("meta")
    if not meta_conn or meta_conn.get("status") != "connected":
        return {"ok": False, "error": "Meta not connected."}

    from webapp.api_bridge import _get_meta_token, _get_page_access_token
    user_token = _get_meta_token(db, brand_id, meta_conn)
    if not user_token:
        return {"ok": False, "error": "Meta token expired."}
    page_token = _get_page_access_token(page_id, user_token)
    if not page_token:
        return {"ok": False, "error": "Could not get page access token."}

    # Build the promo message
    brand_name = brand.get("display_name", "")
    teaser = excerpt[:200] if excerpt else ""
    if teaser:
        message = f"{teaser}\n\nRead more on our blog:"
    else:
        message = f"New on the {brand_name} blog: {title}\n\nRead the full post:"

    # Calculate schedule time
    from datetime import timedelta, timezone
    now = datetime.now(timezone.utc)
    if scheduled_at:
        try:
            sched_dt = datetime.fromisoformat(scheduled_at).replace(tzinfo=timezone.utc)
            fb_sched = sched_dt + timedelta(minutes=15)
        except ValueError:
            fb_sched = now + timedelta(minutes=15)
    else:
        fb_sched = now + timedelta(minutes=10)

    # Ensure at least 10 min in the future (Facebook requirement)
    if fb_sched < now + timedelta(minutes=10):
        fb_sched = now + timedelta(minutes=10)

    unix_ts = int(fb_sched.timestamp())
    fb_sched_str = fb_sched.strftime("%Y-%m-%dT%H:%M:%S")

    import requests as req_lib
    fb_url = f"https://graph.facebook.com/v21.0/{page_id}/feed"
    payload = {
        "access_token": page_token,
        "message": message,
        "link": wp_post_url,
        "scheduled_publish_time": unix_ts,
        "published": "false",
    }

    try:
        resp = req_lib.post(fb_url, data=payload, timeout=30)
        resp_data = resp.json()
    except Exception as exc:
        db.save_scheduled_post(brand_id, "facebook", message, fb_sched_str,
                               link_url=wp_post_url, image_url=featured_image_url or "", post_type="blog_promo")
        return {"ok": False, "error": str(exc)[:200]}

    fb_post_id = resp_data.get("id") or resp_data.get("post_id") or ""
    post_id = db.save_scheduled_post(brand_id, "facebook", message, fb_sched_str,
                                      link_url=wp_post_url, image_url=featured_image_url or "", post_type="blog_promo")

    if resp.status_code == 200 and fb_post_id:
        db.update_scheduled_post_status(post_id, "scheduled", fb_post_id=fb_post_id)
        return {"ok": True, "fb_post_id": fb_post_id}
    else:
        error_msg = resp_data.get("error", {}).get("message", resp.text[:300])
        db.update_scheduled_post_status(post_id, "failed", error_message=error_msg)
        return {"ok": False, "error": error_msg[:200]}


@client_bp.route("/blog/import-csv", methods=["POST"])
@client_login_required
def client_blog_import_csv():
    """Import blog posts from a CSV payload (parsed client-side)."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    data = request.get_json(silent=True) or {}
    posts = data.get("posts", [])
    auto_facebook = data.get("auto_facebook", False)

    if not posts:
        return jsonify(ok=False, error="No posts provided."), 400
    if len(posts) > 200:
        return jsonify(ok=False, error="Maximum 200 posts per import."), 400

    wp_ok = _wp_connected(brand)

    ALLOWED_STATUSES = {"draft", "publish", "published", "schedule", "scheduled"}
    imported = 0
    published = 0
    scheduled = 0
    fb_posts = 0
    errors = 0
    error_details = []

    for idx, post in enumerate(posts):
        title = (post.get("title") or "").strip()
        if not title:
            errors += 1
            error_details.append(f"Row {idx + 1}: Missing title, skipped.")
            continue

        content = (post.get("content") or "").strip()
        excerpt = (post.get("excerpt") or "").strip()
        categories = (post.get("categories") or "").strip()
        tags = (post.get("tags") or "").strip()
        seo_title = (post.get("seo_title") or "").strip()
        seo_description = (post.get("seo_description") or "").strip()
        featured_image_url = (post.get("featured_image_url") or "").strip()
        raw_status = (post.get("status") or "draft").strip().lower()
        sched_at = (post.get("scheduled_at") or "").strip() or None

        if raw_status not in ALLOWED_STATUSES:
            raw_status = "draft"

        # Normalize status
        if raw_status in ("publish", "published"):
            action = "publish"
        elif raw_status in ("schedule", "scheduled"):
            action = "schedule"
        else:
            action = "draft"

        fields = dict(
            title=title, content=content, excerpt=excerpt,
            categories=categories, tags=tags, seo_title=seo_title,
            seo_description=seo_description, featured_image_url=featured_image_url,
        )

        # Try to publish to WordPress
        wp_post_url = ""
        if action == "publish" and wp_ok:
            result = _publish_to_wp(
                brand, title, content,
                excerpt=excerpt, slug=title.lower().replace(' ', '-')[:80],
                seo_title=seo_title, seo_description=seo_description,
                categories=categories, tags=tags,
                featured_image_url=featured_image_url,
            )
            if result["ok"]:
                fields["status"] = "published"
                fields["wp_post_id"] = result["wp_post_id"]
                fields["wp_post_url"] = result["wp_post_url"]
                fields["published_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                wp_post_url = result["wp_post_url"]
                published += 1
            else:
                fields["status"] = "draft"
                error_details.append(f"Row {idx + 1} '{title[:40]}': Publish failed - {result['error'][:80]}")
                errors += 1
        elif action == "schedule" and sched_at:
            fields["status"] = "scheduled"
            fields["scheduled_at"] = sched_at
            scheduled += 1
        else:
            if action == "publish" and not wp_ok:
                error_details.append(f"Row {idx + 1} '{title[:40]}': WordPress not connected, saved as draft.")
            fields["status"] = "draft"

        new_id = db.save_blog_post(
            brand_id, title, content, excerpt=excerpt,
            categories=categories, tags=tags,
            seo_title=seo_title, seo_description=seo_description,
            featured_image_url=featured_image_url,
            status=fields.get("status", "draft"),
            scheduled_at=fields.get("scheduled_at"),
            created_by=session.get("client_user_id", 0),
        )
        if fields.get("wp_post_id"):
            db.update_blog_post(
                new_id,
                wp_post_id=fields["wp_post_id"],
                wp_post_url=fields["wp_post_url"],
                published_at=fields.get("published_at"),
            )

        imported += 1

        # Auto-create Facebook promo post
        if auto_facebook and fields["status"] in ("published", "scheduled"):
            if fields["status"] == "published" and wp_post_url:
                fb_result = _create_fb_promo_for_blog(
                    db, brand, brand_id, title, wp_post_url,
                    excerpt=excerpt, featured_image_url=featured_image_url,
                )
            elif fields["status"] == "scheduled" and sched_at:
                # We don't have wp_post_url yet, use a placeholder
                site_url = (brand.get("wp_site_url") or "").rstrip("/")
                slug = re.sub(r'[^a-z0-9]+', '-', title.lower())[:80].strip('-')
                estimated_url = f"{site_url}/{slug}/" if site_url else ""
                if estimated_url:
                    fb_result = _create_fb_promo_for_blog(
                        db, brand, brand_id, title, estimated_url,
                        excerpt=excerpt, featured_image_url=featured_image_url,
                        scheduled_at=sched_at,
                    )
                else:
                    fb_result = {"ok": False}
            else:
                fb_result = {"ok": False}

            if fb_result.get("ok"):
                fb_posts += 1

    return jsonify(
        ok=True, imported=imported, published=published,
        scheduled=scheduled, fb_posts=fb_posts,
        errors=errors, error_details=error_details[:20],
    )


@client_bp.route("/blog/test-connection", methods=["POST"])
@client_login_required
def client_blog_test_connection():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand or not _wp_connected(brand):
        return jsonify(ok=False, error="WordPress not configured.")

    import requests as req_lib
    import base64
    wp_url = brand["wp_site_url"].strip().rstrip("/")
    wp_user = brand["wp_username"].strip()
    wp_pass = brand["wp_app_password"].strip()

    import logging
    log = logging.getLogger(__name__)
    log.info("[WP-TEST] URL=%s user=%r pass_len=%d pass_preview=%r",
             wp_url, wp_user, len(wp_pass), wp_pass[:4] + "..." if len(wp_pass) > 4 else wp_pass)
    ua_headers = {
        "User-Agent": "GroMore/1.0 (WordPress Blog Publisher; +https://gromore.com)",
        "Accept": "application/json",
    }

    try:
        probe = req_lib.get(f"{wp_url}/wp-json/", headers=ua_headers, timeout=15)
        if probe.status_code == 404:
            return jsonify(ok=False, error=f"REST API not found at {wp_url}/wp-json/. Verify the site URL is correct and that the REST API is not disabled by a security plugin (e.g. Wordfence, iThemes).")
        if probe.status_code >= 500:
            return jsonify(ok=False, error=f"WordPress returned a server error ({probe.status_code}). The site may be down or misconfigured.")
    except req_lib.exceptions.ConnectionError:
        return jsonify(ok=False, error=f"Could not connect to {wp_url}. Check the URL and make sure the site is online.")
    except req_lib.exceptions.Timeout:
        return jsonify(ok=False, error=f"Connection to {wp_url} timed out. The server may be slow or blocking requests.")
    except Exception as e:
        return jsonify(ok=False, error=f"Connection error: {str(e)[:120]}")

    # Step 2: Check if Application Passwords are enabled
    try:
        api_info = probe.json()
        auth_methods = api_info.get("authentication", {})
        if not auth_methods.get("application-passwords"):
            return jsonify(ok=False, error="Application Passwords are not enabled on this WordPress site. Go to WordPress Admin > Users > Your Profile and check that Application Passwords are available. Some hosting providers or security plugins disable this feature.")
    except Exception:
        pass  # Non-standard response, continue anyway

    # Step 3: Authenticate - send both standard header AND custom header.
    # SiteGround nginx strips Authorization; mu-plugin reads X-GM-Auth instead.
    token = base64.b64encode(f"{wp_user}:{wp_pass}".encode()).decode()
    headers = {
        "Authorization": f"Basic {token}",
        "X-GM-Auth": f"Basic {token}",
        "User-Agent": "GroMore/1.0 (WordPress Blog Publisher; +https://gromore.com)",
        "Accept": "application/json",
    }

    try:
        resp = req_lib.get(
            f"{wp_url}/wp-json/wp/v2/users/me?context=edit",
            headers=headers,
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            name = data.get("name", "")
            caps = data.get("capabilities", {})
            roles = data.get("roles", [])
            can_publish = caps.get("publish_posts") or caps.get("edit_posts")
            if can_publish:
                return jsonify(ok=True, message=f"Connected as {name} ({', '.join(roles)})")
            else:
                return jsonify(ok=False, error=f"Connected as {name}, but this user does not have permission to create posts. The WordPress user needs an Editor or Administrator role.")
        elif resp.status_code == 401:
            # Detailed 401 diagnostics
            body = resp.text[:500]
            log.warning("[WP-TEST] 401 response body: %s", body)
            hints = []
            if "incorrect_password" in body or "invalid_password" in body:
                hints.append("WordPress says the password is wrong.")
            if "invalid_username" in body:
                hints.append(f"WordPress says the username '{wp_user}' does not exist. Use your WordPress login username (not your email).")
            if not hints:
                hints.append("WordPress rejected the credentials.")
            hints.append("Make sure you're using an Application Password (not your regular WP login password). Generate one at WordPress Admin > Users > Profile > Application Passwords.")
            # Include WP error code for debugging
            try:
                err_json = resp.json()
                wp_code = err_json.get("code", "")
                wp_msg = err_json.get("message", "")
                if wp_code:
                    hints.append(f"[WP error: {wp_code} - {wp_msg}]")
            except Exception:
                pass
            return jsonify(ok=False, error=" ".join(hints))
        elif resp.status_code == 403:
            return jsonify(ok=False, error="User authenticated but forbidden (403). A security plugin may be blocking REST API access, or your hosting provider may block the Authorization header. Check Wordfence, Sucuri, or .htaccess rules.")
        else:
            return jsonify(ok=False, error=f"WordPress returned HTTP {resp.status_code}. Response: {resp.text[:150]}")
    except Exception as e:
        return jsonify(ok=False, error=str(e)[:150])


@client_bp.route("/blog/ai-generate", methods=["POST"])
@client_login_required
def client_blog_ai_generate():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify(ok=False, error="No OpenAI API key. Add one in Settings > AI Configuration.")

    data = request.get_json(silent=True) or {}
    topic = (data.get("topic") or "").strip()
    title = (data.get("title") or "").strip()

    if not topic and not title:
        return jsonify(ok=False, error="Provide a topic or title.")

    model = _pick_ai_model(brand, "analysis")
    brand_name = brand.get("display_name", "")
    industry = brand.get("industry", "")
    services = brand.get("primary_services", "")
    area = brand.get("service_area", "")
    voice = brand.get("brand_voice", "")

    prompt = f"""Write a complete blog post for a business website.

Business: {brand_name}
Industry: {industry}
Services: {services}
Service Area: {area}
Brand Voice: {voice or 'Professional, helpful, approachable'}

Topic/Brief: {topic or title}

Requirements:
- Write 600-1000 words of high-quality, original content
- Use proper HTML formatting with h2, h3, p, ul/ol, strong tags
- Include an engaging introduction that hooks the reader
- Break content into scannable sections with clear headings
- Include a call to action at the end
- Write naturally, avoid filler phrases like "In today's world" or "In this article"
- Be specific and provide real value to readers
- Optimize for SEO around the main topic
- Do NOT use em dashes

Return a JSON object with these exact keys:
- "title": a compelling blog title (if none was provided)
- "content": the full HTML blog post content (just the body HTML, no wrapper)
- "excerpt": a 1-2 sentence summary (plain text, under 160 chars)
- "seo_title": an SEO-optimized title (under 70 chars)
- "seo_description": meta description (under 160 chars)
- "tags": comma-separated relevant tags

Return ONLY valid JSON, no markdown code fences."""

    import openai
    try:
        client = openai.OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=4000,
        )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        # Robust JSON extraction: find the first { ... } block
        import re as _re
        json_match = _re.search(r'\{.*\}', raw, _re.DOTALL)
        if not json_match:
            return jsonify(ok=False, error="AI returned invalid format. Try again.")

        result = json.loads(json_match.group())
        result["ok"] = True
        _log_agent("spark", "Wrote blog post", result.get("title", topic or title)[:80])
        _log_agent("pulse", "Optimized blog SEO", result.get("seo_title", "")[:80])
        return jsonify(result)
    except json.JSONDecodeError:
        return jsonify(ok=False, error="AI returned invalid format. Try again.")
    except Exception as e:
        return jsonify(ok=False, error=str(e)[:200])


@client_bp.route("/settings/wordpress", methods=["POST"])
@client_login_required
def client_save_wordpress():
    db = _get_db()
    brand_id = session["client_brand_id"]

    wp_site_url = request.form.get("wp_site_url", "").strip().rstrip("/")
    wp_username = request.form.get("wp_username", "").strip()
    wp_app_password = request.form.get("wp_app_password", "").strip()

    db.update_brand_text_field(brand_id, "wp_site_url", wp_site_url)
    db.update_brand_text_field(brand_id, "wp_username", wp_username)
    if wp_app_password:
        db.update_brand_text_field(brand_id, "wp_app_password", wp_app_password)

    flash("WordPress settings saved.", "success")
    return redirect(url_for("client.client_settings"))


# ── Site Builder ──

def _publish_wp_page(brand, title, content, excerpt="", slug="",
                     seo_title="", seo_description="", status="publish", parent_id=0):
    """Publish a *page* (not post) to WordPress via REST API."""
    import requests as req_lib
    import base64

    wp_url = brand["wp_site_url"].strip().rstrip("/")
    wp_user = brand["wp_username"].strip()
    wp_pass = brand["wp_app_password"].strip()

    api_url = f"{wp_url}/wp-json/wp/v2/pages"
    token = base64.b64encode(f"{wp_user}:{wp_pass}".encode()).decode()
    headers = {
        "Authorization": f"Basic {token}",
        "X-GM-Auth": f"Basic {token}",
        "User-Agent": "GroMore/1.0 (WordPress Site Builder; +https://gromore.com)",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    page_data = {
        "title": seo_title or title,
        "content": content,
        "excerpt": excerpt,
        "status": status,
    }
    if slug:
        page_data["slug"] = slug
    if parent_id:
        page_data["parent"] = int(parent_id)
    meta = {}
    if seo_title:
        meta["_yoast_wpseo_title"] = seo_title
    if seo_description:
        meta["_yoast_wpseo_metadesc"] = seo_description
    if meta:
        page_data["meta"] = meta

    try:
        resp = req_lib.post(api_url, json=page_data, headers=headers, timeout=30)
        if resp.status_code in (200, 201):
            wp_page = resp.json()
            return {
                "ok": True,
                "wp_page_id": wp_page.get("id", 0),
                "wp_page_url": wp_page.get("link", ""),
            }
        return {"ok": False, "error": f"WordPress API error {resp.status_code}: {resp.text[:200]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@client_bp.route("/site-builder")
@client_login_required
def client_site_builder():
    """Site Builder landing page - shows WP status, generation form, build history."""
    db = _get_db()
    db.ensure_default_site_builder_kits()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id) or {}
    builds = db.get_site_builds(brand_id, limit=20)
    wp_ok = _wp_connected(brand)
    # GSC detection: check gsc_site_url field OR Google OAuth connection with property selected
    gsc_url = (brand.get("gsc_site_url") or "").strip()
    connections = db.get_brand_connections(brand_id)
    google_conn = connections.get("google") or {}
    google_connected = google_conn.get("status") == "connected"
    gsc_connected = bool(gsc_url)
    # If Google OAuth is connected but no SC property selected, show a different message
    gsc_needs_property = google_connected and not gsc_connected
    brand_palette = _site_builder_brand_palette(brand)
    brand_primary_color = brand_palette[0] if brand_palette else ""
    brand_accent_color = brand_palette[1] if len(brand_palette) > 1 else brand_primary_color
    site_templates = db.get_sb_site_templates(active_only=True)
    default_site_template = db.get_sb_default_site_template() or (site_templates[0] if site_templates else None)

    return render_template(
        "client/client_site_builder.html",
        layout_template="client/client_base.html",
        builder_mode="client",
        mode="landing",
        brand_fields_locked=True,
        show_runtime_wp_fields=False,
        builder_home_url=url_for("client.client_site_builder"),
        builder_generate_url=url_for("client.client_site_builder_generate"),
        builder_settings_url=url_for("client.client_settings"),
        builder_review_endpoint="client.client_site_builder_review",
        builder_delete_endpoint="client.client_site_builder_delete",
        builder_publish_endpoint="client.client_site_builder_publish",
        builder_page_get_endpoint="client.client_site_builder_page_get",
        builder_page_save_endpoint="client.client_site_builder_page_save",
        builder_page_rewrite_endpoint="client.client_site_builder_page_rewrite",
        builder_upload_image_url=url_for("client.client_site_builder_upload_image"),
        builder_seo_intel_url=url_for("client.client_site_builder_seo_intel"),
        builder_brand_picker_url="",
        available_brands=[],
        selected_brand_id=brand_id,
        wp_connected=wp_ok,
        wp_site_url=(brand.get("wp_site_url") or "").strip().rstrip("/"),
        builds=builds,
        brand_services=(brand.get("primary_services") or "").strip(),
        brand_areas=(brand.get("service_area") or "").strip(),
        brand_name=(brand.get("display_name") or "").strip(),
        brand_industry=(brand.get("industry") or "").strip(),
        brand_website=(brand.get("website") or "").strip(),
        brand_voice=(brand.get("brand_voice") or "").strip(),
        brand_target_audience=(brand.get("target_audience") or "").strip(),
        brand_tagline=(brand.get("tagline") or "").strip(),
        brand_phone=(brand.get("phone") or brand.get("business_phone") or "").strip(),
        brand_wp_username=(brand.get("wp_username") or "").strip(),
        brand_active_offers=(brand.get("active_offers") or "").strip(),
        brand_logo_url=_site_builder_brand_logo_url(brand),
        brand_logo_path=(brand.get("logo_path") or "").strip(),
        builder_brand_colors=brand_palette,
        builder_primary_color=brand_primary_color,
        builder_accent_color=brand_accent_color,
        brand_font_heading=(brand.get("font_heading") or "").strip(),
        brand_font_body=(brand.get("font_body") or "").strip(),
        google_font_choices=GOOGLE_FONT_CHOICES,
        font_pair_choices=SITE_BUILDER_FONT_PAIR_CHOICES,
        font_groups=SITE_BUILDER_FONT_GROUPS,
        font_preview_stylesheets=SITE_BUILDER_FONT_PREVIEW_STYLESHEETS,
        editor_font_family_options=SITE_BUILDER_EDITOR_FONT_OPTIONS,
        image_slots=_SITE_BUILDER_INTAKE_IMAGE_SLOTS,
        site_templates=site_templates,
        default_site_template_id=(default_site_template or {}).get("id") or 0,
        wp_admin_url=_site_builder_wp_admin_url(brand),
        gsc_connected=gsc_connected,
        gsc_needs_property=gsc_needs_property,
    )


@client_bp.route("/site-builder/seo-intel", methods=["POST"])
@client_login_required
def client_site_builder_seo_intel():
    """Pull Search Console data and generate Warren's SEO strategy brief via AJAX."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id) or {}

    gsc_url = (brand.get("gsc_site_url") or "").strip()
    if not gsc_url:
        return jsonify(ok=False, error="Search Console not connected. Add your GSC site URL in Settings."), 400

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify(ok=False, error="OpenAI API key not configured."), 400

    model = _pick_ai_model(brand, "analysis")

    # Pull SC data
    seo_data = {}
    try:
        from src.api_search_console import pull_search_console_data
        from datetime import datetime, timedelta
        today = datetime.utcnow()
        first_of_month = today.replace(day=1)
        last_month = (first_of_month - timedelta(days=1))
        month_str = last_month.strftime("%Y-%m")

        # Try OAuth tokens first (from Google connection), fall back to service account
        oauth_tokens = None
        connections = db.get_brand_connections(brand_id)
        google_conn = connections.get("google") or {}
        if google_conn.get("access_token"):
            oauth_tokens = {
                "access_token": google_conn["access_token"],
                "refresh_token": google_conn.get("refresh_token", ""),
                "client_id": (db.get_setting("google_client_id", "") or current_app.config.get("GOOGLE_CLIENT_ID", "")).strip(),
                "client_secret": (db.get_setting("google_client_secret", "") or current_app.config.get("GOOGLE_CLIENT_SECRET", "")).strip(),
            }

        seo_data = pull_search_console_data(gsc_url, month_str, oauth_tokens=oauth_tokens) or {}
    except Exception as exc:
        return jsonify(ok=False, error=f"Failed to pull Search Console data: {str(exc)[:200]}"), 500

    # Generate Warren brief
    warren_brief = ""
    try:
        from webapp.site_builder import build_brand_context, generate_warren_seo_brief
        brand_ctx = build_brand_context(brand)
        warren_brief = generate_warren_seo_brief(brand_ctx, seo_data, api_key, model)
    except Exception as exc:
        current_app.logger.warning("Warren SEO brief failed: %s", exc)

    return jsonify(
        ok=True,
        seo_data={
            "totals": seo_data.get("totals", {}),
            "top_queries": (seo_data.get("top_queries") or [])[:20],
            "opportunity_queries": (seo_data.get("opportunity_queries") or [])[:15],
            "top_pages": (seo_data.get("top_pages") or [])[:10],
        },
        warren_brief=warren_brief,
    )


@client_bp.route("/site-builder/generate", methods=["POST"])
@client_login_required
def client_site_builder_generate():
    """Kick off an AI site build: generate blueprint, content, and schema for all pages."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id) or {}
    is_ajax = request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest"

    from webapp.site_builder import (
        build_brand_context,
        build_site_blueprint,
        generate_page_content,
        generate_warren_seo_brief,
        assemble_page,
    )

    api_key = _get_openai_api_key(brand)
    if not api_key:
        msg = "OpenAI API key not configured. Add it in Settings before generating a site."
        if is_ajax:
            return jsonify(ok=False, error=msg), 400
        flash(msg, "error")
        return redirect(url_for("client.client_settings"))

    model = _pick_ai_model(brand, "analysis")

    # ── Collect intake data ──
    services = (request.form.get("services") or "").strip() or None
    areas = (request.form.get("areas") or "").strip() or None

    intake = {
        "business_name": (request.form.get("business_name") or "").strip(),
        "industry": (request.form.get("industry") or "").strip(),
        "website": (request.form.get("website") or "").strip(),
        "phone": (request.form.get("phone") or "").strip(),
        "email": (request.form.get("email") or "").strip(),
        "address": (request.form.get("address") or "").strip(),
        "brand_voice": (request.form.get("brand_voice") or "").strip(),
        "target_audience": (request.form.get("target_audience") or "").strip(),
        "tagline": (request.form.get("tagline") or "").strip(),
        "active_offers": (request.form.get("active_offers") or "").strip(),
        "unique_selling_points": (request.form.get("unique_selling_points") or "").strip(),
        "services_to_highlight": (request.form.get("services_to_highlight") or "").strip(),
        "service_plan_options": (request.form.get("service_plan_options") or "").strip(),
        "service_add_ons": (request.form.get("service_add_ons") or "").strip(),
        "priority_seo_locations": (request.form.get("priority_seo_locations") or "").strip(),
        "company_story": (request.form.get("company_story") or "").strip(),
        "site_vision": (request.form.get("site_vision") or "").strip(),
        "design_notes": (request.form.get("design_notes") or "").strip(),
        "competitors": (request.form.get("competitors") or "").strip(),
        "content_goals": (request.form.get("content_goals") or "").strip(),
        "lead_form_type": (request.form.get("lead_form_type") or "").strip(),
        "lead_form_shortcode": (request.form.get("lead_form_shortcode") or "").strip(),
        "quote_tool_source": (request.form.get("quote_tool_source") or "").strip(),
        "quote_tool_embed": (request.form.get("quote_tool_embed") or "").strip(),
        "quote_tool_zip_mode": (request.form.get("quote_tool_zip_mode") or "").strip(),
        "quote_tool_collect_dogs": bool(request.form.get("quote_tool_collect_dogs")),
        "quote_tool_collect_frequency": bool(request.form.get("quote_tool_collect_frequency")),
        "quote_tool_collect_last_cleaned": bool(request.form.get("quote_tool_collect_last_cleaned")),
        "quote_tool_phone_mode": (request.form.get("quote_tool_phone_mode") or "").strip(),
        "quote_tool_notes": (request.form.get("quote_tool_notes") or "").strip(),
        "plugins": (request.form.get("plugins") or "").strip(),
        "cta_text": (request.form.get("cta_text") or "").strip(),
        "cta_phone": (request.form.get("cta_phone") or "").strip(),
        # Design tokens
        "color_palette": (request.form.get("color_palette") or "").strip(),
        "font_pair": (request.form.get("font_pair") or "").strip(),
        "layout_style": (request.form.get("layout_style") or "").strip(),
        "wireframe_style": (request.form.get("wireframe_style") or "").strip(),
        "hero_layout": (request.form.get("hero_layout") or "").strip(),
        "services_widget_layout": (request.form.get("services_widget_layout") or "").strip(),
        "proof_widget_layout": (request.form.get("proof_widget_layout") or "").strip(),
        "cta_widget_layout": (request.form.get("cta_widget_layout") or "").strip(),
        "button_style": (request.form.get("button_style") or "").strip(),
        "color_primary": (request.form.get("color_primary") or "").strip(),
        "color_accent": (request.form.get("color_accent") or "").strip(),
        "color_dark": (request.form.get("color_dark") or "").strip(),
        "color_light": (request.form.get("color_light") or "").strip(),
        "font_heading": normalize_google_font_family(request.form.get("font_heading") or ""),
        "font_body": normalize_google_font_family(request.form.get("font_body") or ""),
        "style_preset": (request.form.get("style_preset") or "").strip(),
        "reference_url": (request.form.get("reference_url") or "").strip(),
        "reference_mode": _site_builder_reference_mode(request.form.get("reference_mode")),
    }
    intake["image_slots"] = _site_builder_collect_image_slots(
        brand_id,
        (intake.get("industry") or brand.get("industry") or request.form.get("industry") or "").strip(),
    )

    # ── Parse page selection ──
    page_selection_raw = (request.form.get("page_selection") or "").strip()
    page_selection = [p.strip() for p in page_selection_raw.split(",") if p.strip()] or None

    # ── Parse landing pages ──
    landing_pages = []
    lp_names = request.form.getlist("lp_name[]")
    lp_keywords = request.form.getlist("lp_keyword[]")
    lp_offers = request.form.getlist("lp_offer[]")
    lp_audiences = request.form.getlist("lp_audience[]")
    for i, name in enumerate(lp_names):
        name = (name or "").strip()
        if name:
            landing_pages.append({
                "name": name,
                "keyword": (lp_keywords[i] if i < len(lp_keywords) else "").strip(),
                "offer": (lp_offers[i] if i < len(lp_offers) else "").strip(),
                "audience": (lp_audiences[i] if i < len(lp_audiences) else "").strip(),
            })

    # ── Parse custom pages ──
    custom_pages = []
    cp_names = request.form.getlist("cp_name[]")
    cp_slugs = request.form.getlist("cp_slug[]")
    cp_purposes = request.form.getlist("cp_purpose[]")
    for i, name in enumerate(cp_names):
        name = (name or "").strip()
        if name:
            custom_pages.append({
                "name": name,
                "slug": (cp_slugs[i] if i < len(cp_slugs) else "").strip(),
                "purpose": (cp_purposes[i] if i < len(cp_purposes) else "").strip(),
            })

    # ── Pull Search Console data if connected ──
    seo_data = {}
    gsc_url = (brand.get("gsc_site_url") or "").strip()
    if gsc_url:
        try:
            from src.api_search_console import pull_search_console_data
            from datetime import datetime, timedelta
            today = datetime.utcnow()
            first_of_month = today.replace(day=1)
            last_month = (first_of_month - timedelta(days=1))
            month_str = last_month.strftime("%Y-%m")

            # Try OAuth tokens first, fall back to service account
            oauth_tokens = None
            connections = db.get_brand_connections(brand_id)
            google_conn = connections.get("google") or {}
            if google_conn.get("access_token"):
                oauth_tokens = {
                    "access_token": google_conn["access_token"],
                    "refresh_token": google_conn.get("refresh_token", ""),
                    "client_id": (db.get_setting("google_client_id", "") or current_app.config.get("GOOGLE_CLIENT_ID", "")).strip(),
                    "client_secret": (db.get_setting("google_client_secret", "") or current_app.config.get("GOOGLE_CLIENT_SECRET", "")).strip(),
                }

            seo_data = pull_search_console_data(gsc_url, month_str, oauth_tokens=oauth_tokens) or {}
        except Exception as exc:
            current_app.logger.warning("SC pull for site builder failed: %s", exc)

    # ── Warren SEO brief ──
    warren_brief = ""
    if seo_data and seo_data.get("top_queries"):
        try:
            warren_brief = generate_warren_seo_brief(
                build_brand_context(brand), seo_data, api_key, model
            )
        except Exception as exc:
            current_app.logger.warning("Warren SEO brief failed: %s", exc)

    intake["seo_data"] = seo_data
    intake["warren_brief"] = warren_brief
    if intake.get("reference_url"):
        try:
            intake["reference_site_brief"] = _site_builder_reference_style_brief(
                intake.get("reference_url"),
                intake.get("reference_mode"),
                intake.get("industry") or brand.get("industry"),
                intake.get("business_name") or brand.get("display_name"),
                brand=brand,
            )
        except Exception as exc:
            current_app.logger.warning("Reference site brief failed: %s", exc)
    selected_site_template = None
    selected_site_template_id = int(request.form.get("site_template_id") or 0)
    if selected_site_template_id:
        selected_site_template = db.get_sb_site_template(selected_site_template_id)
    if not selected_site_template:
        selected_site_template = db.get_sb_default_site_template()

    if selected_site_template:
        site_theme = db.get_sb_theme(selected_site_template.get("theme_id")) if int(selected_site_template.get("theme_id") or 0) else {}
        active_templates = []
        template_lookup = {
            int(item.get("id") or 0): item
            for item in db.get_sb_templates(active_only=True)
            if int(item.get("id") or 0)
        }
        for template_id in selected_site_template.get("template_ids") or []:
            template = template_lookup.get(int(template_id or 0))
            if template:
                active_templates.append(template)
        intake["builder_site_template"] = _site_builder_site_template_snapshot(
            selected_site_template,
            theme=site_theme,
            templates=active_templates,
        )
        intake["builder_theme"] = _site_builder_theme_snapshot(site_theme or {})
        intake["builder_templates"] = _site_builder_template_snapshots(active_templates)
    else:
        intake["builder_theme"] = _site_builder_theme_snapshot(db.get_sb_default_theme() or {})
        intake["builder_templates"] = _site_builder_template_snapshots(
            db.get_sb_templates(active_only=True)
        )
    intake["builder_prompt_overrides"] = _site_builder_prompt_override_snapshots(
        db.get_sb_prompt_overrides()
    )

    brand_ctx = build_brand_context(brand, intake=intake)
    blueprint = build_site_blueprint(
        brand_ctx, services=services, areas=areas,
        landing_pages=landing_pages, page_selection=page_selection,
        custom_pages=custom_pages,
    )

    if not blueprint:
        msg = "Could not create a site blueprint. Check that your brand profile has services and a service area."
        if is_ajax:
            return jsonify(ok=False, error=msg), 400
        flash(msg, "warning")
        return redirect(url_for("client.client_settings"))

    build_id = db.create_site_build(
        brand_id, blueprint, model=model,
        created_by=session.get("client_user_id", 0),
        intake=intake,
    )
    db.update_site_build_status(build_id, "running")

    pages_done = 0
    errors = []
    for page_spec in blueprint:
        try:
            content = generate_page_content(page_spec, brand_ctx, api_key, model)
            assembled = assemble_page(page_spec, brand_ctx, content)

            db.save_site_page({
                "build_id": build_id,
                "brand_id": brand_id,
                "page_type": page_spec["page_type"],
                "label": page_spec["label"],
                "slug": page_spec.get("slug", ""),
                "title": content.get("title", ""),
                "content": assembled.get("body_html") or content.get("content", ""),
                "excerpt": content.get("excerpt", ""),
                "seo_title": content.get("seo_title", ""),
                "seo_description": content.get("seo_description", ""),
                "primary_keyword": content.get("primary_keyword", ""),
                "secondary_keywords": content.get("secondary_keywords", ""),
                "faq_items": content.get("faq_items") or [],
                "schemas": assembled.get("schemas") or [],
                "schema_html": assembled.get("schema_html", ""),
                "full_html": assembled.get("full_html", ""),
            })
            pages_done += 1
            db.update_site_build_status(build_id, "running", pages_completed=pages_done)
        except Exception as exc:
            errors.append(f"{page_spec['label']}: {exc}")

    if errors:
        db.update_site_build_status(build_id, "completed", pages_completed=pages_done,
                                    error_message="; ".join(errors)[:500])
    else:
        db.update_site_build_status(build_id, "completed", pages_completed=pages_done)

    if is_ajax:
        return jsonify(
            ok=True,
            build_id=build_id,
            pages_generated=pages_done,
            errors=errors,
        )
    flash(f"Site build complete: {pages_done} pages generated.", "success")
    return redirect(url_for("client.client_site_builder_review", build_id=build_id))


@client_bp.route("/site-builder/<int:build_id>")
@client_login_required
def client_site_builder_review(build_id):
    """Review generated pages before publishing to WordPress."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    build = db.get_site_build(build_id)
    if not build or build["brand_id"] != brand_id:
        flash("Site build not found.", "warning")
        return redirect(url_for("client.client_site_builder"))
    pages = db.get_site_pages(build_id)
    brand = db.get_brand(brand_id) or {}
    wp_ok = _wp_connected(brand)
    unpublished = sum(1 for p in pages if not p.get("wp_page_id"))
    brand_palette = _site_builder_brand_palette(brand)
    brand_primary_color = brand_palette[0] if brand_palette else ""
    brand_accent_color = brand_palette[1] if len(brand_palette) > 1 else brand_primary_color

    is_ajax = (
        request.headers.get("X-Requested-With") in {"XMLHttpRequest", "PJAX"}
        or request.is_json
    )
    if is_ajax:
        return jsonify(ok=True, build=build, pages=pages)

    return render_template(
        "client/client_site_builder.html",
        layout_template="client/client_base.html",
        builder_mode="client",
        mode="review",
        brand_fields_locked=True,
        show_runtime_wp_fields=False,
        builder_home_url=url_for("client.client_site_builder"),
        builder_generate_url=url_for("client.client_site_builder_generate"),
        builder_settings_url=url_for("client.client_settings"),
        builder_review_endpoint="client.client_site_builder_review",
        builder_delete_endpoint="client.client_site_builder_delete",
        builder_publish_endpoint="client.client_site_builder_publish",
        builder_page_get_endpoint="client.client_site_builder_page_get",
        builder_page_save_endpoint="client.client_site_builder_page_save",
        builder_page_rewrite_endpoint="client.client_site_builder_page_rewrite",
        builder_upload_image_url=url_for("client.client_site_builder_upload_image"),
        builder_seo_intel_url=url_for("client.client_site_builder_seo_intel"),
        builder_brand_picker_url="",
        available_brands=[],
        selected_brand_id=brand_id,
        build=build,
        pages=pages,
        wp_connected=wp_ok,
        unpublished_count=unpublished,
        wp_site_url=(brand.get("wp_site_url") or "").strip().rstrip("/"),
        wp_admin_url=_site_builder_wp_admin_url(brand),
        brand_logo_url=_site_builder_brand_logo_url(brand),
        brand_logo_path=(brand.get("logo_path") or "").strip(),
        brand_website=(brand.get("website") or "").strip(),
        brand_phone=(brand.get("phone") or brand.get("business_phone") or "").strip(),
        brand_wp_username=(brand.get("wp_username") or "").strip(),
        builder_brand_colors=brand_palette,
        builder_primary_color=brand_primary_color,
        builder_accent_color=brand_accent_color,
        google_font_choices=GOOGLE_FONT_CHOICES,
        font_pair_choices=SITE_BUILDER_FONT_PAIR_CHOICES,
        font_groups=SITE_BUILDER_FONT_GROUPS,
        font_preview_stylesheets=SITE_BUILDER_FONT_PREVIEW_STYLESHEETS,
        editor_font_family_options=SITE_BUILDER_EDITOR_FONT_OPTIONS,
        image_slots=_SITE_BUILDER_INTAKE_IMAGE_SLOTS,
    )


@client_bp.route("/site-builder/<int:build_id>/delete", methods=["POST"])
@client_login_required
def client_site_builder_delete(build_id):
    """Delete a site build and its published WordPress pages."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    wants_json = request.is_json or request.headers.get("X-Requested-With") in {"XMLHttpRequest", "PJAX"}

    build = db.get_site_build(build_id)
    if not build or build["brand_id"] != brand_id:
        if wants_json:
            return jsonify(ok=False, error="Build not found."), 404
        flash("Site build not found.", "warning")
        return redirect(url_for("client.client_site_builder"))

    brand = db.get_brand(brand_id) or {}
    pages = db.get_site_pages(build_id)
    published_pages = [page for page in pages if int(page.get("wp_page_id") or 0)]

    if published_pages and not _wp_connected(brand):
        msg = "Reconnect WordPress before deleting this build so the published pages can be removed from WordPress too."
        if wants_json:
            return jsonify(ok=False, error=msg), 400
        flash(msg, "error")
        return redirect(url_for("client.client_site_builder_review", build_id=build_id))

    deleted_wp_pages = 0
    errors = []
    for page in published_pages:
        result = _delete_wp_page(brand, page.get("wp_page_id"))
        if result.get("ok"):
            if result.get("deleted"):
                deleted_wp_pages += 1
            continue
        errors.append(f"{page.get('label') or page.get('title') or 'Page'}: {result.get('error', 'WordPress delete failed')}")

    if errors:
        if wants_json:
            return jsonify(ok=False, error="Failed to delete one or more WordPress pages.", errors=errors), 502
        flash(errors[0], "error")
        return redirect(url_for("client.client_site_builder_review", build_id=build_id))

    deleted = db.delete_site_build(build_id, brand_id=brand_id)
    if not deleted:
        if wants_json:
            return jsonify(ok=False, error="Build not found."), 404
        flash("Site build not found.", "warning")
        return redirect(url_for("client.client_site_builder"))

    message = f"Deleted build #{build_id}."
    if deleted_wp_pages:
        message += f" Removed {deleted_wp_pages} WordPress page(s)."

    if wants_json:
        return jsonify(ok=True, deleted_build_id=build_id, deleted_wordpress_pages=deleted_wp_pages)
    flash(message, "success")
    return redirect(url_for("client.client_site_builder"))


@client_bp.route("/site-builder/<int:build_id>/publish", methods=["POST"])
@client_login_required
def client_site_builder_publish(build_id):
    """Publish all generated pages to WordPress."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    build = db.get_site_build(build_id)
    if not build or build["brand_id"] != brand_id:
        return jsonify(ok=False, error="Build not found."), 404

    brand = db.get_brand(brand_id) or {}
    if not _wp_connected(brand):
        return jsonify(ok=False, error="WordPress is not connected. Add credentials in Settings."), 400

    pages = db.get_site_pages(build_id)
    published = 0
    errors = []
    for page in pages:
        if page.get("wp_page_id"):
            published += 1
            continue
        result = _publish_wp_page(
            brand,
            title=page["title"],
            content=page["full_html"] or page["content"],
            excerpt=page.get("excerpt", ""),
            slug=page.get("slug", ""),
            seo_title=page.get("seo_title", ""),
            seo_description=page.get("seo_description", ""),
        )
        if result.get("ok"):
            db.update_site_page_wp(page["id"], result["wp_page_id"], result["wp_page_url"])
            published += 1
        else:
            errors.append(f"{page['label']}: {result.get('error', 'Unknown error')}")

    return jsonify(ok=True, published=published, total=len(pages), errors=errors)


# ── Site Builder: Save page edits (GrapesJS / manual) ──

@client_bp.route("/site-builder/page/<int:page_id>/save", methods=["POST"])
@client_login_required
def client_site_builder_page_save(page_id):
    """Save page content and editor state from the GrapesJS editor."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    page = db.get_site_page(page_id)
    if not page:
        return jsonify(ok=False, error="Page not found."), 404
    build = db.get_site_build(page["build_id"])
    if not build or build["brand_id"] != brand_id:
        return jsonify(ok=False, error="Access denied."), 403

    data = request.get_json(silent=True)
    if data is None and request.get_data(cache=False):
        return jsonify(ok=False, error="Invalid JSON payload."), 400

    try:
        update = _normalize_site_builder_page_save_payload(data)
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400

    if update:
        db.update_site_page_content(page_id, update)

    return jsonify(ok=True)


# ── Site Builder: AI Rewrite ──

@client_bp.route("/site-builder/page/<int:page_id>/rewrite", methods=["POST"])
@client_login_required
def client_site_builder_page_rewrite(page_id):
    """Rewrite a page's content using AI with optional user instructions."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    page = db.get_site_page(page_id)
    if not page:
        return jsonify(ok=False, error="Page not found."), 404
    build = db.get_site_build(page["build_id"])
    if not build or build["brand_id"] != brand_id:
        return jsonify(ok=False, error="Access denied."), 403

    brand = db.get_brand(brand_id) or {}
    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify(ok=False, error="OpenAI API key not configured."), 400

    model = _pick_ai_model(brand, "analysis")
    data = request.get_json(silent=True)
    if data is None and request.get_data(cache=False):
        return jsonify(ok=False, error="Invalid JSON payload."), 400
    if data is not None and not isinstance(data, dict):
        return jsonify(ok=False, error="Invalid rewrite payload."), 400
    instructions = _normalize_site_builder_text(
        (data or {}).get("instructions"),
        "Rewrite instructions",
        max_length=_SITE_BUILDER_MAX_REWRITE_INSTRUCTIONS_LENGTH,
    )

    from webapp.site_builder import (
        build_brand_context, _brand_block, _seo_intel_block,
        _GLOBAL_RULES, _OUTPUT_FORMAT, _system_msg, _extract_json,
    )

    # Rebuild brand context from stored intake if available
    intake = {}
    try:
        intake = json.loads(build.get("intake_json") or "{}")
    except Exception:
        pass
    brand_ctx = build_brand_context(brand, intake=intake)

    existing_content = page.get("content") or ""
    existing_title = page.get("title") or ""
    existing_seo_title = page.get("seo_title") or ""
    existing_seo_desc = page.get("seo_description") or ""

    brand_block = _brand_block(brand_ctx)
    seo_intel = _seo_intel_block(brand_ctx)

    user_msg = (
        f"REWRITE the following website page content.\n\n"
        f"BUSINESS CONTEXT:\n{brand_block}\n\n"
        f"{seo_intel}"
        f"PAGE TYPE: {page.get('page_type', 'generic')}\n"
        f"PAGE LABEL: {page.get('label', '')}\n"
        f"CURRENT TITLE: {existing_title}\n"
        f"CURRENT SEO TITLE: {existing_seo_title}\n"
        f"CURRENT SEO DESCRIPTION: {existing_seo_desc}\n\n"
        f"CURRENT CONTENT:\n{existing_content}\n\n"
    )

    if instructions:
        user_msg += (
            f"USER REWRITE INSTRUCTIONS (follow these closely):\n{instructions}\n\n"
        )
    else:
        user_msg += (
            "REWRITE INSTRUCTIONS:\n"
            "Improve the content quality, conversion copy, and SEO without changing the "
            "core structure or message. Tighten the writing, strengthen CTAs, add specificity.\n\n"
        )

    user_msg += f"{_GLOBAL_RULES}\n{_OUTPUT_FORMAT}"

    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _system_msg()},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.5,
            response_format={"type": "json_object"},
        )
        raw = (response.choices[0].message.content or "{}").strip()
        result = _extract_json(raw)
        if not isinstance(result, dict):
            raise ValueError("AI rewrite returned an unexpected response.")
    except Exception as exc:
        return jsonify(ok=False, error=f"AI rewrite failed: {str(exc)[:200]}"), 500

    # Save the rewritten content
    rewritten_content = _normalize_site_builder_text(
        result.get("content") or existing_content,
        "Page content",
        trim=False,
    )
    try:
        _validate_site_builder_blob_size(rewritten_content, "Page content", _SITE_BUILDER_MAX_CONTENT_BYTES)
        update = {
            "title": _normalize_site_builder_text(
                result.get("title") or existing_title,
                "Page title",
                max_length=_SITE_BUILDER_MAX_TITLE_LENGTH,
            ),
            "content": rewritten_content,
            "excerpt": result.get("excerpt") or page.get("excerpt", ""),
            "seo_title": _normalize_site_builder_text(
                result.get("seo_title") or existing_seo_title,
                "SEO title",
                max_length=_SITE_BUILDER_MAX_SEO_TITLE_LENGTH,
            ),
            "seo_description": _normalize_site_builder_text(
                result.get("seo_description") or existing_seo_desc,
                "SEO description",
                max_length=_SITE_BUILDER_MAX_SEO_DESCRIPTION_LENGTH,
            ),
            "primary_keyword": result.get("primary_keyword") or page.get("primary_keyword", ""),
            "secondary_keywords": result.get("secondary_keywords") or page.get("secondary_keywords", ""),
        }
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400
    if result.get("faq_items"):
        update["faq_items_json"] = json.dumps(result["faq_items"])
    update["full_html"] = update["content"]
    db.update_site_page_content(page_id, update)

    return jsonify(ok=True, page=update)


# ── Site Builder: Image Upload ──

@client_bp.route("/site-builder/upload-image", methods=["POST"])
@client_login_required
def client_site_builder_upload_image():
    """Upload an image for use in the site builder / GrapesJS editor."""
    import os
    import uuid
    from werkzeug.utils import secure_filename

    if "image" not in request.files:
        # GrapesJS asset manager sends as "files[]"
        file = request.files.get("files[]")
        if not file:
            return jsonify(ok=False, error="No image file provided."), 400
    else:
        file = request.files["image"]

    if not file.filename:
        return jsonify(ok=False, error="Empty filename."), 400

    allowed_ext = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in allowed_ext:
        return jsonify(ok=False, error=f"File type {ext} not allowed.", allowed_types=sorted(allowed_ext)), 400

    file.seek(0, 2)
    size = file.tell()
    file.seek(0)
    if size > 10 * 1024 * 1024:
        return jsonify(ok=False, error="Image too large. Max 10MB.", max_size_bytes=10 * 1024 * 1024), 400

    safe_name = secure_filename(f"{uuid.uuid4().hex}{ext}")
    if not safe_name:
        return jsonify(ok=False, error="Could not generate a safe filename."), 400
    upload_dir = os.path.join(current_app.static_folder or "static", "uploads", "site_builder")
    os.makedirs(upload_dir, exist_ok=True)
    save_path = os.path.join(upload_dir, safe_name)
    try:
        file.save(save_path)
    except Exception as exc:
        current_app.logger.exception("site builder image upload failed")
        return jsonify(ok=False, error=f"Image upload failed: {str(exc)[:160]}"), 500

    img_url = url_for("static", filename=f"uploads/site_builder/{safe_name}")
    # Return in GrapesJS asset manager expected format
    return jsonify(ok=True, data=[img_url], url=img_url, filename=safe_name)


# ── Site Builder: Get page JSON (for editor) ──

@client_bp.route("/site-builder/page/<int:page_id>")
@client_login_required
def client_site_builder_page_get(page_id):
    """Return page data as JSON for the editor."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    page = db.get_site_page(page_id)
    if not page:
        return jsonify(ok=False, error="Page not found."), 404
    build = db.get_site_build(page["build_id"])
    if not build or build["brand_id"] != brand_id:
        return jsonify(ok=False, error="Access denied."), 403
    return jsonify(ok=True, page=page)


@client_bp.route("/settings/sng", methods=["POST"])
@client_login_required
def client_save_sng():
    db = _get_db()
    brand_id = session["client_brand_id"]

    api_key = request.form.get("sng_api_key", "").strip()
    org_slug = request.form.get("sng_org_slug", "").strip()

    # Set CRM type to sweepandgo
    db.update_brand_text_field(brand_id, "crm_type", "sweepandgo")

    # Only update key if user entered something (don't blank it)
    if api_key:
        db.update_brand_text_field(brand_id, "crm_api_key", api_key)

    db.update_brand_text_field(brand_id, "crm_pipeline_id", org_slug)

    flash("Sweep and Go settings saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/crm/sng/test", methods=["POST"])
@client_login_required
def client_sng_test():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    if brand.get("crm_type") != "sweepandgo" or not brand.get("crm_api_key"):
        return jsonify(ok=False, error="Sweep and Go not configured. Save your API token first.")

    from webapp.crm_bridge import sng_count_active_clients
    result, error = sng_count_active_clients(brand)
    if error:
        return jsonify(ok=False, error=error)

    count = result.get("data", 0) if isinstance(result, dict) else 0
    return jsonify(ok=True, message=f"Connected - {count} active clients found")


# ── GoHighLevel Settings ──

@client_bp.route("/settings/ghl", methods=["POST"])
@client_login_required
def client_save_ghl():
    db = _get_db()
    brand_id = session["client_brand_id"]

    api_key = request.form.get("ghl_api_key", "").strip()
    location_id = request.form.get("ghl_location_id", "").strip()
    pipeline_id = request.form.get("ghl_pipeline_id", "").strip()

    db.update_brand_text_field(brand_id, "crm_type", "gohighlevel")

    if api_key:
        db.update_brand_text_field(brand_id, "crm_api_key", api_key)

    # Location ID is required for PIT + LeadConnector (services.leadconnectorhq.com)
    db.update_brand_text_field(brand_id, "titan_ghl_location_id", location_id)
    db.update_brand_text_field(brand_id, "crm_pipeline_id", pipeline_id)

    flash("GoHighLevel settings saved.", "success")
    return redirect(url_for("client.client_settings"))


@client_bp.route("/crm/ghl/test", methods=["POST"])
@client_login_required
def client_ghl_test():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    if brand.get("crm_type") != "gohighlevel" or not brand.get("crm_api_key"):
        return jsonify(ok=False, error="GoHighLevel not configured. Save your token first.")

    from webapp.crm_bridge import ghl_test_connection
    message, error = ghl_test_connection(brand)
    if error:
        return jsonify(ok=False, error=error)

    return jsonify(ok=True, message=message)


# ── CRM Dashboard Tab ──


def _build_client_commercial_payload(thread):
    payload = _normalize_client_commercial_payload(_safe_json_object(thread.get("commercial_data_json")))
    payload["name"] = payload.get("name") or (thread.get("lead_name") or "").strip() or payload.get("business_name") or "Commercial Prospect"
    payload["email"] = (thread.get("lead_email") or "").strip().lower() or payload.get("email") or ""
    payload["phone"] = (thread.get("lead_phone") or "").strip() or payload.get("phone") or ""
    payload["business_name"] = payload.get("business_name") or (thread.get("lead_name") or "").strip() or payload["name"]
    payload["source"] = payload.get("source") or (thread.get("source") or "").strip() or "commercial_prospecting"
    payload["stage"] = payload.get("stage") or (thread.get("status") or "").strip() or "new"
    payload["summary"] = (thread.get("summary") or "").strip() or payload.get("summary") or ""
    return _normalize_client_commercial_payload(payload)


def _build_client_commercial_brief(thread, brand=None):
    from webapp.commercial_strategy import build_commercial_outreach_brief

    payload = _build_client_commercial_payload(thread)
    brief = build_commercial_outreach_brief(payload, brand=brand)
    return payload, _apply_client_commercial_outreach_overrides(payload, brief)


def _get_client_commercial_threads(db, brand_id, limit=60):
    threads = db.get_lead_threads(brand_id, limit=limit)
    brand = db.get_brand(brand_id)
    items = []
    for thread in threads:
        if (thread.get("source") or "") != "commercial_prospecting" and (thread.get("commercial_data_json") or "{}").strip() in {"", "{}"}:
            continue
        payload, brief = _build_client_commercial_brief(thread, brand=brand)
        items.append({
            "thread": thread,
            "payload": payload,
            "brief": brief,
        })
    return items

@client_bp.route("/crm")
@client_login_required
def client_crm():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    crm_type = (brand.get("crm_type") or "").strip().lower()
    crm_connected = crm_type in ("sweepandgo", "gohighlevel", "jobber") and bool(brand.get("crm_api_key"))

    return render_template(
        "client_crm.html",
        brand=brand,
        crm_connected=crm_connected,
        crm_type=crm_type,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/lead-assistant")
@client_login_required
def client_lead_assistant():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    try:
        chatbot_channels = set(json.loads(brand.get("sales_bot_channels") or "[]"))
    except Exception:
        chatbot_channels = set()

    lead_form_config = _lead_form_config_for_brand(brand)
    lead_form_share = _lead_form_share_payload(brand)

    return render_template(
        "client_lead_assistant.html",
        brand=brand,
        chatbot_channels=chatbot_channels,
        lead_form_config=lead_form_config,
        lead_form_share=lead_form_share,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/lead-assistant", methods=["POST"])
@client_login_required
def client_save_lead_assistant_profile():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id) or {}

    db.update_brand_number_field(
        brand_id,
        "crm_avg_service_price",
        request.form.get("crm_avg_service_price") or 0,
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_service_menu",
        (request.form.get("sales_bot_service_menu") or "").strip()[:4000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_pricing_notes",
        (request.form.get("sales_bot_pricing_notes") or "").strip()[:4000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_guardrails",
        (request.form.get("sales_bot_guardrails") or "").strip()[:4000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_example_language",
        (request.form.get("sales_bot_example_language") or "").strip()[:4000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_disallowed_language",
        (request.form.get("sales_bot_disallowed_language") or "").strip()[:3000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_handoff_rules",
        (request.form.get("sales_bot_handoff_rules") or "").strip()[:3000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_crm_event_alert_emails",
        (request.form.get("sales_bot_crm_event_alert_emails") or "").strip()[:1000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_handoff_alert_phones",
        (request.form.get("sales_bot_handoff_alert_phones") or "").strip()[:500],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_objection_playbook",
        (request.form.get("sales_bot_objection_playbook") or "").strip()[:4000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_message_templates",
        (request.form.get("sales_bot_message_templates") or "").strip()[:4000],
    )

    # Info collection fields
    collect = []
    if request.form.get("collect_name"):
        collect.append("name")
    if request.form.get("collect_phone"):
        collect.append("phone")
    if request.form.get("collect_email"):
        collect.append("email")
    if request.form.get("collect_address"):
        collect.append("address")
    if request.form.get("collect_service"):
        collect.append("service_needed")
    db.update_brand_text_field(
        brand_id,
        "sales_bot_collect_fields",
        ",".join(collect) if collect else "",
    )

    # Closing procedure
    db.update_brand_text_field(
        brand_id,
        "sales_bot_closing_procedure",
        (request.form.get("sales_bot_closing_procedure") or "").strip()[:4000],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_closing_action",
        (request.form.get("sales_bot_closing_action") or "none").strip()[:50],
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_onboarding_link",
        (request.form.get("sales_bot_onboarding_link") or "").strip()[:500],
    )

    # Booking success message
    db.update_brand_text_field(
        brand_id,
        "sales_bot_booking_success_message",
        (request.form.get("sales_bot_booking_success_message") or "").strip()[:2000],
    )

    # Service area schedule (JSON)
    schedule = {}
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
        areas = (request.form.get(f"schedule_{day}") or "").strip()[:500]
        if areas:
            schedule[day] = areas
    db.update_brand_text_field(
        brand_id,
        "sales_bot_service_area_schedule",
        json.dumps(schedule) if schedule else "",
    )

    lead_form_config = _normalize_warren_lead_form_config(
        {
            "enabled": bool(request.form.get("lead_form_enabled")),
            "headline": (request.form.get("lead_form_headline") or "").strip()[:140],
            "intro": (request.form.get("lead_form_intro") or "").strip()[:800],
            "cta_label": (request.form.get("lead_form_cta_label") or "").strip()[:60],
            "success_title": (request.form.get("lead_form_success_title") or "").strip()[:80],
            "success_message": (request.form.get("lead_form_success_message") or "").strip()[:400],
            "service_label": (request.form.get("lead_form_service_label") or "").strip()[:80],
            "details_label": (request.form.get("lead_form_details_label") or "").strip()[:80],
            "service_options": (request.form.get("lead_form_service_options") or "").strip(),
            "show_service": bool(request.form.get("lead_form_show_service")),
            "show_email": bool(request.form.get("lead_form_show_email")),
            "show_company": bool(request.form.get("lead_form_show_company")),
            "show_address": bool(request.form.get("lead_form_show_address")),
            "show_message": bool(request.form.get("lead_form_show_message")),
            "auto_text_enabled": bool(request.form.get("lead_form_auto_text_enabled")),
            "require_sms_consent": bool(request.form.get("lead_form_require_sms_consent")),
            "consent_label": (request.form.get("lead_form_consent_label") or "").strip()[:220],
        },
        brand=brand,
    )
    db.update_brand_text_field(
        brand_id,
        "sales_bot_lead_form_config",
        json.dumps(lead_form_config, separators=(",", ":")),
    )

    flash("Lead assistant profile saved.", "success")
    return redirect(url_for("client.client_lead_assistant"))


@client_public_bp.route("/warren/form/<brand_slug>", methods=["GET", "POST"])
def public_lead_form(brand_slug):
    db = _get_db()
    brand = db.get_brand_by_slug(brand_slug)
    if not brand:
        abort(404)

    lead_form_config = _lead_form_config_for_brand(brand)
    if not lead_form_config.get("enabled"):
        abort(404)

    is_embed = str(request.args.get("embed") or "").strip().lower() in {"1", "true", "yes"}
    errors = []
    submitted = str(request.args.get("submitted") or "").strip().lower() in {"1", "true", "yes"}
    form_values = {
        "name": "",
        "phone": "",
        "email": "",
        "company": "",
        "service_needed": "",
        "address": "",
        "message": "",
        "sms_consent": False,
    }

    if request.method == "POST":
        form_values = {
            "name": (request.form.get("name") or "").strip()[:120],
            "phone": (request.form.get("phone") or "").strip()[:40],
            "email": (request.form.get("email") or "").strip()[:160],
            "company": (request.form.get("company") or "").strip()[:160],
            "service_needed": (request.form.get("service_needed") or "").strip()[:160],
            "address": (request.form.get("address") or "").strip()[:220],
            "message": (request.form.get("message") or "").strip()[:1500],
            "sms_consent": bool(request.form.get("sms_consent")),
        }

        normalized_phone = _normalize_client_phone_number(form_values["phone"])
        if not form_values["name"]:
            errors.append("Name is required.")
        if not normalized_phone:
            errors.append("A valid mobile number is required.")
        if lead_form_config.get("show_email") and form_values["email"] and "@" not in form_values["email"]:
            errors.append("Enter a valid email address.")
        if lead_form_config.get("show_service") and not form_values["service_needed"]:
            errors.append("Choose the service you need.")
        if lead_form_config.get("show_address") and not form_values["address"]:
            errors.append("Service address is required.")
        if lead_form_config.get("show_message") and not form_values["message"]:
            errors.append("Job details are required.")
        if lead_form_config.get("auto_text_enabled") and lead_form_config.get("require_sms_consent") and not form_values["sms_consent"]:
            errors.append("Text consent is required before Warren can text a quote.")

        if not errors:
            from webapp.warren_webhooks import _ingest_lead_submission

            message_text = form_values["message"]
            if not message_text and form_values["service_needed"]:
                message_text = f"Requested service: {form_values['service_needed']}"

            extra_fields = {
                "company_name": form_values["company"],
                "service_needed": form_values["service_needed"],
                "service_address": form_values["address"],
                "sms_consent": "yes" if form_values["sms_consent"] else "no",
                "form_type": "warren_hosted_form",
                "embed_mode": "iframe" if is_embed else "direct",
            }
            extra_fields = {key: value for key, value in extra_fields.items() if value}

            allow_auto_send = bool(
                lead_form_config.get("auto_text_enabled")
                and form_values["sms_consent"]
                and normalized_phone
            )

            _ingest_lead_submission(
                db,
                brand["id"],
                brand,
                external_id=f"warren_form_{brand_slug}_{uuid.uuid4().hex[:20]}",
                source="warren_hosted_form",
                lead_name=form_values["name"],
                lead_email=form_values["email"].lower(),
                lead_phone=normalized_phone,
                message_text=message_text,
                extra_fields=extra_fields,
                message_header="Warren Hosted Lead Form",
                allow_auto_send=allow_auto_send,
            )

            redirect_args = {"brand_slug": brand_slug, "submitted": 1}
            if is_embed:
                redirect_args["embed"] = 1
            return redirect(url_for("client_public.public_lead_form", **redirect_args))

    return render_template(
        "client/client_public_lead_form.html",
        brand=brand,
        lead_form_config=lead_form_config,
        submitted=submitted,
        errors=errors,
        form_values=form_values,
        is_embed=is_embed,
    )


@client_bp.route("/commercial")
@client_login_required
def client_commercial_prospecting():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    from webapp.commercial_prospector import COMMERCIAL_PROSPECT_TYPES

    maps_api_key = (
        brand.get("google_maps_api_key")
        or os.environ.get("GOOGLE_MAPS_API_KEY")
        or db.get_setting("google_maps_api_key")
        or ""
    ).strip()
    return render_template(
        "client_commercial_prospector.html",
        brand=brand,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        results=[],
        location=(brand.get("service_area") or "").strip(),
        search_criteria="",
        selected_types=[item["key"] for item in COMMERCIAL_PROSPECT_TYPES[:3]],
        prospect_types=COMMERCIAL_PROSPECT_TYPES,
        has_maps_api_key=bool(maps_api_key),
        imported_threads=_get_client_commercial_threads(db, brand_id),
        search_performed=False,
    )


@client_bp.route("/commercial/search", methods=["POST"])
@client_login_required
def client_commercial_search():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    from webapp.commercial_prospector import COMMERCIAL_PROSPECT_TYPES, search_commercial_prospects

    location = request.form.get("location", "").strip()
    search_criteria = _normalize_client_commercial_text(request.form.get("search_criteria", ""), 220)
    selected_types = [value.strip() for value in request.form.getlist("prospect_types") if value.strip()]
    max_results = request.form.get("max_results", "8").strip()
    try:
        max_results = max(3, min(int(max_results or 8), 15))
    except ValueError:
        max_results = 8

    maps_api_key = (
        brand.get("google_maps_api_key")
        or os.environ.get("GOOGLE_MAPS_API_KEY")
        or db.get_setting("google_maps_api_key")
        or ""
    ).strip()
    results = []
    if location:
        try:
            results = search_commercial_prospects(
                location,
                selected_types,
                api_key=maps_api_key,
                max_results_per_type=max_results,
                search_criteria=search_criteria,
            )
        except Exception as exc:
            flash(f"Commercial search failed: {str(exc)[:160]}", "error")
    else:
        flash("Enter a location before searching.", "error")

    return render_template(
        "client_commercial_prospector.html",
        brand=brand,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        results=results,
        location=location,
        search_criteria=search_criteria,
        selected_types=selected_types,
        prospect_types=COMMERCIAL_PROSPECT_TYPES,
        has_maps_api_key=bool(maps_api_key),
        imported_threads=_get_client_commercial_threads(db, brand_id),
        search_performed=True,
    )


@client_bp.route("/commercial/import", methods=["POST"])
@client_login_required
def client_commercial_import():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    raw_results = request.form.getlist("selected_results")
    selected = []
    for raw in raw_results:
        try:
            selected.append(json.loads(raw))
        except Exception:
            continue

    if not selected:
        flash("Select at least one commercial target to import.", "error")
        return redirect(url_for("client.client_commercial_prospecting"))

    from webapp.commercial_strategy import build_commercial_outreach_brief

    imported = 0
    updated = 0
    created_threads = []
    seen_identities = set()

    for item in selected:
        incoming_payload = _normalize_client_commercial_payload(item, default_service_area=brand.get("service_area") or "")
        identity_key = _client_commercial_identity_key(incoming_payload)
        if not identity_key or identity_key in seen_identities:
            continue
        seen_identities.add(identity_key)

        existing = db.find_brand_lead_thread(
            brand_id,
            email=incoming_payload.get("email") or "",
            lead_name=incoming_payload.get("business_name") or incoming_payload.get("name") or "",
            source="commercial_prospecting",
            website=incoming_payload.get("website") or "",
        )
        existing_payload = _build_client_commercial_payload(existing) if existing else {}
        prospect_payload = _merge_client_commercial_payload(existing_payload, incoming_payload) if existing else incoming_payload
        brief = build_commercial_outreach_brief(prospect_payload, brand=brand)
        prospect_payload.update({
            "outreach_angle": brief["outreach_angle"],
            "proposal_status": brief["proposal_readiness"]["status"],
            "pain_points_json": json.dumps(brief["pain_points"]),
            "next_action": (brief["next_actions"] or [""])[0],
            "summary": _build_client_commercial_summary(prospect_payload, brief),
        })
        summary = prospect_payload["summary"]

        if existing:
            thread_id = existing["id"]
            db.update_lead_thread_profile_fields(
                thread_id,
                brand_id,
                lead_name=prospect_payload["business_name"],
                lead_email=prospect_payload["email"] or existing.get("lead_email") or "",
                lead_phone=prospect_payload["phone"] or existing.get("lead_phone") or "",
                summary=summary,
            )
            db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect_payload))
            db.add_lead_event(
                brand_id,
                thread_id,
                "commercial_imported",
                "updated",
                metadata={
                    "account_name": prospect_payload["business_name"],
                    "website": prospect_payload.get("website") or "",
                },
            )
            updated += 1
        else:
            thread_id = db.create_lead_thread(
                brand_id,
                {
                    "lead_name": prospect_payload["business_name"],
                    "lead_email": prospect_payload["email"],
                    "lead_phone": prospect_payload["phone"],
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": summary,
                    "commercial_data_json": json.dumps(prospect_payload),
                },
            )
            db.add_lead_message(
                thread_id,
                "outbound",
                "system",
                f"Commercial target imported. Suggested first step: {prospect_payload['next_action']}",
                channel="commercial",
                metadata={
                    "subject": brief["subject"],
                    "email_body": brief["email_body"],
                    "call_opener": brief["call_opener"],
                },
            )
            db.add_lead_event(
                brand_id,
                thread_id,
                "commercial_imported",
                "created",
                metadata={
                    "account_name": prospect_payload["business_name"],
                    "website": prospect_payload.get("website") or "",
                },
            )
            imported += 1
        created_threads.append(thread_id)

    flash(f"Commercial import finished. {imported} new, {updated} updated in WARREN's pipeline.", "success")
    if len(created_threads) == 1:
        return redirect(url_for("client.client_commercial_thread", thread_id=created_threads[0]))
    return redirect(url_for("client.client_commercial_prospecting"))


@client_bp.route("/commercial/thread/<int:thread_id>")
@client_login_required
def client_commercial_thread(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    prospect, commercial_brief = _build_client_commercial_brief(thread, brand=brand)
    prospect["required_add_ons"] = _safe_json_list(prospect.get("required_add_ons_json"))
    prospect["walkthrough_photo_urls"] = _safe_json_list(prospect.get("walkthrough_photo_urls_json"))
    proposal_quote = db.get_lead_quote_for_thread(thread_id)
    proposal_preview = _build_client_commercial_proposal(prospect, brand=brand, existing_quote=proposal_quote)
    worksheet = _build_client_commercial_worksheet(prospect, commercial_brief, brand=brand)
    commercial_research = _build_client_commercial_research(prospect)
    service_visits = _prepare_client_commercial_service_visits(db.get_commercial_service_visits(thread_id))
    service_recap = _build_client_commercial_service_recap(prospect, service_visits)
    if (thread.get("source") or "") != "commercial_prospecting" and (thread.get("commercial_data_json") or "{}").strip() in {"", "{}"}:
        abort(404)

    return render_template(
        "client_commercial_detail.html",
        brand=brand,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
        thread=thread,
        commercial_prospect=prospect,
        commercial_brief=commercial_brief,
        commercial_research=commercial_research,
        worksheet=worksheet,
        proposal_quote=proposal_quote,
        proposal_preview=proposal_preview,
        service_visits=service_visits,
        service_recap=service_recap,
        proposal_frequency_options=COMMERCIAL_PROPOSAL_FREQUENCY,
        proposal_package_options=COMMERCIAL_PROPOSAL_PACKAGES,
        nurture_sequences=_get_client_commercial_nurture_sequences(db),
        nurture_enrollments=_get_client_commercial_nurture_state(db, thread_id),
        smtp_ready=bool(current_app.config.get("SMTP_USER") and current_app.config.get("SMTP_PASSWORD")),
        outreach_ai_ready=bool(_get_openai_api_key(brand)),
    )


@client_bp.route("/commercial/thread/<int:thread_id>/worksheet", methods=["POST"])
@client_login_required
def client_commercial_thread_save_worksheet(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread or not brand:
        abort(404)

    from webapp.commercial_strategy import build_commercial_outreach_brief

    prospect = _build_client_commercial_payload(thread)
    prospect = _apply_client_commercial_worksheet_form(prospect, request.form)

    builder = _normalize_client_commercial_proposal_builder(prospect.get("proposal_builder_json"), brand=brand, prospect=prospect)
    if not builder.get("property_count"):
        builder["property_count"] = prospect.get("property_count") or ""
    builder["waste_station_count"] = prospect.get("walkthrough_waste_station_count") or builder.get("waste_station_count") or 0
    builder["common_area_count"] = prospect.get("walkthrough_common_area_count") or builder.get("common_area_count") or 0
    builder["relief_area_count"] = prospect.get("walkthrough_relief_area_count") or builder.get("relief_area_count") or 0
    prospect["proposal_builder_json"] = json.dumps(builder)

    brief = build_commercial_outreach_brief(prospect, brand=brand)
    prospect["outreach_angle"] = brief["outreach_angle"]
    prospect["proposal_status"] = brief["proposal_readiness"]["status"]
    prospect["pain_points_json"] = json.dumps(brief["pain_points"])
    prospect["next_action"] = (brief["next_actions"] or [""])[0]
    prospect["summary"] = _build_client_commercial_summary(prospect, brief)

    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.update_lead_thread_profile_fields(
        thread_id,
        brand_id,
        lead_name=prospect.get("business_name") or prospect.get("name") or "Commercial Prospect",
        lead_email=prospect.get("email") or "",
        lead_phone=prospect.get("phone") or "",
        summary=prospect["summary"],
    )
    db.update_lead_thread_status(thread_id, summary=prospect["summary"])
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_worksheet_saved",
        event_value=(prospect.get("walkthrough_property_label") or prospect.get("business_name") or "Worksheet")[:200],
        metadata={
            "property_count": prospect.get("property_count") or "",
            "proposal_status": prospect.get("proposal_status") or "",
        },
    )
    flash("Lead worksheet saved.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/worksheet/research", methods=["POST"])
@client_login_required
def client_commercial_thread_promote_research(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread or not brand:
        abort(404)

    from webapp.commercial_strategy import build_commercial_outreach_brief

    prospect = _build_client_commercial_payload(thread)
    prospect = _apply_client_commercial_worksheet_form(prospect, request.form)
    prospect = _promote_client_commercial_research_to_worksheet(prospect)

    builder = _normalize_client_commercial_proposal_builder(prospect.get("proposal_builder_json"), brand=brand, prospect=prospect)
    if not builder.get("property_count"):
        builder["property_count"] = prospect.get("property_count") or ""
    builder["waste_station_count"] = prospect.get("walkthrough_waste_station_count") or builder.get("waste_station_count") or 0
    builder["common_area_count"] = prospect.get("walkthrough_common_area_count") or builder.get("common_area_count") or 0
    builder["relief_area_count"] = prospect.get("walkthrough_relief_area_count") or builder.get("relief_area_count") or 0
    prospect["proposal_builder_json"] = json.dumps(builder)

    brief = build_commercial_outreach_brief(prospect, brand=brand)
    prospect["outreach_angle"] = brief["outreach_angle"]
    prospect["proposal_status"] = brief["proposal_readiness"]["status"]
    prospect["pain_points_json"] = json.dumps(brief["pain_points"])
    prospect["next_action"] = (brief["next_actions"] or [""])[0]
    prospect["summary"] = _build_client_commercial_summary(prospect, brief)

    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.update_lead_thread_profile_fields(
        thread_id,
        brand_id,
        lead_name=prospect.get("business_name") or prospect.get("name") or "Commercial Prospect",
        lead_email=prospect.get("email") or "",
        lead_phone=prospect.get("phone") or "",
        summary=prospect["summary"],
    )
    db.update_lead_thread_status(thread_id, summary=prospect["summary"])
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_research_promoted",
        event_value=(prospect.get("decision_maker_role") or prospect.get("business_name") or "Research")[:200],
        metadata={
            "complaint_count": len(_safe_json_object(prospect.get("source_details_json")).get("complaint_signals") or []),
            "proposal_status": prospect.get("proposal_status") or "",
        },
    )
    flash("Public research promoted into the worksheet.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/worksheet/ai", methods=["POST"])
@client_login_required
def client_commercial_thread_ai_worksheet(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread or not brand:
        abort(404)

    from webapp.commercial_strategy import build_commercial_outreach_brief

    prospect = _build_client_commercial_payload(thread)
    prospect = _apply_client_commercial_worksheet_form(prospect, request.form)
    brief = build_commercial_outreach_brief(prospect, brand=brand)

    try:
        suggestions = _assist_client_commercial_worksheet(brand, prospect, brief)
    except Exception as exc:
        flash(f"AI worksheet assist failed: {str(exc)[:180]}", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    prospect = _merge_client_commercial_ai_worksheet_suggestions(prospect, suggestions)

    builder = _normalize_client_commercial_proposal_builder(prospect.get("proposal_builder_json"), brand=brand, prospect=prospect)
    if not builder.get("property_count"):
        builder["property_count"] = prospect.get("property_count") or ""
    builder["waste_station_count"] = prospect.get("walkthrough_waste_station_count") or builder.get("waste_station_count") or 0
    builder["common_area_count"] = prospect.get("walkthrough_common_area_count") or builder.get("common_area_count") or 0
    builder["relief_area_count"] = prospect.get("walkthrough_relief_area_count") or builder.get("relief_area_count") or 0
    prospect["proposal_builder_json"] = json.dumps(builder)

    refreshed_brief = build_commercial_outreach_brief(prospect, brand=brand)
    prospect["outreach_angle"] = refreshed_brief["outreach_angle"]
    prospect["proposal_status"] = refreshed_brief["proposal_readiness"]["status"]
    prospect["pain_points_json"] = json.dumps(refreshed_brief["pain_points"])
    prospect["next_action"] = (refreshed_brief["next_actions"] or [""])[0]
    prospect["summary"] = _build_client_commercial_summary(prospect, refreshed_brief)

    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.update_lead_thread_profile_fields(
        thread_id,
        brand_id,
        lead_name=prospect.get("business_name") or prospect.get("name") or "Commercial Prospect",
        lead_email=prospect.get("email") or "",
        lead_phone=prospect.get("phone") or "",
        summary=prospect["summary"],
    )
    db.update_lead_thread_status(thread_id, summary=prospect["summary"])
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_worksheet_ai_assisted",
        event_value=(prospect.get("business_name") or "Commercial Prospect")[:200],
        metadata={"filled_fields": sorted(list(suggestions.keys()))[:10]},
    )
    flash("WARREN filled the blank spots it could from the account context.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/proposal", methods=["POST"])
@client_login_required
def client_commercial_thread_build_proposal(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    prospect = _build_client_commercial_payload(thread)
    builder = _normalize_client_commercial_proposal_builder(
        {
            "selected_package": request.form.get("selected_package"),
            "service_frequency": request.form.get("service_frequency"),
            "service_days": request.form.get("service_days"),
            "property_count": request.form.get("property_count") or prospect.get("property_count"),
            "waste_station_count": request.form.get("waste_station_count"),
            "waste_station_rate": request.form.get("waste_station_rate"),
            "common_area_count": request.form.get("common_area_count"),
            "common_area_rate": request.form.get("common_area_rate"),
            "relief_area_count": request.form.get("relief_area_count"),
            "relief_area_rate": request.form.get("relief_area_rate"),
            "bag_refill_included": request.form.get("bag_refill_included"),
            "bag_refill_fee": request.form.get("bag_refill_fee"),
            "deodorizer_included": request.form.get("deodorizer_included"),
            "deodorizer_fee": request.form.get("deodorizer_fee"),
            "initial_cleanup_required": request.form.get("initial_cleanup_required"),
            "initial_cleanup_fee": request.form.get("initial_cleanup_fee"),
            "monthly_management_fee": request.form.get("monthly_management_fee"),
            "scope_summary": request.form.get("scope_summary"),
            "notes": request.form.get("notes"),
        },
        brand=brand,
        prospect=prospect,
    )
    prospect["proposal_builder_json"] = json.dumps(builder)

    existing_quote = db.get_lead_quote_for_thread(thread_id)
    proposal = _build_client_commercial_proposal(prospect, brand=brand, existing_quote=existing_quote)
    quote_status = (request.form.get("quote_status") or "").strip().lower()
    if quote_status not in {"draft", "sent", "approved"}:
        quote_status = "sent" if _parse_bool_flag(request.form.get("mark_sent")) else "draft"

    sent_at = (existing_quote or {}).get("sent_at") or ""
    accepted_at = (existing_quote or {}).get("accepted_at") or ""
    now_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if quote_status in {"sent", "approved"} and not sent_at:
        sent_at = now_text
    if quote_status == "approved":
        accepted_at = accepted_at or now_text
    elif quote_status != "approved":
        accepted_at = ""

    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.upsert_lead_quote(
        brand_id,
        thread_id,
        status=quote_status,
        quote_mode=proposal["quote"]["quote_mode"],
        amount_low=proposal["quote"]["amount_low"],
        amount_high=proposal["quote"]["amount_high"],
        currency=proposal["quote"]["currency"],
        line_items=proposal["quote"]["line_items"],
        summary=proposal["quote"]["summary"],
        follow_up_text=proposal["quote"]["follow_up_text"],
        sent_at=sent_at,
        accepted_at=accepted_at,
    )
    db.update_lead_thread_status(thread_id, quote_status=quote_status)
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_proposal_built",
        event_value=f"{proposal['builder']['package_label']} - ${proposal['monthly_total']:,.2f} monthly",
        metadata={
            "monthly_total": proposal["monthly_total"],
            "setup_total": proposal["setup_total"],
            "quote_status": quote_status,
            "selected_package": proposal["selected_package"],
        },
    )
    flash("Commercial proposal updated.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/send-email", methods=["POST"])
@client_login_required
def client_commercial_thread_send_email(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    prospect = _build_client_commercial_payload(thread)
    email_address = (prospect.get("email") or thread.get("lead_email") or "").strip().lower()
    if not email_address:
        flash("This commercial target does not have an email address yet.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    subject = (request.form.get("subject") or "").strip()[:255]
    message_text = (request.form.get("message") or "").strip()[:6000]
    if not subject or not message_text:
        flash("Subject and message are required before sending outreach.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    from webapp.email_sender import send_simple_email

    try:
        send_simple_email(
            current_app.config,
            email_address,
            subject,
            message_text,
            html=_build_client_commercial_email_html(message_text),
        )
    except Exception as exc:
        db.add_lead_event(
            brand_id,
            thread_id,
            "commercial_email_failed",
            event_value=subject[:200],
            metadata={"detail": str(exc)[:500], "to": email_address},
        )
        flash(f"Commercial outreach email failed: {str(exc)[:160]}", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    db.add_lead_message(
        thread_id,
        "outbound",
        "user",
        message_text,
        channel="email",
        metadata={
            "manual": True,
            "commercial_outreach": True,
            "subject": subject,
            "to": email_address,
        },
    )
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_email_sent",
        event_value=subject[:200],
        metadata={"to": email_address},
    )
    flash("Commercial outreach email sent.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/outreach", methods=["POST"])
@client_login_required
def client_commercial_thread_save_outreach(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    prospect = _build_client_commercial_payload(thread)
    subject = _normalize_client_commercial_text(request.form.get("subject"), 255)
    email_body = _normalize_client_commercial_text(request.form.get("message"), 6000)
    call_opener = _normalize_client_commercial_text(request.form.get("call_opener"), 2000)
    rewrite_prompt = _normalize_client_commercial_text(request.form.get("rewrite_prompt"), 1000)
    if not subject or not email_body:
        flash("Subject and email body are required to save outreach edits.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    prospect["outreach_subject_override"] = subject
    prospect["outreach_email_body_override"] = email_body
    prospect["outreach_call_opener_override"] = call_opener
    prospect["outreach_rewrite_prompt"] = rewrite_prompt
    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_outreach_saved",
        event_value=subject[:200],
        metadata={"has_call_opener": bool(call_opener), "customized": True},
    )
    flash("Commercial outreach draft saved.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/outreach/rewrite", methods=["POST"])
@client_login_required
def client_commercial_thread_rewrite_outreach(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread or not brand:
        abort(404)

    prospect, commercial_brief = _build_client_commercial_brief(thread, brand=brand)
    rewrite_prompt = _normalize_client_commercial_text(request.form.get("rewrite_prompt"), 1000)
    if not rewrite_prompt:
        flash("Add rewrite instructions before asking WARREN to rewrite the outreach.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    try:
        rewritten = _rewrite_client_commercial_outreach_assets(brand, prospect, commercial_brief, rewrite_prompt)
    except Exception as exc:
        flash(f"Outreach rewrite failed: {str(exc)[:180]}", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    prospect["outreach_subject_override"] = rewritten["subject"]
    prospect["outreach_email_body_override"] = rewritten["email_body"]
    prospect["outreach_call_opener_override"] = rewritten["call_opener"]
    prospect["outreach_rewrite_prompt"] = rewrite_prompt
    prospect["outreach_last_rewrite_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_outreach_rewritten",
        event_value=rewritten["subject"][:200],
        metadata={"rewrite_prompt": rewrite_prompt[:200]},
    )
    flash("WARREN rewrote the outreach draft.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/outreach/reset", methods=["POST"])
@client_login_required
def client_commercial_thread_reset_outreach(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    prospect = _build_client_commercial_payload(thread)
    prospect["outreach_subject_override"] = ""
    prospect["outreach_email_body_override"] = ""
    prospect["outreach_call_opener_override"] = ""
    prospect["outreach_last_rewrite_at"] = ""
    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_outreach_reset",
        event_value="generated_defaults",
        metadata={},
    )
    flash("Commercial outreach reset to WARREN's generated draft.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/enroll-drip", methods=["POST"])
@client_login_required
def client_commercial_thread_enroll_drip(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    try:
        sequence_id = int(request.form.get("sequence_id") or 0)
    except (TypeError, ValueError):
        sequence_id = 0
    if sequence_id <= 0:
        flash("Choose a commercial nurture sequence before enrolling.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    sequence = db.get_drip_sequence(sequence_id)
    if not sequence or not sequence.get("is_active") or (sequence.get("trigger") or "").strip().lower() != "commercial":
        flash("That nurture sequence is not available.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    prospect = _build_client_commercial_payload(thread)
    email_address = (prospect.get("email") or thread.get("lead_email") or "").strip().lower()
    if not email_address:
        flash("This commercial target needs an email address before drip enrollment.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    enrollment_id = db.enroll_in_drip(
        sequence_id,
        email_address,
        prospect.get("business_name") or prospect.get("name") or thread.get("lead_name") or "Commercial Prospect",
        lead_source="client_commercial",
        lead_id=thread_id,
    )
    if not enrollment_id:
        flash("This target is already active in that nurture sequence.", "warning")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    db.add_lead_message(
        thread_id,
        "outbound",
        "system",
        f"Enrolled in commercial nurture sequence '{sequence['name']}'.",
        channel="email",
        metadata={
            "commercial_nurture": True,
            "sequence_id": sequence_id,
            "enrollment_id": enrollment_id,
        },
    )
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_drip_enrolled",
        event_value=sequence["name"][:200],
        metadata={"sequence_id": sequence_id, "enrollment_id": enrollment_id},
    )
    flash(f"Enrolled in '{sequence['name']}'.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/qualification", methods=["POST"])
@client_login_required
def client_commercial_thread_qualification(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)
    brand = db.get_brand(brand_id)

    from webapp.commercial_strategy import (
        COMMERCIAL_QUALIFICATION_CORE_FIELDS,
        COMMERCIAL_QUALIFICATION_FIELDS,
        build_commercial_outreach_brief,
    )

    prospect = _build_client_commercial_payload(thread)
    try:
        answers = json.loads(prospect.get("qualification_answers_json") or "{}")
    except Exception:
        answers = {}
    for field in COMMERCIAL_QUALIFICATION_CORE_FIELDS:
        prospect[field["key"]] = request.form.get(field["key"], "").strip()
    for field in COMMERCIAL_QUALIFICATION_FIELDS:
        answers[field["key"]] = request.form.get(field["key"], "").strip()
    prospect["qualification_answers_json"] = json.dumps(answers)

    brief = build_commercial_outreach_brief(prospect, brand=brand)
    prospect["outreach_angle"] = brief["outreach_angle"]
    prospect["proposal_status"] = brief["proposal_readiness"]["status"]
    prospect["pain_points_json"] = json.dumps(brief["pain_points"])
    prospect["next_action"] = (brief["next_actions"] or [""])[0]
    prospect["summary"] = _build_client_commercial_summary(prospect, brief)

    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.update_lead_thread_profile_fields(
        thread_id,
        brand_id,
        lead_name=prospect.get("business_name") or prospect.get("name") or "Commercial Prospect",
        lead_email=prospect.get("email") or "",
        lead_phone=prospect.get("phone") or "",
        summary=prospect["summary"],
    )
    db.update_lead_thread_status(
        thread_id,
        summary=prospect["summary"],
    )
    db.add_lead_message(
        thread_id,
        "outbound",
        "system",
        f"Commercial qualification updated. {brief['qualification_summary']['complete_count']}/{brief['qualification_summary']['required_count']} required points confirmed.",
        channel="commercial",
    )
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_qualified",
        brief["proposal_readiness"]["status"],
        metadata={
            "complete_required": brief["qualification_summary"]["complete_count"],
            "required_total": brief["qualification_summary"]["required_count"],
        },
    )
    flash("Commercial qualification saved.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/walkthrough", methods=["POST"])
@client_login_required
def client_commercial_thread_walkthrough(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread or not brand:
        abort(404)

    from webapp.commercial_strategy import build_commercial_outreach_brief

    prospect = _build_client_commercial_payload(thread)
    prospect["property_count"] = (request.form.get("property_count") or prospect.get("property_count") or "").strip()[:160]
    prospect["walkthrough_property_label"] = (request.form.get("walkthrough_property_label") or "").strip()[:160]
    prospect["walkthrough_waste_station_count"] = _parse_int_range(request.form.get("walkthrough_waste_station_count"), maximum=500, default=prospect.get("walkthrough_waste_station_count") or 0)
    prospect["walkthrough_common_area_count"] = _parse_int_range(request.form.get("walkthrough_common_area_count"), maximum=500, default=prospect.get("walkthrough_common_area_count") or 0)
    prospect["walkthrough_relief_area_count"] = _parse_int_range(request.form.get("walkthrough_relief_area_count"), maximum=500, default=prospect.get("walkthrough_relief_area_count") or 0)
    prospect["pet_traffic_estimate"] = (request.form.get("pet_traffic_estimate") or "").strip()[:120]
    prospect["site_condition"] = (request.form.get("site_condition") or "").strip()[:220]
    prospect["access_notes"] = (request.form.get("access_notes") or "").strip()[:1000]
    prospect["gate_notes"] = (request.form.get("gate_notes") or "").strip()[:500]
    prospect["disposal_notes"] = (request.form.get("disposal_notes") or "").strip()[:500]
    prospect["walkthrough_notes"] = (request.form.get("walkthrough_notes") or "").strip()[:1200]
    prospect["required_add_ons_json"] = json.dumps(_normalize_client_commercial_list(request.form.get("required_add_ons") or "", max_items=8, item_max_len=120))
    prospect["walkthrough_photo_urls_json"] = json.dumps(_normalize_client_commercial_list(request.form.get("walkthrough_photo_urls") or "", max_items=8, item_max_len=500))

    if any([
        prospect.get("walkthrough_property_label"),
        prospect.get("walkthrough_waste_station_count"),
        prospect.get("walkthrough_common_area_count"),
        prospect.get("walkthrough_relief_area_count"),
        prospect.get("access_notes"),
        prospect.get("walkthrough_notes"),
    ]):
        prospect["walkthrough_completed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    builder = _normalize_client_commercial_proposal_builder(prospect.get("proposal_builder_json"), brand=brand, prospect=prospect)
    if not builder.get("property_count"):
        builder["property_count"] = prospect.get("property_count") or ""
    builder["waste_station_count"] = prospect.get("walkthrough_waste_station_count") or builder.get("waste_station_count") or 0
    builder["common_area_count"] = prospect.get("walkthrough_common_area_count") or builder.get("common_area_count") or 0
    builder["relief_area_count"] = prospect.get("walkthrough_relief_area_count") or builder.get("relief_area_count") or 0
    prospect["proposal_builder_json"] = json.dumps(builder)

    brief = build_commercial_outreach_brief(prospect, brand=brand)
    prospect["outreach_angle"] = brief["outreach_angle"]
    prospect["proposal_status"] = brief["proposal_readiness"]["status"]
    prospect["pain_points_json"] = json.dumps(brief["pain_points"])
    prospect["next_action"] = (brief["next_actions"] or [""])[0]
    prospect["summary"] = _build_client_commercial_summary(prospect, brief)

    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.update_lead_thread_profile_fields(
        thread_id,
        brand_id,
        lead_name=prospect.get("business_name") or prospect.get("name") or "Commercial Prospect",
        lead_email=prospect.get("email") or "",
        lead_phone=prospect.get("phone") or "",
        summary=prospect["summary"],
    )
    db.update_lead_thread_status(thread_id, summary=prospect["summary"])
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_walkthrough_saved",
        event_value=(prospect.get("walkthrough_property_label") or prospect.get("business_name") or "Walkthrough")[:200],
        metadata={
            "waste_station_count": prospect.get("walkthrough_waste_station_count") or 0,
            "common_area_count": prospect.get("walkthrough_common_area_count") or 0,
            "relief_area_count": prospect.get("walkthrough_relief_area_count") or 0,
        },
    )
    flash("Commercial walkthrough saved.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/service-visit", methods=["POST"])
@client_login_required
def client_commercial_thread_service_visit(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    prospect = _build_client_commercial_payload(thread)
    service_date = (request.form.get("service_date") or "").strip()[:20]
    summary = (request.form.get("summary") or "").strip()[:500]
    if not service_date or not summary:
        flash("Service date and visit summary are required.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    property_label = (request.form.get("property_label") or prospect.get("walkthrough_property_label") or prospect.get("business_name") or "Commercial site").strip()[:160]
    completed_by = (request.form.get("completed_by") or session.get("client_brand_name") or "Operations").strip()[:120]
    issues = _normalize_client_commercial_list(request.form.get("issues") or "", max_items=8, item_max_len=180)
    photos = _normalize_client_commercial_list(request.form.get("photo_urls") or "", max_items=8, item_max_len=500)
    client_note = (request.form.get("client_note") or "").strip()[:1200]
    internal_note = (request.form.get("internal_note") or "").strip()[:1200]
    waste_station_count_serviced = _parse_int_range(request.form.get("waste_station_count_serviced"), maximum=500, default=0)
    bags_restocked = _parse_bool_flag(request.form.get("bags_restocked"))
    gate_secured = _parse_bool_flag(request.form.get("gate_secured"))

    visit = db.add_commercial_service_visit(
        brand_id,
        thread_id,
        service_date=service_date,
        completed_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        completed_by=completed_by,
        property_label=property_label,
        summary=summary,
        waste_station_count_serviced=waste_station_count_serviced,
        bags_restocked=bags_restocked,
        gate_secured=gate_secured,
        issues=issues,
        photos=photos,
        client_note=client_note,
        internal_note=internal_note,
    )

    db.add_lead_message(
        thread_id,
        "outbound",
        "system",
        f"Service visit logged for {property_label} on {service_date}. {summary}",
        channel="commercial",
        metadata={"commercial_service_visit": True, "visit_id": (visit or {}).get("id")},
    )
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_service_visit_logged",
        event_value=summary[:200],
        metadata={
            "visit_id": (visit or {}).get("id"),
            "service_date": service_date,
            "property_label": property_label,
            "bags_restocked": bags_restocked,
            "gate_secured": gate_secured,
        },
    )
    flash("Commercial service visit logged.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/refresh", methods=["POST"])
@client_login_required
def client_commercial_thread_refresh(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)
    brand = db.get_brand(brand_id)

    prospect = _build_client_commercial_payload(thread)
    website = (prospect.get("website") or "").strip()
    if not website:
        flash("Add a website before refreshing the commercial audit.", "error")
        return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))

    from webapp.commercial_prospector import _extract_public_emails, extract_commercial_public_intel, extract_commercial_site_intel
    from webapp.competitor_intel import _scrape_website
    from webapp.commercial_strategy import build_commercial_outreach_brief

    try:
        source_details = json.loads(prospect.get("source_details_json") or "{}")
    except Exception:
        source_details = {}

    site_data = _scrape_website({"website": website}) or {}
    site_intel = extract_commercial_site_intel(
        website,
        business_name=prospect.get("business_name") or prospect.get("name") or "",
        prospect_type=prospect.get("account_type") or "",
    )
    public_intel = extract_commercial_public_intel(
        prospect.get("business_name") or prospect.get("name") or "",
        website=website,
        address=source_details.get("address") or "",
        service_area=prospect.get("service_area") or source_details.get("service_area") or "",
        prospect_type=prospect.get("account_type") or "",
        phone=prospect.get("phone") or source_details.get("phone") or "",
    )
    refresh_payload = _normalize_client_commercial_payload(
        {
            **prospect,
            "website": website,
            "emails": (public_intel.get("emails") or []) + _extract_public_emails(website),
            "audit_snapshot": {
                **(site_data if isinstance(site_data, dict) else {}),
                **({"site_intel": site_intel} if site_intel else {}),
                **({"public_intel": public_intel} if public_intel else {}),
            },
            **site_intel,
            "site_signals": site_intel.get("site_signals") or source_details.get("site_signals") or [],
            "scraped_page_count": site_intel.get("scraped_page_count") or source_details.get("scraped_page_count") or 0,
            "service_frequency_hint": site_intel.get("service_frequency_hint") or source_details.get("service_frequency_hint") or "",
            "service_days_hint": site_intel.get("service_days_hint") or source_details.get("service_days_hint") or "",
            "public_mentions": public_intel.get("public_mentions") or source_details.get("public_mentions") or [],
            "complaint_signals": public_intel.get("complaint_signals") or source_details.get("complaint_signals") or [],
            "decision_maker_contacts": public_intel.get("decision_maker_contacts") or source_details.get("decision_maker_contacts") or [],
            "decision_maker_role": public_intel.get("decision_maker_role_hint") or prospect.get("decision_maker_role") or source_details.get("decision_maker_role_hint") or "",
            "management_company": public_intel.get("management_company") or source_details.get("management_company") or "",
            "management_url": public_intel.get("management_url") or source_details.get("management_url") or "",
            "contact_urls": public_intel.get("contact_urls") or source_details.get("contact_urls") or [],
            "source_details_json": json.dumps(
                {
                    **source_details,
                    "website": website,
                    "service_area": prospect.get("service_area") or source_details.get("service_area") or "",
                    "address": source_details.get("address") or "",
                    "review_count": source_details.get("review_count") or 0,
                    "rating": source_details.get("rating"),
                    "maps_url": source_details.get("maps_url") or "",
                    "site_signals": site_intel.get("site_signals") or source_details.get("site_signals") or [],
                    "scraped_page_count": site_intel.get("scraped_page_count") or source_details.get("scraped_page_count") or 0,
                    "service_frequency_hint": site_intel.get("service_frequency_hint") or source_details.get("service_frequency_hint") or "",
                    "service_days_hint": site_intel.get("service_days_hint") or source_details.get("service_days_hint") or "",
                    "public_mentions": public_intel.get("public_mentions") or source_details.get("public_mentions") or [],
                    "complaint_signals": public_intel.get("complaint_signals") or source_details.get("complaint_signals") or [],
                    "decision_maker_contacts": public_intel.get("decision_maker_contacts") or source_details.get("decision_maker_contacts") or [],
                    "decision_maker_role_hint": public_intel.get("decision_maker_role_hint") or source_details.get("decision_maker_role_hint") or "",
                    "management_company": public_intel.get("management_company") or source_details.get("management_company") or "",
                    "management_url": public_intel.get("management_url") or source_details.get("management_url") or "",
                    "contact_urls": public_intel.get("contact_urls") or source_details.get("contact_urls") or [],
                }
            ),
        },
        default_service_area=prospect.get("service_area") or "",
    )
    prospect = _merge_client_commercial_payload(prospect, refresh_payload)
    brief = build_commercial_outreach_brief(prospect, brand=brand)
    prospect["outreach_angle"] = brief["outreach_angle"]
    prospect["proposal_status"] = brief["proposal_readiness"]["status"]
    prospect["pain_points_json"] = json.dumps(brief["pain_points"])
    prospect["next_action"] = (brief["next_actions"] or [""])[0]
    prospect["summary"] = _build_client_commercial_summary(prospect, brief)

    db.update_lead_thread_commercial_data(thread_id, brand_id, json.dumps(prospect))
    db.update_lead_thread_profile_fields(
        thread_id,
        brand_id,
        lead_name=prospect.get("business_name") or prospect.get("name") or "Commercial Prospect",
        lead_email=prospect.get("email") or "",
        lead_phone=prospect.get("phone") or "",
        summary=prospect["summary"],
    )
    db.update_lead_thread_status(thread_id, summary=prospect["summary"])
    db.add_lead_event(
        brand_id,
        thread_id,
        "commercial_refreshed",
        brief["proposal_readiness"]["status"],
        metadata={
            "outreach_angle": brief["outreach_angle"],
            "website": website,
        },
    )
    flash("Commercial brief refreshed.", "success")
    return redirect(url_for("client.client_commercial_thread", thread_id=thread_id))


@client_bp.route("/commercial/thread/<int:thread_id>/delete", methods=["POST"])
@client_login_required
def client_commercial_thread_delete(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        abort(404)

    db.delete_lead_thread(thread_id, brand_id)
    flash("Commercial target deleted.", "success")
    return redirect(url_for("client.client_commercial_prospecting"))


# ── Warren Inbox ──

@client_bp.route("/inbox")
@client_login_required
def client_inbox():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    threads = db.get_lead_threads(brand_id, limit=200)
    active_contacts = db.get_active_lead_contacts(brand_id, limit=150)
    active_profiles = [_build_lead_profile(db, contact) for contact in active_contacts]

    from webapp.warren_pipeline import get_pipeline_metrics
    metrics = get_pipeline_metrics(db, brand_id)

    return render_template(
        "client_inbox.html",
        brand=brand,
        threads=threads,
        active_profiles=active_profiles,
        metrics=metrics,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/inbox/thread/<int:thread_id>")
@client_login_required
def client_inbox_thread(thread_id):
    """JSON: get thread detail + messages."""
    db = _get_db()
    brand_id = session["client_brand_id"]

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return jsonify(error="Thread not found"), 404

    # Mark as read
    db.mark_lead_thread_read(thread_id)

    messages = db.get_lead_messages(thread_id)
    profile = _build_lead_profile(db, thread)
    return jsonify(thread=thread, messages=messages, profile=profile)


@client_bp.route("/inbox/thread/<int:thread_id>/profile", methods=["POST"])
@client_login_required
def client_inbox_profile_save(thread_id):
    db = _get_db()
    brand_id = session["client_brand_id"]

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return jsonify(error="Thread not found"), 404

    data = request.get_json(silent=True) or {}
    db.update_lead_thread_profile_fields(
        thread_id,
        brand_id,
        lead_name=(data.get("lead_name") or "").strip()[:200],
        lead_phone=(data.get("lead_phone") or "").strip()[:100],
        lead_email=(data.get("lead_email") or "").strip()[:255],
        summary=(data.get("summary") or thread.get("summary") or "").strip()[:1000],
    )

    closeability_override = data.get("closeability_pct")
    try:
        if closeability_override not in (None, ""):
            closeability_override = max(0, min(100, int(closeability_override)))
        else:
            closeability_override = None
    except (TypeError, ValueError):
        closeability_override = None

    dog_count = data.get("dog_count")
    try:
        if dog_count not in (None, ""):
            dog_count = max(0, min(20, int(dog_count)))
        else:
            dog_count = None
    except (TypeError, ValueError):
        dog_count = None

    db.save_lead_profile_override(
        thread_id,
        dog_count=dog_count,
        objections_text=(data.get("objections_text") or "").strip()[:1000],
        waiting_on_text=(data.get("waiting_on") or "").strip()[:500],
        closeability_pct=closeability_override,
        profile_notes=(data.get("profile_notes") or "").strip()[:2000],
    )

    refreshed_thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    profile = _build_lead_profile(db, refreshed_thread)
    return jsonify(ok=True, profile=profile, thread=refreshed_thread)


@client_bp.route("/inbox/thread/<int:thread_id>/reply", methods=["POST"])
@client_login_required
def client_inbox_reply(thread_id):
    """Send a manual reply from the inbox."""
    db = _get_db()
    brand_id = session["client_brand_id"]

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return jsonify(error="Thread not found"), 404

    data = request.get_json(silent=True) or {}
    message_text = (data.get("message") or "").strip()
    if not message_text:
        return jsonify(error="Message cannot be empty"), 400
    if len(message_text) > 2000:
        return jsonify(error="Message too long"), 400

    from webapp.warren_sender import send_manual_reply
    success, detail = send_manual_reply(db, brand_id, thread_id, message_text)

    return jsonify(ok=success, detail=detail)


@client_bp.route("/inbox/thread/<int:thread_id>/stage", methods=["POST"])
@client_login_required
def client_inbox_stage(thread_id):
    """Manually change a lead's pipeline stage."""
    db = _get_db()
    brand_id = session["client_brand_id"]

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return jsonify(error="Thread not found"), 404

    data = request.get_json(silent=True) or {}
    new_stage = (data.get("stage") or "").strip().lower()

    from webapp.warren_pipeline import manual_stage_change, PIPELINE_STAGES
    if new_stage not in PIPELINE_STAGES:
        return jsonify(error=f"Invalid stage: {new_stage}"), 400

    success, event_id = manual_stage_change(
        db, thread_id, brand_id, new_stage,
        changed_by=session.get("client_name", "client"),
    )

    if data.get("handoff"):
        db.update_lead_thread_status(thread_id, assigned_to="human")
        db.add_lead_event(brand_id, thread_id, "handoff_triggered", event_value="Manual handoff from inbox")

    return jsonify(ok=success)


@client_bp.route("/inbox/thread/<int:thread_id>/private", methods=["POST"])
@client_login_required
def client_inbox_toggle_private(thread_id):
    """Toggle a thread's private flag - Warren won't auto-reply to private threads."""
    db = _get_db()
    brand_id = session["client_brand_id"]

    new_val = db.toggle_lead_thread_private(thread_id, brand_id)
    if new_val is None:
        return jsonify(error="Thread not found"), 404

    return jsonify(ok=True, is_private=bool(new_val))


@client_bp.route("/inbox/thread/<int:thread_id>/delete", methods=["POST"])
@client_login_required
def client_inbox_delete(thread_id):
    """Delete a lead thread and all associated data."""
    db = _get_db()
    brand_id = session["client_brand_id"]

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return jsonify(error="Thread not found"), 404

    db.delete_lead_thread(thread_id, brand_id)
    return jsonify(ok=True)


@client_bp.route("/inbox/thread/<int:thread_id>/warren-draft", methods=["POST"])
@client_login_required
def client_inbox_warren_draft(thread_id):
    """Generate a Warren draft reply without sending it."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(error="Brand not found"), 404

    thread = db.get_lead_thread(thread_id, brand_id=brand_id)
    if not thread:
        return jsonify(error="Thread not found"), 404

    messages = db.get_lead_messages(thread_id)

    from webapp.warren_brain import generate_response
    result = generate_response(db, brand, thread, messages, channel=thread.get("channel", "sms"))

    if not result or not result.get("reply"):
        return jsonify(error="Warren could not generate a reply"), 500

    return jsonify(
        reply=result["reply"],
        action=result.get("action", "reply"),
        confidence=result.get("confidence", 0),
        internal_notes=result.get("internal_notes", ""),
    )


def _sng_revenue_availability(snapshot):
    if not isinstance(snapshot, dict) or not snapshot:
        return "not_synced"
    if snapshot.get("error"):
        return "unavailable"
    if not str(snapshot.get("synced_at") or "").strip():
        return "not_synced"

    debug_note = str(snapshot.get("debug_note") or "").strip().lower()
    unavailable_markers = (
        "returned no payments",
        "no accepted-payment events",
        "client_details payments and webhook history failed",
    )
    if any(marker in debug_note for marker in unavailable_markers):
        return "unavailable"
    return "available"


def _build_sng_crm_snapshot(data):
    kpis = data.get("kpis") or {}
    revenue = data.get("revenue") or {}

    active_total = int(kpis.get("active_clients") or 0)
    happy_total = int(kpis.get("happy_clients") or 0)
    happy_dogs_total = int(kpis.get("happy_dogs") or 0)
    inactive_total = int((data.get("inactive_pagination") or {}).get("total") or len(data.get("inactive") or []))
    no_subscription_total = int((data.get("no_sub_pagination") or {}).get("total") or len(data.get("no_subscription") or []))
    leads_total = int((data.get("leads_pagination") or {}).get("total") or len(data.get("leads") or []))
    free_quotes_total = len(data.get("free_quotes") or [])
    subscribed_total = max(active_total - no_subscription_total, 0)
    warm_pipeline_total = leads_total + free_quotes_total
    follow_up_queue = no_subscription_total + leads_total + free_quotes_total
    total_known_clients = active_total + inactive_total
    growth_surface_total = no_subscription_total + warm_pipeline_total + inactive_total

    coverage_pct = round((subscribed_total / active_total) * 100) if active_total else 0
    subscription_gap_pct = round((no_subscription_total / active_total) * 100) if active_total else 0
    happy_rate_pct = round((happy_total / active_total) * 100) if active_total else 0
    reactivation_share_pct = round((inactive_total / total_known_clients) * 100) if total_known_clients else 0
    dogs_per_happy_client = round(happy_dogs_total / happy_total, 1) if happy_total else 0.0
    warm_pipeline_pct = round((warm_pipeline_total / active_total) * 100) if active_total else 0

    priorities = []
    if no_subscription_total > 0:
        priorities.append({
            "title": "Convert active clients without a subscription",
            "count": no_subscription_total,
            "detail": f"{no_subscription_total} active clients are outside the recurring base, which is {subscription_gap_pct}% of active clients.",
            "tone": "warning",
        })
    if warm_pipeline_total > 0:
        priorities.append({
            "title": "Work the warm pipeline fast",
            "count": warm_pipeline_total,
            "detail": f"{leads_total} leads and {free_quotes_total} free quotes are sitting in queue right now.",
            "tone": "primary",
        })
    if inactive_total > 0:
        priorities.append({
            "title": "Run a reactivation push",
            "count": inactive_total,
            "detail": f"{inactive_total} inactive clients represent {reactivation_share_pct}% of known client records.",
            "tone": "danger",
        })
    if happy_total > 0:
        priorities.append({
            "title": "Use happy clients for referrals and reviews",
            "count": happy_total,
            "detail": f"{happy_total} happy clients give you a live advocacy pool without needing new acquisition spend.",
            "tone": "success",
        })

    if not priorities:
        priorities.append({
            "title": "No immediate CRM pressure detected",
            "count": 0,
            "detail": "This token is connected, but the current SNG data does not show a follow-up queue yet.",
            "tone": "secondary",
        })

    availability = _sng_revenue_availability(revenue)
    return {
        "panel_mode": "opportunity" if availability != "available" else "revenue",
        "revenue_availability": availability,
        "active_clients": active_total,
        "subscribed_clients": subscribed_total,
        "no_subscription_clients": no_subscription_total,
        "inactive_clients": inactive_total,
        "leads_total": leads_total,
        "free_quotes_total": free_quotes_total,
        "warm_pipeline_total": warm_pipeline_total,
        "growth_surface_total": growth_surface_total,
        "happy_clients": happy_total,
        "happy_dogs_total": happy_dogs_total,
        "follow_up_queue": follow_up_queue,
        "subscription_coverage_pct": coverage_pct,
        "subscription_gap_pct": subscription_gap_pct,
        "happy_client_rate_pct": happy_rate_pct,
        "reactivation_share_pct": reactivation_share_pct,
        "warm_pipeline_pct": warm_pipeline_pct,
        "known_client_base": total_known_clients,
        "dogs_per_happy_client": dogs_per_happy_client,
        "priorities": priorities[:3],
        "headline": (
            "Revenue is not available from this Sweep and Go token."
            if availability == "unavailable"
            else "Live client-base and pipeline totals from Sweep and Go."
        ),
        "subheadline": (
            "Using the live client base, warm pipeline, and reactivation pool instead of invented revenue."
            if availability == "unavailable"
            else "Revenue panel is available when Sweep and Go exposes payment history."
        ),
    }


@client_bp.route("/crm/data")
@client_login_required
def client_crm_data():
    """JSON endpoint: fetch all SNG data for the CRM tab."""
    def _revenue_cache_is_stale(snapshot, max_age_hours=20):
        if not isinstance(snapshot, dict):
            return True
        synced_at = str(snapshot.get("synced_at") or "").strip()
        if not synced_at:
            return True
        try:
            synced_dt = datetime.strptime(synced_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return True
        return (datetime.now() - synced_dt) > timedelta(hours=max_age_hours)

    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(error="Brand not found"), 404

    if brand.get("crm_type") != "sweepandgo" or not brand.get("crm_api_key"):
        return jsonify(error="Sweep and Go not connected"), 400

    from webapp.crm_bridge import (
        sng_count_active_clients, sng_count_happy_clients,
        sng_count_happy_dogs, sng_count_jobs,
        sng_get_active_clients, sng_get_inactive_clients,
        sng_get_active_no_subscription, sng_get_leads,
        sng_get_free_quotes, sng_get_cached_revenue,
        sng_sync_revenue,
    )

    data = {"kpis": {}, "clients": [], "inactive": [], "no_subscription": [],
            "leads": [], "free_quotes": [], "revenue": {}, "crm_snapshot": {}}

    # If ?sync=1 is passed, do a full revenue sync first
    do_sync = request.args.get("sync") == "1"

    # KPIs
    r, _ = sng_count_active_clients(brand)
    data["kpis"]["active_clients"] = r.get("data", 0) if isinstance(r, dict) else 0

    r, _ = sng_count_happy_clients(brand)
    data["kpis"]["happy_clients"] = r.get("data", 0) if isinstance(r, dict) else 0

    r, _ = sng_count_happy_dogs(brand)
    data["kpis"]["happy_dogs"] = r.get("data", 0) if isinstance(r, dict) else 0

    r, _ = sng_count_jobs(brand)
    data["kpis"]["completed_jobs"] = r.get("data", 0) if isinstance(r, dict) else 0

    # Active clients (page 1)
    r, _ = sng_get_active_clients(brand, page=1)
    if isinstance(r, dict):
        data["clients"] = r.get("data", [])
        data["clients_pagination"] = r.get("paginate", {})

    # Inactive clients (page 1)
    r, _ = sng_get_inactive_clients(brand, page=1)
    if isinstance(r, dict):
        data["inactive"] = r.get("data", [])
        data["inactive_pagination"] = r.get("paginate", {})

    # No subscription (page 1)
    r, _ = sng_get_active_no_subscription(brand, page=1)
    if isinstance(r, dict):
        data["no_subscription"] = r.get("data", [])
        data["no_sub_pagination"] = r.get("paginate", {})

    # Leads
    r, _ = sng_get_leads(brand, page=1)
    if isinstance(r, dict):
        data["leads"] = r.get("data", [])
        data["leads_pagination"] = r.get("paginate", {})

    # Free quotes
    r, _ = sng_get_free_quotes(brand)
    if isinstance(r, dict):
        data["free_quotes"] = r.get("free_quotes", [])

    # Revenue intelligence
    _log_agent("bridge", "Pulled CRM data", f"{data['kpis'].get('active_clients', 0)} active clients")
    try:
        if do_sync:
            # Full sync: sample clients, get real payments, cache results
            rev = sng_sync_revenue(brand, db, month=request.args.get("month"))
        else:
            # Normal page load: prefer cache, but auto-refresh if it's missing or stale.
            rev = sng_get_cached_revenue(brand, db)
            if _revenue_cache_is_stale(rev):
                rev = sng_sync_revenue(brand, db, month=request.args.get("month"))

        data["revenue"] = rev

        # Fetch ad spend for ROAS calculation
        rev_month = rev.get("revenue_month") or request.args.get("month") or datetime.now().strftime("%Y-%m")
        try:
            from webapp.report_runner import get_analysis_and_suggestions_for_brand

            force_refresh = (
                do_sync
                or (request.args.get("refresh") == "1")
                or _consume_login_refresh_month("analysis_refresh_month", rev_month)
            )
            analysis, _ = get_analysis_and_suggestions_for_brand(
                db, brand, rev_month, force_refresh=force_refresh
            )
            if analysis:
                roas_info = analysis.get("roas", {})
                data["revenue"]["ad_spend"] = roas_info.get("total_spend", 0)
                data["revenue"]["total_conversions"] = roas_info.get("total_conversions", 0)
                if rev.get("mrr") and roas_info.get("total_spend"):
                    data["revenue"]["blended_roas"] = round(rev["mrr"] / roas_info["total_spend"], 2)
                conversions = roas_info.get("total_conversions", 0)
                spend = roas_info.get("total_spend", 0)
                if conversions > 0 and spend > 0:
                    data["revenue"]["cost_per_acquisition"] = round(spend / conversions, 2)
        except Exception:
            pass  # ad spend data is optional

    except Exception as exc:
        import traceback
        data["revenue"] = {"error": str(exc), "traceback": traceback.format_exc()}

    data["revenue"]["availability"] = _sng_revenue_availability(data.get("revenue"))
    data["crm_snapshot"] = _build_sng_crm_snapshot(data)

    return jsonify(data)


@client_bp.route("/crm/sng/probe")
@client_login_required
def client_sng_probe():
    """Diagnostic: inspect raw SNG API response shapes to discover revenue fields."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand or brand.get("crm_type") != "sweepandgo" or not brand.get("crm_api_key"):
        return jsonify(error="SNG not configured"), 400

    from webapp.crm_bridge import (
        sng_get_active_clients, sng_get_dispatch_board, sng_get_client_details,
        sng_get_free_quotes, sng_welcome_v2, sng_check_token,
    )
    from datetime import datetime

    probe = {}

    # Basic auth/connectivity probes
    r, e = sng_welcome_v2(brand)
    probe["welcome_v2"] = r
    if e:
        probe["welcome_v2_error"] = e

    r, e = sng_check_token(brand)
    probe["check_token"] = r
    if e:
        probe["check_token_error"] = e

    # Sample 1 active client (full record)
    r, e = sng_get_active_clients(brand, page=1)
    if isinstance(r, dict) and r.get("data"):
        first_client = r["data"][0] if r["data"] else {}
        probe["active_client_sample_keys"] = sorted(first_client.keys()) if isinstance(first_client, dict) else str(type(first_client))
        probe["active_client_sample"] = first_client

        # If we have a client ID, probe client_details
        # SNG uses a string id under the `client` field (ex: rcl_XXXX)
        client_id = first_client.get("client") or first_client.get("id") or first_client.get("client_id")
        if client_id:
            dr, de = sng_get_client_details(brand, client_id)
            probe["client_details_sample_keys"] = sorted(dr.keys()) if isinstance(dr, dict) else str(type(dr))
            probe["client_details_sample"] = dr
            if de:
                probe["client_details_error"] = de
    else:
        probe["active_client_error"] = e

    # Sample 1 dispatch board day (today)
    today = datetime.now().strftime("%Y-%m-%d")
    r, e = sng_get_dispatch_board(brand, today)
    if isinstance(r, dict) and r.get("data"):
        first_job = r["data"][0] if r["data"] else {}
        probe["dispatch_job_sample_keys"] = sorted(first_job.keys()) if isinstance(first_job, dict) else str(type(first_job))
        probe["dispatch_job_sample"] = first_job
        probe["dispatch_job_count_today"] = len(r.get("data", []))
    else:
        probe["dispatch_error"] = e
        probe["dispatch_raw"] = r

    # Free quotes sample
    r, e = sng_get_free_quotes(brand)
    if isinstance(r, dict) and r.get("free_quotes"):
        first_quote = r["free_quotes"][0] if r["free_quotes"] else {}
        probe["free_quote_sample_keys"] = sorted(first_quote.keys()) if isinstance(first_quote, dict) else str(type(first_quote))
        probe["free_quote_sample"] = first_quote

    return jsonify(probe)


@client_bp.route("/crm/sng/create-coupon", methods=["POST"])
@client_login_required
def client_sng_create_coupon():
    """Create a coupon in SNG from the CRM tab."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand or brand.get("crm_type") != "sweepandgo":
        return jsonify(ok=False, error="SNG not configured"), 400

    from webapp.crm_bridge import sng_create_coupon
    payload = request.get_json(silent=True) or {}

    result, error = sng_create_coupon(
        brand,
        coupon_id=payload.get("coupon_id"),
        name=payload.get("name"),
        coupon_type=payload.get("coupon_type", "percent"),
        duration=payload.get("duration", "once"),
        percent_off=payload.get("percent_off"),
        amount_off=payload.get("amount_off"),
        redeem_by=payload.get("redeem_by"),
        max_redemptions=payload.get("max_redemptions"),
    )

    if error:
        return jsonify(ok=False, error=error)

    return jsonify(ok=True, coupon=result)


# ── Post Scheduler ──

@client_bp.route("/post-scheduler", methods=["GET", "POST"])
@client_login_required
def client_post_scheduler():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    if request.method == "POST":
        section = request.form.get("section", "")
        if section == "facebook_storytelling_profile":
            storytelling_strategy = request.form.get("facebook_storytelling_strategy", "")[:2000].strip()
            personality_raw = request.form.get("facebook_content_personality", "")
            cta_style_raw = request.form.get("facebook_cta_style", "")
            post_length_raw = request.form.get("facebook_post_length", "")
            storytelling_guardrails = request.form.get("facebook_storytelling_guardrails", "")[:2000].strip()
            recurring_characters = request.form.get("facebook_recurring_characters", "")[:6000].strip()
            content_personality = _normalize_facebook_content_personality(personality_raw, default="") if personality_raw.strip() else ""
            cta_style = _normalize_facebook_cta_style(cta_style_raw, default="") if cta_style_raw.strip() else ""
            post_length = _normalize_facebook_post_length(post_length_raw, default="") if post_length_raw.strip() else ""

            db.update_brand_text_field(brand_id, "facebook_storytelling_strategy", storytelling_strategy)
            db.update_brand_text_field(brand_id, "facebook_content_personality", content_personality)
            db.update_brand_text_field(brand_id, "facebook_cta_style", cta_style)
            db.update_brand_text_field(brand_id, "facebook_post_length", post_length)
            db.update_brand_text_field(brand_id, "facebook_storytelling_guardrails", storytelling_guardrails)
            db.update_brand_text_field(brand_id, "facebook_recurring_characters", recurring_characters)
            flash("Facebook story settings updated.", "success")
        return redirect(url_for("client.client_post_scheduler"))

    # Check Facebook connection
    connections = db.get_brand_connections(brand_id)
    meta_conn = connections.get("meta")
    if meta_conn and meta_conn.get("status") != "connected":
        meta_conn = None
    has_facebook = bool(meta_conn) and bool(brand.get("facebook_page_id"))

    # Check Drive connection
    google_conn = connections.get("google")
    if google_conn and google_conn.get("status") != "connected":
        google_conn = None
    has_drive = bool(google_conn) and "drive" in (google_conn.get("scopes") or "").lower() and bool(brand.get("google_drive_folder_id"))

    posts = db.get_scheduled_posts(brand_id)
    for post in posts:
        raw_type = (post.get("post_type") or "").strip()
        post["post_type_label"] = _facebook_post_type_label(raw_type)
    pending_count = sum(1 for p in posts if p.get("status") in ("pending", "scheduled"))
    has_ai_generation = bool(_get_openai_api_key(brand))

    return render_template(
        "client/client_post_scheduler.html",
        brand=brand,
        has_facebook=has_facebook,
        has_drive=has_drive,
        has_ai_generation=has_ai_generation,
        facebook_post_types=_FACEBOOK_POST_TYPE_OPTIONS,
        facebook_content_mixes=_FACEBOOK_CONTENT_MIXES,
        facebook_personality_options=_FACEBOOK_CONTENT_PERSONALITY_OPTIONS,
        facebook_cta_style_options=_FACEBOOK_CTA_STYLE_OPTIONS,
        facebook_post_length_options=_FACEBOOK_POST_LENGTH_OPTIONS,
        facebook_character_cadence_options=_FACEBOOK_CHARACTER_CADENCE_OPTIONS,
        facebook_recurring_characters=_parse_facebook_recurring_characters(brand.get("facebook_recurring_characters")),
        facebook_storytelling_profile=_build_facebook_storytelling_summary(brand),
        posts=posts,
        pending_count=pending_count,
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/post-scheduler/generate", methods=["POST"])
@client_login_required
def client_generate_facebook_post():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify(ok=False, error="No OpenAI API key. Add one in Settings > AI Configuration."), 400

    data = request.get_json(silent=True) or {}
    post_type = _normalize_facebook_post_type(data.get("post_type"))
    brief = (data.get("brief") or "").strip()[:600]
    character_name = (data.get("character_name") or "").strip()[:120]
    model = _pick_ai_model(brand, "ads")
    system_prompt, user_prompt = _build_facebook_post_generation_messages(brand, post_type, brief, character_name)

    import openai

    try:
        client = openai.OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.8,
            max_tokens=700,
        )
        result = _extract_json_object(resp.choices[0].message.content)
        message = str(result.get("message") or "").replace("—", "-").strip()
        if not message:
            return jsonify(ok=False, error="AI returned an empty message. Try again."), 400
        image_hint = str(result.get("image_hint") or "").replace("—", "-").strip()
        link_url = str(result.get("link_url") or "").strip() or _default_post_link_url(brand, post_type)
        _log_agent("spark", "Generated Facebook post", f"{_facebook_post_type_label(post_type)} for {brand.get('display_name') or brand.get('name') or ''}".strip())
        return jsonify(
            ok=True,
            post_type=post_type,
            post_type_label=_facebook_post_type_label(post_type),
            message=message[:5000],
            image_hint=image_hint[:160],
            link_url=link_url[:500],
        )
    except json.JSONDecodeError:
        return jsonify(ok=False, error="AI returned invalid format. Try again."), 400
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)[:200]), 400
    except Exception as exc:
        return jsonify(ok=False, error=str(exc)[:200]), 500


@client_bp.route("/post-scheduler/generate-calendar", methods=["POST"])
@client_login_required
def client_generate_facebook_calendar():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify(ok=False, error="No OpenAI API key. Add one in Settings > AI Configuration."), 400

    try:
        data = request.get_json(silent=True) or {}
        weeks = 4 if _coerce_int(data.get("weeks"), 2, minimum=2, maximum=4) >= 4 else 2
        posts_per_week = _coerce_int(data.get("posts_per_week"), 3, minimum=2, maximum=4)
        brief = (data.get("brief") or "").strip()[:1200]
        content_mix = _normalize_facebook_content_mix(data.get("content_mix"))
        selected_types = _normalize_facebook_post_type_list(data.get("post_types") or [])
        start_date_raw = (data.get("start_date") or "").strip()
        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()

        start_date = datetime.fromisoformat(start_date_raw).date() if start_date_raw else today + timedelta(days=1)
        if start_date < today:
            return jsonify(ok=False, error="Start date must be today or later."), 400

        calendar_plan = _build_facebook_calendar_plan(start_date, weeks, posts_per_week, selected_types, content_mix)
        if not calendar_plan:
            return jsonify(ok=False, error="Could not build a content calendar plan."), 400

        last_date = datetime.fromisoformat(calendar_plan[-1]["scheduled_at"]).replace(tzinfo=timezone.utc)
        if last_date > now_utc + timedelta(days=75):
            return jsonify(ok=False, error="Calendar must stay within Facebook's 75-day scheduling window."), 400

        model = _pick_ai_model(brand, "ads")
        system_prompt, user_prompt = _build_facebook_calendar_generation_messages(brand, calendar_plan, brief, content_mix)
        calendar_plan = _apply_recurring_characters_to_calendar_plan(
            calendar_plan,
            _parse_facebook_recurring_characters(brand.get("facebook_recurring_characters")),
        )

        import openai

        client = openai.OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.9,
            max_tokens=4500,
        )
        result = _extract_json_object(resp.choices[0].message.content)
        raw_posts = result.get("posts") or []
        if not isinstance(raw_posts, list) or len(raw_posts) < len(calendar_plan):
            return jsonify(ok=False, error="AI returned too few calendar posts. Try again."), 400

        posts = []
        for slot, raw_post in zip(calendar_plan, raw_posts):
            raw_post = raw_post or {}
            post_type = _normalize_facebook_post_type(raw_post.get("post_type") or slot["post_type"], default=slot["post_type"])
            message = str(raw_post.get("message") or "").replace("—", "-").strip()
            if not message:
                return jsonify(ok=False, error="AI returned an empty post in the calendar. Try again."), 400
            image_hint = str(raw_post.get("image_hint") or "").replace("—", "-").strip()
            link_url = str(raw_post.get("link_url") or "").strip() or _default_post_link_url(brand, post_type)
            posts.append(
                {
                    "post_type": post_type,
                    "post_type_label": _facebook_post_type_label(post_type),
                    "scheduled_at": slot["scheduled_at"],
                    "message": message[:5000],
                    "image_hint": image_hint[:160],
                    "link_url": link_url[:500],
                    "character_name": (slot.get("character_name") or "")[:120],
                }
            )

        _log_agent(
            "spark",
            "Generated Facebook calendar",
            f"{len(posts)} posts for {brand.get('display_name') or brand.get('name') or ''}".strip(),
        )
        return jsonify(
            ok=True,
            content_mix=content_mix,
            content_mix_label=_FACEBOOK_CONTENT_MIX_MAP[content_mix]["label"],
            weeks=weeks,
            posts_per_week=posts_per_week,
            total_posts=len(posts),
            posts=posts,
        )
    except json.JSONDecodeError:
        return jsonify(ok=False, error="AI returned invalid format. Try again."), 400
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)[:200]), 400
    except Exception as exc:
        current_app.logger.exception("facebook calendar generation failed for brand %s", brand_id)
        return jsonify(ok=False, error=str(exc)[:200]), 500


@client_bp.route("/post-scheduler/schedule", methods=["POST"])
@client_login_required
def client_schedule_post():
    """Schedule a single Facebook post via the Graph API."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    scheduled_at = (data.get("scheduled_at") or "").strip()
    link_url = (data.get("link_url") or "").strip()
    image_url = (data.get("image_url") or "").strip()
    post_type = _normalize_facebook_post_type(data.get("post_type"))

    if not message:
        return jsonify(ok=False, error="Message is required."), 400
    if not scheduled_at:
        return jsonify(ok=False, error="Schedule date is required."), 400

    # Validate schedule time (10 min to 75 days in the future)
    from datetime import datetime, timedelta, timezone
    try:
        sched_dt = datetime.fromisoformat(scheduled_at).replace(tzinfo=timezone.utc)
    except ValueError:
        return jsonify(ok=False, error="Invalid date format."), 400

    now = datetime.now(timezone.utc)
    if sched_dt < now + timedelta(minutes=10):
        return jsonify(ok=False, error="Must be at least 10 minutes in the future."), 400
    if sched_dt > now + timedelta(days=75):
        return jsonify(ok=False, error="Cannot schedule more than 75 days ahead."), 400

    page_id = brand.get("facebook_page_id", "")
    if not page_id:
        return jsonify(ok=False, error="No Facebook page connected."), 400

    # Get page access token
    from webapp.api_bridge import _get_meta_token, _get_page_access_token
    connections = db.get_brand_connections(brand_id)
    meta_conn = connections.get("meta")
    if not meta_conn or meta_conn.get("status") != "connected":
        return jsonify(ok=False, error="Meta account not connected. Reconnect in Connections."), 400
    user_token = _get_meta_token(db, brand_id, meta_conn)
    if not user_token:
        return jsonify(ok=False, error="Meta access token expired. Reconnect in Connections."), 400
    page_token = _get_page_access_token(page_id, user_token)
    if not page_token:
        return jsonify(ok=False, error="Could not get page access token. Check page permissions."), 400

    # Build the Graph API request
    import requests as req_lib
    unix_ts = int(sched_dt.timestamp())

    if image_url:
        # Photo post with scheduled time
        # If it's a local Drive proxy URL, convert to full URL
        if image_url.startswith("/client/api/drive/download/"):
            image_url = request.host_url.rstrip("/") + image_url

        fb_url = f"https://graph.facebook.com/v21.0/{page_id}/photos"
        payload = {
            "access_token": page_token,
            "url": image_url,
            "message": message,
            "scheduled_publish_time": unix_ts,
            "published": "false",
        }
    else:
        # Text/link post
        fb_url = f"https://graph.facebook.com/v21.0/{page_id}/feed"
        payload = {
            "access_token": page_token,
            "message": message,
            "scheduled_publish_time": unix_ts,
            "published": "false",
        }
        if link_url:
            payload["link"] = link_url

    try:
        resp = req_lib.post(fb_url, data=payload, timeout=30)
        resp_data = resp.json()
    except Exception as exc:
        db.save_scheduled_post(brand_id, "facebook", message, scheduled_at,
                               image_url=image_url, link_url=link_url, post_type=post_type)
        db.update_scheduled_post_status(
            db.get_scheduled_posts(brand_id, status="pending")[-1]["id"],
            "failed", error_message=str(exc))
        return jsonify(ok=False, error=f"Facebook API error: {exc}"), 500

    fb_post_id = resp_data.get("id") or resp_data.get("post_id") or ""

    if resp.status_code == 200 and fb_post_id:
        post_id = db.save_scheduled_post(brand_id, "facebook", message, scheduled_at,
                                         image_url=image_url, link_url=link_url, post_type=post_type)
        db.update_scheduled_post_status(post_id, "scheduled", fb_post_id=fb_post_id)
        return jsonify(ok=True, post_id=post_id, fb_post_id=fb_post_id, post_type=post_type)
    else:
        error_msg = resp_data.get("error", {}).get("message", resp.text[:300])
        post_id = db.save_scheduled_post(brand_id, "facebook", message, scheduled_at,
                                         image_url=image_url, link_url=link_url, post_type=post_type)
        db.update_scheduled_post_status(post_id, "failed", error_message=error_msg)
        return jsonify(ok=False, error=f"Facebook rejected the post: {error_msg}"), 400


@client_bp.route("/post-scheduler/schedule-bulk", methods=["POST"])
@client_login_required
def client_schedule_posts_bulk():
    """Schedule multiple posts from CSV upload."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify(ok=False, error="Brand not found"), 404

    data = request.get_json(silent=True) or {}
    posts = data.get("posts", [])
    if not posts:
        return jsonify(ok=False, error="No posts to schedule."), 400
    if len(posts) > 500:
        return jsonify(ok=False, error="Maximum 500 posts per upload."), 400

    page_id = brand.get("facebook_page_id", "")
    if not page_id:
        return jsonify(ok=False, error="No Facebook page connected."), 400

    from webapp.api_bridge import _get_meta_token, _get_page_access_token
    connections = db.get_brand_connections(brand_id)
    meta_conn = connections.get("meta")
    if not meta_conn or meta_conn.get("status") != "connected":
        return jsonify(ok=False, error="Meta account not connected."), 400
    user_token = _get_meta_token(db, brand_id, meta_conn)
    if not user_token:
        return jsonify(ok=False, error="Meta access token expired. Reconnect in Connections."), 400
    page_token = _get_page_access_token(page_id, user_token)
    if not page_token:
        return jsonify(ok=False, error="Could not get page access token."), 400

    from datetime import datetime, timedelta, timezone
    import requests as req_lib
    import time

    now = datetime.now(timezone.utc)
    scheduled = 0
    errors = 0

    for post in posts:
        message = (post.get("message") or "").strip()
        scheduled_at = (post.get("scheduled_at") or "").strip()
        image_url = (post.get("image_url") or "").strip()
        link_url = (post.get("link_url") or "").strip()
        post_type = _normalize_facebook_post_type(post.get("post_type"))

        if not message or not scheduled_at:
            errors += 1
            continue

        try:
            sched_dt = datetime.fromisoformat(scheduled_at).replace(tzinfo=timezone.utc)
        except ValueError:
            errors += 1
            continue

        if sched_dt < now + timedelta(minutes=10) or sched_dt > now + timedelta(days=75):
            errors += 1
            continue

        unix_ts = int(sched_dt.timestamp())

        if image_url:
            if image_url.startswith("/client/api/drive/download/"):
                image_url = request.host_url.rstrip("/") + image_url
            fb_url = f"https://graph.facebook.com/v21.0/{page_id}/photos"
            payload = {
                "access_token": page_token,
                "url": image_url,
                "message": message,
                "scheduled_publish_time": unix_ts,
                "published": "false",
            }
        else:
            fb_url = f"https://graph.facebook.com/v21.0/{page_id}/feed"
            payload = {
                "access_token": page_token,
                "message": message,
                "scheduled_publish_time": unix_ts,
                "published": "false",
            }
            if link_url:
                payload["link"] = link_url

        try:
            resp = req_lib.post(fb_url, data=payload, timeout=30)
            resp_data = resp.json()
            fb_post_id = resp_data.get("id") or resp_data.get("post_id") or ""

            post_id = db.save_scheduled_post(brand_id, "facebook", message, scheduled_at,
                                             image_url=image_url, link_url=link_url, post_type=post_type)
            if resp.status_code == 200 and fb_post_id:
                db.update_scheduled_post_status(post_id, "scheduled", fb_post_id=fb_post_id)
                scheduled += 1
            else:
                error_msg = resp_data.get("error", {}).get("message", "Unknown error")
                db.update_scheduled_post_status(post_id, "failed", error_message=error_msg)
                errors += 1
        except Exception as exc:
            post_id = db.save_scheduled_post(brand_id, "facebook", message, scheduled_at,
                                             image_url=image_url, link_url=link_url, post_type=post_type)
            db.update_scheduled_post_status(post_id, "failed", error_message=str(exc))
            errors += 1

        # Rate limit: small delay between API calls
        time.sleep(0.3)

    result = {"ok": True, "scheduled": scheduled, "errors": errors, "total": len(posts)}
    if errors and not scheduled:
        result["ok"] = False
        result["error"] = f"All {errors} posts failed. Check dates and content."
    return jsonify(result)


@client_bp.route("/post-scheduler/<int:post_id>", methods=["DELETE"])
@client_login_required
def client_delete_scheduled_post(post_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    post = db.get_scheduled_post(post_id)
    if not post or post.get("brand_id") != brand_id:
        return jsonify(ok=False, error="Post not found"), 404
    db.delete_scheduled_post(post_id, brand_id)
    return jsonify(ok=True)


# ── Help Center ──

@client_bp.route("/help")
@client_login_required
def client_help():
    topic = request.args.get("topic", "")
    guide = (request.args.get("guide", "connections") or "connections").strip().lower()
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id) or {}
    warren_onboarding = _build_warren_onboarding_context(db, brand, brand_id, session.get("client_user_id"))
    onboarding_interview = _build_warren_onboarding_interview(db, brand, brand_id, session.get("client_user_id"))
    if guide == "warren":
        return render_template(
            "client_help_warren.html",
            active_topic=topic,
            help_guide=guide,
            brand_name=session.get("client_brand_name", ""),
            warren_onboarding=warren_onboarding,
            onboarding_interview=onboarding_interview,
        )
    return render_template(
        "client_help.html",
        active_topic=topic,
        help_guide=guide,
        brand_name=session.get("client_brand_name", ""),
        warren_onboarding=warren_onboarding,
        onboarding_interview=onboarding_interview,
    )


@client_bp.route("/warren-onboarding/restart")
@client_login_required
def client_warren_onboarding_restart():
    db = _get_db()
    brand_id = session["client_brand_id"]
    client_user_id = session["client_user_id"]
    db.save_client_onboarding_progress(brand_id, client_user_id, "warren_onboarding_started", is_completed=True)
    db.save_client_onboarding_progress(brand_id, client_user_id, "warren_onboarding_v1", is_dismissed=False)
    return redirect(url_for("client.client_warren_onboarding"))


@client_bp.route("/va")
@client_login_required
def client_va_services():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand_name = session.get("client_brand_name", "")
    requests = db.get_va_requests(brand_id, limit=25)
    token_entries = db.get_va_token_entries(brand_id, limit=8)
    token_balance = db.get_va_token_balance(brand_id)
    active_statuses = {"submitted", "scoped", "queued", "in_progress", "review"}
    active_requests = sum(1 for item in requests if item.get("status") in active_statuses)
    completed_requests = sum(1 for item in requests if item.get("status") == "completed")
    current_role = session.get("client_role", "owner")
    specialty_options = [
        {"key": "wordpress_frontend", "label": "WordPress / Frontend Dev"},
        {"key": "ads_creative_hybrid", "label": "Ads + Creative Hybrid"},
        {"key": "local_seo_gbp", "label": "Local SEO / GBP Specialist"},
        {"key": "crm_automation", "label": "CRM / Automation Specialist"},
        {"key": "generalist_va", "label": "Generalist VA (fast executor, flexible)"},
        {"key": "account_qa", "label": "Account / QA Reviewer (internal)"},
    ]
    token_packs = [
        {
            "tokens": 50,
            "price": 59,
            "bonus_tokens": 0,
            "label": "Safe entry",
            "tagline": "For quick approvals when WARREN finds an obvious win.",
            "coverage": "Usually enough room for a focused burst of polish, cleanup, or iteration.",
            "examples": "Think page fixes, creative swaps, funnel cleanup, form repairs, or small SEO and site adjustments.",
        },
        {
            "tokens": 150,
            "price": 149,
            "bonus_tokens": 0,
            "label": "Most popular",
            "tagline": "Built for meaningful progress without needing to hire.",
            "coverage": "Comfortable room for a real sprint, not just a patch.",
            "examples": "Think audit-plus-implementation work, several creative rounds, landing page refinement, local SEO cleanup, or multiple follow-through tasks.",
        },
        {
            "tokens": 400,
            "price": 349,
            "bonus_tokens": 40,
            "label": "Serious operator",
            "tagline": "For brands actively shipping and clearing bottlenecks.",
            "coverage": "Strong capacity for larger projects or several approved missions moving at once.",
            "examples": "Think heavier page work, ad account cleanup, CRO implementation, local search improvements, and backlog-clearing execution in one push.",
        },
        {
            "tokens": 1000,
            "price": 790,
            "bonus_tokens": 150,
            "label": "Best value",
            "tagline": "Prepaid execution capacity for teams that want speed on standby.",
            "coverage": "Designed for brands that want execution ready whenever WARREN spots the next move.",
            "examples": "Think recurring page, creative, CRM, ads, and ops work across a sustained run instead of one-off tickets.",
        },
        {
            "tokens": 2500,
            "price": 1725,
            "bonus_tokens": 500,
            "label": "Aggressive growth",
            "tagline": "For operators who want backlog pressure gone.",
            "coverage": "Built for brands running multiple growth tracks and approving execution continuously.",
            "examples": "Think embedded execution capacity across campaigns, site work, local visibility, reporting, and repeated implementation waves.",
        },
    ]
    mission_catalog = [
        {
            "title": "Landing Page Audit",
            "tokens": 20,
            "description": "A bounded review with fixes and conversion recommendations WARREN can tee up fast.",
        },
        {
            "title": "Ad Creative Pack",
            "tokens": 30,
            "description": "Fresh ad copy and creative direction for campaigns WARREN flags as underperforming.",
        },
        {
            "title": "GBP + Local SEO Boost",
            "tokens": 45,
            "description": "Tight local visibility execution for brands losing traction in maps or organic local search.",
        },
        {
            "title": "Conversion Optimization Rebuild",
            "tokens": 85,
            "description": "A mission-sized rebuild for pages WARREN identifies as leaking leads.",
        },
        {
            "title": "Google Ads Setup + Optimization",
            "tokens": 95,
            "description": "Structured launch and tuning work when WARREN identifies setup gaps or scaling opportunities.",
        },
        {
            "title": "Full Page Build",
            "tokens": 120,
            "description": "A clearly scoped page implementation, not open-ended hourly development.",
        },
    ]
    execution_steps = [
        {
            "title": "WARREN identifies the problem",
            "copy": "Performance drops, broken pages, creative fatigue, or local visibility issues surface as a clear opportunity.",
        },
        {
            "title": "System creates the mission",
            "copy": "The work is packaged into a bounded mission with a defined scope, token cost, and expected outcome.",
        },
        {
            "title": "You approve execution",
            "copy": "No hiring. No freelancer wrangling. You approve the mission and the execution queue handles the rest.",
        },
    ]
    return render_template(
        "client/client_va_services.html",
        brand_name=brand_name,
        token_balance=token_balance,
        token_entries=token_entries,
        requests=requests,
        active_requests=active_requests,
        completed_requests=completed_requests,
        current_role=current_role,
        specialty_options=specialty_options,
        token_packs=token_packs,
        mission_catalog=mission_catalog,
        execution_steps=execution_steps,
    )


@client_bp.route("/va/request", methods=["POST"])
@client_login_required
def client_va_request_create():
    if not _require_role("owner", "manager"):
        abort(403)

    db = _get_db()
    brand_id = session["client_brand_id"]
    title = (request.form.get("title") or "").strip()
    details = (request.form.get("details") or "").strip()
    specialty_key = (request.form.get("specialty_key") or "generalist_va").strip().lower()
    priority = (request.form.get("priority") or "normal").strip().lower()
    allowed_specialties = {
        "wordpress_frontend",
        "ads_creative_hybrid",
        "local_seo_gbp",
        "crm_automation",
        "generalist_va",
        "account_qa",
    }

    if not title:
        flash("Request title is required.", "error")
        return redirect(url_for("client.client_va_services"))
    if not details:
        flash("Please add a short description so the VA Desk knows what to do.", "error")
        return redirect(url_for("client.client_va_services"))
    if specialty_key not in allowed_specialties:
        specialty_key = "generalist_va"
    if priority not in {"normal", "high", "urgent"}:
        priority = "normal"

    db.create_va_request(
        brand_id,
        title=title,
        details=details,
        specialty_key=specialty_key,
        priority=priority,
        requested_by=session.get("client_user_id"),
    )
    flash("VA request submitted. The desk queue now has your request.", "success")
    return redirect(url_for("client.client_va_services"))


@client_bp.route("/va/request/<int:request_id>/cancel", methods=["POST"])
@client_login_required
def client_va_request_cancel(request_id):
    if not _require_role("owner", "manager"):
        abort(403)

    db = _get_db()
    brand_id = session["client_brand_id"]
    request_row = db.get_va_request(request_id, brand_id)
    if not request_row:
        abort(404)
    if request_row.get("status") in {"completed", "cancelled"}:
        flash("That request is already closed.", "info")
        return redirect(url_for("client.client_va_services"))

    db.update_va_request(
        request_id,
        brand_id,
        status="cancelled",
        closed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        status_note="Cancelled by client",
    )
    flash("VA request cancelled.", "success")
    return redirect(url_for("client.client_va_services"))


# ── Beta Signup (public - no auth) ──

@client_bp.route("/beta", methods=["GET", "POST"])
def beta_signup():
    db = _get_db()
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        name = request.form.get("name", "").strip()
        if not email or not name:
            flash("Name and email are required.", "error")
            return render_template("beta_signup.html")

        existing = db.get_beta_tester_by_email(email)
        if existing:
            flash("That email is already registered for the beta.", "info")
            return render_template("beta_signup.html", success=True)

        data = {
            "name": name,
            "email": email,
            "business_name": request.form.get("business_name", "").strip(),
            "website": request.form.get("website", "").strip(),
            "industry": request.form.get("industry", "").strip(),
            "monthly_ad_spend": request.form.get("monthly_ad_spend", "").strip(),
            "platforms": ",".join(request.form.getlist("platforms")),
            "referral_source": request.form.get("referral_source", "").strip(),
            "meta_login_email": request.form.get("meta_login_email", "").strip().lower(),
            "google_business_email": request.form.get("google_business_email", "").strip().lower(),
            "facebook_page_id": request.form.get("facebook_page_id", "").strip(),
        }
        db.create_beta_tester(data)
        return render_template("beta_signup.html", success=True)
    return render_template("beta_signup.html")


# ── Beta Onboarding (public - token-based access) ──

@client_bp.route("/beta/onboarding/<token>", methods=["GET", "POST"])
def beta_onboarding(token):
    db = _get_db()
    tester = db.get_beta_tester_by_token(token)
    if not tester:
        flash("This onboarding link is invalid or has already been used.", "error")
        return redirect(url_for("client.beta_signup"))

    if request.method == "POST":
        meta_login_email = request.form.get("meta_login_email", "").strip().lower()
        google_business_email = request.form.get("google_business_email", "").strip().lower()
        facebook_page_id = request.form.get("facebook_page_id", "").strip()

        if not meta_login_email or not google_business_email or not facebook_page_id:
            flash("All fields are required.", "error")
            return render_template("beta_onboarding.html", tester=tester)

        db.update_beta_tester_onboarding(tester["id"], facebook_page_id, google_business_email, meta_login_email)
        return render_template("beta_onboarding.html", tester=tester, success=True)

    return render_template("beta_onboarding.html", tester=tester)


# ── Client Feedback ──

@client_bp.route("/feedback")
@client_login_required
def client_feedback():
    db = _get_db()
    brand_id = session["client_brand_id"]
    feedback = db.get_beta_feedback_for_brand(brand_id)
    return render_template(
        "client_feedback.html",
        feedback=feedback,
        brand_name=session.get("client_brand_name", ""),
    )


@client_bp.route("/feedback/submit", methods=["POST"])
@client_login_required
def client_feedback_submit():
    db = _get_db()
    brand_id = session["client_brand_id"]
    client_user_id = session["client_user_id"]
    category = request.form.get("category", "general").strip()
    rating = int(request.form.get("rating", 0) or 0)
    message = request.form.get("message", "").strip()
    page = request.form.get("page", "").strip()
    if not message:
        flash("Please enter your feedback.", "error")
        return redirect(url_for("client.client_feedback"))
    if rating < 0 or rating > 5:
        rating = 0
    db.create_beta_feedback(brand_id, client_user_id, category, rating, message, page)
    flash("Thanks for your feedback!", "success")
    return redirect(url_for("client.client_feedback"))


# ── Your Team (AI Agents) ──

AGENT_ROSTER = [
    {
        "key": "warren",
        "name": "W.A.R.R.E.N.",
        "role": "Chief Strategist",
        "description": "Your senior marketing strategist. Analyzes every data point across all channels, spots what matters, and tells you the single most important move to make right now.",
        "skills": ["Strategy", "Data Analysis", "Decision Making", "Budget Planning"],
    },
    {
        "key": "scout",
        "name": "Scout",
        "role": "Campaign Analyst",
        "description": "Watches your Google and Meta campaigns 24/7. Flags underperformers before they waste budget, identifies winners worth scaling, and tracks every dollar in and out.",
        "skills": ["Google Ads", "Meta Ads", "Performance Tracking", "ROI Analysis"],
    },
    {
        "key": "penny",
        "name": "Penny",
        "role": "Budget Guardian",
        "description": "Keeps your ad spend on track. Monitors daily pacing, catches overspend before it happens, spots wasted budget on bad placements, and makes sure every dollar works hard.",
        "skills": ["Budget Pacing", "Waste Detection", "Spend Alerts", "Cost Optimization"],
    },
    {
        "key": "atlas",
        "name": "Atlas",
        "role": "Market Forecaster",
        "description": "Forecasts demand and performance using your historical results, seasonality, competition signals, and current market trends. Helps you plan ad spend so you do not waste budget in slow periods and you capture busy periods.",
        "skills": ["Forecasting", "Seasonality", "Market Trends", "Budget Strategy"],
    },
    {
        "key": "ace",
        "name": "Ace",
        "role": "Ad Copywriter",
        "description": "Writes headlines that stop the scroll. Generates ad copy, tests variations, and learns what language your audience responds to so every ad gets sharper over time.",
        "skills": ["Headlines", "Ad Copy", "A/B Variations", "Call to Action"],
    },
    {
        "key": "radar",
        "name": "Radar",
        "role": "Reputation Manager",
        "description": "Guards your online reputation. Monitors Google Business reviews, tracks your star rating, flags negative reviews for response, and keeps your local presence strong.",
        "skills": ["Reviews", "Google Business", "Local SEO", "Reputation Alerts"],
    },
    {
        "key": "hawk",
        "name": "Hawk",
        "role": "Competitive Intel",
        "description": "Keeps one eye on your competitors at all times. Tracks their ad activity, website changes, review counts, and market positioning so you always know what you're up against.",
        "skills": ["Competitor Tracking", "Market Analysis", "Ad Monitoring", "Positioning"],
    },
    {
        "key": "pulse",
        "name": "Pulse",
        "role": "SEO & Analytics",
        "description": "Your organic growth engine. Tracks search rankings, monitors website traffic patterns, identifies keyword opportunities, and measures what content drives real leads.",
        "skills": ["SEO", "Google Analytics", "Search Console", "Keyword Tracking"],
    },
    {
        "key": "spark",
        "name": "Spark",
        "role": "Content Creator",
        "description": "Creates content that brings people to your business. Writes blog posts, social captions, and email content tuned to your brand voice and your audience's interests.",
        "skills": ["Blog Writing", "Social Media", "Email Copy", "Brand Voice"],
    },
    {
        "key": "bridge",
        "name": "Bridge",
        "role": "Lead Manager",
        "description": "Connects your marketing to your revenue. Tracks leads from first click to closed deal, monitors your CRM pipeline, and makes sure no opportunity slips through the cracks.",
        "skills": ["CRM", "Lead Tracking", "Pipeline", "Conversion Tracking"],
    },
    {
        "key": "chief",
        "name": "Weave",
        "role": "MAP Orchestrator",
        "description": "Takes feedback from the rest of the team, links related issues, maps KPI dependencies, and turns clutter into a Minimal Actionable Process so one fix does not quietly break another.",
        "skills": ["Cross-Agent Synthesis", "KPI Impact Mapping", "Action Prioritization", "Context Weaving"],
    },
]


@client_bp.route("/team")
@client_login_required
def client_team():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        abort(404)

    hired_agents = json.loads(brand.get("hired_agents") or "{}")

    # Warren is always hired - he's the team lead
    if "warren" not in hired_agents:
        hired_agents["warren"] = {
            "hired_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "trained": True,
            "training_complete": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        db.update_brand_text_field(brand_id, "hired_agents", json.dumps(hired_agents))

    return render_template(
        "client/client_team.html",
        brand=brand,
        agents_json=json.dumps(AGENT_ROSTER),
        hired_agents_json=json.dumps(hired_agents),
        brand_name=session.get("client_brand_name", brand.get("display_name", "")),
    )


@client_bp.route("/team/data")
@client_login_required
def client_team_data():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)

    latest = db.get_agent_latest(brand_id)
    activity = db.get_agent_activity(brand_id, limit=30)

    hired_agents = json.loads((brand or {}).get("hired_agents") or "{}")

    # Count today's tasks
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_count = sum(
        1 for a in activity if a.get("created_at", "").startswith(today_str)
    )
    total_count = db._conn().execute(
        "SELECT COUNT(*) FROM agent_activity WHERE brand_id = ?", (brand_id,)
    ).fetchone()[0]

    return jsonify({
        "agents": AGENT_ROSTER,
        "latest": latest,
        "activity": activity,
        "today_count": today_count,
        "total_count": total_count,
        "hired_agents": hired_agents,
    })


@client_bp.route("/team/hire", methods=["POST"])
@client_login_required
def client_team_hire():
    """Hire (activate) an agent for this brand."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"}), 404

    data = request.get_json(silent=True) or {}
    agent_key = data.get("agent_key", "")
    valid_keys = {a["key"] for a in AGENT_ROSTER}
    if agent_key not in valid_keys:
        return jsonify({"success": False, "error": "Unknown agent"}), 400

    hired = json.loads(brand.get("hired_agents") or "{}")
    if agent_key in hired:
        return jsonify({"success": True, "already": True, "hired_agents": hired})

    hired[agent_key] = {
        "hired_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trained": False,
        "training_complete": None,
    }
    db.update_brand_text_field(brand_id, "hired_agents", json.dumps(hired))
    return jsonify({"success": True, "hired_agents": hired})


@client_bp.route("/team/train", methods=["POST"])
@client_login_required
def client_team_train():
    """Mark an agent as trained (user provided context/instructions)."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"}), 404

    data = request.get_json(silent=True) or {}
    agent_key = data.get("agent_key", "")
    training_notes = data.get("training_notes", "")

    hired = json.loads(brand.get("hired_agents") or "{}")
    if agent_key not in hired:
        return jsonify({"success": False, "error": "Agent not hired yet"}), 400

    hired[agent_key]["trained"] = True
    hired[agent_key]["training_complete"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if training_notes:
        hired[agent_key]["training_notes"] = training_notes

    db.update_brand_text_field(brand_id, "hired_agents", json.dumps(hired))

    # Also save training notes as agent context
    if training_notes:
        agent_ctx = json.loads(brand.get("agent_context") or "{}")
        agent_ctx[agent_key] = training_notes
        db.update_brand_text_field(brand_id, "agent_context", json.dumps(agent_ctx))

    return jsonify({"success": True, "hired_agents": hired})


@client_bp.route("/team/findings")
@client_login_required
def client_team_findings():
    """Get agent findings for the current month."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")
    agent_key = request.args.get("agent")
    severity = request.args.get("severity")

    findings = db.get_agent_findings(
        brand_id, month=month, agent_key=agent_key,
        severity=severity, limit=50,
    )
    return jsonify({"findings": findings, "month": month})


@client_bp.route("/team/findings/<int:finding_id>/dismiss", methods=["POST"])
@client_login_required
def client_dismiss_finding(finding_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    db.dismiss_agent_finding(finding_id, brand_id)
    return jsonify({"success": True})


@client_bp.route("/team/findings/<int:finding_id>/vote", methods=["POST"])
@client_login_required
def client_vote_finding(finding_id):
    """Thumbs up/down on a finding. vote: 1 or -1, feedback: optional text."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    payload = request.get_json(silent=True) or {}
    vote = payload.get("vote", 0)
    if vote not in (1, -1):
        return jsonify({"success": False, "error": "vote must be 1 or -1"}), 400
    feedback = (payload.get("feedback") or "")[:500]
    db.vote_agent_finding(finding_id, brand_id, vote, feedback)
    return jsonify({"success": True})


@client_bp.route("/team/findings/<int:finding_id>/status", methods=["POST"])
@client_login_required
def client_update_finding_status(finding_id):
    """Move a finding through lifecycle: new -> acknowledged -> in_progress -> done."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    payload = request.get_json(silent=True) or {}
    new_status = payload.get("status", "")
    if new_status not in ("acknowledged", "in_progress", "done", "dismissed"):
        return jsonify({"success": False, "error": "Invalid status"}), 400
    db.update_finding_status(finding_id, brand_id, new_status)
    return jsonify({"success": True})


# ── Background agent run tracker ──
_agent_runs = {}  # brand_id -> {"status": "running"|"done"|"error", "result": {...}, "started": float}
_agent_runs_lock = threading.Lock()

_logger = logging.getLogger(__name__)


def _run_agents_background(app, brand_id, brand, api_key, month, instructions=""):
    """Run the full agent pipeline in a background thread."""
    with app.app_context():
        try:
            db = app.db
            db.clear_agent_findings(brand_id, month)

            def _on_progress(stage, detail=""):
                """Update the shared run dict with live progress."""
                with _agent_runs_lock:
                    run = _agent_runs.get(brand_id)
                    if not run:
                        return
                    progress = run.setdefault("progress", [])
                    progress.append({"stage": stage, "detail": detail})
                    run["current_stage"] = stage
                    run["current_detail"] = detail

            from webapp.agent_brains import run_all_agents
            results = run_all_agents(
                db, brand, brand_id, api_key,
                month=month, warren_instructions=instructions,
                progress_callback=_on_progress,
            )

            ran = [k for k, v in results.items() if v is not None and k != "_qa"]
            skipped = [k for k, v in results.items() if v is None and k != "_qa"]
            post_qa_findings = db.get_agent_findings(brand_id, month=month, limit=200)
            total_findings = len(post_qa_findings)

            qa = results.get("_qa", {})
            warren = qa.get("warren", {})
            qa_report = qa.get("qa_report", {})
            retried = qa.get("retried_agents", [])
            applied = warren.get("applied", {})

            with _agent_runs_lock:
                _agent_runs[brand_id] = {
                    "status": "done",
                    "result": {
                        "success": True,
                        "agents_ran": ran,
                        "agents_skipped": skipped,
                        "total_findings": total_findings,
                        "qa": {
                            "overall_grade": warren.get("overall_grade", "N/A"),
                            "overall_notes": warren.get("overall_notes", qa_report.get("team_notes", "")),
                            "shipped": applied.get("shipped", 0),
                            "killed": applied.get("killed", 0),
                            "reworked": applied.get("rework", 0),
                            "retried_agents": retried,
                            "pre_test_issues": len(qa_report.get("pre_test_issues", [])),
                            "weave_reviews": len(qa_report.get("weave_reviews") or qa_report.get("chief_reviews", [])),
                            "chief_reviews": len(qa_report.get("weave_reviews") or qa_report.get("chief_reviews", [])),
                            "map_groups": len(qa_report.get("map_groups") or []),
                            "task_plan": len(warren.get("task_plan") or []),
                            "focus_message": warren.get("focus_message", ""),
                        },
                    },
                }
        except Exception as e:
            _logger.exception("Background agent run failed for brand %s: %s", brand_id, e)
            with _agent_runs_lock:
                _agent_runs[brand_id] = {
                    "status": "error",
                    "result": {"success": False, "error": str(e)[:200]},
                }


@client_bp.route("/team/run", methods=["POST"])
@client_login_required
def client_team_run():
    """Kick off agent run in background thread. Returns immediately."""
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    if not brand:
        return jsonify({"success": False, "error": "Brand not found"}), 404

    api_key = _get_openai_api_key(brand)
    if not api_key:
        return jsonify({"success": False, "error": "No OpenAI API key configured."}), 400

    payload = request.get_json(silent=True) or {}
    month = payload.get("month") or datetime.now().strftime("%Y-%m")
    instructions = (payload.get("instructions") or "").strip()

    # Check if already running
    with _agent_runs_lock:
        existing = _agent_runs.get(brand_id, {})
        if existing.get("status") == "running":
            elapsed = time.time() - existing.get("started", 0)
            if elapsed < 300:  # 5 min safety cap
                return jsonify({"success": True, "status": "running", "elapsed": int(elapsed)})

    # Start background run
    with _agent_runs_lock:
        _agent_runs[brand_id] = {"status": "running", "started": time.time(), "result": None}

    app = current_app._get_current_object()
    t = threading.Thread(
        target=_run_agents_background,
        args=(app, brand_id, brand, api_key, month, instructions),
        daemon=True,
    )
    t.start()

    return jsonify({"success": True, "status": "running"})


@client_bp.route("/team/run/status", methods=["GET"])
@client_login_required
def client_team_run_status():
    """Poll for agent run completion."""
    brand_id = session["client_brand_id"]

    with _agent_runs_lock:
        run = _agent_runs.get(brand_id)

    if not run:
        return jsonify({"status": "idle"})

    if run["status"] == "running":
        elapsed = time.time() - run.get("started", 0)
        return jsonify({
            "status": "running",
            "elapsed": int(elapsed),
            "current_stage": run.get("current_stage", ""),
            "current_detail": run.get("current_detail", ""),
            "progress": run.get("progress", []),
        })

    # Done or error - return result and clear
    result = run.get("result", {})
    with _agent_runs_lock:
        _agent_runs.pop(brand_id, None)

    return jsonify({"status": run["status"], **result})


# ── Drip Unsubscribe (public, no auth) ──

@client_bp.route("/unsubscribe/<int:enrollment_id>")
def drip_unsubscribe(enrollment_id):
    """One-click unsubscribe from drip emails."""
    db = _get_db()
    db.complete_drip_enrollment(enrollment_id, "unsubscribed")
    return """<!DOCTYPE html>
<html><head><title>Unsubscribed</title>
<style>body{font-family:Inter,Arial,sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f9fafb;}
.card{text-align:center;padding:40px;background:#fff;border-radius:12px;box-shadow:0 1px 3px rgba(0,0,0,.1);max-width:420px;}
h2{color:#1f2937;margin-bottom:8px;}p{color:#6b7280;}</style></head>
<body><div class="card"><h2>You've been unsubscribed</h2>
<p>You won't receive any more emails from this sequence. If this was a mistake, reply to any previous email and we'll re-enroll you.</p>
</div></body></html>""", 200


# ── Public Signup (no auth, cross-origin JSON) ──

@client_bp.route("/signup", methods=["POST", "OPTIONS"])
def public_signup():
    """Normal signup intake form - saves lead for admin to build brand."""
    if request.method == "OPTIONS":
        return _cors_preflight()

    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    business_name = (data.get("business_name") or "").strip()
    industry = (data.get("industry") or "").strip()

    if not name or not email or not business_name or not industry:
        return _cors_json({"ok": False, "error": "Name, email, business name, and industry are required."}, 400)

    db = _get_db()
    db._conn().execute(
        """INSERT INTO signup_leads
           (name, email, phone, business_name, website, industry, service_area,
            primary_services, monthly_budget, platforms, goals, referral_source)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            name, email,
            (data.get("phone") or "").strip(),
            business_name,
            (data.get("website") or "").strip(),
            industry,
            (data.get("service_area") or "").strip(),
            (data.get("primary_services") or "").strip(),
            (data.get("monthly_budget") or "").strip(),
            ",".join(data.get("platforms") or []) if isinstance(data.get("platforms"), list) else (data.get("platforms") or ""),
            ",".join(data.get("goals") or []) if isinstance(data.get("goals"), list) else (data.get("goals") or ""),
            (data.get("referral_source") or "").strip(),
        ],
    )
    db._conn().commit()
    return _cors_json({"ok": True})


# ── Public AI Assessment (no auth, cross-origin JSON) ──

@client_bp.route("/assess-widget", methods=["GET"])
def assess_widget():
    """Serve the assessment widget HTML so WordPress can load it via script tag."""
    import os
    tpl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "client", "assessment_form.html")
    with open(tpl, encoding="utf-8") as f:
        html = f.read()
    resp = make_response(html)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Cache-Control"] = "no-cache, max-age=300"
    return resp


@client_bp.route("/assess-ping", methods=["GET"])
def assess_ping():
    """Simple ping to verify the assessment module is loaded."""
    return _cors_json({"ok": True, "v": "ea743a6"})


@client_bp.route("/assess", methods=["POST", "OPTIONS"])
def public_assess():
    """Free AI assessment lead magnet - runs GBP, website, ad, and benchmark checks."""
    if request.method == "OPTIONS":
        return _cors_preflight()

    try:
        return _run_assessment()
    except Exception as exc:
        import traceback, logging
        logging.getLogger(__name__).error("[ASSESS] %s", traceback.format_exc())
        return _cors_json({"ok": False, "error": f"Assessment failed: {str(exc)[:200]}"}, 500)


def _run_assessment():
    import json as _json
    import requests as _req
    from webapp.google_business import get_place_details, score_profile_completeness
    from webapp.competitor_intel import _scrape_website, _scrape_meta_ads

    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    business_name = (data.get("business_name") or "").strip()
    industry = (data.get("industry") or "").strip()
    service_area = (data.get("service_area") or "").strip()
    website = (data.get("website") or "").strip()
    gmb_url = (data.get("gmb_url") or "").strip()
    facebook_url = (data.get("facebook_url") or "").strip()

    if not name or not email or not business_name or not industry:
        return _cors_json({"ok": False, "error": "Name, email, business name, and industry are required."}, 400)

    results = {"business_name": business_name, "industry_label": industry.replace("_", " ").title()}
    scores = []

    # ── 1. GBP Check ──
    gbp_data = {"score": 0, "findings": [], "icon": "geo-alt"}
    place_id = None
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")

    if gmb_url:
        pid_match = re.search(r"place_id[=:]([A-Za-z0-9_-]+)", gmb_url)
        if pid_match:
            place_id = pid_match.group(1)

    if not place_id and api_key and business_name:
        try:
            search_resp = _req.post(
                "https://places.googleapis.com/v1/places:searchText",
                json={"textQuery": f"{business_name} {service_area}", "maxResultCount": 1},
                headers={
                    "X-Goog-Api-Key": api_key,
                    "X-Goog-FieldMask": "places.id",
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            if search_resp.status_code == 200:
                places_list = search_resp.json().get("places", [])
                if places_list:
                    place_id = places_list[0].get("id")
        except Exception:
            pass

    if place_id and api_key:
        place = get_place_details(api_key, place_id)
        if place:
            comp = score_profile_completeness(place)
            gbp_data["score"] = comp["score"]
            gbp_data["found"] = True
            gbp_data["name"] = place.get("displayName", {}).get("text", "")
            gbp_data["rating"] = place.get("rating", 0)
            gbp_data["review_count"] = place.get("userRatingCount", 0)
            gbp_data["missing"] = [f for f, passed in comp["details"].items() if not passed]
            for field, passed in comp["details"].items():
                gbp_data["findings"].append({"label": field, "pass": passed})
            rating = place.get("rating", 0)
            count = place.get("userRatingCount", 0)
            gbp_data["findings"].append({
                "label": f"Rating: {rating} stars from {count} reviews",
                "pass": rating >= 4.0 and count >= 10,
            })
            scores.append(comp["score"])
        else:
            gbp_data["skipped"] = True
            gbp_data["findings"].append({"label": "Could not load Google Business Profile data", "pass": False})
    elif not api_key:
        gbp_data["skipped"] = True
        gbp_data["findings"].append({"label": "GBP check coming soon - claim yours at business.google.com", "pass": False})
    else:
        gbp_data["findings"].append({"label": "No Google Business Profile found - this is costing you leads", "pass": False})
        scores.append(0)

    results["gbp"] = gbp_data

    # ── 2. Website SEO Check ──
    seo_data = {"score": 0, "findings": []}
    if website:
        site_info = _scrape_website({"website": website})
        if site_info and not site_info.get("error"):
            pts = 0
            total = 5

            has_title = bool(site_info.get("title"))
            seo_data["findings"].append({"label": f"Title tag: {site_info.get('title', 'MISSING')[:80]}", "pass": has_title})
            if has_title:
                pts += 1

            has_desc = bool(site_info.get("description"))
            seo_data["findings"].append({
                "label": "Meta description: " + (site_info.get("description", "")[:80] or "MISSING"),
                "pass": has_desc,
            })
            if has_desc:
                pts += 1

            h1s = site_info.get("h1", [])
            good_h1 = len(h1s) >= 1 and h1s[0].lower() not in ("home", "welcome", "")
            seo_data["findings"].append({
                "label": f"H1 heading: {h1s[0][:60] if h1s else 'MISSING'}" + (" (too generic)" if h1s and h1s[0].lower() in ("home", "welcome") else ""),
                "pass": good_h1,
            })
            if good_h1:
                pts += 1

            has_ssl = site_info.get("url", "").startswith("https")
            seo_data["findings"].append({"label": "SSL/HTTPS: " + ("Yes" if has_ssl else "No"), "pass": has_ssl})
            if has_ssl:
                pts += 1

            h2s = site_info.get("h2", [])
            seo_data["findings"].append({"label": f"Content structure: {len(h2s)} section headings found", "pass": len(h2s) >= 3})
            if len(h2s) >= 3:
                pts += 1

            seo_data["score"] = round(pts / total * 100)
            seo_data["title"] = site_info.get("title", "")
            seo_data["description"] = site_info.get("description", "")
            seo_data["ssl"] = has_ssl
            seo_data["has_h1"] = good_h1
            seo_data["h2_count"] = len(h2s)
            seo_data["scanned"] = True
        else:
            err_msg = site_info.get("error", "") if site_info else ""
            seo_data["findings"].append({"label": f"Could not load website{' - ' + err_msg[:60] if err_msg else ''}", "pass": False})
    else:
        seo_data["findings"].append({"label": "No website provided", "pass": False})

    scores.append(seo_data["score"])
    results["website_seo"] = seo_data

    # ── 3. Ad Presence (Meta Ad Library) ──
    ad_data = {"score": 0, "findings": []}
    meta_token = os.environ.get("META_SYSTEM_TOKEN", "")
    search_name = facebook_url.rstrip("/").split("/")[-1] if facebook_url else business_name
    if meta_token and search_name:
        meta_info = _scrape_meta_ads({"name": search_name}, meta_token)
        if meta_info:
            ad_count = meta_info.get("active_ad_count", 0)
            ad_data["active_count"] = ad_count
            ad_data["findings"].append({
                "label": f"{ad_count} active Facebook/Instagram ad(s) found",
                "pass": ad_count > 0,
            })
            if ad_count == 0:
                ad_data["findings"].append({"label": "Your competitors may be running ads while you're invisible on social", "pass": False})
                ad_data["score"] = 20
            else:
                ad_data["score"] = min(80 + ad_count * 2, 100)
                samples = meta_info.get("sample_ads", [])[:3]
                for s in samples:
                    titles = s.get("titles", [])
                    if titles:
                        ad_data["findings"].append({"label": f"Ad: \"{titles[0][:60]}\"", "pass": True})
            scores.append(ad_data["score"])
        else:
            ad_data["active_count"] = 0
            ad_data["findings"].append({"label": "No Facebook ad activity found", "pass": False})
            ad_data["score"] = 10
            scores.append(ad_data["score"])
    else:
        ad_data["active_count"] = 0
        ad_data["skipped"] = True
        ad_data["findings"].append({"label": "Ad presence check coming soon", "pass": False})
    results["ad_presence"] = ad_data

    # ── 4. Industry Benchmarks ──
    bench_data = {"findings": [], "industry": industry}
    benchmarks_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "benchmarks.json")
    benchmarks = {}
    try:
        with open(benchmarks_path) as f:
            benchmarks = _json.load(f)
        g = benchmarks.get("google_ads", {}).get(industry, {})
        m = benchmarks.get("meta_ads", {}).get(industry, {})
        if g:
            bench_data["google_ads"] = g
            bench_data["findings"].append({"label": f"Google Ads avg CPC in your industry: ${g.get('cpc', 'N/A')}", "pass": True})
            bench_data["findings"].append({"label": f"Google Ads avg cost per lead: ${g.get('cpa', 'N/A')}", "pass": True})
            bench_data["findings"].append({"label": f"Google Ads avg conversion rate: {g.get('conversion_rate', 'N/A')}%", "pass": True})
        if m:
            bench_data["meta_ads"] = m
            bench_data["findings"].append({"label": f"Facebook Ads avg CPC: ${m.get('cpc', 'N/A')}", "pass": True})
            bench_data["findings"].append({"label": f"Facebook Ads avg CPM: ${m.get('cpm', 'N/A')}", "pass": True})
    except Exception:
        pass
    results["benchmarks"] = bench_data

    # ── 5. Overall Score ──
    results["overall_score"] = round(sum(scores) / len(scores)) if scores else 0

    # ── 6. AI Recommendations ──
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    recommendations = []
    if openai_key:
        try:
            prompt = (
                f"You are a local marketing expert. A {industry.replace('_', ' ')} business called "
                f"\"{business_name}\" in {service_area or 'an unspecified area'} just ran an automated assessment.\n\n"
                f"GBP Score: {gbp_data['score']}/100\n"
                f"Website SEO Score: {seo_data['score']}/100\n"
                f"Ad Presence Score: {ad_data['score']}/100\n"
                f"Overall: {results['overall_score']}/100\n\n"
                f"GBP findings: {_json.dumps(gbp_data['findings'])}\n"
                f"SEO findings: {_json.dumps(seo_data['findings'])}\n"
                f"Ad findings: {_json.dumps(ad_data['findings'])}\n\n"
                "Give exactly 5 specific, actionable recommendations. Be direct. "
                "Reference their actual data. No fluff. No numbered sub-points. "
                "Each recommendation should be 1-2 sentences max. Return as a JSON object with key \"recommendations\" containing an array of strings."
            )
            ai_resp = _req.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7,
                    "response_format": {"type": "json_object"},
                },
                timeout=30,
            )
            if ai_resp.status_code == 200:
                ai_text = ai_resp.json()["choices"][0]["message"]["content"]
                parsed = _json.loads(ai_text)
                if isinstance(parsed, list):
                    recommendations = parsed[:5]
                elif isinstance(parsed, dict):
                    for v in parsed.values():
                        if isinstance(v, list):
                            recommendations = [str(x) for x in v[:5]]
                            break
        except Exception:
            pass

    if not recommendations:
        default_cpa = benchmarks.get("google_ads", {}).get(industry, {}).get("cpa", 50)
        recommendations = [
            "Claim and fully complete your Google Business Profile if you haven't already.",
            "Make sure your website has a unique title tag and meta description on every page.",
            "Run at least one Facebook awareness campaign to stay visible in your service area.",
            f"Target a cost per lead under ${default_cpa} based on {industry.replace('_', ' ')} benchmarks.",
            "Respond to every Google review within 24 hours to boost your local ranking.",
        ]

    # Convert plain strings to objects the widget can render with priority badges
    results["recommendations"] = [
        {"title": r, "detail": "", "priority": "high" if i < 2 else "medium"}
        for i, r in enumerate(recommendations)
    ]

    # ── Save lead ──
    db = _get_db()
    lead_id = None
    try:
        conn = db._conn()
        conn.execute(
            """INSERT INTO assessment_leads
               (name, email, business_name, industry, service_area, website, gmb_url, facebook_url, phone, overall_score, results_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [name, email, business_name, industry, service_area, website, gmb_url, facebook_url,
             data.get("phone", ""), results["overall_score"], _json.dumps(results)],
        )
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        lead_id = row["id"] if row else None
    except Exception:
        pass  # Don't fail the assessment if DB save fails

    # ── Auto-enroll in drip sequence (only if user consented) ──
    email_consent = data.get("email_consent")
    if email_consent:
        try:
            seq = db.get_active_drip_sequence_for_trigger("assessment")
            if seq:
                db.enroll_in_drip(seq["id"], email, name, lead_source="assessment", lead_id=lead_id)
        except Exception:
            pass

    return _cors_json({"ok": True, "data": results})


# ── CORS helpers for public endpoints ──

def _cors_preflight():
    resp = jsonify({"ok": True})
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "POST"
    return resp


def _cors_json(payload, status=200):
    resp = jsonify(payload)
    resp.status_code = status
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


# ═══════════════════════════════════════════════════════════
# STAFF MANAGEMENT
# ═══════════════════════════════════════════════════════════

@client_bp.route("/staff")
@client_login_required
def client_staff():
    if not _require_role("owner", "manager"):
        abort(403)
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    users = db.get_client_users_for_brand(brand_id)
    return render_template(
        "client/client_staff.html",
        brand=brand,
        users=users,
        brand_name=session.get("client_brand_name", ""),
        current_user_id=session["client_user_id"],
        current_role=session.get("client_role", "owner"),
    )


@client_bp.route("/staff/invite", methods=["POST"])
@client_login_required
def client_staff_invite():
    if not _require_role("owner"):
        return jsonify({"error": "Only the owner can invite staff"}), 403
    db = _get_db()
    brand_id = session["client_brand_id"]
    email = request.form.get("email", "").strip().lower()
    name = request.form.get("name", "").strip()
    role = request.form.get("role", "staff")
    if role not in ("manager", "staff"):
        role = "staff"
    if not email or not name:
        flash("Email and name are required.", "error")
        return redirect(url_for("client.client_staff"))

    # Generate temp password
    import secrets
    temp_password = secrets.token_urlsafe(10)
    user_id = db.create_client_user(
        brand_id, email, temp_password, name,
        role=role, invited_by=session["client_user_id"],
    )
    if not user_id:
        flash("That email is already in use.", "error")
        return redirect(url_for("client.client_staff"))

    # Send invite email
    try:
        from webapp.email_sender import send_staff_invite_email
        brand_name = session.get("client_brand_name", "")
        send_staff_invite_email(current_app.config, email, name, brand_name, temp_password, role)
        flash(f"Invited {name} as {role}. Login credentials sent to {email}.", "success")
    except Exception:
        flash(f"Invited {name} as {role}. Temp password: {temp_password} (email failed to send)", "warning")

    return redirect(url_for("client.client_staff"))


@client_bp.route("/staff/<int:user_id>/role", methods=["POST"])
@client_login_required
def client_staff_update_role(user_id):
    if not _require_role("owner"):
        return jsonify({"error": "Only the owner can change roles"}), 403
    db = _get_db()
    brand_id = session["client_brand_id"]
    # Verify user belongs to this brand
    user = db.get_client_user(user_id)
    if not user or user["brand_id"] != brand_id:
        abort(404)
    if user_id == session["client_user_id"]:
        return jsonify({"error": "Cannot change your own role"}), 400
    role = request.form.get("role", "staff")
    if role not in ("owner", "manager", "staff"):
        role = "staff"
    db.update_client_user_role(user_id, role)
    flash(f"Updated {user['display_name']} to {role}.", "success")
    return redirect(url_for("client.client_staff"))


@client_bp.route("/staff/<int:user_id>/toggle", methods=["POST"])
@client_login_required
def client_staff_toggle(user_id):
    if not _require_role("owner"):
        return jsonify({"error": "Only the owner can deactivate staff"}), 403
    db = _get_db()
    brand_id = session["client_brand_id"]
    user = db.get_client_user(user_id)
    if not user or user["brand_id"] != brand_id:
        abort(404)
    if user_id == session["client_user_id"]:
        flash("You can't deactivate yourself.", "error")
        return redirect(url_for("client.client_staff"))
    db.toggle_client_user_active(user_id)
    status = "deactivated" if user["is_active"] else "reactivated"
    flash(f"{user['display_name']} has been {status}.", "success")
    return redirect(url_for("client.client_staff"))


# ═══════════════════════════════════════════════════════════
# TASK SYSTEM
# ═══════════════════════════════════════════════════════════

@client_bp.route("/tasks")
@client_login_required
def client_tasks():
    db = _get_db()
    brand_id = session["client_brand_id"]
    brand = db.get_brand(brand_id)
    role = session.get("client_role", "owner")
    user_id = session["client_user_id"]

    # Staff only see their own tasks
    if role == "staff":
        tasks = db.get_brand_tasks(brand_id, assigned_to=user_id)
    else:
        tasks = db.get_brand_tasks(brand_id)

    users = db.get_client_users_for_brand(brand_id)
    return render_template(
        "client/client_tasks.html",
        brand=brand,
        tasks=tasks,
        users=[u for u in users if u["is_active"]],
        brand_name=session.get("client_brand_name", ""),
        current_role=role,
        current_user_id=user_id,
    )


@client_bp.route("/tasks/create", methods=["POST"])
@client_login_required
def client_task_create():
    if not _require_role("owner", "manager"):
        return jsonify({"error": "Staff cannot create tasks"}), 403
    db = _get_db()
    brand_id = session["client_brand_id"]
    data = request.get_json() or {}
    title = (data.get("title") or "").strip()
    if not title:
        return jsonify({"error": "Title is required"}), 400
    steps = data.get("steps", [])
    steps_json = json.dumps([
        {"text": s.get("text", ""), "done": False}
        for s in steps if s.get("text", "").strip()
    ])
    task_id = db.create_brand_task(
        brand_id,
        title=title,
        description=(data.get("description") or "").strip(),
        steps_json=steps_json,
        priority=data.get("priority", "normal"),
        source=data.get("source", "manual"),
        source_ref=data.get("source_ref", ""),
        assigned_to=data.get("assigned_to") or None,
        created_by=session["client_user_id"],
        due_date=data.get("due_date", ""),
    )
    return jsonify({"success": True, "task_id": task_id})


@client_bp.route("/tasks/<int:task_id>")
@client_login_required
def client_task_detail(task_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    task = db.get_brand_task(task_id, brand_id)
    if not task:
        abort(404)
    role = session.get("client_role", "owner")
    if role == "staff" and task.get("assigned_to") != session["client_user_id"]:
        abort(403)
    users = db.get_client_users_for_brand(brand_id)
    return jsonify({
        "task": task,
        "users": [{"id": u["id"], "name": u["display_name"], "role": u.get("role", "owner")} for u in users if u["is_active"]],
    })


@client_bp.route("/tasks/<int:task_id>/update", methods=["POST"])
@client_login_required
def client_task_update(task_id):
    db = _get_db()
    brand_id = session["client_brand_id"]
    task = db.get_brand_task(task_id, brand_id)
    if not task:
        abort(404)
    role = session.get("client_role", "owner")
    data = request.get_json() or {}

    # Staff can only update status and check off steps on their own tasks
    if role == "staff":
        if task.get("assigned_to") != session["client_user_id"]:
            abort(403)
        allowed_fields = {}
        if "status" in data:
            allowed_fields["status"] = data["status"]
            if data["status"] == "done":
                allowed_fields["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if "steps_json" in data:
            allowed_fields["steps_json"] = data["steps_json"]
        if allowed_fields:
            db.update_brand_task(task_id, brand_id, **allowed_fields)
    else:
        fields = {}
        for key in ("title", "description", "status", "priority", "assigned_to", "due_date", "steps_json"):
            if key in data:
                fields[key] = data[key]
        if data.get("status") == "done":
            fields["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if fields:
            db.update_brand_task(task_id, brand_id, **fields)

    return jsonify({"success": True})


@client_bp.route("/tasks/<int:task_id>/delete", methods=["POST"])
@client_login_required
def client_task_delete(task_id):
    if not _require_role("owner", "manager"):
        return jsonify({"error": "Staff cannot delete tasks"}), 403
    db = _get_db()
    brand_id = session["client_brand_id"]
    db.delete_brand_task(task_id, brand_id)
    return jsonify({"success": True})


@client_bp.route("/tasks/from-finding", methods=["POST"])
@client_login_required
def client_task_from_finding():
    """Create a task from an agent finding."""
    if not _require_role("owner", "manager"):
        return jsonify({"error": "Staff cannot create tasks"}), 403
    db = _get_db()
    brand_id = session["client_brand_id"]
    data = request.get_json() or {}
    finding_id = data.get("finding_id")
    if not finding_id:
        return jsonify({"error": "finding_id required"}), 400

    # Get the finding
    conn = db._conn()
    row = conn.execute(
        "SELECT * FROM agent_findings WHERE id = ? AND brand_id = ?",
        (finding_id, brand_id),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Finding not found"}), 404
    finding = dict(row)

    agent_names = {
        "scout": "Scout", "penny": "Penny", "ace": "Ace", "radar": "Radar",
        "hawk": "Hawk", "pulse": "Pulse", "spark": "Spark", "bridge": "Bridge",
        "warren": "Warren", "chief": "Weave",
    }
    agent_name = agent_names.get(finding["agent_key"], finding["agent_key"])

    title = data.get("title") or finding["title"]
    description = f"From {agent_name}: {finding['detail']}"
    steps = []
    # Pull steps from the finding's extra_json (generated by agents)
    try:
        extra = json.loads(finding.get("extra_json") or "{}")
    except (json.JSONDecodeError, TypeError):
        extra = {}
    if isinstance(extra.get("steps"), list):
        for s in extra["steps"]:
            if isinstance(s, str) and s.strip():
                steps.append({"text": s, "done": False})
    # Fall back to the action field if no steps were found
    if not steps and finding.get("action"):
        steps.append({"text": finding["action"], "done": False})
    # Add any extra steps from the request (frontend may pass parsed steps)
    for s in data.get("steps", []):
        if s.get("text", "").strip():
            steps.append({"text": s["text"], "done": False})

    priority_map = {"critical": "urgent", "warning": "high", "positive": "normal", "info": "low"}
    priority = priority_map.get(finding["severity"], "normal")

    task_id = db.create_brand_task(
        brand_id,
        title=title,
        description=description,
        steps_json=json.dumps(steps),
        priority=priority,
        source="agent_finding",
        source_ref=str(finding_id),
        assigned_to=data.get("assigned_to") or None,
        created_by=session["client_user_id"],
        due_date=data.get("due_date", ""),
    )
    return jsonify({"success": True, "task_id": task_id})


# ── React SPA catch-all ──

@client_bp.route("/app/")
@client_bp.route("/app/<path:path>")
def react_spa(path=""):
    """Keep unfinished React portal routes off the live client experience."""
    if session.get("client_user_id"):
        return redirect(url_for("client.client_dashboard"))
    return redirect(url_for("client.client_login"))


# ── Helper ──

def _get_db():
    from flask import current_app
    return current_app.db
