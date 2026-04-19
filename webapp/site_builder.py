"""
Site Builder - AI website content generation engine.

Generates SEO-rich page content, JSON-LD schema markup, and structured
metadata for WordPress websites. Designed for local service businesses.
"""

import json
import logging
import re
from datetime import datetime
from webapp.font_catalog import font_css_stack, google_font_stylesheet_href, normalize_google_font_family

logger = logging.getLogger(__name__)

_HEX_COLOR_RE = re.compile(r"#?[0-9a-fA-F]{3}(?:[0-9a-fA-F]{3})?")

# ---------------------------------------------------------------------------
# Page type registry
# ---------------------------------------------------------------------------

PAGE_TYPES = {
    "home": {
        "label": "Home",
        "slug": "",
        "wp_type": "page",
        "schema_types": ["LocalBusiness", "WebSite", "WebPage"],
        "priority": 1,
    },
    "about": {
        "label": "About",
        "slug": "about",
        "wp_type": "page",
        "schema_types": ["Organization", "WebPage"],
        "priority": 2,
    },
    "services": {
        "label": "Services",
        "slug": "services",
        "wp_type": "page",
        "schema_types": ["Service", "WebPage", "BreadcrumbList"],
        "priority": 3,
    },
    "service_detail": {
        "label": "Service Detail",
        "slug": "services/{service_slug}",
        "wp_type": "page",
        "schema_types": ["Service", "WebPage", "FAQPage", "BreadcrumbList"],
        "priority": 4,
    },
    "service_area": {
        "label": "Service Area",
        "slug": "{area_slug}",
        "wp_type": "page",
        "schema_types": ["LocalBusiness", "WebPage", "BreadcrumbList"],
        "priority": 5,
    },
    "contact": {
        "label": "Contact",
        "slug": "contact",
        "wp_type": "page",
        "schema_types": ["LocalBusiness", "ContactPage", "WebPage"],
        "priority": 6,
    },
    "faq": {
        "label": "FAQ",
        "slug": "faq",
        "wp_type": "page",
        "schema_types": ["FAQPage", "WebPage", "BreadcrumbList"],
        "priority": 7,
    },
    "testimonials": {
        "label": "Testimonials",
        "slug": "testimonials",
        "wp_type": "page",
        "schema_types": ["WebPage", "BreadcrumbList"],
        "priority": 8,
    },
    "landing_page": {
        "label": "Landing Page",
        "slug": "lp/{lp_slug}",
        "wp_type": "page",
        "schema_types": ["WebPage", "LocalBusiness"],
        "priority": 9,
    },
    "custom": {
        "label": "Custom Page",
        "slug": "{custom_slug}",
        "wp_type": "page",
        "schema_types": ["WebPage"],
        "priority": 10,
    },
}


# ---------------------------------------------------------------------------
# Brand context builder
# ---------------------------------------------------------------------------

def build_brand_context(brand, intake=None, builder_theme=None, builder_templates=None, prompt_overrides=None):
    """Extract all brand fields relevant to content generation.
    
    intake: optional dict from the site builder intake form with extra
    fields like unique_selling_points, competitors, content_goals,
    lead_form_type, seo_data, warren_brief, etc.
    """
    intake = dict(intake or {})
    if builder_theme is not None and not intake.get("builder_theme"):
        intake["builder_theme"] = builder_theme
    if builder_templates is not None and not intake.get("builder_templates"):
        intake["builder_templates"] = builder_templates
    if prompt_overrides is not None and not intake.get("builder_prompt_overrides"):
        intake["builder_prompt_overrides"] = prompt_overrides

    builder_theme = _normalize_builder_theme(intake.get("builder_theme"))
    builder_templates = _normalize_builder_templates(intake.get("builder_templates"))
    builder_prompt_overrides = _normalize_builder_prompt_overrides(intake.get("builder_prompt_overrides"))
    reference_site_brief = _normalize_reference_site_brief(intake.get("reference_site_brief"))
    brand_colors = _extract_brand_hex_colors(brand)
    theme_primary = _normalize_hex_color(builder_theme.get("primary_color"))
    theme_secondary = _normalize_hex_color(builder_theme.get("secondary_color"))
    theme_accent = _normalize_hex_color(builder_theme.get("accent_color"))
    theme_text = _normalize_hex_color(builder_theme.get("text_color"))
    theme_background = _normalize_hex_color(builder_theme.get("bg_color"))
    brand_primary = _normalize_hex_color((brand or {}).get("primary_color")) or theme_primary or (brand_colors[0] if brand_colors else "")
    brand_accent = _normalize_hex_color((brand or {}).get("accent_color")) or theme_accent or (brand_colors[1] if len(brand_colors) > 1 else brand_primary)
    ctx = {
        "business_name": (brand.get("display_name") or "").strip(),
        "industry": (brand.get("industry") or "").strip(),
        "website": (brand.get("website") or "").strip(),
        "service_area": (brand.get("service_area") or "").strip(),
        "primary_services": (brand.get("primary_services") or "").strip(),
        "brand_voice": (brand.get("brand_voice") or "").strip(),
        "target_audience": (brand.get("target_audience") or "").strip(),
        "active_offers": (brand.get("active_offers") or "").strip(),
        "phone": (brand.get("phone") or brand.get("business_phone") or "").strip(),
        "email": (brand.get("business_email") or brand.get("email") or "").strip(),
        "address": (brand.get("address") or brand.get("business_address") or "").strip(),
        "hours": (brand.get("business_hours") or "").strip(),
        "tagline": (brand.get("tagline") or "").strip(),
        "year_founded": (brand.get("year_founded") or "").strip(),
        "license_info": (brand.get("license_info") or "").strip(),
        "certifications": (brand.get("certifications") or "").strip(),
        # Intake overrides and extras
        "unique_selling_points": (intake.get("unique_selling_points") or "").strip(),
        "services_to_highlight": (intake.get("services_to_highlight") or "").strip(),
        "service_plan_options": (intake.get("service_plan_options") or "").strip(),
        "service_add_ons": (intake.get("service_add_ons") or "").strip(),
        "company_story": (intake.get("company_story") or "").strip(),
        "site_vision": (intake.get("site_vision") or "").strip(),
        "design_notes": (intake.get("design_notes") or "").strip(),
        "competitors": (intake.get("competitors") or "").strip(),
        "content_goals": (intake.get("content_goals") or "").strip(),
        "lead_form_type": (intake.get("lead_form_type") or "").strip(),
        "lead_form_shortcode": (intake.get("lead_form_shortcode") or "").strip(),
        "quote_tool_source": (intake.get("quote_tool_source") or "").strip(),
        "quote_tool_embed": (intake.get("quote_tool_embed") or "").strip(),
        "quote_tool_zip_mode": (intake.get("quote_tool_zip_mode") or "").strip(),
        "quote_tool_collect_dogs": bool(intake.get("quote_tool_collect_dogs")),
        "quote_tool_collect_frequency": bool(intake.get("quote_tool_collect_frequency")),
        "quote_tool_collect_last_cleaned": bool(intake.get("quote_tool_collect_last_cleaned")),
        "quote_tool_phone_mode": (intake.get("quote_tool_phone_mode") or "").strip(),
        "quote_tool_notes": (intake.get("quote_tool_notes") or "").strip(),
        "plugins": (intake.get("plugins") or "").strip(),
        "cta_text": (intake.get("cta_text") or "").strip(),
        "cta_phone": (intake.get("cta_phone") or "").strip(),
        "brand_logo_path": (brand.get("logo_path") or "").strip(),
        "brand_colors": brand_colors,
        "builder_theme": builder_theme,
        "builder_theme_name": (builder_theme.get("name") or "").strip(),
        "builder_templates": builder_templates,
        "builder_prompt_overrides": builder_prompt_overrides,
        "reference_url": str(intake.get("reference_url") or "").strip(),
        "reference_mode": str(intake.get("reference_mode") or reference_site_brief.get("mode") or "vibe").strip(),
        "reference_site_brief": reference_site_brief,
        "button_style": (
            intake.get("button_style")
            or (builder_theme.get("button_style") or "")
            or reference_site_brief.get("button_style_hint")
            or ""
        ).strip(),
        "custom_css": (builder_theme.get("custom_css") or "").strip(),
        "color_secondary": theme_secondary,
        "color_text": theme_text,
        "color_background": theme_background,
    }
    # Intake can override brand-level fields
    for key in ("brand_voice", "target_audience", "active_offers", "tagline"):
        if intake.get(key):
            ctx[key] = intake[key].strip()
    # SEO intelligence
    ctx["seo_data"] = intake.get("seo_data") or {}
    ctx["warren_brief"] = (intake.get("warren_brief") or "").strip()
    ctx["priority_seo_locations"] = (intake.get("priority_seo_locations") or "").strip()
    # Design tokens
    ctx["color_palette"] = (intake.get("color_palette") or "").strip()
    ctx["font_pair"] = (intake.get("font_pair") or "").strip()
    ctx["layout_style"] = (
        intake.get("layout_style")
        or reference_site_brief.get("layout_style_hint")
        or (builder_theme.get("layout_style") or "")
    ).strip()
    ctx["color_primary"] = (intake.get("color_primary") or brand_primary or "").strip()
    ctx["color_accent"] = (intake.get("color_accent") or brand_accent or "").strip()
    ctx["color_dark"] = (intake.get("color_dark") or "").strip()
    ctx["color_light"] = (intake.get("color_light") or "").strip()
    ctx["font_heading"] = (
        intake.get("font_heading")
        or (brand.get("font_heading") or "")
        or (builder_theme.get("font_heading") or "")
        or reference_site_brief.get("heading_font_hint")
        or ""
    ).strip()
    ctx["font_body"] = (
        intake.get("font_body")
        or (brand.get("font_body") or "")
        or (builder_theme.get("font_body") or "")
        or reference_site_brief.get("body_font_hint")
        or ""
    ).strip()
    ctx["style_preset"] = (intake.get("style_preset") or reference_site_brief.get("style_preset_hint") or "").strip()
    ctx["wireframe_style"] = (intake.get("wireframe_style") or "").strip()
    ctx["image_slots"] = intake.get("image_slots") or {}
    ctx["font_heading"] = normalize_google_font_family(ctx["font_heading"])
    ctx["font_body"] = normalize_google_font_family(ctx["font_body"])
    return ctx


def _normalize_hex_color(value):
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


def _extract_brand_hex_colors(brand):
    colors = []
    seen = set()

    def add_color(value):
        color = _normalize_hex_color(value)
        if not color or color in seen:
            return
        seen.add(color)
        colors.append(color)

    for key in ("primary_color", "accent_color"):
        add_color((brand or {}).get(key))

    raw = ((brand or {}).get("brand_colors") or "").strip()
    for match in _HEX_COLOR_RE.finditer(raw):
        add_color(match.group(0))

    return colors


def _normalize_builder_theme(raw_theme):
    if not isinstance(raw_theme, dict):
        return {}

    normalized = {}
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
    ):
        value = raw_theme.get(key)
        normalized[key] = str(value).strip() if isinstance(value, str) else value
    return normalized


def _normalize_builder_templates(raw_templates):
    normalized = []
    for item in raw_templates or []:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "id": item.get("id"),
            "name": str(item.get("name") or "").strip(),
            "category": str(item.get("category") or "section").strip().lower(),
            "page_types": str(item.get("page_types") or "").strip(),
            "html_content": str(item.get("html_content") or ""),
            "css_content": str(item.get("css_content") or ""),
            "description": str(item.get("description") or "").strip(),
            "sort_order": int(item.get("sort_order") or 0),
        })
    normalized.sort(key=lambda item: (item.get("sort_order", 0), item.get("name", "")))
    return normalized


def _normalize_builder_prompt_overrides(raw_overrides):
    normalized = {}
    for item in raw_overrides or []:
        if not isinstance(item, dict):
            continue
        page_type = str(item.get("page_type") or "").strip()
        section = str(item.get("section") or "user_prompt").strip()
        content = str(item.get("content") or "").strip()
        if not page_type or not section or not content:
            continue
        normalized.setdefault(page_type, {})[section] = content
    return normalized


def _normalize_reference_site_brief(raw_brief):
    if not isinstance(raw_brief, dict):
        return {}

    normalized = {}
    for key in (
        "requested_url",
        "resolved_url",
        "mode",
        "title",
        "description",
        "layout_style_hint",
        "style_preset_hint",
        "heading_font_hint",
        "body_font_hint",
        "button_style_hint",
        "hero_layout_hint",
        "error",
    ):
        normalized[key] = str(raw_brief.get(key) or "").strip()

    for key in ("nav_items", "headings", "cta_texts", "color_hints", "notes", "design_traits", "vision_notes"):
        values = []
        for item in raw_brief.get(key) or []:
            text = str(item or "").strip()
            if text:
                values.append(text[:160])
        normalized[key] = values[:8]

    patterns = []
    for item in raw_brief.get("section_patterns") or []:
        if not isinstance(item, dict):
            continue
        pattern = {
            "category": str(item.get("category") or "content").strip()[:60],
            "heading": str(item.get("heading") or "").strip()[:140],
            "summary": str(item.get("summary") or "").strip()[:180],
            "layout_hint": str(item.get("layout_hint") or "").strip()[:60],
            "item_count": int(item.get("item_count") or 0),
            "cta_texts": [],
        }
        for cta in item.get("cta_texts") or []:
            text = str(cta or "").strip()
            if text:
                pattern["cta_texts"].append(text[:80])
        patterns.append(pattern)
        if len(patterns) >= 10:
            break
    normalized["section_patterns"] = patterns

    image_assets = []
    for item in raw_brief.get("image_assets") or []:
        if not isinstance(item, dict):
            continue
        asset = {
            "role": str(item.get("role") or "gallery").strip()[:40],
            "alt": str(item.get("alt") or "").strip()[:160],
            "asset_url": str(item.get("asset_url") or "").strip()[:300],
            "reference_url": str(item.get("reference_url") or "").strip()[:300],
            "query": str(item.get("query") or "").strip()[:140],
        }
        if asset["asset_url"]:
            image_assets.append(asset)
        if len(image_assets) >= 8:
            break
    normalized["image_assets"] = image_assets

    try:
        normalized["section_count"] = int(raw_brief.get("section_count") or 0)
    except Exception:
        normalized["section_count"] = 0

    return normalized


def _intake_flag_enabled(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _template_matches_page_type(template, page_type):
    targets = str(template.get("page_types") or "").strip().lower()
    if not targets or targets in {"*", "all", "any"}:
        return True
    allowed = {item.strip() for item in targets.split(",") if item.strip()}
    return page_type in allowed


def _template_category_matches(template, *names):
    category = str(template.get("category") or "").strip().lower()
    name = str(template.get("name") or "").strip().lower()
    allowed = {item.strip().lower() for item in names if item}
    if category in allowed:
        return True
    return any(token in name for token in allowed)


def _truncate_html_for_prompt(value, limit=700):
    raw = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(raw) <= limit:
        return raw
    return raw[: limit - 3].rstrip() + "..."


def _select_builder_template(templates, page_type, *categories):
    for template in templates or []:
        if not _template_matches_page_type(template, page_type):
            continue
        if _template_category_matches(template, *categories):
            return template
    return None


def _builder_theme_block(ctx):
    theme = ctx.get("builder_theme") or {}
    if not theme:
        return ""

    parts = ["APPROVED THEME PRESET (default to this look unless the intake explicitly overrides it):"]
    if theme.get("name"):
        parts.append(f"- Theme preset: {theme['name']}")
    if theme.get("description"):
        parts.append(f"- Theme intent: {theme['description']}")
    if theme.get("primary_color"):
        parts.append(f"- Theme primary color: {theme['primary_color']}")
    if theme.get("secondary_color"):
        parts.append(f"- Theme secondary color: {theme['secondary_color']}")
    if theme.get("accent_color"):
        parts.append(f"- Theme accent color: {theme['accent_color']}")
    if theme.get("text_color"):
        parts.append(f"- Theme text color: {theme['text_color']}")
    if theme.get("bg_color"):
        parts.append(f"- Theme background color: {theme['bg_color']}")
    if theme.get("font_heading"):
        parts.append(f"- Theme heading font: {theme['font_heading']}")
    if theme.get("font_body"):
        parts.append(f"- Theme body font: {theme['font_body']}")
    if theme.get("button_style"):
        parts.append(f"- Button style: {theme['button_style']}")
    if theme.get("layout_style"):
        parts.append(f"- Preferred layout style: {theme['layout_style']}")
    return "\n".join(parts) + "\n"


def _reference_site_block(ctx):
    brief = ctx.get("reference_site_brief") or {}
    reference_url = ctx.get("reference_url") or brief.get("resolved_url") or brief.get("requested_url") or ""
    if not reference_url and not brief:
        return ""

    if brief.get("error"):
        return ""

    mode = (ctx.get("reference_mode") or brief.get("mode") or "vibe").strip().lower()
    mode_notes = {
        "vibe": "Match the overall visual feel and section rhythm, but not the exact layout.",
        "layout": "Stay fairly close to the reference layout structure and pacing, while keeping this brand's own identity.",
        "sections": "Borrow the strongest section ideas and order, but rebuild them through this business's own brand system.",
    }

    parts = [
        "REFERENCE SITE DIRECTION (inspiration only - do not copy text, logos, brand names, or images):",
    ]
    if reference_url:
        parts.append(f"- Reference URL: {reference_url}")
    parts.append(f"- Match mode: {mode}. {mode_notes.get(mode, mode_notes['vibe'])}")
    if brief.get("title"):
        parts.append(f"- Reference title: {brief['title']}")
    if brief.get("description"):
        parts.append(f"- Positioning cue: {brief['description']}")
    if brief.get("layout_style_hint"):
        parts.append(f"- Suggested layout style from reference: {brief['layout_style_hint']}")
    if brief.get("style_preset_hint"):
        parts.append(f"- Suggested style preset from reference: {brief['style_preset_hint']}")
    if brief.get("nav_items"):
        parts.append(f"- Navigation pattern: {', '.join(brief['nav_items'][:6])}")
    if brief.get("headings"):
        parts.append(f"- Heading rhythm: {', '.join(brief['headings'][:5])}")
    if brief.get("cta_texts"):
        parts.append(f"- CTA language cues: {', '.join(brief['cta_texts'][:4])}")
    if brief.get("color_hints"):
        parts.append(f"- Reference color mood cues: {', '.join(brief['color_hints'][:4])}")
    if brief.get("heading_font_hint"):
        parts.append(f"- Heading font vibe from the rendered page: {brief['heading_font_hint']}")
    if brief.get("body_font_hint"):
        parts.append(f"- Body font vibe from the rendered page: {brief['body_font_hint']}")
    if brief.get("button_style_hint"):
        parts.append(f"- Button treatment cue: {brief['button_style_hint']}")
    if brief.get("hero_layout_hint"):
        parts.append(f"- Hero composition cue: {brief['hero_layout_hint']}")
    if brief.get("section_count"):
        parts.append(f"- Approximate section count on the reference page: {brief['section_count']}")
    if brief.get("section_patterns"):
        parts.append("- Reference section patterns to echo in the new build:")
        for pattern in brief.get("section_patterns")[:6]:
            category = pattern.get("category") or "content"
            heading = pattern.get("heading") or pattern.get("summary") or "Untitled section"
            layout_hint = pattern.get("layout_hint") or "stacked"
            line = f"  - {category}: {heading} [{layout_hint}]"
            if pattern.get("cta_texts"):
                line += f" | CTA cues: {', '.join(pattern['cta_texts'][:2])}"
            parts.append(line)
    if brief.get("image_assets"):
        parts.append("- Approved replacement imagery to use instead of copying the source site's images:")
        for asset in brief.get("image_assets")[:4]:
            role = asset.get("role") or "gallery"
            alt = asset.get("alt") or "Reference-inspired stock image"
            query = asset.get("query") or alt
            parts.append(f"  - {role}: {alt} | use {asset.get('asset_url')} | stock query: {query}")
    if brief.get("design_traits"):
        parts.append("- Rendered design traits to preserve:")
        for trait in brief.get("design_traits")[:4]:
            parts.append(f"  - {trait}")
    if brief.get("vision_notes"):
        parts.append("- Screenshot-level composition cues:")
        for note in brief.get("vision_notes")[:4]:
            parts.append(f"  - {note}")
    for note in (brief.get("notes") or [])[:3]:
        parts.append(f"- {note}")
    parts.append("- Recreate the structural feel using this business's own brand colors, tone, offers, SEO targets, and approved assets.")
    return "\n".join(parts) + "\n"


def _template_library_block(page_spec, ctx):
    templates = ctx.get("builder_templates") or []
    if not templates:
        return ""

    page_type = page_spec.get("page_type") or ""
    matched = [
        template for template in templates
        if _template_matches_page_type(template, page_type)
    ]
    if not matched:
        return ""

    parts = ["APPROVED TEMPLATE LIBRARY:"]
    header_template = _select_builder_template(matched, page_type, "navigation", "nav", "header")
    footer_template = _select_builder_template(matched, page_type, "footer")
    if header_template:
        parts.append(f"- Shared header/navigation template: {header_template.get('name') or 'Unnamed header'}")
    if footer_template:
        parts.append(f"- Shared footer template: {footer_template.get('name') or 'Unnamed footer'}")

    section_templates = [
        template for template in matched
        if template is not header_template and template is not footer_template
    ]
    if not section_templates:
        return "\n".join(parts) + "\n"

    parts.append("- Use these approved section patterns when they fit the page. Keep the structure and intent aligned to the template, but rewrite the copy for this business and page:")
    for template in section_templates[:6]:
        label = template.get("name") or "Unnamed template"
        category = template.get("category") or "section"
        description = template.get("description") or _truncate_html_for_prompt(template.get("html_content"), 220)
        parts.append(f"  - {label} [{category}]: {description}")

    return "\n".join(parts) + "\n"


def _prompt_override_text(ctx, page_type, section):
    overrides = ctx.get("builder_prompt_overrides") or {}
    parts = []
    for scope in ("global", "all", "*", page_type):
        content = ((overrides.get(scope) or {}).get(section) or "").strip()
        if content:
            parts.append(content)
    return "\n\n".join(parts).strip()


def _apply_prompt_overrides(system_msg, user_msg, page_spec, ctx):
    page_type = page_spec.get("page_type") or ""
    system_override = _prompt_override_text(ctx, page_type, "system_prompt")
    user_override = _prompt_override_text(ctx, page_type, "user_prompt")

    if system_override:
        system_msg = f"{system_msg}\n\nADDITIONAL BUILDER SYSTEM RULES:\n{system_override}"
    if user_override:
        user_msg = f"{user_msg}\n\nADDITIONAL BUILDER PAGE INSTRUCTIONS:\n{user_override}"
    return system_msg, user_msg


def _template_token_map(page_spec, brand_ctx, content=None):
    page_context = page_spec.get("context") or {}
    content = content or {}
    return {
        "business_name": brand_ctx.get("business_name") or "",
        "industry": brand_ctx.get("industry") or "",
        "service_area": brand_ctx.get("service_area") or "",
        "phone": brand_ctx.get("phone") or brand_ctx.get("cta_phone") or "",
        "email": brand_ctx.get("email") or "",
        "address": brand_ctx.get("address") or "",
        "cta_text": brand_ctx.get("cta_text") or "Contact Us",
        "website": brand_ctx.get("website") or "",
        "tagline": brand_ctx.get("tagline") or "",
        "page_title": content.get("title") or page_spec.get("label") or "",
        "page_slug": page_spec.get("slug") or "",
        "service_name": page_context.get("service_name") or "",
        "area_name": page_context.get("area_name") or "",
    }


def _render_builder_template_html(template, page_spec, brand_ctx, content=None):
    html = str(template.get("html_content") or "")
    if not html:
        return ""

    rendered = html
    for key, value in _template_token_map(page_spec, brand_ctx, content).items():
        rendered = rendered.replace(f"{{{{{key}}}}}", str(value or ""))
    return rendered.strip()


def _theme_style_tag(brand_ctx):
    theme = brand_ctx.get("builder_theme") or {}
    heading_font = normalize_google_font_family(brand_ctx.get("font_heading") or theme.get("font_heading") or "")
    body_font = normalize_google_font_family(brand_ctx.get("font_body") or theme.get("font_body") or "")
    css_parts = []
    variables = []
    token_map = {
        "--sb-primary": brand_ctx.get("color_primary") or theme.get("primary_color") or "",
        "--sb-secondary": brand_ctx.get("color_secondary") or theme.get("secondary_color") or "",
        "--sb-accent": brand_ctx.get("color_accent") or theme.get("accent_color") or "",
        "--sb-text": brand_ctx.get("color_text") or theme.get("text_color") or "",
        "--sb-bg": brand_ctx.get("color_background") or theme.get("bg_color") or "",
    }
    for key, value in token_map.items():
        if value:
            variables.append(f"  {key}: {value};")
    if heading_font:
        variables.append(f"  --sb-font-heading: {font_css_stack(heading_font)};")
    if body_font:
        variables.append(f"  --sb-font-body: {font_css_stack(body_font)};")
    if variables:
        css_parts.append(":root {\n" + "\n".join(variables) + "\n}")
    type_css = []
    if body_font:
        type_css.append("body { font-family: var(--sb-font-body); }")
    if heading_font:
        type_css.append("h1, h2, h3, h4, h5, h6, .sb-heading { font-family: var(--sb-font-heading); }")
    if type_css:
        css_parts.append("\n".join(type_css))
    if theme.get("custom_css"):
        css_parts.append(theme["custom_css"])
    if not css_parts:
        return ""
    stylesheet_href = google_font_stylesheet_href([heading_font, body_font])
    font_link = f'<link rel="preconnect" href="https://fonts.googleapis.com">\n<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n<link href="{stylesheet_href}" rel="stylesheet">\n' if stylesheet_href else ""
    return font_link + "<style>\n" + "\n\n".join(css_parts) + "\n</style>"


def _image_slots_block(ctx):
    slots = ctx.get("image_slots") or {}
    if not isinstance(slots, dict):
        return ""

    labeled = []
    for key, slot in slots.items():
        if not isinstance(slot, dict):
            continue
        assets = slot.get("assets") or []
        use_stock = bool(slot.get("use_stock"))
        if not assets and not use_stock:
            continue
        label = slot.get("label") or key.replace("_", " ").title()
        note = str(slot.get("note") or "").strip()
        line = f"- {label}: "
        if assets:
            asset_names = ", ".join(a.get("original_name") or a.get("path") or "uploaded asset" for a in assets[:4])
            line += f"use uploaded image assets ({asset_names})"
        elif use_stock:
            line += "use a relevant stock image fallback"
        if note:
            line += f". Creative note: {note}"
        labeled.append(line)

    if not labeled:
        return ""
    return "IMAGE DIRECTION:\n" + "\n".join(labeled) + "\n"


def _template_style_tag(*templates):
    css_parts = []
    for template in templates:
        css = str((template or {}).get("css_content") or "").strip()
        if css:
            css_parts.append(css)
    if not css_parts:
        return ""
    return "<style>\n" + "\n\n".join(css_parts) + "\n</style>"


def _reference_image_assets_for_page(page_spec, brand_ctx):
    brief = brand_ctx.get("reference_site_brief") or {}
    assets = [asset for asset in (brief.get("image_assets") or []) if asset.get("asset_url")]
    if not assets:
        return []

    page_type = page_spec.get("page_type") or ""
    preferred_roles = {
        "home": ("hero", "services", "testimonials", "gallery", "cta"),
        "landing_page": ("hero", "cta", "services", "gallery"),
        "about": ("about", "hero", "gallery", "testimonials"),
        "services": ("services", "hero", "gallery", "cta"),
        "service_detail": ("services", "gallery", "hero", "cta"),
        "service_area": ("hero", "services", "gallery", "cta"),
        "testimonials": ("testimonials", "gallery", "hero"),
        "contact": ("contact", "hero", "cta"),
        "faq": ("hero", "services", "gallery"),
        "custom": ("hero", "services", "gallery"),
    }.get(page_type, ("hero", "services", "gallery", "about", "cta"))

    ordered = []
    seen = set()
    for role in preferred_roles:
        for asset in assets:
            key = asset.get("asset_url")
            if asset.get("role") == role and key and key not in seen:
                ordered.append(asset)
                seen.add(key)
    for asset in assets:
        key = asset.get("asset_url")
        if key and key not in seen:
            ordered.append(asset)
            seen.add(key)
    return ordered[:3]


def _inject_reference_images(body_html, page_spec, brand_ctx):
    html = str(body_html or "").strip()
    if not html:
        return html
    if re.search(r"<(img|picture)\b", html, re.I) or "background-image" in html.lower():
        return html

    assets = _reference_image_assets_for_page(page_spec, brand_ctx)
    if not assets:
        return html

    hero_asset = assets[0]
    hero_html = (
        '<figure class="sb-reference-image sb-reference-image-hero" '
        'style="margin:0 0 2rem;">'
        f'<img src="{hero_asset["asset_url"]}" alt="{hero_asset.get("alt") or "Reference-inspired stock image"}" '
        'loading="lazy" referrerpolicy="no-referrer" '
        'style="display:block;width:100%;max-height:560px;object-fit:cover;border-radius:24px;box-shadow:0 24px 60px rgba(15,23,42,.16);">'
        '</figure>'
    )

    gallery_html = ""
    if len(assets) > 1:
        cards = []
        for asset in assets[1:3]:
            cards.append(
                '<figure class="sb-reference-image-card" style="margin:0;">'
                f'<img src="{asset["asset_url"]}" alt="{asset.get("alt") or "Reference-inspired stock image"}" '
                'loading="lazy" referrerpolicy="no-referrer" '
                'style="display:block;width:100%;height:260px;object-fit:cover;border-radius:18px;box-shadow:0 18px 40px rgba(15,23,42,.12);">'
                '</figure>'
            )
        gallery_html = (
            '<section class="sb-reference-gallery" '
            'style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin:2rem 0;">'
            + "".join(cards)
            + '</section>'
        )

    return "\n".join(part for part in (hero_html, html, gallery_html) if part)


def _intake_image_assets_for_page(page_spec, brand_ctx):
    slots = brand_ctx.get("image_slots") or {}
    if not isinstance(slots, dict):
        return []

    page_type = page_spec.get("page_type") or ""
    preferred_slots = {
        "home": ("hero_desktop", "hero_mobile", "services_overview", "proof_image", "about_team", "gallery_images", "contact_location"),
        "landing_page": ("hero_desktop", "hero_mobile", "proof_image", "about_team", "gallery_images"),
        "about": ("about_team", "gallery_images", "proof_image", "hero_desktop"),
        "services": ("services_overview", "proof_image", "gallery_images", "hero_desktop"),
        "service_detail": ("services_overview", "proof_image", "gallery_images", "hero_desktop"),
        "service_area": ("contact_location", "hero_desktop", "gallery_images"),
        "contact": ("contact_location", "hero_desktop", "gallery_images"),
        "testimonials": ("proof_image", "gallery_images", "hero_desktop"),
        "faq": ("hero_desktop", "services_overview", "gallery_images"),
        "custom": ("hero_desktop", "gallery_images", "proof_image", "about_team"),
    }.get(page_type, ("hero_desktop", "gallery_images", "proof_image", "about_team"))

    assets = []
    seen = set()
    for slot_key in preferred_slots:
        slot = slots.get(slot_key) or {}
        if not isinstance(slot, dict):
            continue
        label = slot.get("label") or slot_key.replace("_", " ").title()
        note = str(slot.get("note") or "").strip()
        slot_assets = slot.get("assets") or []
        for asset in slot_assets:
            url = str(asset.get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            assets.append({
                "asset_url": url,
                "alt": note or label,
                "role": slot_key,
                "source": "upload",
            })
        stock_url = str(slot.get("stock_url") or "").strip()
        if stock_url and stock_url not in seen:
            seen.add(stock_url)
            assets.append({
                "asset_url": stock_url,
                "alt": note or label,
                "role": slot_key,
                "source": "stock",
            })
    return assets


def _inject_intake_images(body_html, page_spec, brand_ctx):
    html = str(body_html or "").strip()
    assets = _intake_image_assets_for_page(page_spec, brand_ctx)
    if not assets:
        return html

    existing_sources = set(re.findall(r'src=["\']([^"\']+)["\']', html, re.I))
    assets = [asset for asset in assets if asset.get("asset_url") not in existing_sources]
    if not assets:
        return html

    hero_asset = assets[0]
    hero_html = (
        '<figure class="sb-intake-image sb-intake-image-hero" '
        'style="margin:0 0 2rem;">'
        f'<img src="{hero_asset["asset_url"]}" alt="{hero_asset.get("alt") or "Brand image"}" '
        'loading="lazy" '
        'style="display:block;width:100%;max-height:560px;object-fit:cover;border-radius:24px;box-shadow:0 24px 60px rgba(15,23,42,.16);">'
        '</figure>'
    )

    gallery_html = ""
    if len(assets) > 1:
        cards = []
        for asset in assets[1:4]:
            cards.append(
                '<figure class="sb-intake-image-card" style="margin:0;">'
                f'<img src="{asset["asset_url"]}" alt="{asset.get("alt") or "Brand image"}" '
                'loading="lazy" '
                'style="display:block;width:100%;height:260px;object-fit:cover;border-radius:18px;box-shadow:0 18px 40px rgba(15,23,42,.12);">'
                '</figure>'
            )
        gallery_html = (
            '<section class="sb-intake-gallery" '
            'style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin:2rem 0;">'
            + "".join(cards)
            + '</section>'
        )

    return "\n".join(part for part in (hero_html, html, gallery_html) if part)


def _inject_builder_images(body_html, page_spec, brand_ctx):
    html = _inject_intake_images(body_html, page_spec, brand_ctx)
    return _inject_reference_images(html, page_spec, brand_ctx)


def _quote_tool_block(ctx):
    source = ctx.get("quote_tool_source") or ""
    embed = ctx.get("quote_tool_embed") or ""
    zip_mode = ctx.get("quote_tool_zip_mode") or ""
    phone_mode = ctx.get("quote_tool_phone_mode") or ""
    notes = ctx.get("quote_tool_notes") or ""
    wants_dogs = _intake_flag_enabled(ctx.get("quote_tool_collect_dogs"))
    wants_frequency = _intake_flag_enabled(ctx.get("quote_tool_collect_frequency"))
    wants_last_cleaned = _intake_flag_enabled(ctx.get("quote_tool_collect_last_cleaned"))

    if not any((source, embed, zip_mode, phone_mode, notes, wants_dogs, wants_frequency, wants_last_cleaned)):
        return ""

    parts = ["QUOTE TOOL CONFIGURATION:"]
    if source == "warren_hosted":
        parts.append(
            "- Use Warren's hosted quote flow as the primary quote CTA on this page. "
            "Design the section so the quote tool feels like the main conversion path."
        )
    elif source == "sng_plugin":
        widget = embed or "[sng_quote_tool]"
        parts.append(
            f"- Use the Sweep and Go / CRM quote widget or plugin flow. Place {widget} where the instant quote tool should appear."
        )
    elif source == "wp_shortcode":
        widget = embed or "[your_quote_tool_shortcode]"
        parts.append(
            f"- Use the WordPress quote tool shortcode or plugin embed {widget} in the primary quote section."
        )
    elif source == "external_url":
        target = embed or "https://example.com/quote"
        parts.append(
            f"- Route quote CTAs to the external quote tool URL {target}. Make the call to action clearly communicate that visitors will continue into a quote flow."
        )
    elif embed:
        parts.append(f"- Use this quote tool embed or reference in the page: {embed}")
    else:
        parts.append("- Include a clear instant-quote section or CTA on this page.")

    if zip_mode == "collect":
        parts.append("- Collect the visitor ZIP code as part of the quote flow.")
    elif zip_mode == "verify":
        parts.append("- Collect and verify the visitor ZIP code before moving them deeper into the quote flow. Make service-area qualification obvious in the copy.")

    if wants_dogs:
        parts.append("- The quote tool should ask for number of dogs.")
    if wants_frequency:
        parts.append("- The quote tool should ask for service frequency.")
    if wants_last_cleaned:
        parts.append("- The quote tool should optionally ask when the yard was last cleaned.")

    if phone_mode == "optional":
        parts.append("- Phone number should be optional, not required.")
    elif phone_mode == "required":
        parts.append("- Phone number should be required before submission.")
    elif phone_mode == "hidden":
        parts.append("- Do not ask for a phone number in the quote tool.")

    if notes:
        parts.append(f"- Quote-tool notes: {notes}")

    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------------------
# Blueprint builder
# ---------------------------------------------------------------------------

def build_site_blueprint(brand_ctx, services=None, areas=None, landing_pages=None, page_selection=None, custom_pages=None):
    """
    Build a complete site blueprint from brand context.

    Args:
        brand_ctx: dict from build_brand_context
        services: CSV string of services (overrides brand profile)
        areas: CSV string of service areas (overrides brand profile)
        landing_pages: list of dicts with {name, keyword, offer} for standalone landing pages
        page_selection: list of page types to include (e.g. ['home','about','services','contact'])
                        If None, includes all standard pages.
        custom_pages: list of dicts with {name, description, keyword} for user-defined pages

    Returns a list of page specs, each with:
        page_type, label, slug, schema_types, context (page-specific data)
    """
    pages = []

    # Determine which standard pages to include
    standard_types = ("home", "about", "services", "contact", "faq", "testimonials")
    if page_selection:
        selected = set(page_selection)
    else:
        selected = set(standard_types)

    # Static pages
    for ptype in standard_types:
        if ptype not in selected:
            continue
        meta = PAGE_TYPES[ptype]
        pages.append({
            "page_type": ptype,
            "label": meta["label"],
            "slug": meta["slug"],
            "wp_type": meta["wp_type"],
            "schema_types": meta["schema_types"],
            "priority": meta["priority"],
            "context": {},
        })

    # Service detail pages
    service_list = _parse_csv(services or brand_ctx.get("primary_services") or "")
    for svc in service_list:
        slug = _slugify(svc)
        pages.append({
            "page_type": "service_detail",
            "label": svc,
            "slug": f"services/{slug}",
            "wp_type": "page",
            "schema_types": PAGE_TYPES["service_detail"]["schema_types"],
            "priority": PAGE_TYPES["service_detail"]["priority"],
            "context": {"service_name": svc, "service_slug": slug},
        })

    # Service area pages
    area_list = _parse_csv(areas or brand_ctx.get("service_area") or "")
    for area in area_list:
        if "service_area" not in selected and page_selection:
            break
        slug = _slugify(area)
        pages.append({
            "page_type": "service_area",
            "label": area,
            "slug": slug,
            "wp_type": "page",
            "schema_types": PAGE_TYPES["service_area"]["schema_types"],
            "priority": PAGE_TYPES["service_area"]["priority"],
            "context": {"area_name": area, "area_slug": slug},
        })

    # Landing pages (standalone conversion pages)
    for lp in (landing_pages or []):
        name = (lp.get("name") or "").strip()
        if not name:
            continue
        slug = _slugify(name)
        pages.append({
            "page_type": "landing_page",
            "label": name,
            "slug": f"lp/{slug}",
            "wp_type": "page",
            "schema_types": PAGE_TYPES["landing_page"]["schema_types"],
            "priority": PAGE_TYPES["landing_page"]["priority"],
            "context": {
                "lp_name": name,
                "lp_slug": slug,
                "lp_keyword": (lp.get("keyword") or "").strip(),
                "lp_offer": (lp.get("offer") or "").strip(),
                "lp_audience": (lp.get("audience") or "").strip(),
            },
        })

    # Custom pages (user-defined)
    for cp in (custom_pages or []):
        name = (cp.get("name") or "").strip()
        if not name:
            continue
        slug = (cp.get("slug") or "").strip() or _slugify(name)
        pages.append({
            "page_type": "custom",
            "label": name,
            "slug": slug,
            "wp_type": "page",
            "schema_types": PAGE_TYPES["custom"]["schema_types"],
            "priority": PAGE_TYPES["custom"]["priority"],
            "context": {
                "custom_name": name,
                "custom_slug": slug,
                "custom_purpose": (cp.get("purpose") or cp.get("description") or "").strip(),
                "custom_keyword": (cp.get("keyword") or "").strip(),
            },
        })

    pages.sort(key=lambda p: (p["priority"], p["label"]))
    return pages


# ---------------------------------------------------------------------------
# Prompt builders - one per page type
# ---------------------------------------------------------------------------

_GLOBAL_RULES = (
    "VOICE RULES (apply to every page):\n"
    "- Write at an 8th grade reading level. Short sentences. Active voice.\n"
    "- Do NOT use em dashes. Use commas, periods, colons, or regular dashes.\n"
    "- NEVER use these AI-tell words: harness, elevate, empower, streamline, cutting-edge, "
    "holistic, synergy, leverage (as a verb), robust, seamless, revolutionize, game-changing, "
    "next-level, unleash, supercharge, skyrocket, turbocharge.\n"
    "- NEVER use these filler openers: 'In today's world', 'In today's competitive landscape', "
    "'Are you tired of', 'Look no further', 'What if I told you', 'Welcome to our website', "
    "'Are you looking for'.\n"
    "- Use 'you' more than 'we'. The reader is the main character.\n"
    "- Numbers beat adjectives. '$2,400 in wasted ad spend' beats 'significant savings'.\n"
    "- One idea per paragraph. White space matters.\n"
    "- Do not hedge. 'We will' not 'We might be able to help you'. 'You get' not 'You may benefit from'.\n"
    "- Contractions are fine when natural, but don't force every sentence into one.\n"
    "\n"
    "CONVERSION RULES:\n"
    "- Every page has ONE primary CTA. Everything points toward it.\n"
    "- Headlines do the heavy lifting. If the reader only sees headlines and CTA, "
    "they should still understand the pitch.\n"
    "- Subheads must be scannable and benefit-driven. Not clever. Clear.\n"
    "- Proof comes BEFORE the ask. Social proof, specifics, results, then the CTA.\n"
    "- Remove anything that does not build desire or remove an objection.\n"
    "- The first screen must answer: What is this? Who is it for? Why should I care? "
    "What do I do next?\n"
    "\n"
    "SEO RULES:\n"
    "- Naturally incorporate the primary keyword in the first 100 words, in at least one H2, "
    "and in the meta description.\n"
    "- This page must target a UNIQUE primary keyword. Do not target the same keyword "
    "as other pages on the site. Each page owns a distinct keyword cluster.\n"
    "- Include internal link placeholders like [LINK:/services] or [LINK:/contact] where relevant.\n"
    "- Use proper HTML: h2, h3, p, ul/ol, strong, blockquote. No h1 (theme handles that).\n"
    "- No two pages on the site should have the same H1 or same H2s. Your headings must be "
    "unique to this page's specific topic.\n"
    "\n"
    "CONTENT QUALITY RULES:\n"
    "- Each heading and paragraph must add real information. No padding.\n"
    "- If a section could appear on any competitor's website by swapping the business name, "
    "it is too generic. Make it specific to this business.\n"
    "- Do not repeat the same proof points, trust signals, or messaging that would appear "
    "on other pages of this site. Each page earns its space with unique value.\n"
    "- All content must be factually defensible. No invented statistics or fake awards.\n"
    "- Never use placeholder stats like '1000+ customers served' unless that data is in the context.\n"
    "\n"
    "EMOTIONAL ARC:\n"
    "- Every page should follow this arc: Hook (earn 3 seconds) > Tension (name the problem) "
    "> Credibility (prove you solve it) > Vision (show life after) > Release (clear next step).\n"
    "- Do not dump all information at the same energy level. Build toward the CTA.\n"
    "- After making a claim, provide proof immediately. Do not save all proof for a separate section.\n"
    "- Vary section structure. Do not repeat the same layout (heading, paragraph, bullets) "
    "for every section. Use questions, short punchy lines, scenarios, and pattern interrupts.\n"
    "\n"
    "ANTI-PATTERNS (never do these):\n"
    "- Do not include 'Who This Is For' sections that list obvious audiences.\n"
    "- Do not include stats or numbers you cannot verify from the provided context.\n"
    "- Do not write feature lists without benefits. Every feature needs a 'which means...' follow-up.\n"
    "- Do not use the same sentence structure for every bullet point.\n"
    "- Do not make every paragraph the same length. Vary rhythm.\n"
)

_OUTPUT_FORMAT = (
    "\nReturn ONLY valid JSON with these exact keys:\n"
    '- "title": the page H1 title (under 70 chars)\n'
    '- "content": full HTML body content (no doctype/head/body wrapper, no h1)\n'
    '- "excerpt": 1-2 sentence page summary (under 160 chars)\n'
    '- "seo_title": SEO title tag (under 70 chars, include location if local)\n'
    '- "seo_description": meta description (under 160 chars, include CTA)\n'
    '- "primary_keyword": the main keyword this page targets\n'
    '- "secondary_keywords": comma-separated secondary keywords\n'
    '- "faq_items": array of {"question": "...", "answer": "..."} (3-6 items, '
    "relevant to the page topic). Empty array if not applicable.\n"
    '- "schema_hints": object with extra structured data fields the schema '
    "generator should use (e.g. priceRange, openingHours). Empty object if none.\n"
)


def _brand_block(ctx):
    """Format brand context for injection into prompts."""
    lines = []
    if ctx.get("business_name"):
        lines.append(f"Business Name: {ctx['business_name']}")
    if ctx.get("industry"):
        lines.append(f"Industry: {ctx['industry']}")
    if ctx.get("service_area"):
        lines.append(f"Service Area: {ctx['service_area']}")
    if ctx.get("primary_services"):
        lines.append(f"Services Offered: {ctx['primary_services']}")
    if ctx.get("target_audience"):
        lines.append(f"Target Audience: {ctx['target_audience']}")
    if ctx.get("brand_voice"):
        lines.append(f"Brand Voice: {ctx['brand_voice']}")
    if ctx.get("phone"):
        lines.append(f"Phone: {ctx['phone']}")
    if ctx.get("email"):
        lines.append(f"Email: {ctx['email']}")
    if ctx.get("address"):
        lines.append(f"Address: {ctx['address']}")
    if ctx.get("hours"):
        lines.append(f"Hours: {ctx['hours']}")
    if ctx.get("tagline"):
        lines.append(f"Tagline: {ctx['tagline']}")
    if ctx.get("year_founded"):
        lines.append(f"Founded: {ctx['year_founded']}")
    if ctx.get("license_info"):
        lines.append(f"Licensing: {ctx['license_info']}")
    if ctx.get("certifications"):
        lines.append(f"Certifications: {ctx['certifications']}")
    if ctx.get("active_offers"):
        lines.append(f"Current Offers: {ctx['active_offers']}")
    if ctx.get("unique_selling_points"):
        lines.append(f"Unique Selling Points: {ctx['unique_selling_points']}")
    if ctx.get("services_to_highlight"):
        lines.append(f"Services to Highlight: {ctx['services_to_highlight']}")
    if ctx.get("service_plan_options"):
        lines.append(f"Service Plan / Frequency Options: {ctx['service_plan_options']}")
    if ctx.get("service_add_ons"):
        lines.append(f"Service Add-Ons / Upsells: {ctx['service_add_ons']}")
    if ctx.get("company_story"):
        lines.append(f"Company Story: {ctx['company_story']}")
    if ctx.get("site_vision"):
        lines.append(f"Site Vision: {ctx['site_vision']}")
    if ctx.get("competitors"):
        lines.append(f"Competitors to Differentiate From: {ctx['competitors']}")
    if ctx.get("content_goals"):
        lines.append(f"Content Goals: {ctx['content_goals']}")
    if ctx.get("cta_text"):
        lines.append(f"Primary CTA: {ctx['cta_text']}")
    if ctx.get("cta_phone"):
        lines.append(f"CTA Phone: {ctx['cta_phone']}")
    return "\n".join(lines)


def _seo_intel_block(ctx):
    """Format Search Console data and Warren's SEO brief for prompt injection."""
    parts = []
    seo_data = ctx.get("seo_data") or {}
    priority_locations = ctx.get("priority_seo_locations") or ""

    if priority_locations:
        parts.append(
            "PRIORITY GEO TARGETS (treat these as must-cover locations in headings, service areas, and internal linking):\n"
            f"  - {priority_locations}"
        )

    if seo_data.get("top_queries"):
        top = seo_data["top_queries"][:15]
        query_lines = []
        for q in top:
            query_lines.append(
                f"  - \"{q['query']}\" (clicks: {q['clicks']}, impressions: {q['impressions']}, "
                f"pos: {q['position']}, ctr: {q['ctr']}%)"
            )
        parts.append(
            "SEARCH CONSOLE - TOP PERFORMING KEYWORDS (use these as your SEO foundation):\n"
            + "\n".join(query_lines)
        )

    if seo_data.get("opportunity_queries"):
        opps = seo_data["opportunity_queries"][:10]
        opp_lines = []
        for q in opps:
            opp_lines.append(
                f"  - \"{q['query']}\" (pos: {q['position']}, impressions: {q['impressions']})"
            )
        parts.append(
            "SEARCH CONSOLE - OPPORTUNITY KEYWORDS (position 4-20, high impressions - target these):\n"
            + "\n".join(opp_lines)
        )

    if seo_data.get("top_pages"):
        pg_lines = []
        for p in seo_data["top_pages"][:5]:
            pg_lines.append(f"  - {p['page']} (clicks: {p['clicks']}, pos: {p['position']})")
        parts.append(
            "SEARCH CONSOLE - TOP PAGES:\n" + "\n".join(pg_lines)
        )

    if seo_data.get("totals"):
        t = seo_data["totals"]
        parts.append(
            f"SEARCH CONSOLE - TOTALS: {t.get('clicks',0)} clicks, "
            f"{t.get('impressions',0)} impressions, "
            f"{t.get('ctr',0)}% CTR, avg position {t.get('avg_position',0)}"
        )

    warren = ctx.get("warren_brief") or ""
    if warren:
        parts.append(f"WARREN'S SEO STRATEGY BRIEF:\n{warren}")

    if not parts:
        return ""
    return "\nSEO INTELLIGENCE (from real Search Console data - use this to inform your keyword targeting):\n" + "\n\n".join(parts) + "\n"


def _lead_form_block(ctx):
    """Format lead form / plugin integration instructions."""
    form_type = ctx.get("lead_form_type") or ""
    shortcode = ctx.get("lead_form_shortcode") or ""
    plugins = ctx.get("plugins") or ""
    quote_tool = _quote_tool_block(ctx)

    if not form_type and not shortcode and not plugins:
        return quote_tool

    parts = ["LEAD FORM & PLUGIN INTEGRATION:"]
    if form_type == "wpforms":
        sc = shortcode or "[wpforms id=\"FORM_ID\"]"
        parts.append(
            f"- Include a WPForms lead capture form on this page. "
            f"Place the shortcode {sc} in the content where the form should appear. "
            f"Wrap it in a clear CTA section with a compelling headline above the form."
        )
    elif form_type == "cf7":
        sc = shortcode or "[contact-form-7 id=\"FORM_ID\" title=\"Contact\"]"
        parts.append(
            f"- Include a Contact Form 7 form. Place {sc} in the content "
            f"where the lead form should appear."
        )
    elif form_type == "gravity":
        sc = shortcode or "[gravityform id=\"FORM_ID\" title=\"true\" description=\"true\"]"
        parts.append(
            f"- Include a Gravity Forms form. Place {sc} in the content."
        )
    elif form_type == "custom" and shortcode:
        parts.append(
            f"- Include this custom form embed: {shortcode}"
        )
    elif form_type:
        parts.append(
            f"- Include a [LEAD_FORM_PLACEHOLDER] where the lead capture form should appear. "
            "Wrap it in a CTA section with a compelling, benefit-driven headline."
        )

    if plugins:
        parts.append(f"- Additional plugins/integrations to reference: {plugins}")

    if quote_tool:
        parts.append("")
        parts.append(quote_tool.rstrip())

    return "\n".join(parts) + "\n"


def _design_block(ctx):
    """Format design tokens for injection into prompts."""
    parts = []
    preset = ctx.get("style_preset") or ""
    primary = ctx.get("color_primary") or ""
    accent = ctx.get("color_accent") or ""
    secondary = ctx.get("color_secondary") or ""
    text_color = ctx.get("color_text") or ""
    background_color = ctx.get("color_background") or ""
    dark = ctx.get("color_dark") or ""
    light = ctx.get("color_light") or ""
    font_h = ctx.get("font_heading") or ""
    font_b = ctx.get("font_body") or ""
    color_palette = ctx.get("color_palette") or ""
    font_pair = ctx.get("font_pair") or ""
    layout_style = ctx.get("layout_style") or ""
    wireframe_style = ctx.get("wireframe_style") or ""
    button_style = ctx.get("button_style") or ""
    site_vision = ctx.get("site_vision") or ""
    design_notes = ctx.get("design_notes") or ""
    theme_block = _builder_theme_block(ctx)
    reference_block = _reference_site_block(ctx)
    image_block = _image_slots_block(ctx)

    if not any([preset, primary, secondary, text_color, background_color, font_h, color_palette, font_pair, layout_style, wireframe_style, button_style, site_vision, design_notes, theme_block, reference_block, image_block]):
        return ""

    parts.append("DESIGN GUIDELINES (apply CSS classes and inline styles to match):")
    if reference_block:
        parts.append(reference_block.rstrip())
    if theme_block:
        parts.append(theme_block.rstrip())

    # High-level palette
    palette_desc = {
        "blue-professional": "Blue Professional palette: navy/royal blue primary with slate accents. Conveys trust, corporate reliability.",
        "green-natural": "Green Natural palette: forest/sage greens with earthy neutrals. Eco-friendly, health, growth.",
        "red-bold": "Red Bold palette: vibrant reds with dark contrasts. Urgency, energy, appetite appeal.",
        "dark-luxury": "Dark Luxury palette: charcoal/black backgrounds, gold or cream accents, high contrast. Premium feel.",
        "warm-earthy": "Warm Earthy palette: terracottas, warm grays, natural tones. Construction, home, organic.",
        "clean-minimal": "Clean Minimal palette: whites, light grays, single accent color. Modern, tech, SaaS.",
        "bright-playful": "Bright Playful palette: vibrant multi-color accents on white. Kids, creative, events.",
    }
    if color_palette and color_palette in palette_desc:
        parts.append(f"- Color scheme: {palette_desc[color_palette]}")

    # Font pairing
    font_desc = {
        "inter-system": "Inter (headings) + system sans-serif (body) - clean, fast loading",
        "playfair-lato": "Playfair Display (headings) + Lato (body) - elegant, editorial",
        "montserrat-opensans": "Montserrat (headings) + Open Sans (body) - modern, versatile",
        "roboto-slab-roboto": "Roboto Slab (headings) + Roboto (body) - technical, structured",
        "poppins-nunito": "Poppins (headings) + Nunito (body) - friendly, approachable",
        "oswald-source": "Oswald (headings) + Source Sans Pro (body) - bold, industrial",
        "plusjakarta-inter": "Plus Jakarta Sans (headings) + Inter (body) - crisp, premium, modern service brand",
        "spacegrotesk-inter": "Space Grotesk (headings) + Inter (body) - assertive, modern, slightly editorial",
        "manrope-dmsans": "Manrope (headings) + DM Sans (body) - polished, contemporary, conversion-focused",
        "archivo-worksans": "Archivo (headings) + Work Sans (body) - sturdy, practical, operational",
        "bebas-mulish": "Bebas Neue (headings) + Mulish (body) - bold headlines with clean supporting copy",
        "raleway-lora": "Raleway (headings) + Lora (body) - refined, trustworthy, slightly upscale",
        "librebaskerville-source": "Libre Baskerville (headings) + Source Sans 3 (body) - classic authority with readable body copy",
    }
    if font_pair and font_pair in font_desc:
        parts.append(f"- Typography: {font_desc[font_pair]}")

    # Layout style
    layout_desc = {
        "modern-sections": "Modern Sections: full-width alternating background sections, generous whitespace, centered content blocks.",
        "classic-stacked": "Classic Stacked: traditional top-to-bottom flow, contained width, clear section dividers.",
        "hero-driven": "Hero-Driven: large hero images/banners per section, overlaid text, visual-first layout.",
        "card-grid": "Card Grid: content organized in card-based grids, modular sections, flexible arrangement.",
        "sidebar-content": "Sidebar + Content: main content column with persistent sidebar for navigation/CTAs.",
    }
    if layout_style and layout_style in layout_desc:
        parts.append(f"- Layout: {layout_desc[layout_style]}")

    if preset:
        presets_desc = {
            "clean-minimal": "Clean and minimal: lots of white space, thin borders, subtle shadows, light backgrounds. Think Apple-like restraint.",
            "bold-modern": "Bold and modern: strong color blocks, large headlines, sharp contrasts, dark sections with bright accents. High energy.",
            "warm-traditional": "Warm and traditional: earthy tones, rounded corners, friendly feel, serif accents for headings. Approachable.",
            "dark-premium": "Dark premium: dark backgrounds, light text, gold/silver accents, sophisticated typography. Luxury feel.",
        }
        parts.append(f"- Style: {presets_desc.get(preset, preset)}")

    if site_vision:
        parts.append(f"- Desired site vision: {site_vision}")
    if design_notes:
        parts.append(f"- Extra design notes: {design_notes}")

    if primary:
        parts.append(f"- Primary brand color: {primary}")
    if secondary:
        parts.append(f"- Secondary brand color: {secondary}")
    if accent:
        parts.append(f"- Accent color: {accent}")
    if text_color:
        parts.append(f"- Preferred text color: {text_color}")
    if background_color:
        parts.append(f"- Preferred background color: {background_color}")
    if dark:
        parts.append(f"- Dark color: {dark}")
    if light:
        parts.append(f"- Light/background color: {light}")
    if font_h:
        parts.append(f"- Heading font: {font_h}")
    if font_b:
        parts.append(f"- Body font: {font_b}")
    if button_style:
        parts.append(f"- Button treatment: {button_style}")
    if ctx.get("brand_logo_path"):
        parts.append("- A saved brand logo already exists. Leave visual room for the logo in hero or header treatments instead of relying on text-only branding.")

    parts.append(
        "- Use inline styles or CSS classes referencing these colors/fonts in the HTML. "
        "Example: style=\"color: {primary}; font-family: '{font_h}', sans-serif;\" "
        "Use the brand colors for CTAs, section backgrounds, and accents. "
        "Keep the design consistent across all sections."
    )
    wireframe_desc = {
        "conversion": "Wireframe bias: conversion-first, with rapid trust stacking and repeated CTA placements.",
        "story": "Wireframe bias: story-driven, with more narrative progression, proof, and pacing between CTAs.",
        "catalog": "Wireframe bias: catalog-style, with service grids, comparison cards, and browse-friendly organization.",
    }
    if wireframe_style and wireframe_style in wireframe_desc:
        parts.append(f"- {wireframe_desc[wireframe_style]}")
    if image_block:
        parts.append(image_block.rstrip())

    return "\n".join(parts) + "\n"


def build_page_prompt(page_spec, brand_ctx):
    """
    Build a complete prompt for generating a single page's content.

    Args:
        page_spec: dict from build_site_blueprint
        brand_ctx: dict from build_brand_context

    Returns:
        (system_message, user_message) tuple
    """
    ptype = page_spec["page_type"]
    builder = _PROMPT_BUILDERS.get(ptype, _prompt_generic)
    system_msg, user_msg = builder(page_spec, brand_ctx)
    template_block = _template_library_block(page_spec, brand_ctx)
    if template_block:
        user_msg = f"{user_msg}\n\n{template_block.rstrip()}"
    return _apply_prompt_overrides(system_msg, user_msg, page_spec, brand_ctx)


def _system_msg():
    return (
        "You are a direct-response copywriter and SEO strategist who writes website pages "
        "for local service businesses: plumbers, HVAC techs, roofers, painters, cleaners, "
        "electricians, landscapers, and similar trades.\n\n"
        "You think like a business owner, not a marketer. You know what the plumber is thinking "
        "when he reads a landing page. You know these people are busy, skeptical of marketing, "
        "have been burned by agencies, want results not promises, and respect directness.\n\n"
        "You write copy that converts. Not copy that sounds good. Every sentence either builds "
        "desire, removes an objection, or moves the reader toward the next action. If a sentence "
        "does none of those, delete it.\n\n"
        "You produce SEO-rich content with proper heading hierarchy, FAQ content for rich snippets, "
        "and natural keyword placement. You never use em dashes. You never use AI-tell words like "
        "harness, elevate, empower, streamline, robust, or seamless. You return only valid JSON.\n\n"
        "PAIN POINTS YOU KNOW COLD - service business owners feel:\n"
        "- Burned by agencies that took their money and delivered nothing measurable\n"
        "- Frustrated they don't understand their own marketing numbers\n"
        "- Afraid of wasting money on marketing that might not work\n"
        "- Tired of being talked down to by marketers who don't understand their trade\n"
        "- Desperate for leads but unsure which channel actually works\n\n"
        "OBJECTION HANDLING - every page should preemptively address the top objections:\n"
        "- 'Too expensive' - show the cost of NOT doing it, anchor against what they waste\n"
        "- 'Been burned before' - acknowledge directly, no-risk messaging\n"
        "- 'I don't have time' - show how simple the next step is\n"
        "- 'How is this different' - concrete differentiators they can verify"
    )


def _prompt_home(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    user_msg = (
        f"Write the HOME PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: AIDA (Attention-Interest-Desire-Action)\n"
        "Build through a full arc from stranger to 'I should call these people.'\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- HERO: Open with a value proposition that names who you serve and what they get. "
        "Not 'Welcome to...'. Not a generic tagline. A specific promise. "
        "Example pattern: '[Outcome] for [audience] in [area]'.\n"
        "- SERVICES OVERVIEW: Brief descriptions of 3-6 core services. Each service gets "
        "a benefit-first sentence, not a feature description. Link to detail pages with "
        "[LINK:/services/slug].\n"
        "- TRUST SECTION: Concrete differentiators only. Years in business, licensing, "
        "guarantees, response time. Only include what is in the context. Do not invent "
        "credentials. If the brand has licensing info, lead with it.\n"
        "- SOCIAL PROOF: Include [TESTIMONIAL_PLACEHOLDER] markers where real reviews "
        "should be inserted. Frame the section around results, not generic praise.\n"
        "- CTA SECTION: Phone number and contact link. Make the next step feel easy and "
        "low-risk: 'free estimate', 'no obligation', 'call now and we pick up'.\n"
        "- LOCAL SEO: Mention the primary service area in the hero, in at least one H2, "
        "and naturally 2-3 more times throughout. Do not stuff.\n"
        "- PATTERN INTERRUPT: After the services section, include a short, punchy line or "
        "question that breaks the rhythm. Something specific to the industry that makes "
        "the reader think 'they get it'.\n"
        "- Target word count: 800-1200 words.\n"
        "- Primary keyword: '[industry] in [primary area]' (e.g. 'plumber in Springfield').\n"
        "- This page is the hub. It links to services, about, and contact. Make those "
        "connections explicit with [LINK:] placeholders.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_about(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    design = _design_block(ctx)
    user_msg = (
        f"Write the ABOUT PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: Star-Story-Solution\n"
        "The star is the founder/team. The story is why this business exists. "
        "The solution is how that origin drives how they serve customers today.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- COMPANY STORY: Tell it like a real person talking. Not corporate history. "
        "Use founding year if available. Why did this person start this business? "
        "What problem were they solving? Keep it to 2-3 paragraphs max.\n"
        "- CREDENTIALS: Licenses, certifications, insurance, BBB rating. Only mention "
        "what is provided in the context. Present these as proof of commitment, not a "
        "brag list. 'Licensed and insured since 2008' carries more weight than a bullet list.\n"
        "- VALUES/TEAM: What the company stands for in practice, not in theory. "
        "'We show up on time' beats 'We value punctuality'. Tie values to customer outcomes.\n"
        "- LOCAL ROOTS: If a service area is provided, connect the company to the community. "
        "They live and work here. They are your neighbor, not a faceless company.\n"
        "- CTA: Close with a warm push to contact or request a quote. The reader just learned "
        "about these people. Now make it easy to reach them.\n"
        "- DEDUP NOTE: This page's unique value is the HUMAN story behind the business. "
        "Do not repeat service descriptions (those belong on /services). Do not repeat "
        "pricing details. Do not list services with descriptions.\n"
        "- Target word count: 600-900 words.\n"
        "- Primary keyword: '[business name] [city/area]' pattern.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_services(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    user_msg = (
        f"Write the SERVICES OVERVIEW PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: One-to-One Conversation\n"
        "Write like you are sitting across the table from a homeowner explaining what you do.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- INTRO: 1-2 paragraphs about the company's service range. Frame it around "
        "customer problems solved, not a company capability statement.\n"
        "- SERVICE SECTIONS: One H2 per service. For each:\n"
        "  * Lead with the customer problem ('Your water heater is leaking at 2am')\n"
        "  * Explain the solution in 2-3 sentences\n"
        "  * End with an internal link to the detail page: [LINK:/services/service-slug]\n"
        "  * Each service section must read differently. Vary the structure. Do not repeat "
        "the same pattern of [problem sentence, solution sentence, link] for every one.\n"
        "- CTA: End with a 'not sure what service you need?' section. Phone number, "
        "'call us and describe the problem, we will figure out the right fix'.\n"
        "- DEDUP NOTE: This page is the HUB. Keep service descriptions to 2-3 sentences each. "
        "The detail pages go deep. This page gives the bird's-eye view and routes people "
        "to the right detail page. Do NOT write 400 words per service here.\n"
        "- Target word count: 600-1000 words.\n"
        "- Primary keyword: '[industry] services in [area]' pattern.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_service_detail(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    svc_name = page_spec.get("context", {}).get("service_name", "")
    user_msg = (
        f"Write a SERVICE DETAIL PAGE for: {svc_name}\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: PAS (Problem-Agitate-Solve)\n"
        "Name the problem this service solves. Make it sting (what happens if they wait). "
        "Present the fix with confidence.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        f"- This page is entirely about '{svc_name}'. Go deep on this one service.\n"
        "- PROBLEM SECTION: What situation leads someone to search for this service? "
        "Be specific. A homeowner wakes up to a flooded basement. A business owner "
        "notices the AC unit is louder than usual. Make the reader see themselves.\n"
        "- AGITATE: What happens if they ignore it? Higher water bill. Mold risk. "
        "Emergency call at 3x the price. Name real consequences in the industry.\n"
        "- WHAT WE DO: Explain the actual process. What happens when they call? "
        "Who shows up? What does the work involve? How long does it take? "
        "Real details build trust. Vague descriptions build suspicion.\n"
        "- SIGNS YOU NEED THIS: A scannable list of warning signs. These become "
        "long-tail keyword targets and match how real people search.\n"
        "- PRICING TRANSPARENCY: If the brand has active offers, mention them. "
        "If not, use 'free estimates' or 'upfront pricing, no surprises' framing. "
        "Address the 'too expensive' objection before the reader thinks it.\n"
        "- FAQ: 3-6 questions specific to THIS service. Not generic company FAQs. "
        "'How long does a drain cleaning take?' not 'What areas do you serve?'\n"
        "- INTERNAL LINKS: Link to related services and to the contact page.\n"
        "- DEDUP NOTE: This page goes DEEP on one service. Do not repeat the company "
        "story (that's /about). Do not repeat general trust signals that appear on the "
        "home page. This page's unique value is the specific expertise in {svc_name}.\n"
        "- Target word count: 900-1400 words.\n"
        f"- Primary keyword: '{svc_name} [area]' pattern.\n"
        "- schema_hints should include 'serviceType' and 'areaServed'.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_service_area(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    area = page_spec.get("context", {}).get("area_name", "")
    user_msg = (
        f"Write a SERVICE AREA PAGE for: {area}\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: BAB (Before-After-Bridge)\n"
        f"Before: the homeowner in {area} has a problem and doesn't know who to call locally. "
        f"After: they find a trusted, nearby company that serves {area} specifically. "
        "Bridge: this page connects them.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        f"- This page targets customers specifically in {area}.\n"
        f"- LOCAL AUTHORITY: Mention {area} naturally 3-5 times. Not stuffed, woven in. "
        "Reference neighborhoods, landmarks, or local context where it adds authenticity. "
        f"The goal is for someone searching '{ctx.get('industry', 'services')} in {area}' "
        "to immediately see this page is about THEIR area.\n"
        "- SERVICES IN THIS AREA: List the services available. Keep descriptions to one "
        "sentence each with links to detail pages. Example: 'Need drain cleaning in "
        f"{area}? [LINK:/services/drain-cleaning]'\n"
        "- RESPONSE TIME: Include a 'local team, fast response' angle. Service businesses "
        "win on proximity and speed. 'We are based near [area]. When you call, we are on "
        "our way, not driving 45 minutes from across the metro.'\n"
        "- CTA with phone number and [LINK:/contact].\n"
        f"- DEDUP NOTE: This page's unique value is the LOCAL angle for {area}. Do not "
        "repeat the detailed service descriptions from /services/[slug]. Do not repeat "
        "the company story from /about. Mention services briefly and link out.\n"
        "- Target word count: 600-900 words.\n"
        f"- Primary keyword: '{ctx.get('industry', 'services')} in {area}' pattern.\n"
        f"- Secondary keywords: specific service + {area} combos.\n"
        "- schema_hints should include 'areaServed' with the area name.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_contact(page_spec, ctx):
    brand = _brand_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    user_msg = (
        f"Write the CONTACT PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: One-to-One Conversation\n"
        "The reader already decided they want to reach out. Do not re-sell. "
        "Make the process feel easy and human.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- OPENING: Lead with a clear, inviting line. Not 'Contact us today!' "
        "Something that reduces friction: 'Got a question? A leaking pipe? A project "
        "you need a quote on? Here is how to reach us.'\n"
        "- CONTACT METHODS: List all methods provided: phone, email, address. "
        "Format phone as a clickable link. Make each method scannable.\n"
        "- BUSINESS HOURS: Include if available. For emergency trades (plumbing, "
        "HVAC, electrical, roofing), add a note about after-hours/emergency availability.\n"
        "- WHAT HAPPENS NEXT: Describe the process after they reach out. 'You call. "
        "We answer (or call back within 30 minutes). We schedule a time that works. "
        "We show up on time.' This reduces the anxiety of the unknown.\n"
        "- SERVICE AREA: Mention it for local SEO. 'We serve [area] and surrounding communities.'\n"
        "- CTA: Even on a contact page, give a clear nudge. 'Call now for a free estimate' "
        "or 'Fill out the form and we will get back to you today.'\n"
        "- DEDUP NOTE: This page is purely about making contact easy. Do not retell the "
        "company story. Do not list services with descriptions. A brief mention of what "
        "you do is fine, but link to /services for details.\n"
        "- Target word count: 300-500 words.\n"
        "- Primary keyword: 'contact [business name]' or '[industry] near me'.\n"
        "- schema_hints should include 'telephone', 'email', and 'address' from the context.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_faq(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    design = _design_block(ctx)
    user_msg = (
        f"Write the FAQ PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: FAQ_Objection_Block\n"
        "Every FAQ answer is an objection removed. The reader who finishes this page "
        "should have zero reasons left not to call.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "- Generate 8-12 real questions a customer would ask BEFORE HIRING. "
        "Not questions about the website. Questions about the actual service.\n"
        "- GROUP BY THEME: Pricing, Process, Qualifications, Emergency, Guarantees.\n"
        "- ANSWER QUALITY: Each answer should be 2-4 sentences. Specific, not vague. "
        "'Yes, we offer free estimates for any job' beats 'We offer competitive pricing'. "
        "Reference the company's actual credentials and service area in answers.\n"
        "- OBJECTION-FIRST QUESTIONS: Include at least:\n"
        "  * A pricing/cost question ('How much does [service] cost?')\n"
        "  * A trust/qualification question ('Are you licensed and insured?')\n"
        "  * An emergency question ('Do you handle emergencies/after-hours calls?')\n"
        "  * A process question ('What should I expect when I call?')\n"
        "- LICENSING: If license_info or certifications are provided in context, "
        "include a specific question about it and answer with the actual credentials.\n"
        "- CTA: End with a 'Still have questions?' section. Phone number. "
        "'We would rather answer your questions in person. Call us.'\n"
        "- DEDUP NOTE: This page's unique value is the Q&A format optimized for "
        "FAQPage schema rich results in Google. Do not repeat paragraphs of content "
        "from other pages. The answers should be concise and direct.\n"
        "- Target word count: 800-1200 words.\n"
        "- Primary keyword: '[industry] FAQ' or '[industry] questions [area]'.\n"
        "- faq_items MUST contain all Q&A pairs from the content (needed for FAQPage schema).\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_testimonials(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    design = _design_block(ctx)
    user_msg = (
        f"Write the TESTIMONIALS / REVIEWS PAGE for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: Proof_Wall\n"
        "Social proof is the single strongest conversion lever for local service businesses. "
        "This page exists so every other page can link here when they need to back up a claim.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        "ABSOLUTE RULE: Do NOT invent fake testimonials. Real businesses live and die by "
        "authentic reviews. Fake ones destroy trust faster than no reviews at all.\n\n"
        "- OPENING SECTION: Skip the 'we value our customers' filler. Start with a bold, "
        "specific claim: the number of 5-star reviews, years of repeat customers, or a "
        "stat about referral rate. If the data isn't available, frame it as: 'Don't take "
        "our word for it. Here's what our customers say.'\n"
        "- TESTIMONIAL SLOTS: Create 6-8 placeholder blocks marked with [TESTIMONIAL_PLACEHOLDER]. "
        "Each placeholder should include structure for: customer first name, service type, "
        "star rating, and the review text. Format them as cards/blocks that look designed, "
        "not like an afterthought list.\n"
        "- VARIETY: Mix the placeholder types. Some short one-liners, some 2-3 sentence stories. "
        "Include a suggested video testimonial placeholder. Variety signals authenticity.\n"
        "- TRUST SIGNALS AROUND THE REVIEWS: Google rating badge placeholder, BBB or "
        "industry certification badges, 'Verified on Google' callout. These frame the "
        "reviews as real and vetted.\n"
        "- RESPONSE SECTION: Include a 'What happens when something goes wrong?' section. "
        "This is counterintuitive but powerful. Address it head-on: the business responds "
        "to every review, stands behind its work, and makes things right. This builds more "
        "trust than 100 five-star reviews.\n"
        "- LEAVE A REVIEW CTA: A clear section inviting happy customers to share their "
        "experience. Include placeholder links for Google Reviews and Facebook Reviews. "
        "Make it dead simple, one click.\n"
        "- BOTTOM CTA: 'Ready to become our next success story?' or something in that vein. "
        "Link to contact page. Do not be corny about it.\n"
        "- DEDUP NOTE: This page is ONLY about social proof and reviews. Do not retell the "
        "company story (that's /about). Do not list services (that's /services). This page's "
        "unique value is concentrated proof that real people trust this business.\n"
        "- Target word count: 500-800 words (excluding placeholder content).\n"
        "- Primary keyword: '[business name] reviews' or '[industry] reviews [area]'.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_generic(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    label = page_spec.get("label", "Page")
    user_msg = (
        f"Write a website page titled '{label}' for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "- Target word count: 500-800 words.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_landing_page(page_spec, ctx):
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    lp_ctx = page_spec.get("context", {})
    lp_name = lp_ctx.get("lp_name", "")
    lp_keyword = lp_ctx.get("lp_keyword", "")
    lp_offer = lp_ctx.get("lp_offer", "")
    lp_audience = lp_ctx.get("lp_audience", ctx.get("target_audience", ""))

    user_msg = (
        f"Write a HIGH-CONVERTING LANDING PAGE for: {lp_name}\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
        "CONVERSION FRAMEWORK: AIDA with urgency layer\n"
        "This is a LANDING PAGE, not a standard website page. It has ONE job: convert the visitor "
        "into a lead or customer. Every word either moves them toward the CTA or gets cut.\n\n"
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        f"- TARGET KEYWORD: '{lp_keyword}' (weave naturally, first 100 words, H2, meta desc).\n"
        f"- TARGET AUDIENCE: {lp_audience}\n"
    )

    if lp_offer:
        user_msg += f"- OFFER: {lp_offer}. Lead with this. Make it the hero.\n"

    user_msg += (
        "- HERO SECTION: Big, bold value proposition. What they get, who it's for, and one clear CTA button. "
        "No navigation distractions. No 'welcome to'. Straight to the point.\n"
        "- PROBLEM SECTION: Name the exact pain. Make them feel seen. Be specific to the industry.\n"
        "- SOLUTION SECTION: Show how you solve it. Process steps, not vague promises. 3-4 steps max.\n"
        "- PROOF SECTION: Include [TESTIMONIAL_PLACEHOLDER] markers. Social proof, stats, trust badges.\n"
        "- OBJECTION HANDLING: Address 2-3 top objections inline. 'What if it doesn't work?' "
        "'Is it worth the cost?' 'How is this different?'\n"
        "- CTA SECTION: Repeat the offer with urgency. Phone number prominent. Form placement "
        "marked with [LEAD_FORM_PLACEHOLDER] if no specific form shortcode provided.\n"
        "- NO NAVIGATION: This page should not link to other site pages except via the CTA. "
        "No sidebar. No footer links. Single-purpose page.\n"
        "- MOBILE FIRST: Short paragraphs. Big buttons. Scannable headings.\n"
        "- Target word count: 600-1000 words.\n"
        f"- Primary keyword: '{lp_keyword}' or '{ctx.get('industry', '')} {ctx.get('service_area', '').split(',')[0].strip()}'.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


def _prompt_custom(page_spec, ctx):
    """Prompt builder for user-defined custom pages."""
    brand = _brand_block(ctx)
    seo_intel = _seo_intel_block(ctx)
    lead_form = _lead_form_block(ctx)
    design = _design_block(ctx)
    cp_ctx = page_spec.get("context", {})
    cp_name = cp_ctx.get("custom_name", page_spec.get("label", "Page"))
    cp_desc = cp_ctx.get("custom_purpose", "") or cp_ctx.get("custom_description", "")
    cp_keyword = cp_ctx.get("custom_keyword", "")

    user_msg = (
        f"Write a website page titled '{cp_name}' for this local service business.\n\n"
        f"BUSINESS CONTEXT:\n{brand}\n\n"
        f"{seo_intel}"
        f"{lead_form}"
        f"{design}"
        f"{_GLOBAL_RULES}\n"
    )

    if cp_desc:
        user_msg += f"PAGE DESCRIPTION (from the business owner): {cp_desc}\n\n"
    if cp_keyword:
        user_msg += f"TARGET KEYWORD: {cp_keyword}\n\n"

    user_msg += (
        "PAGE-SPECIFIC REQUIREMENTS:\n"
        f"- This is a custom page the business owner requested: '{cp_name}'.\n"
        "- Write content that matches the intent described above.\n"
        "- Include proper SEO structure with headings, keyword placement, and a clear CTA.\n"
        "- Include internal links to other pages where relevant.\n"
        "- Target word count: 500-900 words.\n"
        f"{_OUTPUT_FORMAT}"
    )
    return _system_msg(), user_msg


_PROMPT_BUILDERS = {
    "home": _prompt_home,
    "about": _prompt_about,
    "services": _prompt_services,
    "service_detail": _prompt_service_detail,
    "service_area": _prompt_service_area,
    "contact": _prompt_contact,
    "faq": _prompt_faq,
    "testimonials": _prompt_testimonials,
    "landing_page": _prompt_landing_page,
    "custom": _prompt_custom,
}


# ---------------------------------------------------------------------------
# Schema markup generators
# ---------------------------------------------------------------------------

def build_schema_markup(page_spec, page_content, brand_ctx):
    """
    Build JSON-LD schema markup for a page.

    Returns a list of schema objects (each becomes a <script type="application/ld+json"> block).
    """
    schemas = []
    schema_types = page_spec.get("schema_types") or []
    hints = {}
    if isinstance(page_content, dict):
        hints = page_content.get("schema_hints") or {}

    site_url = brand_ctx.get("website") or ""
    page_slug = page_spec.get("slug") or ""
    page_url = f"{site_url.rstrip('/')}/{page_slug}".rstrip("/") if site_url else ""

    for schema_type in schema_types:
        builder = _SCHEMA_BUILDERS.get(schema_type)
        if builder:
            schema = builder(page_spec, page_content, brand_ctx, hints, page_url)
            if schema:
                schemas.append(schema)

    return schemas


def _schema_local_business(page_spec, content, ctx, hints, page_url):
    biz = {
        "@context": "https://schema.org",
        "@type": "LocalBusiness",
        "name": ctx.get("business_name") or "",
        "url": ctx.get("website") or page_url,
    }
    if ctx.get("phone"):
        biz["telephone"] = ctx["phone"]
    if ctx.get("email"):
        biz["email"] = ctx["email"]
    if ctx.get("address"):
        biz["address"] = {
            "@type": "PostalAddress",
            "streetAddress": ctx["address"],
        }
    if ctx.get("service_area"):
        areas = _parse_csv(ctx["service_area"])
        if areas:
            biz["areaServed"] = [{"@type": "City", "name": a} for a in areas]
    if ctx.get("hours"):
        biz["openingHours"] = ctx["hours"]
    if hints.get("priceRange"):
        biz["priceRange"] = hints["priceRange"]
    if ctx.get("industry"):
        biz["description"] = (
            f"{ctx['business_name']} provides {ctx['industry']} services"
            + (f" in {ctx['service_area']}" if ctx.get("service_area") else "")
            + "."
        )
    return biz


def _schema_website(page_spec, content, ctx, hints, page_url):
    site_url = ctx.get("website") or page_url
    if not site_url:
        return None
    schema = {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": ctx.get("business_name") or "",
        "url": site_url,
    }
    # Add SearchAction for site search if on home page
    if page_spec.get("page_type") == "home":
        schema["potentialAction"] = {
            "@type": "SearchAction",
            "target": f"{site_url.rstrip('/')}/?s={{search_term_string}}",
            "query-input": "required name=search_term_string",
        }
    return schema


def _schema_webpage(page_spec, content, ctx, hints, page_url):
    title = ""
    description = ""
    if isinstance(content, dict):
        title = content.get("seo_title") or content.get("title") or ""
        description = content.get("seo_description") or content.get("excerpt") or ""
    schema = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "name": title,
        "url": page_url or ctx.get("website") or "",
    }
    if description:
        schema["description"] = description
    if ctx.get("business_name"):
        schema["isPartOf"] = {
            "@type": "WebSite",
            "name": ctx["business_name"],
            "url": ctx.get("website") or "",
        }
    return schema


def _schema_organization(page_spec, content, ctx, hints, page_url):
    org = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": ctx.get("business_name") or "",
        "url": ctx.get("website") or "",
    }
    if ctx.get("phone"):
        org["telephone"] = ctx["phone"]
    if ctx.get("email"):
        org["email"] = ctx["email"]
    if ctx.get("year_founded"):
        org["foundingDate"] = ctx["year_founded"]
    if ctx.get("address"):
        org["address"] = {
            "@type": "PostalAddress",
            "streetAddress": ctx["address"],
        }
    return org


def _schema_service(page_spec, content, ctx, hints, page_url):
    svc_name = page_spec.get("context", {}).get("service_name") or ""
    if not svc_name and isinstance(content, dict):
        svc_name = content.get("title") or ""

    schema = {
        "@context": "https://schema.org",
        "@type": "Service",
        "name": svc_name,
        "url": page_url,
        "provider": {
            "@type": "LocalBusiness",
            "name": ctx.get("business_name") or "",
            "url": ctx.get("website") or "",
        },
    }
    if hints.get("serviceType"):
        schema["serviceType"] = hints["serviceType"]
    if hints.get("areaServed") or ctx.get("service_area"):
        area = hints.get("areaServed") or ctx.get("service_area") or ""
        areas = _parse_csv(area) if isinstance(area, str) else [area]
        schema["areaServed"] = [{"@type": "City", "name": a} for a in areas]
    if isinstance(content, dict) and content.get("seo_description"):
        schema["description"] = content["seo_description"]
    return schema


def _schema_faq_page(page_spec, content, ctx, hints, page_url):
    faq_items = []
    if isinstance(content, dict):
        faq_items = content.get("faq_items") or []
    if not faq_items:
        return None

    entities = []
    for item in faq_items:
        q = (item.get("question") or "").strip()
        a = (item.get("answer") or "").strip()
        if q and a:
            entities.append({
                "@type": "Question",
                "name": q,
                "acceptedAnswer": {
                    "@type": "Answer",
                    "text": a,
                },
            })
    if not entities:
        return None

    return {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": entities,
    }


def _schema_contact_page(page_spec, content, ctx, hints, page_url):
    return {
        "@context": "https://schema.org",
        "@type": "ContactPage",
        "name": "Contact",
        "url": page_url or "",
        "mainEntity": {
            "@type": "LocalBusiness",
            "name": ctx.get("business_name") or "",
            "telephone": ctx.get("phone") or "",
            "email": ctx.get("email") or "",
        },
    }


def _schema_breadcrumb(page_spec, content, ctx, hints, page_url):
    site_url = (ctx.get("website") or "").rstrip("/")
    if not site_url:
        return None

    slug = page_spec.get("slug") or ""
    parts = [p for p in slug.split("/") if p]
    if not parts:
        return None

    items = [{
        "@type": "ListItem",
        "position": 1,
        "name": "Home",
        "item": site_url,
    }]

    running = site_url
    for i, part in enumerate(parts, start=2):
        running = f"{running}/{part}"
        name = part.replace("-", " ").title()
        items.append({
            "@type": "ListItem",
            "position": i,
            "name": name,
            "item": running,
        })

    return {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": items,
    }


_SCHEMA_BUILDERS = {
    "LocalBusiness": _schema_local_business,
    "WebSite": _schema_website,
    "WebPage": _schema_webpage,
    "Organization": _schema_organization,
    "Service": _schema_service,
    "FAQPage": _schema_faq_page,
    "ContactPage": _schema_contact_page,
    "BreadcrumbList": _schema_breadcrumb,
}


# ---------------------------------------------------------------------------
# Content generation pipeline
# ---------------------------------------------------------------------------

def generate_page_content(page_spec, brand_ctx, api_key, model="gpt-4o-mini"):
    """
    Generate content for a single page via OpenAI.

    Returns dict with: title, content, excerpt, seo_title, seo_description,
    primary_keyword, secondary_keywords, faq_items, schema_hints
    """
    import openai

    system_msg, user_msg = build_page_prompt(page_spec, brand_ctx)

    client = openai.OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.5,
        response_format={"type": "json_object"},
    )
    raw = (response.choices[0].message.content or "{}").strip()
    result = _extract_json(raw)
    return result


def assemble_page(page_spec, brand_ctx, content):
    """
    Assemble final page output: combine generated content with schema markup.

    Returns:
        {
            "page_spec": {...},
            "content": {...},  (AI-generated fields)
            "schemas": [...],  (JSON-LD objects)
            "schema_html": "...",  (ready-to-inject script tags)
            "full_html": "...",  (content + schema scripts appended)
        }
    """
    schemas = build_schema_markup(page_spec, content, brand_ctx)
    schema_html = "\n".join(
        f'<script type="application/ld+json">\n{json.dumps(s, indent=2)}\n</script>'
        for s in schemas
    )

    body_html = _inject_builder_images(content.get("content") or "", page_spec, brand_ctx)
    page_type = page_spec.get("page_type") or ""
    theme_style = _theme_style_tag(brand_ctx)
    template_style = ""
    header_html = ""
    footer_html = ""
    if page_type != "landing_page":
        templates = brand_ctx.get("builder_templates") or []
        header_template = _select_builder_template(templates, page_type, "navigation", "nav", "header")
        footer_template = _select_builder_template(templates, page_type, "footer")
        template_style = _template_style_tag(header_template, footer_template)
        if header_template:
            header_html = _render_builder_template_html(header_template, page_spec, brand_ctx, content)
        if footer_template:
            footer_html = _render_builder_template_html(footer_template, page_spec, brand_ctx, content)

    parts = []
    if theme_style:
        parts.append(theme_style)
    if template_style:
        parts.append(template_style)
    if header_html:
        parts.append(header_html)
    if body_html:
        parts.append(body_html)
    if footer_html:
        parts.append(footer_html)
    if schema_html:
        parts.append(f"<!-- Schema Markup -->\n{schema_html}")
    full_html = "\n\n".join(part for part in parts if part)

    return {
        "page_spec": page_spec,
        "content": content,
        "body_html": body_html,
        "schemas": schemas,
        "schema_html": schema_html,
        "full_html": full_html,
    }


def generate_site(brand_ctx, blueprint, api_key, model="gpt-4o-mini", progress_cb=None):
    """
    Generate all pages for a site build.

    Args:
        brand_ctx: from build_brand_context
        blueprint: from build_site_blueprint
        api_key: OpenAI API key
        model: model name
        progress_cb: optional callback(page_index, total, page_spec) for progress

    Returns list of assembled page dicts.
    """
    pages = []
    total = len(blueprint)
    for i, page_spec in enumerate(blueprint):
        if progress_cb:
            progress_cb(i, total, page_spec)

        content = generate_page_content(page_spec, brand_ctx, api_key, model)
        assembled = assemble_page(page_spec, brand_ctx, content)
        pages.append(assembled)

    return pages


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _parse_csv(text):
    """Parse comma-separated values, stripping whitespace and empties."""
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def _slugify(text):
    """Convert text to URL-safe slug."""
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def _extract_json(raw):
    """Extract JSON from a response that may have markdown fences."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]  # remove opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse AI response as JSON, returning empty dict")
        return {}


# ---------------------------------------------------------------------------
# Warren SEO brief generator
# ---------------------------------------------------------------------------

def generate_warren_seo_brief(brand_ctx, seo_data, api_key, model="gpt-4o-mini"):
    """
    Have Warren analyze Search Console data and produce an SEO strategy brief
    for the site builder. Returns a plain-text brief string.
    """
    import openai

    brand = _brand_block(brand_ctx)

    # Format SC data into a readable block
    sc_lines = []
    if seo_data.get("totals"):
        t = seo_data["totals"]
        sc_lines.append(
            f"Overall: {t.get('clicks',0)} clicks, {t.get('impressions',0)} impressions, "
            f"{t.get('ctr',0)}% CTR, avg position {t.get('avg_position',0)}"
        )

    if seo_data.get("top_queries"):
        sc_lines.append("\nTop performing queries:")
        for q in seo_data["top_queries"][:20]:
            sc_lines.append(
                f"  - \"{q['query']}\" - clicks: {q['clicks']}, imp: {q['impressions']}, "
                f"pos: {q['position']}, ctr: {q['ctr']}%"
            )

    if seo_data.get("opportunity_queries"):
        sc_lines.append("\nOpportunity queries (high impressions, position 4-20):")
        for q in seo_data["opportunity_queries"][:15]:
            sc_lines.append(
                f"  - \"{q['query']}\" - imp: {q['impressions']}, pos: {q['position']}"
            )

    if seo_data.get("top_pages"):
        sc_lines.append("\nTop pages by clicks:")
        for p in seo_data["top_pages"][:10]:
            sc_lines.append(f"  - {p['page']} - clicks: {p['clicks']}, pos: {p['position']}")

    sc_block = "\n".join(sc_lines)

    system = (
        "You are Warren, an SEO strategist for a marketing platform that builds websites "
        "for local service businesses. You have access to real Google Search Console data. "
        "Your job is to analyze this data and produce a concise, actionable SEO strategy brief "
        "that will guide the AI content generator when building website pages.\n\n"
        "Be specific. Reference actual queries from the data. Identify patterns, gaps, and opportunities. "
        "Do NOT use em dashes. Do NOT use AI-tell words like harness, leverage, elevate, robust, seamless."
    )

    user_msg = (
        f"Analyze this business's Search Console data and write an SEO strategy brief.\n\n"
        f"BUSINESS:\n{brand}\n\n"
        f"SEARCH CONSOLE DATA:\n{sc_block}\n\n"
        "Produce a brief (300-500 words) covering:\n"
        "1. KEYWORD CLUSTERS: Group the queries into 3-6 topic clusters. For each cluster, "
        "identify which page type should target it (home, service detail, service area, FAQ, landing page).\n"
        "2. QUICK WINS: Queries in position 4-15 with high impressions that could move to page 1 "
        "with better on-page content. Name the specific queries.\n"
        "3. CONTENT GAPS: Topics the business SHOULD rank for based on their services but currently "
        "has no visibility on. Suggest specific page types or landing pages to create.\n"
        "4. KEYWORD MAPPING: For each page that will be generated, suggest the primary keyword "
        "and 2-3 secondary keywords based on the actual search data.\n"
        "5. INTERNAL LINKING STRATEGY: How pages should link to each other to build topic authority.\n\n"
        "Write in plain text, not JSON. Be direct and specific. Reference actual query data."
    )

    try:
        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.4,
            max_tokens=1500,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.warning("Warren SEO brief generation failed: %s", exc)
        return ""
