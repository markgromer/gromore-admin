"""Tests for the AI site builder: blueprint, prompts, schema, and end-to-end generation."""

import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch, MagicMock

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-sitebuilder-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from webapp.app import create_app
from webapp.site_builder import (
    build_brand_context,
    build_site_blueprint,
    build_page_prompt,
    build_schema_markup,
    assemble_page,
    generate_warren_seo_brief,
    PAGE_TYPES,
    _slugify,
    _parse_csv,
    _seo_intel_block,
    _lead_form_block,
    _design_block,
)


class _FakeChatResponse:
    def __init__(self, content):
        self.choices = [type("Choice", (), {"message": type("Message", (), {"content": content})()})()]


# ---------------------------------------------------------------------------
# Brand context shared across tests
# ---------------------------------------------------------------------------

_BRAND = {
    "display_name": "Ace Plumbing",
    "industry": "plumbing",
    "website": "https://aceplumbing.com",
    "service_area": "Springfield, Shelbyville",
    "primary_services": "Drain Cleaning, Water Heater Repair, Sewer Line Replacement",
    "brand_voice": "Friendly, honest, no-nonsense.",
    "target_audience": "Homeowners in Springfield area",
    "phone": "(555) 123-4567",
    "business_email": "info@aceplumbing.com",
    "address": "123 Main St, Springfield, IL 62701",
    "business_hours": "Mon-Fri 7am-6pm, Sat 8am-2pm",
    "year_founded": "2008",
    "license_info": "IL Licensed #042-123456",
    "certifications": "BBB A+ Rated, EPA Lead-Safe Certified",
    "primary_color": "#0f172a",
    "accent_color": "#f97316",
    "brand_colors": "#0f172a, #f97316, #e2e8f0",
    "font_heading": "Poppins",
    "font_body": "Source Sans Pro",
    "logo_path": "logos/test/logo.png",
}


class BlueprintTests(unittest.TestCase):
    """Test site blueprint generation."""

    def test_blueprint_includes_core_pages(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx)
        types = [p["page_type"] for p in bp]
        for core in ("home", "about", "services", "contact", "faq", "testimonials"):
            self.assertIn(core, types, f"Missing core page: {core}")

    def test_blueprint_creates_service_detail_pages(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx)
        detail_pages = [p for p in bp if p["page_type"] == "service_detail"]
        self.assertEqual(len(detail_pages), 3)
        labels = {p["label"] for p in detail_pages}
        self.assertIn("Drain Cleaning", labels)
        self.assertIn("Water Heater Repair", labels)
        self.assertIn("Sewer Line Replacement", labels)

    def test_blueprint_creates_service_area_pages(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx)
        area_pages = [p for p in bp if p["page_type"] == "service_area"]
        self.assertEqual(len(area_pages), 2)
        labels = {p["label"] for p in area_pages}
        self.assertIn("Springfield", labels)
        self.assertIn("Shelbyville", labels)

    def test_blueprint_overrides_services_and_areas(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx, services="Leak Detection, Pipe Repair", areas="Capital City")
        detail_labels = {p["label"] for p in bp if p["page_type"] == "service_detail"}
        area_labels = {p["label"] for p in bp if p["page_type"] == "service_area"}
        self.assertEqual(detail_labels, {"Leak Detection", "Pipe Repair"})
        self.assertEqual(area_labels, {"Capital City"})

    def test_blueprint_slugs_are_url_safe(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx)
        for page in bp:
            slug = page["slug"]
            if slug:
                self.assertNotIn(" ", slug, f"Slug has spaces: {slug}")
                self.assertEqual(slug, slug.lower(), f"Slug not lowercase: {slug}")

    def test_empty_brand_still_produces_core_pages(self):
        ctx = build_brand_context({})
        bp = build_site_blueprint(ctx)
        types = [p["page_type"] for p in bp]
        self.assertIn("home", types)
        self.assertIn("contact", types)


class PromptTests(unittest.TestCase):
    """Test prompt generation for each page type."""

    def setUp(self):
        self.ctx = build_brand_context(_BRAND)
        self.blueprint = build_site_blueprint(self.ctx)

    def test_every_page_type_produces_prompts(self):
        for page_spec in self.blueprint:
            sys_msg, user_msg = build_page_prompt(page_spec, self.ctx)
            self.assertTrue(len(sys_msg) > 50, f"Empty system message for {page_spec['page_type']}")
            self.assertTrue(len(user_msg) > 100, f"Short user message for {page_spec['page_type']}")

    def test_home_prompt_includes_brand_context(self):
        home = next(p for p in self.blueprint if p["page_type"] == "home")
        _, user_msg = build_page_prompt(home, self.ctx)
        self.assertIn("Ace Plumbing", user_msg)
        self.assertIn("plumbing", user_msg)
        self.assertIn("Springfield", user_msg)
        self.assertIn("(555) 123-4567", user_msg)

    def test_service_detail_prompt_references_specific_service(self):
        detail = next(p for p in self.blueprint if p["page_type"] == "service_detail")
        _, user_msg = build_page_prompt(detail, self.ctx)
        self.assertIn(detail["context"]["service_name"], user_msg)

    def test_service_area_prompt_references_area(self):
        area = next(p for p in self.blueprint if p["page_type"] == "service_area")
        _, user_msg = build_page_prompt(area, self.ctx)
        self.assertIn(area["context"]["area_name"], user_msg)

    def test_prompts_ban_em_dashes(self):
        for page_spec in self.blueprint:
            _, user_msg = build_page_prompt(page_spec, self.ctx)
            self.assertIn("Do NOT use em dashes", user_msg)

    def test_prompts_request_json_output(self):
        for page_spec in self.blueprint:
            _, user_msg = build_page_prompt(page_spec, self.ctx)
            self.assertIn("Return ONLY valid JSON", user_msg)
            self.assertIn("seo_title", user_msg)
            self.assertIn("seo_description", user_msg)
            self.assertIn("faq_items", user_msg)
            self.assertIn("schema_hints", user_msg)

    def test_prompts_require_modern_html_structure_hooks(self):
        home = next(p for p in self.blueprint if p["page_type"] == "home")
        system_msg, user_msg = build_page_prompt(home, self.ctx)

        self.assertIn("modern web designer", system_msg)
        self.assertIn("product designer and art director", system_msg)
        self.assertIn("semantic <main> and <section> blocks", user_msg)
        self.assertIn("flagship, component-driven React marketing site", user_msg)
        self.assertIn("services-grid", user_msg)
        self.assertIn("proof-grid", user_msg)
        self.assertIn("section-shell", user_msg)
        self.assertIn("spotlight-card", user_msg)
        self.assertIn("Do not wrap every section in the same rounded white card treatment", user_msg)
        self.assertIn("Design for a premium modern service business site from this decade", user_msg)

    def test_faq_prompt_requires_faq_items_for_schema(self):
        faq = next(p for p in self.blueprint if p["page_type"] == "faq")
        _, user_msg = build_page_prompt(faq, self.ctx)
        self.assertIn("faq_items MUST contain all Q&A pairs", user_msg)

    def test_prompt_uses_template_library_and_overrides(self):
        ctx = build_brand_context(
            _BRAND,
            intake={
                "builder_templates": [
                    {
                        "name": "Primary Header",
                        "category": "header",
                        "page_types": "all",
                        "html_content": "<header>{{business_name}}</header>",
                        "description": "A compact top navigation with phone CTA.",
                        "sort_order": 1,
                    },
                    {
                        "name": "Proof Strip",
                        "category": "social-proof",
                        "page_types": "home,about",
                        "html_content": "<section>Proof</section>",
                        "description": "A social proof strip with badges and short credibility copy.",
                        "sort_order": 2,
                    },
                ],
                "builder_prompt_overrides": [
                    {"page_type": "global", "section": "system_prompt", "content": "Always keep paragraphs under three lines."},
                    {"page_type": "home", "section": "user_prompt", "content": "Feature the financing CTA above the fold."},
                ],
            },
        )
        home = next(p for p in build_site_blueprint(ctx) if p["page_type"] == "home")

        system_msg, user_msg = build_page_prompt(home, ctx)

        self.assertIn("ADDITIONAL BUILDER SYSTEM RULES", system_msg)
        self.assertIn("Always keep paragraphs under three lines.", system_msg)
        self.assertIn("APPROVED TEMPLATE LIBRARY", user_msg)
        self.assertIn("Primary Header", user_msg)
        self.assertIn("Proof Strip", user_msg)
        self.assertIn("Feature the financing CTA above the fold.", user_msg)

    def test_prompt_mentions_selected_site_template_and_page_shell(self):
        ctx = build_brand_context(
            _BRAND,
            intake={
                "builder_site_template": {
                    "name": "Premium Service Kit",
                    "description": "A higher-ticket trust-heavy site kit.",
                    "prompt_notes": "Keep social proof above the first CTA.",
                },
                "builder_templates": [
                    {
                        "name": "Service Shell",
                        "category": "page_shell",
                        "page_types": "home,about",
                        "html_content": "<div class='page-shell'>{{page_content}}</div>",
                        "description": "Deterministic layout shell with hero, trust band, and CTA blocks.",
                        "sort_order": 1,
                    }
                ],
            },
        )
        home = next(p for p in build_site_blueprint(ctx) if p["page_type"] == "home")

        _, user_msg = build_page_prompt(home, ctx)

        self.assertIn("Selected site template: Premium Service Kit", user_msg)
        self.assertIn("Site template intent: A higher-ticket trust-heavy site kit.", user_msg)
        self.assertIn("Site template notes: Keep social proof above the first CTA.", user_msg)
        self.assertIn("Required page shell: Service Shell", user_msg)

    def test_brand_context_uses_builder_theme_as_fallback(self):
        ctx = build_brand_context(
            {"display_name": "Fallback Plumbing"},
            intake={
                "builder_theme": {
                    "name": "Warm Service",
                    "primary_color": "#123456",
                    "secondary_color": "#abcdef",
                    "accent_color": "#ff6600",
                    "text_color": "#111111",
                    "bg_color": "#faf7f2",
                    "font_heading": "Oswald",
                    "font_body": "Lato",
                    "button_style": "pill",
                    "layout_style": "modern-sections",
                }
            },
        )

        self.assertEqual(ctx["builder_theme_name"], "Warm Service")
        self.assertEqual(ctx["color_primary"], "#123456")
        self.assertEqual(ctx["color_secondary"], "#abcdef")
        self.assertEqual(ctx["color_accent"], "#ff6600")
        self.assertEqual(ctx["color_text"], "#111111")
        self.assertEqual(ctx["color_background"], "#faf7f2")
        self.assertEqual(ctx["font_heading"], "Oswald")
        self.assertEqual(ctx["font_body"], "Lato")
        self.assertEqual(ctx["layout_style"], "modern-sections")

    def test_prompt_uses_reference_site_direction(self):
        ctx = build_brand_context(
            _BRAND,
            intake={
                "reference_url": "https://example.com",
                "reference_mode": "layout",
                "reference_site_brief": {
                    "resolved_url": "https://example.com",
                    "mode": "layout",
                    "title": "Example Home Services",
                    "description": "A clean local-service homepage with strong social proof and a clear estimate CTA.",
                    "layout_style_hint": "modern-sections",
                    "style_preset_hint": "clean-minimal",
                    "heading_font_hint": "Oswald, sans-serif",
                    "body_font_hint": "Lato, sans-serif",
                    "button_style_hint": "solid pill",
                    "hero_layout_hint": "split",
                    "nav_items": ["Home", "Services", "About", "Contact"],
                    "headings": ["Fast Service", "Why Choose Us", "Recent Work"],
                    "cta_texts": ["Get Estimate", "Call Now"],
                    "color_hints": ["#112233", "#ff6600"],
                    "section_count": 6,
                    "section_patterns": [
                        {"category": "hero", "heading": "Fast Service", "summary": "Fast Service", "layout_hint": "split", "cta_texts": ["Get Estimate"]},
                        {"category": "services", "heading": "Our Services", "summary": "Our Services", "layout_hint": "grid", "cta_texts": []},
                        {"category": "testimonials", "heading": "Why Customers Stay", "summary": "Why Customers Stay", "layout_hint": "stacked", "cta_texts": []},
                    ],
                    "image_assets": [
                        {"role": "hero", "alt": "Plumber service van in driveway", "asset_url": "https://source.unsplash.com/featured/1600x900/?plumber&sig=1", "query": "plumber service van in driveway"},
                        {"role": "services", "alt": "Plumber repairing sink", "asset_url": "https://source.unsplash.com/featured/1600x900/?sink-repair&sig=2", "query": "plumber repairing sink"},
                    ],
                    "design_traits": [
                        "Hero uses a split layout with copy paired beside media instead of stacked content.",
                        "Mid-page sections rely on card or grid groupings to keep services and proof scannable.",
                    ],
                    "vision_notes": [
                        "The page uses generous spacing between section bands so cards do not feel crowded.",
                    ],
                    "notes": ["Top navigation uses a visible CTA button."]
                },
            },
        )
        home = next(p for p in build_site_blueprint(ctx) if p["page_type"] == "home")

        _, user_msg = build_page_prompt(home, ctx)

        self.assertIn("REFERENCE SITE DIRECTION", user_msg)
        self.assertIn("https://example.com", user_msg)
        self.assertIn("Match mode: layout", user_msg)
        self.assertIn("Navigation pattern: Home, Services, About, Contact", user_msg)
        self.assertIn("Reference section patterns to echo in the new build", user_msg)
        self.assertIn("hero: Fast Service [split] | CTA cues: Get Estimate", user_msg)
        self.assertIn("Approved replacement imagery to use instead of copying the source site's images", user_msg)
        self.assertIn("https://source.unsplash.com/featured/1600x900/?plumber&sig=1", user_msg)
        self.assertIn("Heading font vibe from the rendered page: Oswald, sans-serif", user_msg)
        self.assertIn("Button treatment cue: solid pill", user_msg)
        self.assertIn("Rendered design traits to preserve", user_msg)
        self.assertIn("Screenshot-level composition cues", user_msg)
        self.assertIn("do not copy text, logos, brand names, or images", user_msg)

    def test_brand_context_uses_reference_style_fallbacks(self):
        ctx = build_brand_context(
            {"display_name": "Reference Plumbing"},
            intake={
                "reference_site_brief": {
                    "layout_style_hint": "card-grid",
                    "style_preset_hint": "clean-minimal",
                }
            },
        )

        self.assertEqual(ctx["layout_style"], "card-grid")
        self.assertEqual(ctx["style_preset"], "clean-minimal")


class SchemaTests(unittest.TestCase):
    """Test JSON-LD schema markup generation."""

    def setUp(self):
        self.ctx = build_brand_context(_BRAND)

    def test_home_page_schemas(self):
        page_spec = {"page_type": "home", "slug": "", "schema_types": ["LocalBusiness", "WebSite", "WebPage"]}
        content = {
            "title": "Ace Plumbing - Springfield IL",
            "seo_title": "Ace Plumbing - Springfield IL",
            "seo_description": "Professional plumbing services in Springfield.",
            "faq_items": [],
            "schema_hints": {},
        }
        schemas = build_schema_markup(page_spec, content, self.ctx)
        types = [s["@type"] for s in schemas]
        self.assertIn("LocalBusiness", types)
        self.assertIn("WebSite", types)
        self.assertIn("WebPage", types)

    def test_local_business_schema_fields(self):
        page_spec = {"page_type": "home", "slug": "", "schema_types": ["LocalBusiness"]}
        content = {"schema_hints": {}}
        schemas = build_schema_markup(page_spec, content, self.ctx)
        biz = schemas[0]
        self.assertEqual(biz["@type"], "LocalBusiness")
        self.assertEqual(biz["name"], "Ace Plumbing")
        self.assertEqual(biz["telephone"], "(555) 123-4567")
        self.assertEqual(biz["email"], "info@aceplumbing.com")
        self.assertIn("areaServed", biz)
        self.assertEqual(len(biz["areaServed"]), 2)
        self.assertEqual(biz["areaServed"][0]["@type"], "City")

    def test_faq_schema_from_content(self):
        page_spec = {"page_type": "faq", "slug": "faq", "schema_types": ["FAQPage"]}
        content = {
            "faq_items": [
                {"question": "Do you offer free estimates?", "answer": "Yes, all estimates are free."},
                {"question": "Are you licensed?", "answer": "Yes, IL Licensed #042-123456."},
            ],
            "schema_hints": {},
        }
        schemas = build_schema_markup(page_spec, content, self.ctx)
        faq_schema = next(s for s in schemas if s["@type"] == "FAQPage")
        self.assertEqual(len(faq_schema["mainEntity"]), 2)
        self.assertEqual(faq_schema["mainEntity"][0]["@type"], "Question")
        self.assertEqual(faq_schema["mainEntity"][0]["name"], "Do you offer free estimates?")
        self.assertEqual(faq_schema["mainEntity"][0]["acceptedAnswer"]["@type"], "Answer")

    def test_empty_faq_items_skips_faq_schema(self):
        page_spec = {"page_type": "faq", "slug": "faq", "schema_types": ["FAQPage"]}
        content = {"faq_items": [], "schema_hints": {}}
        schemas = build_schema_markup(page_spec, content, self.ctx)
        faq_schemas = [s for s in schemas if s.get("@type") == "FAQPage"]
        self.assertEqual(len(faq_schemas), 0)

    def test_service_schema_includes_area_served(self):
        page_spec = {
            "page_type": "service_detail",
            "slug": "services/drain-cleaning",
            "schema_types": ["Service"],
            "context": {"service_name": "Drain Cleaning"},
        }
        content = {
            "seo_description": "Drain cleaning in Springfield.",
            "schema_hints": {"serviceType": "Drain Cleaning", "areaServed": "Springfield"},
        }
        schemas = build_schema_markup(page_spec, content, self.ctx)
        svc = next(s for s in schemas if s["@type"] == "Service")
        self.assertEqual(svc["name"], "Drain Cleaning")
        self.assertIn("areaServed", svc)
        self.assertEqual(svc["provider"]["name"], "Ace Plumbing")

    def test_breadcrumb_schema(self):
        page_spec = {
            "page_type": "service_detail",
            "slug": "services/drain-cleaning",
            "schema_types": ["BreadcrumbList"],
            "context": {},
        }
        content = {"schema_hints": {}}
        schemas = build_schema_markup(page_spec, content, self.ctx)
        bc = next(s for s in schemas if s["@type"] == "BreadcrumbList")
        self.assertEqual(len(bc["itemListElement"]), 3)  # Home > Services > Drain Cleaning
        self.assertEqual(bc["itemListElement"][0]["name"], "Home")
        self.assertEqual(bc["itemListElement"][0]["position"], 1)

    def test_website_schema_has_search_action_on_home(self):
        page_spec = {"page_type": "home", "slug": "", "schema_types": ["WebSite"]}
        content = {"schema_hints": {}}
        schemas = build_schema_markup(page_spec, content, self.ctx)
        ws = schemas[0]
        self.assertIn("potentialAction", ws)
        self.assertEqual(ws["potentialAction"]["@type"], "SearchAction")

    def test_contact_page_schema(self):
        page_spec = {"page_type": "contact", "slug": "contact", "schema_types": ["ContactPage"]}
        content = {"schema_hints": {}}
        schemas = build_schema_markup(page_spec, content, self.ctx)
        cp = schemas[0]
        self.assertEqual(cp["@type"], "ContactPage")
        self.assertEqual(cp["mainEntity"]["telephone"], "(555) 123-4567")

    def test_all_schemas_have_context(self):
        page_spec = {"page_type": "home", "slug": "", "schema_types": ["LocalBusiness", "WebSite", "WebPage"]}
        content = {"title": "Test", "seo_title": "Test", "seo_description": "Desc", "faq_items": [], "schema_hints": {}}
        schemas = build_schema_markup(page_spec, content, self.ctx)
        for s in schemas:
            self.assertEqual(s["@context"], "https://schema.org")


class AssemblyTests(unittest.TestCase):
    """Test page assembly (content + schema)."""

    def test_assemble_adds_schema_scripts(self):
        ctx = build_brand_context(_BRAND)
        page_spec = {"page_type": "home", "slug": "", "schema_types": ["LocalBusiness", "WebPage"]}
        content = {
            "title": "Ace Plumbing",
            "content": "<h2>Your Trusted Plumber</h2><p>We serve Springfield.</p>",
            "seo_title": "Ace Plumbing",
            "seo_description": "Plumbing services.",
            "faq_items": [],
            "schema_hints": {},
        }
        result = assemble_page(page_spec, ctx, content)
        self.assertIn("full_html", result)
        self.assertIn("schema_html", result)
        self.assertIn('<script type="application/ld+json">', result["full_html"])
        self.assertIn("LocalBusiness", result["full_html"])
        self.assertIn("Your Trusted Plumber", result["full_html"])

    def test_assemble_includes_all_schema_objects(self):
        ctx = build_brand_context(_BRAND)
        page_spec = {"page_type": "home", "slug": "", "schema_types": ["LocalBusiness", "WebSite", "WebPage"]}
        content = {"title": "T", "seo_title": "T", "seo_description": "D", "faq_items": [], "schema_hints": {}}
        result = assemble_page(page_spec, ctx, content)
        self.assertEqual(len(result["schemas"]), 3)

    def test_assemble_wraps_shared_templates_and_theme_css(self):
        ctx = build_brand_context(
            {
                "display_name": "Ace Plumbing",
                "phone": "(555) 123-4567",
            },
            intake={
                "builder_theme": {
                    "primary_color": "#123456",
                    "accent_color": "#ff6600",
                    "custom_css": ".site-shell { padding: 8px; }",
                },
                "builder_templates": [
                    {
                        "name": "Main Header",
                        "category": "header",
                        "page_types": "all",
                        "html_content": "<header class=\"site-shell\">{{business_name}}</header>",
                        "css_content": ".site-shell header{display:flex;}",
                        "sort_order": 1,
                    },
                    {
                        "name": "Main Footer",
                        "category": "footer",
                        "page_types": "all",
                        "html_content": "<footer>{{phone}}</footer>",
                        "sort_order": 2,
                    },
                ],
            },
        )
        page_spec = {"page_type": "home", "slug": "", "schema_types": []}
        content = {"title": "Home", "content": "<main><p>Body</p></main>", "faq_items": [], "schema_hints": {}}

        result = assemble_page(page_spec, ctx, content)

        self.assertIn("<style>", result["full_html"])
        self.assertIn("--sb-primary: #123456;", result["full_html"])
        self.assertIn(".sb-site-shell", result["full_html"])
        self.assertIn("<div class=\"sb-site-shell", result["full_html"])
        self.assertIn(".site-shell { padding: 8px; }", result["full_html"])
        self.assertIn(".site-shell header{display:flex;}", result["full_html"])
        self.assertIn("<header class=\"site-shell\">Ace Plumbing</header>", result["full_html"])
        self.assertIn("<main><p>Body</p></main>", result["full_html"])
        self.assertIn("<footer>(555) 123-4567</footer>", result["full_html"])

    def test_assemble_imports_google_fonts_when_context_sets_them(self):
        ctx = build_brand_context(
            {
                "display_name": "Ace Plumbing",
                "font_heading": "Space Grotesk",
                "font_body": "DM Sans",
            }
        )
        page_spec = {"page_type": "home", "slug": "", "schema_types": []}
        content = {"title": "Home", "content": "<main><p>Body</p></main>", "faq_items": [], "schema_hints": {}}

        result = assemble_page(page_spec, ctx, content)

        self.assertIn("fonts.googleapis.com", result["full_html"])
        self.assertIn("Space+Grotesk", result["full_html"])
        self.assertIn("DM+Sans", result["full_html"])
        self.assertIn("--sb-font-heading", result["full_html"])
        self.assertIn("--sb-font-body", result["full_html"])

    def test_landing_page_skips_shared_templates(self):
        ctx = build_brand_context(
            _BRAND,
            intake={
                "builder_templates": [
                    {
                        "name": "Main Header",
                        "category": "header",
                        "page_types": "all",
                        "html_content": "<header>{{business_name}}</header>",
                    },
                    {
                        "name": "Main Footer",
                        "category": "footer",
                        "page_types": "all",
                        "html_content": "<footer>{{phone}}</footer>",
                    },
                ]
            },
        )
        page_spec = {"page_type": "landing_page", "slug": "lp/test", "schema_types": []}
        content = {"title": "Offer", "content": "<main><p>Landing</p></main>", "faq_items": [], "schema_hints": {}}

        result = assemble_page(page_spec, ctx, content)

        self.assertNotIn("<header>", result["full_html"])
        self.assertNotIn("<footer>", result["full_html"])
        self.assertIn("<main><p>Landing</p></main>", result["full_html"])

    def test_landing_page_uses_page_shell_template_and_css(self):
        ctx = build_brand_context(
            _BRAND,
            intake={
                "builder_templates": [
                    {
                        "name": "Landing Shell",
                        "category": "page_shell",
                        "page_types": "landing_page",
                        "html_content": "<div class=\"lp-shell\"><section class=\"lp-shell-inner\">{{page_content}}</section></div>",
                        "css_content": ".lp-shell{padding:40px}.lp-shell-inner{max-width:880px;margin:0 auto}",
                    }
                ]
            },
        )
        page_spec = {"page_type": "landing_page", "slug": "lp/test", "schema_types": []}
        content = {"title": "Offer", "content": "<main><p>Landing</p></main>", "faq_items": [], "schema_hints": {}}

        result = assemble_page(page_spec, ctx, content)

        self.assertIn(".lp-shell{padding:40px}", result["full_html"])
        self.assertIn("<div class=\"lp-shell\">", result["full_html"])
        self.assertIn("<main><p>Landing</p></main>", result["full_html"])

    def test_assemble_injects_reference_images_when_content_has_none(self):
        ctx = build_brand_context(
            _BRAND,
            intake={
                "reference_site_brief": {
                    "image_assets": [
                        {"role": "hero", "alt": "Plumber service van in driveway", "asset_url": "https://source.unsplash.com/featured/1600x900/?plumber&sig=1"},
                        {"role": "services", "alt": "Plumber repairing kitchen sink", "asset_url": "https://source.unsplash.com/featured/1600x900/?sink-repair&sig=2"},
                    ]
                }
            },
        )
        page_spec = {"page_type": "home", "slug": "", "schema_types": []}
        content = {"title": "Home", "content": "<main><p>Plain copy only</p></main>", "faq_items": [], "schema_hints": {}}

        result = assemble_page(page_spec, ctx, content)

        self.assertIn("sb-reference-image-hero", result["body_html"])
        self.assertIn("https://source.unsplash.com/featured/1600x900/?plumber&sig=1", result["body_html"])
        self.assertIn("https://source.unsplash.com/featured/1600x900/?sink-repair&sig=2", result["full_html"])

    def test_assemble_injects_intake_images_ahead_of_reference_images(self):
        ctx = build_brand_context(
            _BRAND,
            intake={
                "image_slots": {
                    "hero_desktop": {
                        "label": "Homepage hero image - desktop",
                        "note": "Use the branded truck parked at the curb.",
                        "use_stock": True,
                        "stock_url": "https://example.com/hero-stock.jpg",
                        "assets": [],
                    },
                    "about_team": {
                        "label": "About / team image",
                        "note": "Show the owner with the crew.",
                        "use_stock": False,
                        "assets": [{"url": "https://example.com/team-upload.jpg", "original_name": "team.jpg"}],
                    },
                },
                "reference_site_brief": {
                    "image_assets": [
                        {"role": "hero", "alt": "Reference hero", "asset_url": "https://example.com/reference-hero.jpg"},
                    ]
                },
            },
        )
        page_spec = {"page_type": "home", "slug": "", "schema_types": []}
        content = {"title": "Home", "content": "<main><section class=\"hero\"><div class=\"hero-copy\"><h1>Plain copy only</h1></div></section></main>", "faq_items": [], "schema_hints": {}}

        result = assemble_page(page_spec, ctx, content)

        self.assertIn("<section class=\"hero\">", result["body_html"])
        self.assertIn("sb-intake-image-hero", result["body_html"])
        self.assertIn("sb-hero-media", result["body_html"])
        self.assertIn("https://example.com/hero-stock.jpg", result["body_html"])
        self.assertIn("https://example.com/team-upload.jpg", result["full_html"])
        self.assertNotIn("https://example.com/reference-hero.jpg", result["full_html"])


class DatabaseSiteBuilderTests(unittest.TestCase):
    """Test database operations for site builds and pages."""

    def setUp(self):
        self.db_file = _TEST_ROOT / f"sitebuilder-db-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_create_and_get_site_build(self):
        with self.app.app_context():
            brand_id = self.app.db.create_brand({"slug": f"sb-{uuid.uuid4().hex[:8]}", "display_name": "Test Co"})
            blueprint = [{"page_type": "home", "label": "Home", "slug": ""}]
            build_id = self.app.db.create_site_build(brand_id, blueprint, model="gpt-4o-mini")
            build = self.app.db.get_site_build(build_id)
        self.assertIsNotNone(build)
        self.assertEqual(build["brand_id"], brand_id)
        self.assertEqual(build["status"], "pending")
        self.assertEqual(build["page_count"], 1)
        self.assertEqual(build["blueprint"], blueprint)

    def test_update_build_status(self):
        with self.app.app_context():
            brand_id = self.app.db.create_brand({"slug": f"sb-{uuid.uuid4().hex[:8]}", "display_name": "Test Co"})
            build_id = self.app.db.create_site_build(brand_id, [])
            self.app.db.update_site_build_status(build_id, "completed", pages_completed=5)
            build = self.app.db.get_site_build(build_id)
        self.assertEqual(build["status"], "completed")
        self.assertEqual(build["pages_completed"], 5)
        self.assertTrue(build["completed_at"])

    def test_save_and_get_site_pages(self):
        with self.app.app_context():
            brand_id = self.app.db.create_brand({"slug": f"sb-{uuid.uuid4().hex[:8]}", "display_name": "Test Co"})
            build_id = self.app.db.create_site_build(brand_id, [])
            page_id = self.app.db.save_site_page({
                "build_id": build_id,
                "brand_id": brand_id,
                "page_type": "home",
                "label": "Home",
                "slug": "",
                "title": "Ace Plumbing",
                "content": "<p>Test content</p>",
                "seo_title": "Ace Plumbing - Home",
                "seo_description": "Best plumber.",
                "primary_keyword": "plumber springfield",
                "faq_items": [{"question": "Q?", "answer": "A."}],
                "schemas": [{"@type": "LocalBusiness", "name": "Ace"}],
                "schema_html": '<script type="application/ld+json">{"@type":"LocalBusiness"}</script>',
                "full_html": "<p>Test</p>\n<script>...</script>",
            })
            pages = self.app.db.get_site_pages(build_id)
            page = self.app.db.get_site_page(page_id)
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["page_type"], "home")
        self.assertEqual(pages[0]["primary_keyword"], "plumber springfield")
        self.assertEqual(len(pages[0]["faq_items"]), 1)
        self.assertEqual(len(pages[0]["schemas"]), 1)
        self.assertIsNotNone(page)
        self.assertEqual(page["title"], "Ace Plumbing")

    def test_update_page_wp_status(self):
        with self.app.app_context():
            brand_id = self.app.db.create_brand({"slug": f"sb-{uuid.uuid4().hex[:8]}", "display_name": "Test Co"})
            build_id = self.app.db.create_site_build(brand_id, [])
            page_id = self.app.db.save_site_page({
                "build_id": build_id, "brand_id": brand_id,
                "page_type": "about", "label": "About", "title": "About Us",
            })
            self.app.db.update_site_page_wp(page_id, 42, "https://example.com/about/")
            page = self.app.db.get_site_page(page_id)
        self.assertEqual(page["wp_page_id"], 42)
        self.assertEqual(page["wp_page_url"], "https://example.com/about/")
        self.assertEqual(page["status"], "published")

    def test_get_site_builds_returns_most_recent_first(self):
        with self.app.app_context():
            brand_id = self.app.db.create_brand({"slug": f"sb-{uuid.uuid4().hex[:8]}", "display_name": "Test Co"})
            id1 = self.app.db.create_site_build(brand_id, [{"page_type": "home"}])
            id2 = self.app.db.create_site_build(brand_id, [{"page_type": "about"}])
            builds = self.app.db.get_site_builds(brand_id)
        self.assertEqual(len(builds), 2)
        self.assertEqual(builds[0]["id"], id2)


class EndToEndRouteTests(unittest.TestCase):
    """Test the site builder routes end-to-end with mocked AI."""

    def setUp(self):
        self.db_file = _TEST_ROOT / f"sitebuilder-e2e-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"
        os.environ["OPENAI_API_KEY"] = "test-openai-key"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"e2e-site-{uuid.uuid4().hex[:8]}",
                "display_name": "Ace Plumbing",
            })
            # Set brand fields
            for field, value in [
                ("industry", "plumbing"),
                ("service_area", "Springfield"),
                ("primary_services", "Drain Cleaning, Water Heater Repair"),
                ("brand_voice", "Friendly and professional."),
                ("website", "https://aceplumbing.com"),
            ]:
                self.app.db.update_brand_text_field(self.brand_id, field, value)

            self.client_user_id = self.app.db.create_client_user(
                self.brand_id, f"owner-{uuid.uuid4().hex[:8]}@example.com", "Password123", "Owner"
            )

        with self.client.session_transaction() as sess:
            sess["client_user_id"] = self.client_user_id
            sess["client_brand_id"] = self.brand_id
            sess["client_user_name"] = "Owner"

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def _mock_ai_response(self, page_spec):
        """Build a mock AI response for any page type."""
        return json.dumps({
            "title": f"{page_spec.get('label', 'Page')} - Ace Plumbing",
            "content": f"<h2>{page_spec.get('label', 'Page')}</h2><p>Professional plumbing content for {page_spec.get('label', 'this page')}.</p>",
            "excerpt": "Professional plumbing services in Springfield.",
            "seo_title": f"{page_spec.get('label', 'Page')} - Ace Plumbing Springfield",
            "seo_description": "Trusted plumbing services for Springfield homeowners.",
            "primary_keyword": "plumber springfield",
            "secondary_keywords": "drain cleaning, water heater repair",
            "faq_items": [
                {"question": "Do you offer free estimates?", "answer": "Yes, all estimates are free."},
                {"question": "Are you licensed?", "answer": "Yes, fully licensed in IL."},
            ],
            "schema_hints": {"serviceType": "Plumbing", "areaServed": "Springfield"},
        })

    @patch("openai.OpenAI")
    def test_generate_creates_build_with_pages(self, mock_openai):
        mock_client = mock_openai.return_value
        # Each page gets a separate AI call, so return_value handles all
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            self._mock_ai_response({"label": "Home"})
        )

        response = self.client.post(
            "/client/site-builder/generate",
            data={"services": "Drain Cleaning", "areas": "Springfield"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        # 6 core pages + 1 service detail + 1 area = 8
        self.assertEqual(body["pages_generated"], 8)
        self.assertGreater(body["build_id"], 0)

        with self.app.app_context():
            build = self.app.db.get_site_build(body["build_id"])
            pages = self.app.db.get_site_pages(body["build_id"])

        self.assertEqual(build["status"], "completed")
        self.assertEqual(len(pages), 8)

        # Verify schema markup was generated
        home_page = next(p for p in pages if p["page_type"] == "home")
        self.assertIn("application/ld+json", home_page["schema_html"])
        self.assertIn("LocalBusiness", home_page["schema_html"])
        self.assertTrue(len(home_page["faq_items"]) >= 1)

    @patch("openai.OpenAI")
    def test_generate_stores_seo_metadata(self, mock_openai):
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            self._mock_ai_response({"label": "Home"})
        )

        response = self.client.post(
            "/client/site-builder/generate",
            data={"services": "Drain Cleaning", "areas": "Springfield"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        body = response.get_json()

        with self.app.app_context():
            pages = self.app.db.get_site_pages(body["build_id"])

        for page in pages:
            self.assertTrue(page["seo_title"], f"Missing seo_title on {page['page_type']}")
            self.assertTrue(page["seo_description"], f"Missing seo_description on {page['page_type']}")
            self.assertTrue(page["primary_keyword"], f"Missing primary_keyword on {page['page_type']}")

    @patch("openai.OpenAI")
    def test_review_returns_build_and_pages(self, mock_openai):
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            self._mock_ai_response({"label": "Home"})
        )

        gen_resp = self.client.post(
            "/client/site-builder/generate",
            data={"services": "Drain Cleaning", "areas": "Springfield"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        build_id = gen_resp.get_json()["build_id"]

        review_resp = self.client.get(
            f"/client/site-builder/{build_id}",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(review_resp.status_code, 200)
        body = review_resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(len(body["pages"]), 8)

    @patch("openai.OpenAI")
    def test_publish_sends_pages_to_wordpress(self, mock_openai):
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            self._mock_ai_response({"label": "Home"})
        )

        # Set WP credentials
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "wp_site_url", "https://aceplumbing.com")
            self.app.db.update_brand_text_field(self.brand_id, "wp_username", "admin")
            self.app.db.update_brand_text_field(self.brand_id, "wp_app_password", "xxxx-xxxx-xxxx")

        gen_resp = self.client.post(
            "/client/site-builder/generate",
            data={"services": "Drain Cleaning", "areas": "Springfield"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        build_id = gen_resp.get_json()["build_id"]

        with patch("webapp.client_portal._publish_wp_page") as mock_wp:
            mock_wp.return_value = {"ok": True, "wp_page_id": 99, "wp_page_url": "https://aceplumbing.com/about/"}
            pub_resp = self.client.post(
                f"/client/site-builder/{build_id}/publish",
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(pub_resp.status_code, 200)
        body = pub_resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["published"], 8)
        self.assertEqual(body["total"], 8)

        with self.app.app_context():
            pages = self.app.db.get_site_pages(build_id)
        for page in pages:
            self.assertEqual(page["wp_page_id"], 99)
            self.assertEqual(page["status"], "published")

    def test_publish_fails_without_wp_credentials(self):
        with self.app.app_context():
            build_id = self.app.db.create_site_build(self.brand_id, [])
        resp = self.client.post(
            f"/client/site-builder/{build_id}/publish",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.get_json()["ok"])


class UtilityTests(unittest.TestCase):
    """Test helper functions."""

    def test_slugify(self):
        self.assertEqual(_slugify("Drain Cleaning"), "drain-cleaning")
        self.assertEqual(_slugify("Water Heater  Repair"), "water-heater-repair")
        self.assertEqual(_slugify("HVAC & Cooling"), "hvac-cooling")
        self.assertEqual(_slugify("  Spaces  "), "spaces")

    def test_parse_csv(self):
        self.assertEqual(_parse_csv("a, b, c"), ["a", "b", "c"])
        self.assertEqual(_parse_csv(""), [])
        self.assertEqual(_parse_csv(None), [])
        self.assertEqual(_parse_csv("single"), ["single"])


# ---------------------------------------------------------------------------
# Intake, SEO intel, Warren, landing page, and lead form tests
# ---------------------------------------------------------------------------

class IntakeContextTests(unittest.TestCase):
    """Test build_brand_context with intake overrides."""

    def test_intake_overrides_brand_voice(self):
        intake = {"brand_voice": "Authoritative and technical."}
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["brand_voice"], "Authoritative and technical.")

    def test_intake_overrides_target_audience(self):
        intake = {"target_audience": "Commercial property managers"}
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["target_audience"], "Commercial property managers")

    def test_intake_adds_unique_selling_points(self):
        intake = {"unique_selling_points": "24/7 emergency service, 50 years combined experience"}
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["unique_selling_points"], "24/7 emergency service, 50 years combined experience")

    def test_intake_adds_competitors(self):
        intake = {"competitors": "Joe's Plumbing, Springfield Plumbers Inc"}
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["competitors"], "Joe's Plumbing, Springfield Plumbers Inc")

    def test_intake_adds_content_goals(self):
        intake = {"content_goals": "Generate Leads, Rank in Google"}
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["content_goals"], "Generate Leads, Rank in Google")

    def test_intake_adds_expanded_service_and_design_fields(self):
        intake = {
            "services_to_highlight": "Drain cleaning, sewer repair, emergency plumbing",
            "service_plan_options": "Weekly, twice weekly, monthly, one-time",
            "service_add_ons": "Deodorizer, sanitizer, haul waste away",
            "priority_seo_locations": "Springfield, Shelbyville, Chatham",
            "company_story": "Family-owned with transparent pricing.",
            "site_vision": "A premium but direct lead generation site.",
            "design_notes": "Use stronger before/after imagery and more breathing room.",
        }
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["services_to_highlight"], "Drain cleaning, sewer repair, emergency plumbing")
        self.assertEqual(ctx["service_plan_options"], "Weekly, twice weekly, monthly, one-time")
        self.assertEqual(ctx["service_add_ons"], "Deodorizer, sanitizer, haul waste away")
        self.assertEqual(ctx["priority_seo_locations"], "Springfield, Shelbyville, Chatham")
        self.assertEqual(ctx["company_story"], "Family-owned with transparent pricing.")
        self.assertEqual(ctx["site_vision"], "A premium but direct lead generation site.")
        self.assertEqual(ctx["design_notes"], "Use stronger before/after imagery and more breathing room.")

    def test_intake_includes_seo_data(self):
        seo = {"totals": {"clicks": 100}, "top_queries": [{"query": "plumber near me"}]}
        intake = {"seo_data": seo}
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["seo_data"]["totals"]["clicks"], 100)

    def test_intake_includes_warren_brief(self):
        intake = {"warren_brief": "Focus on drain cleaning keywords."}
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["warren_brief"], "Focus on drain cleaning keywords.")

    def test_no_intake_leaves_defaults(self):
        ctx = build_brand_context(_BRAND)
        self.assertEqual(ctx["brand_voice"], "Friendly, honest, no-nonsense.")
        self.assertEqual(ctx["unique_selling_points"], "")
        self.assertEqual(ctx["seo_data"], {})
        self.assertEqual(ctx["warren_brief"], "")

    def test_intake_lead_form_fields(self):
        intake = {
            "lead_form_type": "wpforms",
            "lead_form_shortcode": '[wpforms id="42"]',
            "plugins": "WP Booking Calendar",
        }
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["lead_form_type"], "wpforms")
        self.assertIn("42", ctx["lead_form_shortcode"])
        self.assertEqual(ctx["plugins"], "WP Booking Calendar")

    def test_intake_quote_tool_fields(self):
        intake = {
            "quote_tool_source": "wp_shortcode",
            "quote_tool_embed": "[sng_quote_tool]",
            "quote_tool_zip_mode": "verify",
            "quote_tool_collect_dogs": True,
            "quote_tool_collect_frequency": True,
            "quote_tool_collect_last_cleaned": True,
            "quote_tool_phone_mode": "optional",
            "quote_tool_notes": "Lead with pricing clarity.",
        }
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["quote_tool_source"], "wp_shortcode")
        self.assertEqual(ctx["quote_tool_embed"], "[sng_quote_tool]")
        self.assertEqual(ctx["quote_tool_zip_mode"], "verify")
        self.assertTrue(ctx["quote_tool_collect_dogs"])
        self.assertTrue(ctx["quote_tool_collect_frequency"])
        self.assertTrue(ctx["quote_tool_collect_last_cleaned"])
        self.assertEqual(ctx["quote_tool_phone_mode"], "optional")
        self.assertEqual(ctx["quote_tool_notes"], "Lead with pricing clarity.")

    def test_brand_context_uses_saved_brand_design_defaults(self):
        ctx = build_brand_context(_BRAND)
        self.assertEqual(ctx["color_primary"], "#0f172a")
        self.assertEqual(ctx["color_accent"], "#f97316")
        self.assertEqual(ctx["font_heading"], "Poppins")
        self.assertEqual(ctx["font_body"], "Source Sans Pro")
        self.assertEqual(ctx["brand_logo_path"], "logos/test/logo.png")
        self.assertEqual(ctx["brand_colors"][:2], ["#0f172a", "#f97316"])

    def test_intake_normalizes_fonts_and_keeps_wireframe_and_image_slots(self):
        intake = {
            "font_heading": "  Space   Grotesk!!! ",
            "font_body": "DM Sans<script>",
            "wireframe_style": "conversion",
            "image_slots": {
                "hero_desktop": {
                    "label": "Hero Desktop",
                    "use_stock": True,
                    "note": "Show a clean service truck at the curb.",
                    "assets": [],
                }
            },
        }
        ctx = build_brand_context(_BRAND, intake=intake)
        self.assertEqual(ctx["font_heading"], "Space Grotesk")
        self.assertEqual(ctx["font_body"], "DM Sansscript")
        self.assertEqual(ctx["wireframe_style"], "conversion")
        self.assertIn("hero_desktop", ctx["image_slots"])


class LandingPageBlueprintTests(unittest.TestCase):
    """Test landing page generation in blueprints."""

    def test_landing_pages_added_to_blueprint(self):
        ctx = build_brand_context(_BRAND)
        lps = [
            {"name": "Summer AC Special", "keyword": "ac repair springfield", "offer": "20% off"},
            {"name": "Emergency Plumber", "keyword": "emergency plumber", "offer": "Call now"},
        ]
        bp = build_site_blueprint(ctx, landing_pages=lps)
        lp_pages = [p for p in bp if p["page_type"] == "landing_page"]
        self.assertEqual(len(lp_pages), 2)
        slugs = {p["slug"] for p in lp_pages}
        self.assertIn("lp/summer-ac-special", slugs)
        self.assertIn("lp/emergency-plumber", slugs)

    def test_landing_page_context_has_keyword_and_offer(self):
        ctx = build_brand_context(_BRAND)
        lps = [{"name": "Spring Special", "keyword": "drain cleaning deals", "offer": "10% off first visit", "audience": "Homeowners"}]
        bp = build_site_blueprint(ctx, landing_pages=lps)
        lp = next(p for p in bp if p["page_type"] == "landing_page")
        self.assertEqual(lp["context"]["lp_keyword"], "drain cleaning deals")
        self.assertEqual(lp["context"]["lp_offer"], "10% off first visit")
        self.assertEqual(lp["context"]["lp_audience"], "Homeowners")

    def test_empty_landing_page_name_skipped(self):
        ctx = build_brand_context(_BRAND)
        lps = [{"name": "", "keyword": "test"}, {"name": "  ", "keyword": "test2"}]
        bp = build_site_blueprint(ctx, landing_pages=lps)
        lp_pages = [p for p in bp if p["page_type"] == "landing_page"]
        self.assertEqual(len(lp_pages), 0)

    def test_landing_page_type_in_page_types(self):
        self.assertIn("landing_page", PAGE_TYPES)
        self.assertEqual(PAGE_TYPES["landing_page"]["priority"], 9)


class PageSelectionTests(unittest.TestCase):
    """Test page_selection filtering in blueprints."""

    def test_page_selection_filters_standard_pages(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx, page_selection=["home", "contact"])
        standard = [p for p in bp if p["page_type"] in ("home", "about", "services", "contact", "faq", "testimonials")]
        types = {p["page_type"] for p in standard}
        self.assertEqual(types, {"home", "contact"})

    def test_page_selection_still_includes_service_details(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx, page_selection=["home", "services"])
        detail_pages = [p for p in bp if p["page_type"] == "service_detail"]
        self.assertGreater(len(detail_pages), 0)

    def test_page_selection_filters_service_areas(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx, page_selection=["home", "services"])
        area_pages = [p for p in bp if p["page_type"] == "service_area"]
        self.assertEqual(len(area_pages), 0)

    def test_no_page_selection_includes_all(self):
        ctx = build_brand_context(_BRAND)
        bp = build_site_blueprint(ctx, page_selection=None)
        types = {p["page_type"] for p in bp}
        for core in ("home", "about", "services", "contact", "faq", "testimonials"):
            self.assertIn(core, types)


class SeoIntelBlockTests(unittest.TestCase):
    """Test _seo_intel_block formatting."""

    def test_empty_seo_data_returns_empty(self):
        ctx = {"seo_data": {}, "warren_brief": ""}
        self.assertEqual(_seo_intel_block(ctx), "")

    def test_top_queries_formatted(self):
        ctx = {
            "seo_data": {
                "top_queries": [
                    {"query": "plumber near me", "clicks": 50, "impressions": 500, "position": 3.2, "ctr": 10.0}
                ]
            },
            "warren_brief": "",
        }
        block = _seo_intel_block(ctx)
        self.assertIn("plumber near me", block)
        self.assertIn("SEO INTELLIGENCE", block)
        self.assertIn("TOP PERFORMING KEYWORDS", block)

    def test_opportunity_queries_formatted(self):
        ctx = {
            "seo_data": {
                "opportunity_queries": [
                    {"query": "drain cleaning springfield", "impressions": 300, "position": 8.5}
                ]
            },
            "warren_brief": "",
        }
        block = _seo_intel_block(ctx)
        self.assertIn("drain cleaning springfield", block)
        self.assertIn("OPPORTUNITY KEYWORDS", block)

    def test_warren_brief_included(self):
        ctx = {
            "seo_data": {"totals": {"clicks": 100, "impressions": 1000, "ctr": 10.0, "avg_position": 5.0}},
            "warren_brief": "Focus on drain cleaning cluster.",
        }
        block = _seo_intel_block(ctx)
        self.assertIn("WARREN'S SEO STRATEGY BRIEF", block)
        self.assertIn("Focus on drain cleaning cluster.", block)

    def test_totals_formatted(self):
        ctx = {
            "seo_data": {"totals": {"clicks": 200, "impressions": 5000, "ctr": 4.0, "avg_position": 12.3}},
            "warren_brief": "",
        }
        block = _seo_intel_block(ctx)
        self.assertIn("200 clicks", block)
        self.assertIn("5000 impressions", block)

    def test_priority_locations_formatted_without_search_console_data(self):
        ctx = {
            "seo_data": {},
            "warren_brief": "",
            "priority_seo_locations": "Springfield, Shelbyville, Chatham",
        }
        block = _seo_intel_block(ctx)
        self.assertIn("PRIORITY GEO TARGETS", block)
        self.assertIn("Springfield, Shelbyville, Chatham", block)


class LeadFormBlockTests(unittest.TestCase):
    """Test _lead_form_block formatting."""

    def test_no_form_returns_empty(self):
        ctx = {"lead_form_type": "", "lead_form_shortcode": "", "plugins": ""}
        self.assertEqual(_lead_form_block(ctx), "")

    def test_wpforms_block(self):
        ctx = {"lead_form_type": "wpforms", "lead_form_shortcode": '[wpforms id="42"]', "plugins": ""}
        block = _lead_form_block(ctx)
        self.assertIn("WPForms", block)
        self.assertIn('[wpforms id="42"]', block)

    def test_cf7_block(self):
        ctx = {"lead_form_type": "cf7", "lead_form_shortcode": "", "plugins": ""}
        block = _lead_form_block(ctx)
        self.assertIn("Contact Form 7", block)

    def test_gravity_block(self):
        ctx = {"lead_form_type": "gravity", "lead_form_shortcode": "", "plugins": ""}
        block = _lead_form_block(ctx)
        self.assertIn("Gravity Forms", block)

    def test_custom_form_with_shortcode(self):
        ctx = {"lead_form_type": "custom", "lead_form_shortcode": "<div id='myform'></div>", "plugins": ""}
        block = _lead_form_block(ctx)
        self.assertIn("<div id='myform'></div>", block)

    def test_plugins_appended(self):
        ctx = {"lead_form_type": "wpforms", "lead_form_shortcode": "", "plugins": "WP Booking Calendar, Yoast SEO"}
        block = _lead_form_block(ctx)
        self.assertIn("WP Booking Calendar", block)
        self.assertIn("Yoast SEO", block)

    def test_quote_tool_details_appended(self):
        ctx = {
            "lead_form_type": "wpforms",
            "lead_form_shortcode": '[wpforms id="42"]',
            "plugins": "",
            "quote_tool_source": "wp_shortcode",
            "quote_tool_embed": "[sng_quote_tool]",
            "quote_tool_zip_mode": "verify",
            "quote_tool_collect_dogs": True,
            "quote_tool_collect_frequency": True,
            "quote_tool_collect_last_cleaned": True,
            "quote_tool_phone_mode": "optional",
            "quote_tool_notes": "Keep the quote tool above the fold.",
        }
        block = _lead_form_block(ctx)
        self.assertIn("QUOTE TOOL CONFIGURATION", block)
        self.assertIn("[sng_quote_tool]", block)
        self.assertIn("verify the visitor ZIP code", block)
        self.assertIn("number of dogs", block)
        self.assertIn("service frequency", block)
        self.assertIn("Phone number should be optional", block)


class DesignBlockTests(unittest.TestCase):
    """Test _design_block formatting."""

    def test_design_block_includes_site_vision_and_notes(self):
        ctx = {
            "site_vision": "A premium but direct lead generation site.",
            "design_notes": "Use stronger before/after imagery and more breathing room.",
            "font_pair": "plusjakarta-inter",
        }
        block = _design_block(ctx)
        self.assertIn("Desired site vision", block)
        self.assertIn("A premium but direct lead generation site.", block)
        self.assertIn("Extra design notes", block)
        self.assertIn("Use stronger before/after imagery and more breathing room.", block)
        self.assertIn("Plus Jakarta Sans", block)

    def test_design_block_includes_hero_and_widget_layout_choices(self):
        ctx = {
            "hero_layout": "split-right",
            "services_widget_layout": "image-cards",
            "proof_widget_layout": "before-after-grid",
            "cta_widget_layout": "split-form",
            "image_slots": {
                "hero_desktop": {
                    "assets": [{"url": "https://example.com/hero.jpg"}],
                    "stock_url": "",
                }
            },
        }
        block = _design_block(ctx)
        self.assertIn("copy on the left, primary image on the right", block)
        self.assertIn("visual service cards", block)
        self.assertIn("visual before/after or result gallery grid", block)
        self.assertIn("form or booking block beside it", block)
        self.assertIn("selected hero image must be integrated inside the hero section", block)


class WarrenSEOBriefTests(unittest.TestCase):
    """Test generate_warren_seo_brief."""

    @patch("openai.OpenAI")
    def test_generates_brief_from_seo_data(self, mock_openai):
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            "Focus on drain cleaning keywords. Target 'plumber near me' on the home page."
        )
        ctx = build_brand_context(_BRAND)
        seo_data = {
            "totals": {"clicks": 100, "impressions": 1000, "ctr": 10.0, "avg_position": 5.0},
            "top_queries": [{"query": "plumber near me", "clicks": 50, "impressions": 500, "position": 3.2, "ctr": 10.0}],
            "opportunity_queries": [{"query": "drain cleaning springfield", "impressions": 300, "position": 8.5}],
            "top_pages": [{"page": "/", "clicks": 80, "position": 4.0}],
        }
        brief = generate_warren_seo_brief(ctx, seo_data, "test-key")
        self.assertIn("drain cleaning", brief)
        # Verify API was called
        mock_client.chat.completions.create.assert_called_once()
        call_kwargs = mock_client.chat.completions.create.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        system_msg = messages[0]["content"]
        self.assertIn("Warren", system_msg)

    @patch("openai.OpenAI")
    def test_returns_empty_on_failure(self, mock_openai):
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.side_effect = Exception("API error")
        ctx = build_brand_context(_BRAND)
        brief = generate_warren_seo_brief(ctx, {"totals": {}}, "test-key")
        self.assertEqual(brief, "")


class DatabaseIntakeTests(unittest.TestCase):
    """Test intake_json storage in site_builds."""

    def setUp(self):
        self.db_file = _TEST_ROOT / f"sitebuilder-intake-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_create_build_with_intake(self):
        intake = {
            "brand_voice": "Professional",
            "unique_selling_points": "24/7 service",
            "lead_form_type": "wpforms",
            "content_goals": "Generate Leads, Rank in Google",
        }
        with self.app.app_context():
            brand_id = self.app.db.create_brand({"slug": f"intake-{uuid.uuid4().hex[:8]}", "display_name": "Test Co"})
            build_id = self.app.db.create_site_build(brand_id, [{"page_type": "home"}], intake=intake)
            build = self.app.db.get_site_build(build_id)
        self.assertIsNotNone(build.get("intake"))
        self.assertEqual(build["intake"]["brand_voice"], "Professional")
        self.assertEqual(build["intake"]["lead_form_type"], "wpforms")

    def test_create_build_without_intake(self):
        with self.app.app_context():
            brand_id = self.app.db.create_brand({"slug": f"nointake-{uuid.uuid4().hex[:8]}", "display_name": "Test Co"})
            build_id = self.app.db.create_site_build(brand_id, [{"page_type": "home"}])
            build = self.app.db.get_site_build(build_id)
        self.assertEqual(build.get("intake"), {})


if __name__ == "__main__":
    unittest.main()
