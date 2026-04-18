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
    PAGE_TYPES,
    _slugify,
    _parse_csv,
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

    def test_faq_prompt_requires_faq_items_for_schema(self):
        faq = next(p for p in self.blueprint if p["page_type"] == "faq")
        _, user_msg = build_page_prompt(faq, self.ctx)
        self.assertIn("faq_items MUST contain all Q&A pairs", user_msg)


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


if __name__ == "__main__":
    unittest.main()
