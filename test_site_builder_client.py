"""
Tests for the client-facing Site Builder UI:
- Landing page route (GET /client/site-builder)
- Review page route (GET /client/site-builder/<id>)
- Generate route (POST /client/site-builder/generate)
- Publish route (POST /client/site-builder/<id>/publish)
- WP connection status display
- Navigation link in sidebar
"""
import os
import sys
import json
import io
import uuid
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)


class _FakeChatResponse:
    def __init__(self, content):
        self.choices = [type("Choice", (), {"message": type("Message", (), {"content": content})()})()]


def _make_test_app():
    db_file = _TEST_ROOT / f"sb-client-{uuid.uuid4().hex}.db"
    os.environ["DATABASE_PATH"] = str(db_file)
    os.environ.setdefault("SECRET_KEY", "test-secret")
    os.environ.setdefault("APP_URL", "http://localhost:5000")
    os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

    from webapp.app import create_app
    app = create_app()
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    return app, db_file


def _cleanup_db(db_path):
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        if p.exists():
            p.unlink()


def _login_client(client, app):
    """Create a brand + client user and log in as them."""
    db = app.db

    # Create a brand
    brand_id = db.create_brand({
        "display_name": "Ace Plumbing",
        "slug": f"ace-plumbing-{uuid.uuid4().hex[:6]}",
        "industry": "plumbing",
        "website": "https://aceplumbing.com",
        "primary_services": "Drain Cleaning, Water Heater Repair",
        "service_area": "Springfield, Shelbyville",
    })

    # Set WP credentials via update (create_brand doesn't insert these columns)
    db.update_brand_text_field(brand_id, "wp_site_url", "https://aceplumbing.com")
    db.update_brand_text_field(brand_id, "wp_username", "admin")
    db.update_brand_text_field(brand_id, "wp_app_password", "xxxx xxxx xxxx xxxx")
    db.update_brand_text_field(brand_id, "brand_colors", "#0f172a, #f97316, #e2e8f0")
    db.update_brand_text_field(brand_id, "primary_color", "#0f172a")
    db.update_brand_text_field(brand_id, "accent_color", "#f97316")
    db.update_brand_text_field(brand_id, "logo_path", "logos/test/logo.png")

    # Create a client user
    email = f"owner-{uuid.uuid4().hex[:6]}@aceplumbing.com"
    user_id = db.create_client_user(
        brand_id, email, "testpass123", "Test Owner", role="owner"
    )

    # Set session
    with client.session_transaction() as sess:
        sess["client_user_id"] = user_id
        sess["client_brand_id"] = brand_id
        sess["client_role"] = "owner"

    return brand_id, user_id


def _login_client_no_wp(client, app):
    """Create a brand WITHOUT WP credentials and log in."""
    db = app.db

    brand_id = db.create_brand({
        "display_name": "No WP Biz",
        "slug": f"no-wp-biz-{uuid.uuid4().hex[:6]}",
        "industry": "cleaning",
        "primary_services": "House Cleaning",
        "service_area": "Portland",
    })
    db.update_brand_text_field(brand_id, "brand_colors", "#14532d, #84cc16")
    db.update_brand_text_field(brand_id, "primary_color", "#14532d")
    db.update_brand_text_field(brand_id, "accent_color", "#84cc16")
    db.update_brand_text_field(brand_id, "logo_path", "logos/test/logo-nowp.png")

    email = f"owner-{uuid.uuid4().hex[:6]}@nowp.com"
    user_id = db.create_client_user(
        brand_id, email, "testpass123", "No WP Owner", role="owner"
    )

    with client.session_transaction() as sess:
        sess["client_user_id"] = user_id
        sess["client_brand_id"] = brand_id
        sess["client_role"] = "owner"

    return brand_id, user_id


# ---------------------------------------------------------------------------
# Landing Page Tests
# ---------------------------------------------------------------------------

class SiteBuilderLandingTests(unittest.TestCase):
    """Test the client site builder landing page."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    def test_landing_requires_login(self):
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 302)

    def test_landing_loads_with_wp_connected(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Site Builder", resp.data)
        self.assertIn(b"WordPress Connected", resp.data)

    def test_landing_shows_saved_brand_kit(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Brand Kit Ready", resp.data)
        self.assertIn(b"logos/test/logo.png", resp.data)
        self.assertIn(b"#0f172a", resp.data)

    def test_landing_loads_without_wp(self):
        _login_client_no_wp(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Connect Your WordPress Site", resp.data)

    def test_landing_shows_brand_services(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Drain Cleaning", resp.data)

    def test_landing_shows_build_history(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}], model="gpt-4o-mini")
        db.update_site_build_status(build_id, "completed", pages_completed=1)

        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Build History", resp.data)
        self.assertIn(f"Build #{build_id}".encode(), resp.data)

    def test_landing_empty_history(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"No builds yet", resp.data)


# ---------------------------------------------------------------------------
# Review Page Tests
# ---------------------------------------------------------------------------

class SiteBuilderReviewTests(unittest.TestCase):
    """Test the site build review page."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    def test_review_page_loads(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        db.update_site_build_status(build_id, "completed", pages_completed=1)
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "home",
            "label": "Home",
            "slug": "",
            "title": "Ace Plumbing - Springfield's Trusted Plumber",
            "content": "<h2>Welcome</h2><p>Test content</p>",
            "seo_title": "Ace Plumbing | Best Plumber in Springfield",
            "seo_description": "Licensed plumber in Springfield IL.",
            "primary_keyword": "plumber springfield",
        })

        resp = self.client.get(f"/client/site-builder/{build_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Review Site Build", resp.data)
        self.assertIn(b"Home", resp.data)
        self.assertIn(b"plumber springfield", resp.data)

    def test_review_page_shows_seo_meta(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "about"}])
        db.update_site_build_status(build_id, "completed", pages_completed=1)
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "about",
            "label": "About",
            "slug": "about",
            "title": "About Us",
            "content": "<p>About page</p>",
            "seo_title": "About Ace Plumbing",
            "seo_description": "Learn about our team.",
            "primary_keyword": "ace plumbing about",
        })

        resp = self.client.get(f"/client/site-builder/{build_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"About Ace Plumbing", resp.data)
        self.assertIn(b"/about", resp.data)

    def test_review_wrong_brand_redirects(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db

        # Create build for a different brand
        other_brand_id = db.create_brand({
            "display_name": "Other Biz", "slug": "other",
        })
        build_id = db.create_site_build(other_brand_id, [])

        resp = self.client.get(f"/client/site-builder/{build_id}")
        self.assertEqual(resp.status_code, 302)

    def test_review_nonexistent_build_redirects(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder/99999")
        self.assertEqual(resp.status_code, 302)

    def test_review_ajax_returns_json(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        db.update_site_build_status(build_id, "completed")

        resp = self.client.get(
            f"/client/site-builder/{build_id}",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertIn("build", data)

    def test_review_shows_publish_button_when_wp_connected(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        db.update_site_build_status(build_id, "completed", pages_completed=1)
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "home",
            "label": "Home",
            "title": "Home",
            "content": "<p>Test</p>",
        })

        resp = self.client.get(f"/client/site-builder/{build_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Publish", resp.data)

    def test_review_shows_connect_wp_when_not_connected(self):
        brand_id, _ = _login_client_no_wp(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        db.update_site_build_status(build_id, "completed", pages_completed=1)
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "home",
            "label": "Home",
            "title": "Home",
            "content": "<p>Test</p>",
        })

        resp = self.client.get(f"/client/site-builder/{build_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Connect WordPress First", resp.data)

    def test_review_page_exposes_editor_shell_controls(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        db.update_site_build_status(build_id, "completed", pages_completed=1)
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "home",
            "label": "Home",
            "slug": "",
            "title": "Home",
            "content": "<section><h2>Welcome</h2><p>Builder test content</p></section>",
        })

        resp = self.client.get(f"/client/site-builder/{build_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"gjsMoveBlockUp", resp.data)
        self.assertIn(b"gjsPresetHero", resp.data)
        self.assertIn(b"Trust Strip", resp.data)
        self.assertIn(b"Offer Stack", resp.data)
        self.assertIn(b"Before / After", resp.data)

    def test_review_page_exposes_header_footer_controls(self):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        db.update_site_build_status(build_id, "completed", pages_completed=1)
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "home",
            "label": "Home",
            "title": "Home",
            "content": "<section><h2>Welcome</h2><p>Builder test content</p></section>",
        })

        resp = self.client.get(f"/client/site-builder/{build_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"gjsSetHeader", resp.data)
        self.assertIn(b"gjsSetFooter", resp.data)
        self.assertIn(b"gjsToggleStickyHeader", resp.data)
        self.assertIn(b"Header Nav", resp.data)
        self.assertIn(b"Footer Columns", resp.data)


# ---------------------------------------------------------------------------
# Generate Route Tests
# ---------------------------------------------------------------------------

class SiteBuilderGenerateTests(unittest.TestCase):
    """Test the site builder generation endpoint."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    def test_generate_creates_build(self, mock_model, mock_key):
        brand_id, _ = _login_client(self.client, self.app)

        # Mock the site builder functions
        fake_content = {
            "title": "Test Page",
            "content": "<p>Generated content</p>",
            "excerpt": "Test excerpt",
            "seo_title": "Test SEO Title",
            "seo_description": "Test description",
            "primary_keyword": "test keyword",
            "secondary_keywords": "kw1, kw2",
            "faq_items": [],
        }
        fake_assembled = {
            "schemas": [],
            "schema_html": "",
            "full_html": "<p>Full HTML</p>",
        }

        with patch("webapp.site_builder.build_brand_context") as mock_ctx, \
             patch("webapp.site_builder.build_site_blueprint") as mock_bp, \
             patch("webapp.site_builder.generate_page_content", return_value=fake_content), \
             patch("webapp.site_builder.assemble_page", return_value=fake_assembled):

            mock_ctx.return_value = {"business_name": "Ace Plumbing"}
            mock_bp.return_value = [
                {"page_type": "home", "label": "Home", "slug": "", "schema_types": []},
            ]

            resp = self.client.post(
                "/client/site-builder/generate",
                data={"services": "Drain Cleaning", "areas": "Springfield"},
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["pages_generated"], 1)
        self.assertIn("build_id", data)

    def test_generate_requires_login(self):
        resp = self.client.post("/client/site-builder/generate")
        self.assertEqual(resp.status_code, 302)

    @patch("webapp.client_portal._get_openai_api_key", return_value=None)
    def test_generate_no_api_key_returns_error(self, mock_key):
        _login_client(self.client, self.app)
        resp = self.client.post(
            "/client/site-builder/generate",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertFalse(data["ok"])
        self.assertIn("API key", data["error"])


# ---------------------------------------------------------------------------
# Publish Route Tests
# ---------------------------------------------------------------------------

class SiteBuilderPublishTests(unittest.TestCase):
    """Test the publish-to-WordPress endpoint."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    def test_publish_requires_wp_connection(self):
        brand_id, _ = _login_client_no_wp(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [])

        resp = self.client.post(f"/client/site-builder/{build_id}/publish")
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertFalse(data["ok"])
        self.assertIn("WordPress", data["error"])

    def test_publish_wrong_brand_returns_404(self):
        _login_client(self.client, self.app)
        db = self.app.db
        other_brand = db.create_brand({"display_name": "Other", "slug": "other"})
        build_id = db.create_site_build(other_brand, [])

        resp = self.client.post(f"/client/site-builder/{build_id}/publish")
        self.assertEqual(resp.status_code, 404)

    @patch("webapp.client_portal._publish_wp_page")
    def test_publish_success(self, mock_publish):
        mock_publish.return_value = {
            "ok": True,
            "wp_page_id": 42,
            "wp_page_url": "https://aceplumbing.com/about/",
        }

        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "about"}])
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "about",
            "label": "About",
            "slug": "about",
            "title": "About Us",
            "content": "<p>About page</p>",
            "full_html": "<p>About page full</p>",
        })

        resp = self.client.post(f"/client/site-builder/{build_id}/publish")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["published"], 1)

        # Page should now have WP ID
        pages = db.get_site_pages(build_id)
        self.assertEqual(pages[0]["wp_page_id"], 42)

    @patch("webapp.client_portal._publish_wp_page")
    def test_publish_skips_already_published(self, mock_publish):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        page_id = db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "home",
            "label": "Home",
            "title": "Home",
            "content": "<p>Home</p>",
        })
        db.update_site_page_wp(page_id, 99, "https://example.com/")

        resp = self.client.post(f"/client/site-builder/{build_id}/publish")
        data = resp.get_json()
        self.assertEqual(data["published"], 1)
        mock_publish.assert_not_called()

    @patch("webapp.client_portal._publish_wp_page")
    def test_publish_handles_wp_error(self, mock_publish):
        mock_publish.return_value = {"ok": False, "error": "403 Forbidden"}

        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        build_id = db.create_site_build(brand_id, [{"page_type": "home"}])
        db.save_site_page({
            "build_id": build_id,
            "brand_id": brand_id,
            "page_type": "home",
            "label": "Home",
            "title": "Home",
            "content": "<p>Home</p>",
        })

        resp = self.client.post(f"/client/site-builder/{build_id}/publish")
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["published"], 0)
        self.assertEqual(len(data["errors"]), 1)
        self.assertIn("403 Forbidden", data["errors"][0])


# ---------------------------------------------------------------------------
# Page Editor Route Tests
# ---------------------------------------------------------------------------

class SiteBuilderEditorRouteTests(unittest.TestCase):
    """Test page load/save/rewrite/upload routes used by the visual editor."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()
        self.brand_id, _ = _login_client(self.client, self.app)
        self.db = self.app.db
        self.build_id = self.db.create_site_build(self.brand_id, [{"page_type": "home"}])
        self.page_id = self.db.save_site_page({
            "build_id": self.build_id,
            "brand_id": self.brand_id,
            "page_type": "home",
            "label": "Home",
            "title": "Home",
            "content": "<p>Original</p>",
            "seo_title": "Original SEO",
            "seo_description": "Original description",
        })

    def tearDown(self):
        _cleanup_db(self._db_file)

    def test_page_get_returns_editor_page_json(self):
        resp = self.client.get(f"/client/site-builder/page/{self.page_id}")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["page"]["id"], self.page_id)

    def test_page_get_denies_cross_brand_access(self):
        other_brand_id = self.db.create_brand({"display_name": "Other Biz", "slug": f"other-{uuid.uuid4().hex[:6]}"})
        other_build_id = self.db.create_site_build(other_brand_id, [{"page_type": "home"}])
        other_page_id = self.db.save_site_page({
            "build_id": other_build_id,
            "brand_id": other_brand_id,
            "page_type": "home",
            "label": "Other Home",
            "title": "Other",
            "content": "<p>Other</p>",
        })

        resp = self.client.get(f"/client/site-builder/page/{other_page_id}")
        self.assertEqual(resp.status_code, 403)

    def test_page_save_persists_content_editor_json_and_css(self):
        payload = {
            "content": "<section><h1>Updated</h1></section>",
            "page_css": ".hero{padding:24px;}",
            "editor_json": {"pages": [{"frames": [{"component": "<div>Updated</div>"}]}]},
            "seo_title": "Updated SEO",
            "seo_description": "Updated description",
            "title": "Updated Home",
        }
        resp = self.client.post(
            f"/client/site-builder/page/{self.page_id}/save",
            json=payload,
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()["ok"])

        page = self.db.get_site_page(self.page_id)
        self.assertEqual(page["content"], payload["content"])
        self.assertEqual(page["full_html"], payload["content"])
        self.assertEqual(page["page_css"], payload["page_css"])
        self.assertEqual(page["seo_title"], payload["seo_title"])
        self.assertEqual(page["seo_description"], payload["seo_description"])
        self.assertEqual(page["title"], payload["title"])
        self.assertEqual(json.loads(page["editor_json"]), payload["editor_json"])

    def test_page_save_rejects_invalid_json_payload(self):
        resp = self.client.post(
            f"/client/site-builder/page/{self.page_id}/save",
            data='{"content":',
            content_type="application/json",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Invalid JSON", resp.get_json()["error"])

    def test_page_save_rejects_invalid_editor_json_string(self):
        resp = self.client.post(
            f"/client/site-builder/page/{self.page_id}/save",
            json={"editor_json": "{not valid json}"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Editor state", resp.get_json()["error"])

    def test_page_save_rejects_oversized_content(self):
        resp = self.client.post(
            f"/client/site-builder/page/{self.page_id}/save",
            json={"content": "x" * (1024 * 1024 + 1)},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("too large", resp.get_json()["error"])

    def test_page_save_denies_cross_brand_access(self):
        other_brand_id = self.db.create_brand({"display_name": "Other Biz", "slug": f"other-save-{uuid.uuid4().hex[:6]}"})
        other_build_id = self.db.create_site_build(other_brand_id, [{"page_type": "home"}])
        other_page_id = self.db.save_site_page({
            "build_id": other_build_id,
            "brand_id": other_brand_id,
            "page_type": "home",
            "label": "Other Home",
            "title": "Other",
            "content": "<p>Other</p>",
        })

        resp = self.client.post(
            f"/client/site-builder/page/{other_page_id}/save",
            json={"content": "<p>Hack</p>"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 403)

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    @patch("openai.OpenAI")
    def test_page_rewrite_updates_page_content(self, mock_openai, mock_model, mock_key):
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse(json.dumps({
            "title": "Better Home",
            "content": "<section><h1>Better page</h1></section>",
            "seo_title": "Better SEO",
            "seo_description": "Better description",
            "primary_keyword": "better plumber",
            "secondary_keywords": "better, plumber",
        }))

        resp = self.client.post(
            f"/client/site-builder/page/{self.page_id}/rewrite",
            json={"instructions": "Make it more direct."},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["page"]["title"], "Better Home")

        page = self.db.get_site_page(self.page_id)
        self.assertEqual(page["title"], "Better Home")
        self.assertEqual(page["content"], "<section><h1>Better page</h1></section>")
        self.assertEqual(page["seo_title"], "Better SEO")

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    @patch("openai.OpenAI")
    def test_page_rewrite_invalid_ai_payload_preserves_existing_content(self, mock_openai, mock_model, mock_key):
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse("[]")

        resp = self.client.post(
            f"/client/site-builder/page/{self.page_id}/rewrite",
            json={"instructions": "Make it better."},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 500)
        self.assertIn("AI rewrite failed", resp.get_json()["error"])

        page = self.db.get_site_page(self.page_id)
        self.assertEqual(page["content"], "<p>Original</p>")
        self.assertEqual(page["title"], "Home")

    def test_upload_image_rejects_bad_extension(self):
        resp = self.client.post(
            "/client/site-builder/upload-image",
            data={"files[]": (io.BytesIO(b"not-an-image"), "bad.exe")},
            content_type="multipart/form-data",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertFalse(body["ok"])
        self.assertIn("allowed_types", body)

    def test_upload_image_rejects_oversized_file(self):
        resp = self.client.post(
            "/client/site-builder/upload-image",
            data={"files[]": (io.BytesIO(b"x" * (10 * 1024 * 1024 + 1)), "big.png")},
            content_type="multipart/form-data",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertFalse(body["ok"])
        self.assertEqual(body["max_size_bytes"], 10 * 1024 * 1024)


# ---------------------------------------------------------------------------
# Navigation Tests
# ---------------------------------------------------------------------------

class SiteBuilderNavTests(unittest.TestCase):
    """Test that site builder appears in client sidebar."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    def test_sidebar_has_site_builder_link(self):
        _login_client(self.client, self.app)
        # site_builder flag is auto-seeded by db.init()
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Site Builder", resp.data)
        self.assertIn(b"bi-globe2", resp.data)


# ---------------------------------------------------------------------------
# Intake Wizard Tests
# ---------------------------------------------------------------------------

class SiteBuilderIntakeWizardTests(unittest.TestCase):
    """Test that the expanded intake wizard renders correctly."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    def test_landing_shows_step_indicator(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"step-indicator", resp.data)
        self.assertIn(b"Business", resp.data)
        self.assertIn(b"SEO Intel", resp.data)

    def test_landing_shows_brand_fields_prefilled(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Ace Plumbing", resp.data)
        self.assertIn(b"plumbing", resp.data)

    def test_landing_shows_reference_site_controls(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"reference_url", resp.data)
        self.assertIn(b"reference_mode", resp.data)
        self.assertIn(b"Reference Website Style", resp.data)

    def test_landing_shows_content_goal_chips(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Generate Leads", resp.data)
        self.assertIn(b"Rank in Google", resp.data)

    def test_landing_shows_page_selection_chips(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        # Page type checkboxes use page_type_ prefix for each type
        self.assertIn(b"page_type_home", resp.data)

    def test_landing_shows_integrations_step(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"lead_form_type", resp.data)
        self.assertIn(b"WPForms", resp.data)
        self.assertIn(b"Quote Tool Configuration", resp.data)
        self.assertIn(b"quote_tool_source", resp.data)

    def test_landing_shows_wordpress_admin_shortcut(self):
        _login_client(self.client, self.app)
        resp = self.client.get("/client/site-builder")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"WordPress Admin", resp.data)
        self.assertIn(b"https://aceplumbing.com/wp-admin/", resp.data)


# ---------------------------------------------------------------------------
# SEO Intel Endpoint Tests
# ---------------------------------------------------------------------------

class SiteBuilderSeoIntelTests(unittest.TestCase):
    """Test the /site-builder/seo-intel AJAX endpoint."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    def test_seo_intel_requires_login(self):
        resp = self.client.post("/client/site-builder/seo-intel")
        self.assertEqual(resp.status_code, 302)

    def test_seo_intel_no_gsc_returns_400(self):
        _login_client(self.client, self.app)
        # Default brand has no gsc_site_url
        resp = self.client.post(
            "/client/site-builder/seo-intel",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertFalse(data["ok"])
        self.assertIn("Search Console", data["error"])

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    def test_seo_intel_with_gsc_returns_data(self, mock_model, mock_key):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db
        db.update_brand_api_field(brand_id, "gsc_site_url", "sc-domain:aceplumbing.com")

        mock_sc_data = {
            "totals": {"clicks": 150, "impressions": 3000, "ctr": 5.0, "avg_position": 8.2},
            "top_queries": [
                {"query": "plumber springfield", "clicks": 30, "impressions": 400, "position": 4.1, "ctr": 7.5}
            ],
            "opportunity_queries": [
                {"query": "drain cleaning near me", "impressions": 200, "position": 9.3}
            ],
            "top_pages": [
                {"page": "/", "clicks": 80, "position": 5.0}
            ],
        }

        with patch("src.api_search_console.pull_search_console_data", return_value=mock_sc_data), \
             patch("webapp.site_builder.generate_warren_seo_brief", return_value="Focus on drain cleaning."), \
             patch("webapp.site_builder.build_brand_context", return_value={"brand_name": "Ace Plumbing"}):
            resp = self.client.post(
                "/client/site-builder/seo-intel",
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["warren_brief"], "Focus on drain cleaning.")
        self.assertEqual(data["seo_data"]["totals"]["clicks"], 150)
        self.assertEqual(data["seo_data"]["top_queries"][0]["query"], "plumber springfield")


class SiteBuilderReferenceExtractionTests(unittest.TestCase):
    """Test reference-site section extraction for builder inspiration mode."""

    def test_reference_style_brief_extracts_section_patterns(self):
        from webapp.client_portal import _site_builder_reference_style_brief

        html_doc = """
        <html>
          <head>
            <title>Example Plumbing</title>
            <meta name="description" content="Fast plumbing service with same-day estimates.">
          </head>
          <body>
            <header>
              <nav>
                <a href="/">Home</a>
                <a href="/services">Services</a>
                <a href="/about">About</a>
                <a href="/contact">Contact</a>
                <a href="/quote">Get Quote</a>
              </nav>
            </header>
            <main>
              <section class="hero split-layout">
                <h1>Fast Plumbing Help</h1>
                <p>Emergency help and same-day service.</p>
                <a href="/quote">Get Quote</a>
              </section>
              <section class="services-grid">
                <h2>Our Services</h2>
                <div>Drain Cleaning</div>
                <div>Water Heater Repair</div>
                <div>Sewer Line Replacement</div>
              </section>
              <section class="trust testimonials">
                <h2>Why Customers Stay</h2>
                <p>Real reviews from homeowners.</p>
              </section>
              <section class="cta-band">
                <h2>Ready for a Same-Day Visit?</h2>
                <a href="tel:5551234567">Call Now</a>
              </section>
            </main>
          </body>
        </html>
        """

        class _Resp:
            status_code = 200
            url = "https://example.com"
            text = html_doc

            def raise_for_status(self):
                return None

        with patch("requests.get", return_value=_Resp()):
            brief = _site_builder_reference_style_brief("https://example.com", "sections")

        self.assertEqual(brief["resolved_url"], "https://example.com")
        self.assertEqual(brief["mode"], "sections")
        self.assertTrue(brief["section_patterns"])
        categories = [item["category"] for item in brief["section_patterns"]]
        self.assertIn("hero", categories)
        self.assertIn("services", categories)
        self.assertIn("testimonials", categories)
        self.assertIn("cta", categories)


# ---------------------------------------------------------------------------
# Generate with Intake Tests
# ---------------------------------------------------------------------------

class SiteBuilderGenerateWithIntakeTests(unittest.TestCase):
    """Test generate endpoint with expanded intake fields."""

    def setUp(self):
        self.app, self._db_file = _make_test_app()
        self.client = self.app.test_client()

    def tearDown(self):
        _cleanup_db(self._db_file)

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    def test_generate_with_intake_fields(self, mock_model, mock_key):
        brand_id, _ = _login_client(self.client, self.app)

        fake_content = {
            "title": "Test Page",
            "content": "<p>Generated content</p>",
            "excerpt": "Test excerpt",
            "seo_title": "Test SEO Title",
            "seo_description": "Test description",
            "primary_keyword": "test keyword",
            "secondary_keywords": "kw1, kw2",
            "faq_items": [],
        }
        fake_assembled = {
            "schemas": [],
            "schema_html": "",
            "full_html": "<p>Full HTML</p>",
        }

        with patch("webapp.site_builder.generate_page_content", return_value=fake_content), \
             patch("webapp.site_builder.assemble_page", return_value=fake_assembled):

            resp = self.client.post(
                "/client/site-builder/generate",
                data={
                    "services": "Drain Cleaning",
                    "areas": "Springfield",
                    "brand_voice": "Professional and authoritative",
                    "target_audience": "Commercial property managers",
                    "unique_selling_points": "24/7 emergency service",
                    "competitors": "Joe's Plumbing",
                    "content_goals": "Generate Leads, Rank in Google",
                    "lead_form_type": "wpforms",
                    "lead_form_shortcode": '[wpforms id="42"]',
                    "quote_tool_source": "wp_shortcode",
                    "quote_tool_embed": "[sng_quote_tool]",
                    "quote_tool_zip_mode": "verify",
                    "quote_tool_collect_dogs": "1",
                    "quote_tool_collect_frequency": "1",
                    "quote_tool_collect_last_cleaned": "1",
                    "quote_tool_phone_mode": "optional",
                    "quote_tool_notes": "Use the quote tool in the hero section.",
                    "page_selection": "home,about,services,contact",
                    "cta_text": "Get Your Free Quote",
                    "cta_phone": "(555) 999-1234",
                },
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertIn("build_id", data)

        # Verify intake was stored
        with self.app.app_context():
            build = self.app.db.get_site_build(data["build_id"])
        self.assertIsNotNone(build.get("intake"))
        self.assertEqual(build["intake"]["brand_voice"], "Professional and authoritative")
        self.assertEqual(build["intake"]["lead_form_type"], "wpforms")
        self.assertEqual(build["intake"]["content_goals"], "Generate Leads, Rank in Google")
        self.assertEqual(build["intake"]["quote_tool_source"], "wp_shortcode")
        self.assertEqual(build["intake"]["quote_tool_embed"], "[sng_quote_tool]")
        self.assertEqual(build["intake"]["quote_tool_zip_mode"], "verify")
        self.assertTrue(build["intake"]["quote_tool_collect_dogs"])
        self.assertTrue(build["intake"]["quote_tool_collect_frequency"])
        self.assertTrue(build["intake"]["quote_tool_collect_last_cleaned"])
        self.assertEqual(build["intake"]["quote_tool_phone_mode"], "optional")

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    @patch("webapp.client_portal._site_builder_reference_style_brief")
    def test_generate_stores_reference_site_brief(self, mock_reference_brief, mock_model, mock_key):
        _login_client(self.client, self.app)
        mock_reference_brief.return_value = {
            "resolved_url": "https://example.com",
            "mode": "layout",
            "layout_style_hint": "modern-sections",
            "style_preset_hint": "clean-minimal",
            "nav_items": ["Home", "Services", "Contact"],
        }

        fake_content = {
            "title": "Test Page",
            "content": "<p>Generated content</p>",
            "excerpt": "Test excerpt",
            "seo_title": "Test SEO Title",
            "seo_description": "Test description",
            "primary_keyword": "test keyword",
            "secondary_keywords": "kw1, kw2",
            "faq_items": [],
        }
        fake_assembled = {
            "schemas": [],
            "schema_html": "",
            "full_html": "<p>Full HTML</p>",
        }

        with patch("webapp.site_builder.generate_page_content", return_value=fake_content), \
             patch("webapp.site_builder.assemble_page", return_value=fake_assembled):

            resp = self.client.post(
                "/client/site-builder/generate",
                data={
                    "services": "Drain Cleaning",
                    "areas": "Springfield",
                    "reference_url": "https://example.com",
                    "reference_mode": "layout",
                },
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])

        build = self.app.db.get_site_build(data["build_id"])
        intake = build.get("intake") or {}
        self.assertEqual(intake["reference_url"], "https://example.com")
        self.assertEqual(intake["reference_mode"], "layout")
        self.assertEqual(intake["reference_site_brief"]["layout_style_hint"], "modern-sections")
        mock_reference_brief.assert_called_once_with("https://example.com", "layout")

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    def test_generate_with_landing_pages(self, mock_model, mock_key):
        brand_id, _ = _login_client(self.client, self.app)

        fake_content = {
            "title": "Test Page",
            "content": "<p>Generated content</p>",
            "excerpt": "Test excerpt",
            "seo_title": "Test SEO Title",
            "seo_description": "Test description",
            "primary_keyword": "test keyword",
            "secondary_keywords": "kw1, kw2",
            "faq_items": [],
        }
        fake_assembled = {
            "schemas": [],
            "schema_html": "",
            "full_html": "<p>Full HTML</p>",
        }

        with patch("webapp.site_builder.generate_page_content", return_value=fake_content), \
             patch("webapp.site_builder.assemble_page", return_value=fake_assembled):

            resp = self.client.post(
                "/client/site-builder/generate",
                data={
                    "services": "Drain Cleaning",
                    "areas": "Springfield",
                    "lp_name[]": ["Summer Special"],
                    "lp_keyword[]": ["ac repair deal"],
                    "lp_offer[]": ["20% off"],
                    "lp_audience[]": ["Homeowners"],
                },
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        # Should have core pages + service detail + area + 1 landing page
        self.assertGreater(data["pages_generated"], 7)

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    def test_generate_with_page_selection(self, mock_model, mock_key):
        brand_id, _ = _login_client(self.client, self.app)

        fake_content = {
            "title": "Test Page",
            "content": "<p>Generated content</p>",
            "excerpt": "Test excerpt",
            "seo_title": "Test SEO Title",
            "seo_description": "Test description",
            "primary_keyword": "test keyword",
            "secondary_keywords": "kw1, kw2",
            "faq_items": [],
        }
        fake_assembled = {
            "schemas": [],
            "schema_html": "",
            "full_html": "<p>Full HTML</p>",
        }

        with patch("webapp.site_builder.generate_page_content", return_value=fake_content), \
             patch("webapp.site_builder.assemble_page", return_value=fake_assembled):

            resp = self.client.post(
                "/client/site-builder/generate",
                data={
                    "services": "Drain Cleaning",
                    "areas": "Springfield",
                    "page_selection": "home,services,contact",
                },
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        # home + services + contact + 1 service detail + 0 areas (filtered) = 4
        self.assertEqual(data["pages_generated"], 4)

    @patch("webapp.client_portal._get_openai_api_key", return_value="test-key")
    @patch("webapp.client_portal._pick_ai_model", return_value="gpt-4o-mini")
    def test_generate_snapshots_builder_library_assets(self, mock_model, mock_key):
        brand_id, _ = _login_client(self.client, self.app)
        db = self.app.db

        db.create_sb_theme({
            "name": "Service Blue",
            "primary_color": "#123456",
            "secondary_color": "#345678",
            "accent_color": "#ff6600",
            "font_heading": "Oswald",
            "font_body": "Lato",
            "layout_style": "modern-sections",
            "is_default": 1,
            "is_active": 1,
        })
        db.create_sb_template({
            "name": "Main Header",
            "category": "header",
            "page_types": "all",
            "html_content": "<header>{{business_name}}</header>",
            "description": "Shared navigation shell",
            "sort_order": 1,
            "is_active": 1,
        })
        db.save_sb_prompt_override(
            "home",
            "user_prompt",
            "Lead with financing if the business offers it.",
            updated_by="tests",
        )

        fake_content = {
            "title": "Test Page",
            "content": "<p>Generated content</p>",
            "excerpt": "Test excerpt",
            "seo_title": "Test SEO Title",
            "seo_description": "Test description",
            "primary_keyword": "test keyword",
            "secondary_keywords": "kw1, kw2",
            "faq_items": [],
        }
        fake_assembled = {
            "schemas": [],
            "schema_html": "",
            "full_html": "<p>Full HTML</p>",
        }

        with patch("webapp.site_builder.generate_page_content", return_value=fake_content), \
             patch("webapp.site_builder.assemble_page", return_value=fake_assembled):

            resp = self.client.post(
                "/client/site-builder/generate",
                data={"services": "Drain Cleaning", "areas": "Springfield"},
                headers={"X-Requested-With": "XMLHttpRequest"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])

        build = self.app.db.get_site_build(data["build_id"])
        intake = build.get("intake") or {}
        self.assertEqual(intake["builder_theme"]["name"], "Service Blue")
        self.assertEqual(intake["builder_theme"]["primary_color"], "#123456")
        self.assertEqual(len(intake["builder_templates"]), 1)
        self.assertEqual(intake["builder_templates"][0]["name"], "Main Header")
        self.assertEqual(len(intake["builder_prompt_overrides"]), 1)
        self.assertEqual(intake["builder_prompt_overrides"][0]["page_type"], "home")


if __name__ == "__main__":
    unittest.main()
