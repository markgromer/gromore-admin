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
import uuid
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)


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


if __name__ == "__main__":
    unittest.main()
