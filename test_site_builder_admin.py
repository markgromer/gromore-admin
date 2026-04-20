"""
Tests for the Site Builder Admin panel:
- Database CRUD for sb_templates, sb_themes, sb_site_templates, sb_prompt_overrides,
  sb_image_categories, sb_images
- Admin routes (templates, themes, site templates, prompts, images, bulk, WP publish)
"""
import os
import sys
import json
import io
import uuid
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from webapp.database import WebDB

_TEST_ROOT = Path(__file__).parent / "data" / "database"
_TEST_ROOT.mkdir(parents=True, exist_ok=True)


def _make_db():
    """Create a temp file-based DB with tables initialized."""
    db_path = _TEST_ROOT / f"sb-test-{uuid.uuid4().hex}.db"
    db = WebDB(str(db_path))
    db.init()
    return db, db_path


def _cleanup_db(db_path):
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        if p.exists():
            p.unlink()


class SBTemplatesDBTests(unittest.TestCase):
    """Test sb_templates CRUD."""

    def setUp(self):
        self.db, self._db_path = _make_db()

    def tearDown(self):
        _cleanup_db(self._db_path)

    def test_create_and_get_template(self):
        tid = self.db.create_sb_template({
            "name": "Hero Section",
            "category": "hero",
            "page_types": "home, about",
            "html_content": "<section class='hero'>Test</section>",
            "description": "A hero section",
        })
        self.assertIsNotNone(tid)
        t = self.db.get_sb_template(tid)
        self.assertEqual(t["name"], "Hero Section")
        self.assertEqual(t["category"], "hero")
        self.assertEqual(t["page_types"], "home, about")
        self.assertIn("<section", t["html_content"])
        self.assertEqual(t["is_active"], 1)

    def test_list_templates_filters(self):
        self.db.create_sb_template({"name": "A", "category": "hero"})
        self.db.create_sb_template({"name": "B", "category": "section"})
        self.db.create_sb_template({"name": "C", "category": "hero", "is_active": 0})

        all_active = self.db.get_sb_templates()
        self.assertEqual(len(all_active), 2)  # C is inactive

        heroes_all = self.db.get_sb_templates(category="hero", active_only=False)
        self.assertEqual(len(heroes_all), 2)  # A and C

        heroes_active = self.db.get_sb_templates(category="hero", active_only=True)
        self.assertEqual(len(heroes_active), 1)  # just A

    def test_update_template(self):
        tid = self.db.create_sb_template({"name": "Old"})
        self.db.update_sb_template(tid, {"name": "New", "category": "widget"})
        t = self.db.get_sb_template(tid)
        self.assertEqual(t["name"], "New")
        self.assertEqual(t["category"], "widget")

    def test_delete_template(self):
        tid = self.db.create_sb_template({"name": "Gone"})
        self.db.delete_sb_template(tid)
        self.assertIsNone(self.db.get_sb_template(tid))


class SBThemesDBTests(unittest.TestCase):
    """Test sb_themes CRUD."""

    def setUp(self):
        self.db, self._db_path = _make_db()

    def tearDown(self):
        _cleanup_db(self._db_path)

    def test_create_and_get_theme(self):
        tid = self.db.create_sb_theme({
            "name": "Brand Blue",
            "primary_color": "#0000ff",
            "font_heading": "Poppins",
            "layout_style": "bold",
        })
        t = self.db.get_sb_theme(tid)
        self.assertEqual(t["name"], "Brand Blue")
        self.assertEqual(t["primary_color"], "#0000ff")
        self.assertEqual(t["font_heading"], "Poppins")
        self.assertEqual(t["layout_style"], "bold")

    def test_default_theme_mechanics(self):
        t1 = self.db.create_sb_theme({"name": "A", "is_default": 1})
        t2 = self.db.create_sb_theme({"name": "B"})

        default = self.db.get_sb_default_theme()
        self.assertEqual(default["name"], "A")

        # Set B as default, A should lose default
        self.db.update_sb_theme(t2, {"is_default": 1})
        default = self.db.get_sb_default_theme()
        self.assertEqual(default["name"], "B")
        a = self.db.get_sb_theme(t1)
        self.assertEqual(a["is_default"], 0)

    def test_list_themes(self):
        self.db.create_sb_theme({"name": "X"})
        self.db.create_sb_theme({"name": "Y", "is_active": 0})
        active = self.db.get_sb_themes(active_only=True)
        all_t = self.db.get_sb_themes(active_only=False)
        self.assertEqual(len(active), 1)
        self.assertEqual(len(all_t), 2)

    def test_delete_theme(self):
        tid = self.db.create_sb_theme({"name": "Gone"})
        self.db.delete_sb_theme(tid)
        self.assertIsNone(self.db.get_sb_theme(tid))


class SBSiteTemplatesDBTests(unittest.TestCase):
    """Test sb_site_templates CRUD."""

    def setUp(self):
        self.db, self._db_path = _make_db()

    def tearDown(self):
        _cleanup_db(self._db_path)

    def test_create_and_get_site_template(self):
        theme_id = self.db.create_sb_theme({"name": "Warm Pro"})
        header_id = self.db.create_sb_template({"name": "Main Header", "category": "navigation"})
        shell_id = self.db.create_sb_template({"name": "Home Shell", "category": "page_shell"})

        stid = self.db.create_sb_site_template({
            "name": "Service Blue",
            "slug": "service-blue",
            "description": "A high-trust local service kit.",
            "theme_id": theme_id,
            "template_ids": [header_id, shell_id],
            "prompt_notes": "Use the page shell exactly.",
            "is_default": 1,
        })

        site_template = self.db.get_sb_site_template(stid)
        self.assertEqual(site_template["name"], "Service Blue")
        self.assertEqual(site_template["theme_id"], theme_id)
        self.assertEqual(site_template["theme_name"], "Warm Pro")
        self.assertEqual(site_template["template_ids"], [header_id, shell_id])
        self.assertEqual(site_template["template_count"], 2)
        self.assertEqual(site_template["is_default"], 1)

    def test_default_site_template_mechanics(self):
        first_id = self.db.create_sb_site_template({
            "name": "First",
            "slug": "first",
            "is_default": 1,
        })
        second_id = self.db.create_sb_site_template({
            "name": "Second",
            "slug": "second",
            "is_default": 1,
        })

        default = self.db.get_sb_default_site_template()
        self.assertEqual(default["name"], "Second")
        self.assertEqual(self.db.get_sb_site_template(first_id)["is_default"], 0)

        self.db.update_sb_site_template(first_id, {"is_default": 1})
        default = self.db.get_sb_default_site_template()
        self.assertEqual(default["name"], "First")
        self.assertEqual(self.db.get_sb_site_template(second_id)["is_default"], 0)

    def test_delete_site_template(self):
        stid = self.db.create_sb_site_template({"name": "Gone", "slug": "gone"})
        self.db.delete_sb_site_template(stid)
        self.assertIsNone(self.db.get_sb_site_template(stid))

    def test_seed_default_site_builder_kits_is_idempotent(self):
        first = self.db.seed_default_site_builder_kits()
        second = self.db.seed_default_site_builder_kits()

        self.assertEqual(first["kits"], 5)
        self.assertEqual(first["themes_created"], 5)
        self.assertEqual(first["site_templates_created"], 5)
        self.assertGreaterEqual(first["templates_created"], 20)
        self.assertEqual(second["themes_created"], 0)
        self.assertEqual(second["site_templates_created"], 0)
        self.assertEqual(second["kits"], 5)

        site_templates = self.db.get_sb_site_templates(active_only=False)
        self.assertEqual(len(site_templates), 5)
        self.assertTrue(any(item["is_default"] for item in site_templates))
        self.assertTrue(any(item["slug"] == "authority-local-operator" for item in site_templates))
        page_shells = self.db.get_sb_templates(category="page_shell", active_only=False)
        self.assertGreaterEqual(len(page_shells), 32)


class SBPromptOverridesDBTests(unittest.TestCase):
    """Test sb_prompt_overrides CRUD."""

    def setUp(self):
        self.db, self._db_path = _make_db()

    def tearDown(self):
        _cleanup_db(self._db_path)

    def test_save_and_get_override(self):
        self.db.save_sb_prompt_override("home", "user_prompt", "Extra home rules", notes="test")
        o = self.db.get_sb_prompt_override("home", "user_prompt")
        self.assertIsNotNone(o)
        self.assertEqual(o["content"], "Extra home rules")
        self.assertEqual(o["notes"], "test")
        self.assertEqual(o["is_active"], 1)

    def test_upsert_override(self):
        self.db.save_sb_prompt_override("faq", "extra_rules", "V1")
        self.db.save_sb_prompt_override("faq", "extra_rules", "V2")
        o = self.db.get_sb_prompt_override("faq", "extra_rules")
        self.assertEqual(o["content"], "V2")

    def test_list_overrides(self):
        self.db.save_sb_prompt_override("home", "user_prompt", "a")
        self.db.save_sb_prompt_override("about", "user_prompt", "b")
        self.db.save_sb_prompt_override("home", "extra_rules", "c")
        overrides = self.db.get_sb_prompt_overrides()
        self.assertEqual(len(overrides), 3)

    def test_toggle_override(self):
        self.db.save_sb_prompt_override("contact", "user_prompt", "x")
        o = self.db.get_sb_prompt_override("contact", "user_prompt")
        self.assertEqual(o["is_active"], 1)
        self.db.toggle_sb_prompt_override(o["id"], 0)
        o = self.db.get_sb_prompt_override("contact", "user_prompt")
        self.assertEqual(o["is_active"], 0)

    def test_delete_override(self):
        self.db.save_sb_prompt_override("faq", "user_prompt", "gone")
        o = self.db.get_sb_prompt_override("faq", "user_prompt")
        self.db.delete_sb_prompt_override(o["id"])
        self.assertIsNone(self.db.get_sb_prompt_override("faq", "user_prompt"))


class SBImageCategoriesDBTests(unittest.TestCase):
    """Test sb_image_categories CRUD."""

    def setUp(self):
        self.db, self._db_path = _make_db()

    def tearDown(self):
        _cleanup_db(self._db_path)

    def test_create_and_list_categories(self):
        c1 = self.db.create_sb_image_category("Heroes", "heroes", "Hero images")
        c2 = self.db.create_sb_image_category("Staff", "staff")
        cats = self.db.get_sb_image_categories()
        self.assertEqual(len(cats), 2)
        self.assertEqual(cats[0]["name"], "Heroes")
        self.assertEqual(cats[0]["image_count"], 0)

    def test_delete_category_unlinks_images(self):
        cat_id = self.db.create_sb_image_category("Test", "test")
        img_id = self.db.create_sb_image({
            "filename": "a.jpg",
            "file_path": "uploads/a.jpg",
            "category_id": cat_id,
        })
        self.db.delete_sb_image_category(cat_id)
        img = self.db.get_sb_image(img_id)
        self.assertIsNone(img["category_id"])


class SBImagesDBTests(unittest.TestCase):
    """Test sb_images CRUD."""

    def setUp(self):
        self.db, self._db_path = _make_db()

    def tearDown(self):
        _cleanup_db(self._db_path)

    def test_create_and_get_image(self):
        iid = self.db.create_sb_image({
            "filename": "hero.jpg",
            "original_name": "hero-banner.jpg",
            "file_path": "uploads/sb_images/hero.jpg",
            "file_size": 50000,
            "mime_type": "image/jpeg",
            "width": 1920,
            "height": 1080,
            "alt_text": "Hero banner",
            "tags": "hero, banner, header",
            "industry": "plumbing",
        })
        img = self.db.get_sb_image(iid)
        self.assertEqual(img["filename"], "hero.jpg")
        self.assertEqual(img["width"], 1920)
        self.assertEqual(img["tags"], "hero, banner, header")

    def test_filter_by_category(self):
        cat_id = self.db.create_sb_image_category("Staff", "staff")
        self.db.create_sb_image({"filename": "a.jpg", "file_path": "a.jpg", "category_id": cat_id})
        self.db.create_sb_image({"filename": "b.jpg", "file_path": "b.jpg"})
        cat_imgs = self.db.get_sb_images(category_id=cat_id)
        self.assertEqual(len(cat_imgs), 1)
        self.assertEqual(cat_imgs[0]["filename"], "a.jpg")

    def test_filter_by_industry(self):
        self.db.create_sb_image({"filename": "a.jpg", "file_path": "a.jpg", "industry": "plumbing"})
        self.db.create_sb_image({"filename": "b.jpg", "file_path": "b.jpg", "industry": "hvac"})
        results = self.db.get_sb_images(industry="plumbing")
        self.assertEqual(len(results), 1)

    def test_filter_by_tags(self):
        self.db.create_sb_image({"filename": "a.jpg", "file_path": "a.jpg", "tags": "hero, banner"})
        self.db.create_sb_image({"filename": "b.jpg", "file_path": "b.jpg", "tags": "team, staff"})
        results = self.db.get_sb_images(tags="hero")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["filename"], "a.jpg")

    def test_count_images(self):
        self.db.create_sb_image({"filename": "a.jpg", "file_path": "a.jpg"})
        self.db.create_sb_image({"filename": "b.jpg", "file_path": "b.jpg"})
        self.db.create_sb_image({"filename": "c.jpg", "file_path": "c.jpg"})
        self.assertEqual(self.db.count_sb_images(), 3)

    def test_soft_delete(self):
        iid = self.db.create_sb_image({"filename": "gone.jpg", "file_path": "g.jpg"})
        self.db.delete_sb_image(iid)
        # Soft delete - still in DB but inactive
        img = self.db.get_sb_image(iid)
        self.assertEqual(img["is_active"], 0)
        # Not returned in active queries
        self.assertEqual(len(self.db.get_sb_images()), 0)

    def test_update_image(self):
        iid = self.db.create_sb_image({"filename": "x.jpg", "file_path": "x.jpg"})
        self.db.update_sb_image(iid, {"alt_text": "Updated alt", "tags": "new, tags"})
        img = self.db.get_sb_image(iid)
        self.assertEqual(img["alt_text"], "Updated alt")
        self.assertEqual(img["tags"], "new, tags")

    def test_bulk_update(self):
        id1 = self.db.create_sb_image({"filename": "a.jpg", "file_path": "a.jpg"})
        id2 = self.db.create_sb_image({"filename": "b.jpg", "file_path": "b.jpg"})
        self.db.bulk_update_sb_images([id1, id2], {"tags": "bulk-tag"})
        self.assertEqual(self.db.get_sb_image(id1)["tags"], "bulk-tag")
        self.assertEqual(self.db.get_sb_image(id2)["tags"], "bulk-tag")

    def test_pagination(self):
        for i in range(10):
            self.db.create_sb_image({"filename": f"{i}.jpg", "file_path": f"{i}.jpg"})
        page1 = self.db.get_sb_images(limit=3, offset=0)
        page2 = self.db.get_sb_images(limit=3, offset=3)
        self.assertEqual(len(page1), 3)
        self.assertEqual(len(page2), 3)
        # Different images
        ids1 = {img["id"] for img in page1}
        ids2 = {img["id"] for img in page2}
        self.assertTrue(ids1.isdisjoint(ids2))


class SBAdminRouteTests(unittest.TestCase):
    """Test the admin routes return proper responses."""

    def setUp(self):
        self.db_file = _TEST_ROOT / f"sb-admin-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ.setdefault("FLASK_SECRET", "test-secret")
        from webapp.app import create_app
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.app.config["WTF_CSRF_ENABLED"] = False
        self.db = self.app.db
        self.client = self.app.test_client()
        # Login with the auto-created default admin
        self.client.post("/login", data={"username": "admin", "password": "changeme123"})

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

        upload_dir = Path(self.app.static_folder) / "uploads" / "sb_images"
        if upload_dir.exists():
            for path in upload_dir.iterdir():
                if path.is_file() and path.name.endswith(".svg") and path.name != ".gitkeep":
                    path.unlink()

    def test_admin_dashboard_loads(self):
        resp = self.client.get("/site-builder-admin")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Site Builder Admin", resp.data)

    def test_admin_dashboard_auto_seeds_production_kits_when_missing(self):
        self.assertEqual(len(self.db.get_sb_site_templates(active_only=False)), 0)

        resp = self.client.get("/site-builder-admin")

        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Lead Engine", resp.data)
        self.assertIn(b"Authority Local Operator", resp.data)
        self.assertEqual(len(self.db.get_sb_site_templates(active_only=False)), 5)

    def test_admin_tabs(self):
        for tab in ("templates", "site-templates", "themes", "prompts", "images"):
            resp = self.client.get(f"/site-builder-admin?tab={tab}")
            self.assertEqual(resp.status_code, 200)

    def test_create_template_via_route(self):
        self.client.get("/site-builder-admin")
        before_count = len(self.db.get_sb_templates(active_only=False))

        resp = self.client.post("/site-builder-admin/templates", data={
            "name": "Test Hero",
            "category": "hero",
            "html_content": "<div>hero</div>",
            "is_active": "1",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        templates = self.db.get_sb_templates(active_only=False)
        self.assertEqual(len(templates), before_count + 1)
        self.assertTrue(any(template["name"] == "Test Hero" for template in templates))

    def test_delete_template_via_route(self):
        tid = self.db.create_sb_template({"name": "Del"})
        resp = self.client.post(f"/site-builder-admin/templates/{tid}/delete", follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertIsNone(self.db.get_sb_template(tid))

    def test_template_api_get(self):
        tid = self.db.create_sb_template({"name": "API Test", "category": "section"})
        resp = self.client.get(f"/api/site-builder-admin/templates/{tid}")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["name"], "API Test")

    def test_template_api_404(self):
        resp = self.client.get("/api/site-builder-admin/templates/9999")
        self.assertEqual(resp.status_code, 404)

    def test_create_theme_via_route(self):
        self.client.get("/site-builder-admin")
        before_count = len(self.db.get_sb_themes(active_only=False))

        resp = self.client.post("/site-builder-admin/themes", data={
            "name": "Dark Mode",
            "primary_color": "#000000",
            "is_active": "1",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        themes = self.db.get_sb_themes(active_only=False)
        self.assertEqual(len(themes), before_count + 1)
        self.assertTrue(any(theme["name"] == "Dark Mode" for theme in themes))

    def test_create_theme_via_route_normalizes_font_names(self):
        self.client.get("/site-builder-admin")

        resp = self.client.post("/site-builder-admin/themes", data={
            "name": "Font Test",
            "font_heading": "Space   Grotesk!!!",
            "font_body": "DM Sans<script>",
            "is_active": "1",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        theme = next(theme for theme in self.db.get_sb_themes(active_only=False) if theme["name"] == "Font Test")
        self.assertEqual(theme["font_heading"], "Space Grotesk")
        self.assertEqual(theme["font_body"], "DM Sansscript")

    def test_create_site_template_via_route(self):
        theme_id = self.db.create_sb_theme({"name": "Warm Pro"})
        header_id = self.db.create_sb_template({"name": "Header", "category": "navigation"})
        shell_id = self.db.create_sb_template({"name": "Shell", "category": "page_shell"})

        resp = self.client.post("/site-builder-admin/site-templates", data={
            "name": "Premium Service",
            "slug": "premium-service",
            "description": "Premium local-service template kit",
            "theme_id": str(theme_id),
            "template_ids": [str(header_id), str(shell_id)],
            "prompt_notes": "Use the shell exactly.",
            "sort_order": "3",
            "is_default": "1",
            "is_active": "1",
        }, follow_redirects=True)

        self.assertEqual(resp.status_code, 200)
        site_templates = self.db.get_sb_site_templates(active_only=False)
        self.assertEqual(len(site_templates), 1)
        self.assertEqual(site_templates[0]["name"], "Premium Service")
        self.assertEqual(site_templates[0]["template_ids"], [header_id, shell_id])
        self.assertEqual(site_templates[0]["theme_name"], "Warm Pro")

    def test_site_template_api_get(self):
        stid = self.db.create_sb_site_template({"name": "API Kit", "slug": "api-kit"})
        resp = self.client.get(f"/api/site-builder-admin/site-templates/{stid}")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["name"], "API Kit")

    def test_delete_site_template_via_route(self):
        stid = self.db.create_sb_site_template({"name": "Delete Me", "slug": "delete-me"})
        resp = self.client.post(f"/site-builder-admin/site-templates/{stid}/delete", follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertIsNone(self.db.get_sb_site_template(stid))

    def test_install_default_site_kits_via_route(self):
        resp = self.client.post("/site-builder-admin/site-templates/install-defaults", follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        site_templates = self.db.get_sb_site_templates(active_only=False)
        self.assertEqual(len(site_templates), 5)
        self.assertTrue(any(item["slug"] == "authority-local-operator" for item in site_templates))
        self.assertIn(b"Installed production site kits", resp.data)
        page_shells = self.db.get_sb_templates(category="page_shell", active_only=False)
        self.assertGreaterEqual(len(page_shells), 32)

    def test_save_prompt_override_via_route(self):
        resp = self.client.post("/site-builder-admin/prompts", data={
            "page_type": "home",
            "section": "user_prompt",
            "content": "Always mention emergency service",
            "notes": "Industry requirement",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        o = self.db.get_sb_prompt_override("home", "user_prompt")
        self.assertIsNotNone(o)
        self.assertIn("emergency", o["content"])

    def test_toggle_prompt_override(self):
        self.db.save_sb_prompt_override("faq", "extra_rules", "test")
        o = self.db.get_sb_prompt_override("faq", "extra_rules")
        resp = self.client.post(f"/site-builder-admin/prompts/{o['id']}/toggle", data={
            "is_active": "0",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        o = self.db.get_sb_prompt_override("faq", "extra_rules")
        self.assertEqual(o["is_active"], 0)

    def test_create_image_category_via_route(self):
        resp = self.client.post("/site-builder-admin/image-categories", data={
            "name": "Heroes",
            "slug": "heroes",
            "description": "Hero images",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        cats = self.db.get_sb_image_categories()
        self.assertEqual(len(cats), 1)

    def test_admin_image_upload_route_saves_file_and_record(self):
        payload = {
            "images": (io.BytesIO(b"<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10'></svg>"), "hero-test.svg"),
            "tags": "hero, homepage",
            "industry": "plumbing",
        }

        resp = self.client.post(
            "/site-builder-admin/images/upload",
            data=payload,
            content_type="multipart/form-data",
            follow_redirects=True,
        )

        self.assertEqual(resp.status_code, 200)
        images = self.db.get_sb_images()
        self.assertEqual(len(images), 1)
        self.assertEqual(images[0]["original_name"], "hero-test.svg")
        self.assertEqual(images[0]["industry"], "plumbing")
        saved_file = Path(self.app.static_folder) / images[0]["file_path"]
        self.assertTrue(saved_file.exists())

    def test_images_api_endpoint(self):
        self.db.create_sb_image({"filename": "a.jpg", "file_path": "uploads/a.jpg"})
        self.db.create_sb_image({"filename": "b.jpg", "file_path": "uploads/b.jpg"})
        resp = self.client.get("/api/site-builder-admin/images")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["total"], 2)
        self.assertEqual(len(data["images"]), 2)

    def test_image_edit_via_route(self):
        iid = self.db.create_sb_image({"filename": "edit.jpg", "file_path": "uploads/edit.jpg"})
        resp = self.client.post(f"/site-builder-admin/images/{iid}/edit", data={
            "alt_text": "Updated alt",
            "tags": "new, tag",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        img = self.db.get_sb_image(iid)
        self.assertEqual(img["alt_text"], "Updated alt")

    def test_bulk_delete_images(self):
        id1 = self.db.create_sb_image({"filename": "a.jpg", "file_path": "a.jpg"})
        id2 = self.db.create_sb_image({"filename": "b.jpg", "file_path": "b.jpg"})
        resp = self.client.post("/site-builder-admin/images/bulk", data={
            "action": "delete",
            "image_ids": [str(id1), str(id2)],
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.db.count_sb_images(), 0)

    def test_bulk_categorize_images(self):
        cat_id = self.db.create_sb_image_category("Staff", "staff")
        id1 = self.db.create_sb_image({"filename": "a.jpg", "file_path": "a.jpg"})
        id2 = self.db.create_sb_image({"filename": "b.jpg", "file_path": "b.jpg"})
        resp = self.client.post("/site-builder-admin/images/bulk", data={
            "action": "categorize",
            "image_ids": [str(id1), str(id2)],
            "bulk_category_id": str(cat_id),
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        imgs = self.db.get_sb_images(category_id=cat_id)
        self.assertEqual(len(imgs), 2)

    def test_empty_template_name_rejected(self):
        self.client.get("/site-builder-admin")
        before_count = len(self.db.get_sb_templates(active_only=False))

        resp = self.client.post("/site-builder-admin/templates", data={
            "name": "",
            "category": "hero",
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"required", resp.data)
        self.assertEqual(len(self.db.get_sb_templates(active_only=False)), before_count)

    def test_requires_login(self):
        # Logout
        self.client.get("/logout")
        resp = self.client.get("/site-builder-admin")
        # Should redirect to login
        self.assertIn(resp.status_code, (302, 401))


if __name__ == "__main__":
    unittest.main()
