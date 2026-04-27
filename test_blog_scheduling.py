import os
import io
import json
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-blog-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app
from webapp.client_portal import _publish_to_wp, _publish_wp_page


class BlogSchedulingTests(unittest.TestCase):
    def setUp(self):
        self._db_file = _TEST_ROOT / f"blog-scheduling-{uuid.uuid4().hex}.db"
        self.db_path = str(self._db_file)
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            db = self.app.db
            self.brand_id = db.create_brand({
                "slug": f"blog-brand-{uuid.uuid4().hex[:8]}",
                "display_name": "Blog Test Brand",
            })
            self.other_brand_id = db.create_brand({
                "slug": f"blog-brand-{uuid.uuid4().hex[:8]}-other",
                "display_name": "Other Blog Brand",
            })
            self.user_id = db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Blog Test Brand"

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        os.environ.pop("APP_URL", None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(self.db_path + suffix)
            if path.exists():
                path.unlink()

    def test_schedule_save_keeps_post_unpublished(self):
        with patch("webapp.client_portal._publish_to_wp", side_effect=AssertionError("schedule should not publish immediately")):
            response = self.client.post(
                "/client/blog/save",
                data={
                    "title": "Scheduled Post",
                    "content": "<p>Scheduled content</p>",
                    "excerpt": "Short summary",
                    "action": "schedule",
                    "scheduled_at": "2099-05-01T10:30",
                },
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            posts = self.app.db.get_blog_posts(self.brand_id)
            self.assertEqual(len(posts), 1)
            post = posts[0]
            self.assertEqual(post["status"], "scheduled")
            self.assertEqual(post["scheduled_at"], "2099-05-01 10:30:00")
            self.assertFalse(post.get("published_at"))
            self.assertEqual(self.app.db.get_due_blog_posts(self.brand_id), [])

    def test_due_blog_posts_are_scoped_to_brand(self):
        with self.app.app_context():
            self.app.db.save_blog_post(
                self.brand_id,
                "Brand One Due",
                "<p>Content</p>",
                status="scheduled",
                scheduled_at="2026-01-01 09:00:00",
            )
            self.app.db.save_blog_post(
                self.other_brand_id,
                "Brand Two Due",
                "<p>Content</p>",
                status="scheduled",
                scheduled_at="2026-01-01 09:00:00",
            )

            due_posts = self.app.db.get_due_blog_posts(self.brand_id)

        self.assertEqual(len(due_posts), 1)
        self.assertEqual(due_posts[0]["brand_id"], self.brand_id)
        self.assertEqual(due_posts[0]["title"], "Brand One Due")

    @patch("webapp.client_portal.time.sleep")
    @patch("requests.get")
    @patch("requests.post")
    def test_publish_uploads_featured_image_to_wp_media_before_post(self, mock_post, mock_get, _mock_sleep):
        mock_get.return_value = Mock(
            status_code=200,
            headers={"Content-Type": "image/jpeg"},
            content=b"fake-image-bytes",
        )
        mock_post.side_effect = [
            Mock(status_code=201, json=lambda: {"id": 321, "source_url": "https://example.com/wp-content/uploads/feature.jpg"}, text=""),
            Mock(status_code=201, json=lambda: {"id": 654, "link": "https://example.com/?p=654"}, text=""),
            Mock(status_code=200, json=lambda: {"id": 654, "link": "https://example.com/?p=654"}, text=""),
            Mock(status_code=200, json=lambda: {"id": 654, "link": "https://example.com/blog/scheduled-post/"}, text=""),
        ]

        result = _publish_to_wp(
            {
                "wp_site_url": "https://example.com",
                "wp_username": "editor",
                "wp_app_password": "app-password",
            },
            "Scheduled Post",
            "<p>Scheduled content</p>",
            featured_image_url="https://cdn.example.com/feature.jpg",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["wp_post_id"], 654)
        self.assertEqual(result["wp_media_id"], 321)
        self.assertEqual(mock_post.call_count, 4)
        self.assertEqual(mock_post.call_args_list[0].args[0], "https://example.com/wp-json/wp/v2/media")
        self.assertEqual(mock_post.call_args_list[1].args[0], "https://example.com/wp-json/wp/v2/posts")
        self.assertEqual(mock_post.call_args_list[1].kwargs["json"], {
            "title": "Scheduled Post",
            "status": "draft",
        })
        self.assertEqual(mock_post.call_args_list[2].args[0], "https://example.com/wp-json/wp/v2/posts/654")
        self.assertEqual(mock_post.call_args_list[2].kwargs["json"]["featured_media"], 321)
        self.assertEqual(mock_post.call_args_list[2].kwargs["json"]["content"], "<p>Scheduled content</p>")
        self.assertEqual(mock_post.call_args_list[3].kwargs["json"], {"status": "publish"})

    @patch("webapp.client_portal.time.sleep")
    @patch("requests.post")
    def test_publish_splits_create_update_publish_and_sanitizes_content(self, mock_post, _mock_sleep):
        mock_post.side_effect = [
            Mock(status_code=201, json=lambda: {"id": 654, "link": "https://example.com/?p=654"}, text=""),
            Mock(status_code=200, json=lambda: {"id": 654, "link": "https://example.com/?p=654"}, text=""),
            Mock(status_code=200, json=lambda: {"id": 654, "link": "https://example.com/blog/scheduled-post/"}, text=""),
        ]

        result = _publish_to_wp(
            {
                "wp_site_url": "https://example.com",
                "wp_username": "editor",
                "wp_app_password": "app-password",
            },
            "Scheduled Post",
            '<p onclick="bad()">Safe</p><script>alert("x")</script><a href="javascript:bad()">Bad</a>',
        )

        self.assertTrue(result["ok"])
        self.assertEqual(mock_post.call_count, 3)

        create_payload = mock_post.call_args_list[0].kwargs["json"]
        update_payload = mock_post.call_args_list[1].kwargs["json"]
        publish_payload = mock_post.call_args_list[2].kwargs["json"]
        headers = mock_post.call_args_list[0].kwargs["headers"]

        self.assertEqual(create_payload, {"title": "Scheduled Post", "status": "draft"})
        self.assertNotIn("content", create_payload)
        self.assertIn("<p>Safe</p>", update_payload["content"])
        self.assertNotIn("<script", update_payload["content"].lower())
        self.assertNotIn("onclick", update_payload["content"].lower())
        self.assertNotIn("javascript:bad", update_payload["content"].lower())
        self.assertEqual(publish_payload, {"status": "publish"})
        self.assertIn("Mozilla/5.0", headers["User-Agent"])
        self.assertEqual(headers["Accept"], "application/json")
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_blog_save_accepts_uploaded_featured_image_for_draft(self):
        response = self.client.post(
            "/client/blog/save",
            data={
                "title": "Draft With Upload",
                "content": "<p>Draft content</p>",
                "action": "draft",
                "featured_image_file": (io.BytesIO(b"fake-image-bytes"), "feature.jpg"),
            },
            content_type="multipart/form-data",
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            posts = self.app.db.get_blog_posts(self.brand_id)

        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["status"], "draft")
        self.assertIn("/client/uploads/blog_featured/", posts[0]["featured_image_url"])

    @patch("requests.post")
    def test_publish_error_surfaces_security_challenge_clearly(self, mock_post):
        mock_post.return_value = Mock(
            status_code=202,
            text='<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=2"></head></html>',
        )

        result = _publish_to_wp(
            {
                "wp_site_url": "https://example.com",
                "wp_username": "editor",
                "wp_app_password": "app-password",
            },
            "Scheduled Post",
            "<p>Scheduled content</p>",
        )

        self.assertFalse(result["ok"])
        self.assertTrue(
            "captcha challenge" in result["error"].lower()
            or "security challenge" in result["error"].lower()
        )
        self.assertIn("siteground", result["error"].lower())

    @patch("requests.post")
    def test_publish_uses_warren_endpoint_fallback_when_waf_blocks_posts_route(self, mock_post):
        mock_post.side_effect = [
            Mock(
                status_code=202,
                text='<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=2"></head></html>',
            ),
            Mock(status_code=201, json=lambda: {"ok": True, "id": 777, "link": "https://example.com/blog/fallback/"}, text=""),
        ]

        result = _publish_to_wp(
            {
                "wp_site_url": "https://example.com",
                "wp_username": "editor",
                "wp_app_password": "app-password",
            },
            "Fallback Post",
            "<p>Scheduled content</p>",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["wp_post_id"], 777)
        self.assertEqual(result["wp_post_url"], "https://example.com/blog/fallback/")
        self.assertEqual(mock_post.call_args_list[1].args[0], "https://example.com/wp-admin/admin-ajax.php")
        fallback_form = mock_post.call_args_list[1].kwargs["data"]
        fallback_payload = json.loads(fallback_form["payload"])
        self.assertEqual(fallback_form["action"], "gromore_warren_publish")
        self.assertEqual(fallback_form["gm_user"], "editor")
        self.assertEqual(fallback_form["gm_app_password"], "app-password")
        self.assertEqual(fallback_payload["type"], "post")

    @patch("webapp.client_portal.time.sleep")
    @patch("requests.post")
    def test_site_builder_page_publish_splits_large_payload(self, mock_post, _mock_sleep):
        mock_post.side_effect = [
            Mock(status_code=201, json=lambda: {"id": 321, "link": "https://example.com/?page_id=321"}, text=""),
            Mock(status_code=200, json=lambda: {"id": 321, "link": "https://example.com/?page_id=321"}, text=""),
            Mock(status_code=200, json=lambda: {"id": 321, "link": "https://example.com/service/"}, text=""),
        ]

        result = _publish_wp_page(
            {
                "wp_site_url": "https://example.com",
                "wp_username": "editor",
                "wp_app_password": "app-password",
            },
            "Service Page",
            '<section onclick="bad()"><h1>Service</h1><script>alert("x")</script></section>',
            slug="service",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_post.call_args_list[0].args[0], "https://example.com/wp-json/wp/v2/pages")
        self.assertEqual(mock_post.call_args_list[0].kwargs["json"], {
            "title": "Service Page",
            "status": "draft",
            "slug": "service",
        })
        self.assertNotIn("content", mock_post.call_args_list[0].kwargs["json"])
        self.assertEqual(mock_post.call_args_list[1].args[0], "https://example.com/wp-json/wp/v2/pages/321")
        self.assertNotIn("<script", mock_post.call_args_list[1].kwargs["json"]["content"].lower())
        self.assertNotIn("onclick", mock_post.call_args_list[1].kwargs["json"]["content"].lower())
        self.assertEqual(mock_post.call_args_list[2].kwargs["json"], {"status": "publish"})


if __name__ == "__main__":
    unittest.main()
