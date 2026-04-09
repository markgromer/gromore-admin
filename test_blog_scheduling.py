import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-blog-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


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


if __name__ == "__main__":
    unittest.main()