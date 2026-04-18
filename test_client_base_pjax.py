import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-client-base-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientBasePjaxTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"client-base-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"client-base-{uuid.uuid4().hex[:8]}",
                "display_name": "Client Base Test Brand",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            conn = self.app.db._conn()
            conn.execute(
                "INSERT INTO beta_testers (name, email, status, brand_id, client_user_id) VALUES (?, ?, 'approved', ?, ?)",
                ("Owner User", f"beta-{uuid.uuid4().hex[:8]}@example.com", self.brand_id, self.user_id),
            )
            conn.commit()
            conn.close()

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Client Base Test Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)

        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_pjax_uses_exported_loader_message_helper(self):
        response = self.client.get("/client/actions")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Talk to W.A.R.R.E.N.", html)
        self.assertIn("typeof window._pageLoaderMessagesForHref === 'function'", html)
        self.assertIn("loaderMessages = window._pageLoaderMessagesForHref(url);", html)
        self.assertNotIn("window.showPageLoader(messagesForHref(url));", html)
        self.assertIn("Loading a new mission track", html)
        self.assertIn("window._pjaxNavigate(btn.href, true)", html)
        self.assertIn("class=\"track-btn", html)


if __name__ == "__main__":
    unittest.main()