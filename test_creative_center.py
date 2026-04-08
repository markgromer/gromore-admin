import os
import unittest
import uuid
from pathlib import Path

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-creative-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class CreativeCenterRouteTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"creative-center-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"creative-brand-{uuid.uuid4().hex[:8]}",
                "display_name": "Creative Test Brand",
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
            session["client_brand_name"] = "Creative Test Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)

        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_creative_center_renders_canvas_and_asset_loader(self):
        response = self.client.get("/client/creative")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="fabricCanvas"', html)
        self.assertIn("window.__creativeCenterAssetsPromise", html)


if __name__ == "__main__":
    unittest.main()