import json
import os
import unittest
import uuid
from pathlib import Path

from webapp.vertical_intelligence import build_vertical_profile

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-vertical-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class VerticalEngagementTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"vertical-engagement-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"vertical-{uuid.uuid4().hex[:8]}",
                "display_name": "Clean Route Co",
                "industry": "commercial cleaning",
                "primary_services": "office cleaning, janitorial, recurring cleaning",
                "service_area": "Phoenix",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            self.app.db.create_report(self.brand_id, "2026-05", "internal.html", "client.html")
            self.app.db.upsert_dashboard_snapshot(
                self.brand_id,
                "2026-05",
                json.dumps({
                    "_snapshot_version": 3,
                    "health_summary": {"summary": "Lead flow is stable."},
                    "actions": [{
                        "key": "fix_offer",
                        "mission_name": "Tighten the quote offer",
                        "why": "The top service page has traffic but no quote requests.",
                        "reward": "More visitors see the right commercial cleaning offer.",
                        "xp": 150,
                    }],
                    "channels": {},
                    "kpi_status": {"targets": {}, "actual": {}, "evaluation": {}},
                }),
                source="test",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_name"] = "Owner User"
            session["client_brand_name"] = "Clean Route Co"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_vertical_profile_adapts_service_lanes_without_feature_locking(self):
        profile = build_vertical_profile({
            "industry": "mobile detailing",
            "primary_services": "fleet washing, car detailing",
            "service_area": "Tucson",
        })

        self.assertEqual(profile["key"], "auto_services")
        self.assertIn("fleet", " ".join(profile["commercial_targets"]).lower())
        self.assertEqual([item["key"] for item in profile["mission_lenses"]], ["lead_assistant", "ads", "commercial", "content"])

    def test_dashboard_data_includes_today_win_feed_and_open_lanes(self):
        response = self.client.get("/client/dashboard/data?month=2026-05")
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        engagement = data["dashboard"]["engagement"]
        self.assertEqual(engagement["today_win"]["title"], "Tighten the quote offer")
        self.assertIn("open_lanes", engagement)
        lane_labels = {lane["label"] for lane in engagement["open_lanes"]}
        self.assertTrue({"Chatbot", "Ads", "Commercial", "Organic", "Creative", "Website"}.issubset(lane_labels))
        self.assertEqual(data["dashboard"]["vertical_profile"]["key"], "home_services")

    def test_completed_action_becomes_win_feed_activity(self):
        complete = self.client.post(
            "/client/actions/dismiss",
            json={"action_key": "fix_offer", "month": "2026-05"},
        )
        self.assertEqual(complete.status_code, 200)

        response = self.client.get("/client/dashboard/data?month=2026-05")
        engagement = response.get_json()["dashboard"]["engagement"]

        self.assertEqual(engagement["momentum"]["completed_this_month"], 1)
        self.assertTrue(any("Tighten the quote offer" in item["title"] for item in engagement["win_feed"]))


if __name__ == "__main__":
    unittest.main()
