import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-client-commercial-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class ClientCommercialProspectingTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"client-commercial-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"client-commercial-{uuid.uuid4().hex[:8]}",
                "display_name": "Scoopy Yard Care",
                "service_area": "Phoenix, AZ",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Scoopy Yard Care"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_client_commercial_page_loads(self):
        response = self.client.get("/client/commercial")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Commercial Lead Engine", response.data)
        self.assertIn(b"Search commercial targets", response.data)

    @patch("webapp.commercial_prospector.search_commercial_prospects")
    def test_client_commercial_search_renders_results(self, search_mock):
        search_mock.return_value = [
            {
                "business_name": "Mesa Property Group",
                "contact_name": "Mesa Property Group",
                "website": "https://mesaproperty.example.com",
                "address": "123 Main St, Mesa, AZ",
                "phone": "+14805551234",
                "rating": 4.6,
                "review_count": 31,
                "emails": ["leasing@mesaproperty.example.com"],
                "prospect_type": "property_manager",
                "prospect_type_label": "Property Managers",
                "service_area": "Mesa, AZ",
                "source_query": "property management companies in Mesa, AZ",
                "score": 85,
            }
        ]

        response = self.client.post(
            "/client/commercial/search",
            data={
                "location": "Mesa, AZ",
                "prospect_types": ["property_manager"],
                "max_results": "5",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Mesa Property Group", response.data)
        self.assertIn(b"Import selected targets into WARREN", response.data)

    def test_client_commercial_import_creates_brand_thread(self):
        payload = {
            "business_name": "Skyline HOA Services",
            "contact_name": "Skyline HOA Services",
            "website": "https://skyline-hoa.example.com",
            "address": "456 Camelback Rd, Phoenix, AZ",
            "phone": "+16025550123",
            "emails": ["board@skyline-hoa.example.com"],
            "prospect_type": "hoa",
            "prospect_type_label": "HOAs",
            "service_area": "Phoenix, AZ",
            "source_query": "HOA management companies in Phoenix, AZ",
            "score": 81,
            "audit_snapshot": {
                "title": "Skyline HOA Services",
                "description": "Community management and board support for Phoenix HOAs.",
                "h1": ["HOA management with faster response times"],
            },
        }

        response = self.client.post(
            "/client/commercial/import",
            data={"selected_results": json.dumps(payload)},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            threads = self.app.db.get_lead_threads(self.brand_id)
            self.assertEqual(len(threads), 1)
            thread = threads[0]
            self.assertEqual(thread["source"], "commercial_prospecting")
            self.assertEqual(thread["channel"], "commercial")
            self.assertIn("Skyline HOA Services", thread.get("commercial_data_json") or "")

            messages = self.app.db.get_lead_messages(thread["id"])
            self.assertEqual(len(messages), 1)
            self.assertIn("Commercial target imported", messages[0]["content"])

    def test_client_commercial_detail_and_qualification_save(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target imported - property manager.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "phone": "+14805551234",
                        "business_name": "Mesa Property Group",
                        "website": "https://mesaproperty.example.com",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "service_area": "Mesa, AZ",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({
                            "emails": ["leasing@mesaproperty.example.com"],
                            "service_area": "Mesa, AZ",
                        }),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.get(f"/client/commercial/thread/{thread_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Proposal Qualification", response.data)
        self.assertIn(b"Commercial Strategy Brief", response.data)

        save_response = self.client.post(
            f"/client/commercial/thread/{thread_id}/qualification",
            data={
                "property_count": "6 properties / 420 units",
                "decision_maker_role": "regional manager",
                "current_vendor_status": "replacing incumbent agency",
                "service_scope": "paid ads, landing page cleanup, reporting",
                "buying_timeline": "needs recommendation before next month",
                "decision_process": "regional manager recommends, ownership approves",
                "commercial_goal": "increase occupancy on two underperforming sites",
                "budget_range": "$4k-$6k monthly",
            },
            follow_redirects=False,
        )

        self.assertEqual(save_response.status_code, 302)
        self.assertTrue(save_response.headers["Location"].endswith(f"/client/commercial/thread/{thread_id}"))

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            self.assertIn("ownership approves", thread.get("commercial_data_json") or "")
            self.assertIn("Proposal-ready", thread.get("summary") or "")


if __name__ == "__main__":
    unittest.main()