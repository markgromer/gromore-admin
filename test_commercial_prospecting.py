import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-commercial-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class CommercialProspectingTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"commercial-prospecting-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.admin_id = self.app.db.create_user("admin", "changeme123", "Admin")
            self.sequence_id = self.app.db.create_drip_sequence("Commercial nurture", "Commercial property outreach", "commercial")

        with self.client.session_transaction() as session:
            session["user_id"] = self.admin_id
            session["user_name"] = "Admin"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_commercial_prospecting_page_loads(self):
        response = self.client.get("/crm/commercial")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Commercial Lead Engine", response.data)
        self.assertIn(b"Search commercial prospects", response.data)

    @patch("webapp.commercial_prospector.search_commercial_prospects")
    def test_commercial_search_renders_results(self, search_mock):
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
            "/crm/commercial/search",
            data={
                "location": "Mesa, AZ",
                "prospect_types": ["property_manager"],
                "max_results": "5",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Mesa Property Group", response.data)
        self.assertIn(b"leasing@mesaproperty.example.com", response.data)
        self.assertIn(b"Import selected prospects", response.data)

    def test_commercial_import_creates_prospect_and_enrolls_in_drip(self):
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
            "/crm/commercial/import",
            data={
                "selected_results": json.dumps(payload),
                "sequence_id": str(self.sequence_id),
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/crm"))

        with self.app.app_context():
            prospects = self.app.db.get_agency_prospects()
            self.assertEqual(len(prospects), 1)
            prospect = prospects[0]
            self.assertEqual(prospect["business_name"], "Skyline HOA Services")
            self.assertEqual(prospect["email"], "board@skyline-hoa.example.com")
            self.assertEqual(prospect["source"], "commercial_scrape")
            self.assertIn("Skyline HOA Services", prospect["audit_snapshot_json"])

            enrollments = self.app.db.get_drip_enrollments(sequence_id=self.sequence_id)
            self.assertEqual(len(enrollments), 1)
            self.assertEqual(enrollments[0]["email"], "board@skyline-hoa.example.com")
            self.assertEqual(enrollments[0]["lead_source"], "commercial_scrape")

    def test_commercial_prospect_detail_renders_strategy_brief(self):
        with self.app.app_context():
            prospect_id = self.app.db.create_agency_prospect(
                name="Mesa Property Group",
                email="leasing@mesaproperty.example.com",
                business_name="Mesa Property Group",
                website="https://mesaproperty.example.com",
                industry="Property Managers",
                service_area="Mesa, AZ",
                source="commercial_scrape",
                account_type="property_manager",
                source_details_json=json.dumps({
                    "emails": ["leasing@mesaproperty.example.com"],
                    "address": "123 Main St, Mesa, AZ",
                    "service_area": "Mesa, AZ",
                    "rating": 4.6,
                    "review_count": 31,
                }),
            )

        response = self.client.get(f"/crm/prospect/{prospect_id}")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Commercial Strategy Brief", response.data)
        self.assertIn(b"Outreach Assets", response.data)
        self.assertIn(b"Qualification Questions", response.data)

    @patch("webapp.competitor_intel._scrape_website")
    @patch("webapp.commercial_prospector._extract_public_emails")
    def test_commercial_refresh_updates_strategy_fields(self, email_mock, scrape_mock):
        email_mock.return_value = ["hello@skyline-hoa.example.com"]
        scrape_mock.return_value = {
            "title": "Skyline HOA Services",
            "description": "Community management for HOA boards in Phoenix.",
            "h1": ["HOA management with faster resident response"],
        }

        with self.app.app_context():
            prospect_id = self.app.db.create_agency_prospect(
                name="Skyline HOA Services",
                business_name="Skyline HOA Services",
                website="https://skyline-hoa.example.com",
                industry="HOAs",
                service_area="Phoenix, AZ",
                source="commercial_scrape",
                account_type="hoa",
                source_details_json=json.dumps({"service_area": "Phoenix, AZ"}),
            )

        response = self.client.post(
            f"/crm/prospect/{prospect_id}/commercial-refresh",
            data={},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith(f"/crm/prospect/{prospect_id}"))

        with self.app.app_context():
            prospect = self.app.db.get_agency_prospect(prospect_id)
            self.assertTrue((prospect.get("outreach_angle") or "").strip())
            self.assertTrue((prospect.get("proposal_status") or "").strip())
            self.assertTrue((prospect.get("next_action") or "").strip())
            self.assertIn("hello@skyline-hoa.example.com", prospect.get("source_details_json") or "")
            self.assertIn("Skyline HOA Services", prospect.get("audit_snapshot_json") or "")


if __name__ == "__main__":
    unittest.main()