import os
import json
import unittest
import uuid
from pathlib import Path
from datetime import datetime

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
os.environ.setdefault("DATABASE_PATH", str(_TEST_ROOT / "partner-bootstrap.db"))
os.environ.setdefault("SECRET_KEY", "test-secret")

from webapp.app import create_app
from webapp.database import WebDB


class PartnerProgramDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"partner-db-{uuid.uuid4().hex}.db"
        self.db = WebDB(str(self.db_file))
        self.db.init()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_invoice_commission_is_idempotent_and_batchable(self):
        plan_id = self.db.create_commission_plan(
            name="Test 25 no hold",
            default_rate=0.25,
            hold_days=0,
        )
        partner_id = self.db.create_partner(
            name="Referral Partner",
            email="partner@example.com",
            status="active",
            default_commission_plan_id=plan_id,
            referral_code="REF25",
        )
        brand_id = self.db.create_brand({
            "slug": f"partner-brand-{uuid.uuid4().hex[:8]}",
            "display_name": "Partner Brand",
            "partner_id": partner_id,
            "referral_code": "ref25",
            "attribution": {"referral_code": "ref25"},
        })
        self.db.assign_partner_to_brand(partner_id, brand_id, commission_plan_id=plan_id, attribution={"referral_code": "ref25"})

        invoice = {"id": "in_test_1", "amount_paid": 10000, "currency": "usd", "subscription": "sub_1"}
        first = self.db.create_partner_commissions_for_invoice(brand_id, invoice, source_event_id="evt_1")
        second = self.db.create_partner_commissions_for_invoice(brand_id, invoice, source_event_id="evt_2")
        commissions = self.db.get_partner_commissions(partner_id=partner_id)

        self.assertEqual(first["created"], 1)
        self.assertEqual(second["created"], 0)
        self.assertEqual(len(commissions), 1)
        self.assertEqual(commissions[0]["commission_amount"], 25.0)

        batch_id = self.db.create_partner_payout_batch(notes="test batch")
        self.assertIsNotNone(batch_id)
        approved = self.db.get_partner_commissions(partner_id=partner_id, status="approved")
        self.assertEqual(len(approved), 1)
        self.assertEqual(approved[0]["payout_batch_id"], batch_id)


class PartnerProgramRouteTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"partner-app-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()
        with self.app.app_context():
            self.partner_id = self.app.db.create_partner(
                name="Agency Partner",
                email="agency@example.com",
                status="active",
                partner_type="agency",
                referral_code="agencyref",
            )
            self.partner_user_id = self.app.db.create_partner_user(
                self.partner_id,
                "agency@example.com",
                "Password123",
                "Agency User",
            )
            admin = self.app.db.authenticate("admin", "changeme123")
            self.admin_id = admin["id"]
        with self.client.session_transaction() as session:
            session["user_id"] = self.admin_id
            session["user_name"] = "Admin"

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_public_signup_referral_survives_crm_conversion(self):
        response = self.client.post(
            "/client/signup?ref=agencyref&utm_source=partner&utm_campaign=launch",
            json={
                "name": "Owner",
                "email": "owner@example.com",
                "business_name": "Referral Co",
                "industry": "plumbing",
                "website": "https://example.com",
            },
        )
        self.assertEqual(response.status_code, 200)

        with self.app.app_context():
            imported = self.app.db.import_signup_leads_to_crm()
            prospect = self.app.db.find_agency_prospect(email="owner@example.com", website="", business_name="")
        self.assertEqual(imported, 1)
        self.assertEqual(prospect["partner_id"], self.partner_id)
        self.assertEqual(prospect["referral_code"], "agencyref")

        response = self.client.post(f"/crm/prospect/{prospect['id']}/convert", data={})
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            prospect = self.app.db.get_agency_prospect(prospect["id"])
            brand = self.app.db.get_brand(prospect["converted_brand_id"])
            assignments = self.app.db.get_partner_brand_assignments(brand_id=brand["id"])

        self.assertEqual(brand["partner_id"], self.partner_id)
        self.assertEqual(brand["referral_code"], "agencyref")
        self.assertEqual(len(assignments), 1)
        self.assertEqual(assignments[0]["partner_id"], self.partner_id)

    def test_admin_partner_page_renders(self):
        response = self.client.get("/crm/partners")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Partners", response.data)
        self.assertIn(b"Agency Partner", response.data)

    def test_beta_style_demo_flag_does_not_trigger_affiliate_demo_mode(self):
        with self.app.app_context():
            brand_id = self.app.db.create_brand({
                "slug": f"beta-demo-flag-{uuid.uuid4().hex[:8]}",
                "display_name": "Beta Demo Flag Co",
                "industry": "home services",
            })
            self.app.db.update_brand_demo_fields(brand_id, is_demo=1, demo_status="demo_until_activated")
            client_user_id = self.app.db.create_client_user(
                brand_id,
                f"beta-{uuid.uuid4().hex[:8]}@example.test",
                "Password123",
                "Beta User",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = client_user_id
            session["client_brand_id"] = brand_id
            session["client_name"] = "Beta User"
            session["client_brand_name"] = "Beta Demo Flag Co"
            session["client_role"] = "owner"
            session["client_demo_mode"] = True
            session["client_demo_session_id"] = 999999
            session["client_demo_partner_id"] = 999999

        response = self.client.get("/client/api/me")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["brand"]["is_demo"])
        with self.client.session_transaction() as session:
            self.assertIsNone(session.get("client_demo_mode"))
            self.assertIsNone(session.get("client_demo_session_id"))

        response = self.client.get("/client/dashboard", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Demo mode:", response.data)

    def test_partner_can_create_demo_and_nurture_business(self):
        response = self.client.post(
            "/partners/login",
            data={"email": "agency@example.com", "password": "Password123"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/partners"))

        response = self.client.post(
            "/partners/demo/new",
            data={
                "business_name": "Demo Plumbing",
                "contact_name": "Demo Owner",
                "contact_email": "owner@demoplumbing.test",
                "contact_phone": "555-123-4567",
                "website": "https://demoplumbing.test",
                "industry": "plumbing",
                "service_area": "Phoenix",
                "primary_services": "Drain cleaning, water heaters",
                "avg_job_value": "650",
                "monthly_leads": "42",
                "crm_used": "Jobber",
                "lead_sources": "Facebook forms, missed calls",
                "pain_points": "Slow follow-up",
                "owner_goals": "Book more jobs",
                "good_lead_definition": "Homeowner in service area",
                "profitable_services": "Water heaters",
                "next_follow_up": "2026-05-15",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/partners/demo/", response.headers["Location"])

        with self.app.app_context():
            demos = self.app.db.get_partner_demo_sessions(self.partner_id)
            prospect = self.app.db.find_agency_prospect(email="owner@demoplumbing.test", website="", business_name="")
            brand = self.app.db.get_brand(demos[0]["demo_brand_id"]) if demos else None
            threads = self.app.db.get_lead_threads(brand["id"]) if brand else []
            current_month = datetime.now().strftime("%Y-%m")
            dashboard_snapshot = self.app.db.get_dashboard_snapshot(brand["id"], current_month, max_age_hours=8760) if brand else None
            heatmap_scans = self.app.db.get_heatmap_scans(brand["id"], limit=5) if brand else []
            tasks = self.app.db.get_brand_tasks(brand["id"], status="open") if brand else []
            posts = self.app.db.get_scheduled_posts(brand["id"], limit=5) if brand else []

        self.assertEqual(len(demos), 1)
        self.assertEqual(demos[0]["business_name"], "Demo Plumbing")
        self.assertEqual(demos[0]["demo_snapshot"]["metrics"]["monthly_leads"], 42)
        self.assertIsNotNone(demos[0]["demo_brand_id"])
        self.assertIsNotNone(brand)
        self.assertEqual(brand["is_demo"], 1)
        self.assertEqual(brand["demo_status"], "demo_until_activated")
        self.assertAlmostEqual(float(brand["business_lat"]), 33.4484, places=3)
        self.assertAlmostEqual(float(brand["business_lng"]), -112.0740, places=3)
        self.assertEqual(brand["ai_provider"], "openai")
        self.assertGreaterEqual(len(threads), 3)
        self.assertIsNotNone(dashboard_snapshot)
        dashboard_data = json.loads(dashboard_snapshot["snapshot_json"])
        self.assertTrue(dashboard_data["demo_mode"])
        self.assertIn("shared_tokens", demos[0]["demo_snapshot"])
        self.assertGreaterEqual(len(heatmap_scans), 1)
        self.assertGreaterEqual(len(tasks), 1)
        self.assertGreaterEqual(len(posts), 1)
        self.assertEqual(prospect["partner_id"], self.partner_id)
        self.assertEqual(prospect["source"], "affiliate_demo")

        response = self.client.get(f"/partners/demo/live/{demos[0]['demo_token']}", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Auto WARREN", response.data)
        self.assertIn(b"Live-style WARREN demo workspace", response.data)
        self.assertIn(b"Start interactive tour", response.data)
        self.assertIn(b"WARREN demo feature board", response.data)
        with self.client.session_transaction() as session:
            self.assertEqual(session.get("client_brand_id"), brand["id"])
            self.assertTrue(session.get("client_demo_mode"))
        blocked = self.client.post(
            f"/client/inbox/thread/{threads[0]['id']}/reply",
            json={"message": "Send this"},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        self.assertEqual(blocked.status_code, 423)
        self.assertTrue(blocked.get_json()["demo_mode"])

        response = self.client.post(
            "/partners/login",
            data={"email": "agency@example.com", "password": "Password123"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        demo_id = demos[0]["id"]
        response = self.client.post(
            f"/partners/demo/{demo_id}/nurture",
            data={
                "nurture_status": "follow_up",
                "next_follow_up": "2026-05-20",
                "note": "Owner wants spouse to review recap.",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            demo = self.app.db.get_partner_demo_session(demo_id, self.partner_id)
            events = self.app.db.get_partner_demo_events(demo_id, self.partner_id)

        self.assertEqual(demo["nurture_status"], "follow_up")
        self.assertTrue(any(event["event_type"] == "nurture" for event in events))

    def test_live_demo_repairs_legacy_demo_without_brand(self):
        with self.app.app_context():
            snapshot = {
                "metrics": {"monthly_leads": 18, "estimated_unfollowed_leads": 4, "projected_recovered_revenue": 1200},
                "sample_leads": [
                    {"name": "Legacy Lead", "source": "Facebook Lead Form", "need": "cleanup", "stage": "Hot", "value": 300, "warren_action": "Qualified the job and prepared a booking reply."}
                ],
                "connection_plan": [],
            }
            demo_id = self.app.db.create_partner_demo_session(
                self.partner_id,
                self.partner_user_id,
                {
                    "status": "demo_ready",
                    "nurture_status": "new",
                    "business_name": "Legacy Demo Co",
                    "contact_name": "Legacy Owner",
                    "contact_email": "legacy@example.test",
                    "industry": "pet waste removal",
                    "service_area": "Tucson",
                    "primary_services": "Pet waste removal",
                    "monthly_leads": 18,
                    "avg_job_value": 300,
                    "demo_snapshot": snapshot,
                    "owner_intake": {"good_lead_definition": "Homeowner in service area"},
                },
            )
            demo = self.app.db.get_partner_demo_session(demo_id, self.partner_id)

        response = self.client.get(f"/partners/demo/live/{demo['demo_token']}", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Auto WARREN", response.data)
        self.assertIn(b"Live-style WARREN demo workspace", response.data)
        self.assertIn(b"WARREN tour", response.data)
        self.assertIn(b"Commercial", response.data)

        with self.app.app_context():
            repaired = self.app.db.get_partner_demo_session(demo_id, self.partner_id)
            brand = self.app.db.get_brand(repaired["demo_brand_id"])
            threads = self.app.db.get_lead_threads(brand["id"])

        self.assertIsNotNone(repaired["demo_brand_id"])
        self.assertEqual(brand["is_demo"], 1)
        self.assertGreaterEqual(len(threads), 1)
        self.assertTrue(any(thread["lead_name"] == "Legacy Lead" for thread in threads))


if __name__ == "__main__":
    unittest.main()
