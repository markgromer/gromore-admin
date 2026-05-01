import os
import unittest
import uuid
from pathlib import Path

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

        self.assertEqual(len(demos), 1)
        self.assertEqual(demos[0]["business_name"], "Demo Plumbing")
        self.assertEqual(demos[0]["demo_snapshot"]["metrics"]["monthly_leads"], 42)
        self.assertEqual(prospect["partner_id"], self.partner_id)
        self.assertEqual(prospect["source"], "affiliate_demo")

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


if __name__ == "__main__":
    unittest.main()
