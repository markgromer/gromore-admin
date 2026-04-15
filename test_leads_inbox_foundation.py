import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-leads-inbox-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app
from webapp.database import WebDB


class LeadsInboxDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"leads-inbox-db-{uuid.uuid4().hex}.db"
        self.db = WebDB(str(self.db_file))
        self.db.init()
        self.brand_id = self.db.create_brand({
            "slug": f"lead-brand-{uuid.uuid4().hex[:8]}",
            "display_name": "Lead Brand",
        })

    def tearDown(self):
        self._cleanup_db_files(str(self.db_file))

    def _cleanup_db_files(self, base_path):
        for suffix in ("", "-wal", "-shm"):
            path = Path(base_path + suffix)
            if path.exists():
                path.unlink()

    def test_lead_thread_message_and_quote_flow(self):
        thread_id = self.db.upsert_lead_thread(
            self.brand_id,
            "sms",
            "thread-123",
            {
                "lead_name": "Taylor Prospect",
                "lead_email": "Taylor@example.com",
                "lead_phone": "+15555550123",
                "source": "meta_lead_form",
            },
        )

        same_thread_id = self.db.upsert_lead_thread(
            self.brand_id,
            "sms",
            "thread-123",
            {
                "lead_name": "Taylor Prospect Updated",
                "summary": "Asked for a quote on monthly service.",
            },
        )
        self.assertEqual(thread_id, same_thread_id)

        self.db.add_lead_message(
            thread_id,
            "inbound",
            "lead",
            "Can I get pricing for weekly service?",
            channel="sms",
            external_message_id="msg-in-1",
        )
        self.db.add_lead_message(
            thread_id,
            "outbound",
            "assistant",
            "We can help. A typical range depends on yard size and frequency.",
            channel="sms",
            external_message_id="msg-out-1",
        )
        self.db.add_lead_event(
            self.brand_id,
            thread_id,
            "call_logged",
            "+15555550123",
            {"duration_seconds": 92},
        )
        quote = self.db.upsert_lead_quote(
            self.brand_id,
            thread_id,
            status="draft",
            quote_mode="hybrid",
            amount_low=125,
            amount_high=175,
            summary="Weekly cleanup and haul-away",
            follow_up_text="We can lock this in for next Tuesday.",
        )

        thread = self.db.get_lead_thread(thread_id, self.brand_id)
        self.assertEqual(thread["lead_name"], "Taylor Prospect Updated")
        self.assertEqual(thread["lead_email"], "taylor@example.com")
        self.assertEqual(thread["unread_count"], 1)
        self.assertEqual(thread["quote_status"], "draft")

        messages = self.db.get_lead_messages(thread_id)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["direction"], "inbound")
        self.assertEqual(messages[1]["direction"], "outbound")

        events = self.db.get_lead_events(thread_id)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "call_logged")

        self.assertIsNotNone(quote)
        self.assertEqual(quote["amount_low"], 125.0)
        self.assertEqual(quote["amount_high"], 175.0)

        self.db.mark_lead_thread_read(thread_id)
        refreshed = self.db.get_lead_thread(thread_id, self.brand_id)
        self.assertEqual(refreshed["unread_count"], 0)

    def test_manual_threads_without_external_ids_can_coexist(self):
        first_thread = self.db.upsert_lead_thread(
            self.brand_id,
            "lead_forms",
            "",
            {"lead_name": "Manual Lead One"},
        )
        second_thread = self.db.upsert_lead_thread(
            self.brand_id,
            "lead_forms",
            None,
            {"lead_name": "Manual Lead Two"},
        )

        self.assertNotEqual(first_thread, second_thread)
        threads = self.db.get_lead_threads(self.brand_id)
        self.assertEqual(len(threads), 2)

    def test_get_active_lead_contacts_excludes_closed_threads(self):
        active_thread = self.db.create_lead_thread(
            self.brand_id,
            {"lead_name": "Open Lead", "channel": "sms", "status": "engaged"},
        )
        won_thread = self.db.create_lead_thread(
            self.brand_id,
            {"lead_name": "Won Lead", "channel": "sms", "status": "won"},
        )
        lost_thread = self.db.create_lead_thread(
            self.brand_id,
            {"lead_name": "Lost Lead", "channel": "sms", "status": "lost"},
        )

        contacts = self.db.get_active_lead_contacts(self.brand_id)
        contact_ids = {thread["id"] for thread in contacts}

        self.assertIn(active_thread, contact_ids)
        self.assertNotIn(won_thread, contact_ids)
        self.assertNotIn(lost_thread, contact_ids)


class LeadsAssistantSettingsRouteTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"leads-inbox-app-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"settings-brand-{uuid.uuid4().hex[:8]}",
                "display_name": "Settings Brand",
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
            session["client_brand_name"] = "Settings Brand"

    def tearDown(self):
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("SECRET_KEY", None)
        os.environ.pop("APP_URL", None)
        self._cleanup_db_files(str(self.db_file))

    def _cleanup_db_files(self, base_path):
        for suffix in ("", "-wal", "-shm"):
            path = Path(base_path + suffix)
            if path.exists():
                path.unlink()

    def test_client_can_save_leads_assistant_settings(self):
        response = self.client.post(
            "/client/settings/leads-assistant",
            data={
                "sales_bot_enabled": "1",
                "sales_bot_channels": ["sms", "lead_forms", "calls"],
                "sales_bot_quote_mode": "hybrid",
                "sales_bot_business_hours": "Mon-Fri 8am-5pm. After-hours emergencies should be escalated.",
                "sales_bot_reply_tone": "Direct and helpful",
                "sales_bot_reply_delay_seconds": "17",
                "sales_bot_payment_reminders_enabled": "1",
                "sales_bot_payment_reminder_days_before": "5",
                "sales_bot_payment_reminder_billing_day": "1",
                "sales_bot_payment_reminder_channels": ["email", "sms"],
                "sales_bot_payment_reminder_template": "Hi {client_name}, your billing date is {due_date}.",
                "sales_bot_transcript_export": "1",
                "sales_bot_meta_lead_forms": "1",
                "sales_bot_call_logging": "1",
                "sales_bot_auto_push_crm": "1",
                "quo_api_key": "quo_test_key_123",
                "quo_phone_number": "+15555550123",
                "sales_bot_quo_webhook_secret": "quo-secret",
                "sales_bot_incoming_webhook_secret": "incoming-secret",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/settings"))

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)

        self.assertEqual(brand["sales_bot_enabled"], 1.0)
        self.assertEqual(brand["sales_bot_quote_mode"], "hybrid")
        self.assertEqual(brand["sales_bot_business_hours"], "Mon-Fri 8am-5pm. After-hours emergencies should be escalated.")
        self.assertEqual(brand["sales_bot_reply_tone"], "Direct and helpful")
        self.assertEqual(brand["sales_bot_reply_delay_seconds"], 17.0)
        self.assertEqual(brand["sales_bot_payment_reminders_enabled"], 1.0)
        self.assertEqual(brand["sales_bot_payment_reminder_days_before"], 5.0)
        self.assertEqual(brand["sales_bot_payment_reminder_billing_day"], 1.0)
        self.assertEqual(brand["sales_bot_payment_reminder_channels"], '["email", "sms"]')
        self.assertEqual(brand["sales_bot_payment_reminder_template"], "Hi {client_name}, your billing date is {due_date}.")
        self.assertEqual(brand["sales_bot_transcript_export"], 1.0)
        self.assertEqual(brand["sales_bot_meta_lead_forms"], 1.0)
        self.assertEqual(brand["sales_bot_call_logging"], 1.0)
        self.assertEqual(brand["sales_bot_auto_push_crm"], 1.0)
        self.assertEqual(brand["quo_api_key"], "quo_test_key_123")
        self.assertEqual(brand["quo_phone_number"], "+15555550123")
        self.assertEqual(brand["sales_bot_quo_webhook_secret"], "quo-secret")
        self.assertEqual(brand["sales_bot_incoming_webhook_secret"], "incoming-secret")
        self.assertEqual(brand["sales_bot_channels"], '["sms", "lead_forms", "calls"]')

    def test_settings_page_shows_generic_lead_webhook_url(self):
        response = self.client.get("/client/settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Generic Incoming Lead Webhook URL", response.data)
        self.assertIn(b"/webhooks/leads/", response.data)

    @patch("webapp.quo_sms.get_phone_numbers")
    def test_client_can_test_openphone_connection(self, mock_get_phone_numbers):
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "quo_api_key", "quo_test_key_123")

        mock_get_phone_numbers.return_value = (
            [
                {"phoneNumber": "+15555550123"},
                {"formattedPhoneNumber": "+15555550999"},
            ],
            None,
        )

        response = self.client.post("/client/api/warren/test-openphone", json={})

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["phone_numbers"], ["+15555550123", "+15555550999"])
        self.assertEqual(data["count"], 2)

    @patch("webapp.quo_sms.send_test_sms")
    def test_client_can_send_openphone_test_sms(self, mock_send_test_sms):
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "quo_api_key", "quo_test_key_123")
            self.app.db.update_brand_text_field(self.brand_id, "quo_phone_number", "+15555550123")

        mock_send_test_sms.return_value = {
            "ok": True,
            "status_code": 202,
            "response_body": {"id": "msg_123"},
            "request_body": {
                "from": "+15555550123",
                "to": ["+15208672540"],
                "content": "Test SMS from Gromore...",
            },
        }

        response = self.client.post(
            "/client/api/warren/send-test-sms",
            json={"to_phone": "5208672540"},
        )

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        mock_send_test_sms.assert_called_once_with("quo_test_key_123", "+15555550123", "+15208672540")

    def test_client_can_open_and_save_lead_assistant_workspace(self):
        response = self.client.get("/client/lead-assistant")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Lead Assistant Workspace", response.data)

        save_response = self.client.post(
            "/client/lead-assistant",
            data={
                "crm_avg_service_price": "245",
                "sales_bot_service_menu": "Weekly lawn service: $95-$145\nFirst cleanup: starts at $175",
                "sales_bot_pricing_notes": "Protect margin and use a photo request before tightening quotes.",
                "sales_bot_guardrails": "Never promise same-day service without confirmation.",
                "sales_bot_example_language": "Most jobs like this land between $125 and $175.",
                "sales_bot_disallowed_language": "Do not say guaranteed lowest price.",
                "sales_bot_handoff_rules": "Escalate angry leads and commercial jobs.",
            },
            follow_redirects=False,
        )

        self.assertEqual(save_response.status_code, 302)
        self.assertTrue(save_response.headers["Location"].endswith("/client/lead-assistant"))

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)

        self.assertEqual(brand["crm_avg_service_price"], 245.0)
        self.assertEqual(brand["sales_bot_service_menu"], "Weekly lawn service: $95-$145\nFirst cleanup: starts at $175")
        self.assertEqual(brand["sales_bot_pricing_notes"], "Protect margin and use a photo request before tightening quotes.")
        self.assertEqual(brand["sales_bot_guardrails"], "Never promise same-day service without confirmation.")
        self.assertEqual(brand["sales_bot_example_language"], "Most jobs like this land between $125 and $175.")
        self.assertEqual(brand["sales_bot_disallowed_language"], "Do not say guaranteed lowest price.")
        self.assertEqual(brand["sales_bot_handoff_rules"], "Escalate angry leads and commercial jobs.")


if __name__ == "__main__":
    unittest.main()
