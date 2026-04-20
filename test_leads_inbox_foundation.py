import json
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
                "sales_bot_handoff_alert_phones": "+15555550124, +15555550125",
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
        self.assertEqual(brand["sales_bot_handoff_alert_phones"], "+15555550124, +15555550125")
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

    def test_client_can_save_automations(self):
        response = self.client.post(
            "/client/automations/save",
            data={
                "sales_bot_enabled": "1",
                "sales_bot_channels": ["sms", "lead_forms"],
                "sales_bot_quote_mode": "hybrid",
                "sales_bot_business_hours": "Mon-Fri 8am-5pm.",
                "sales_bot_reply_tone": "Direct",
                "sales_bot_reply_delay_seconds": "11",
                "sales_bot_payment_reminders_enabled": "1",
                "sales_bot_payment_reminder_days_before": "4",
                "sales_bot_payment_reminder_billing_day": "2",
                "sales_bot_payment_reminder_channels": ["email", "sms"],
                "sales_bot_payment_reminder_template": "Billing reminder for {client_name}",
                "sales_bot_appointment_reminders_enabled": "1",
                "sales_bot_appointment_reminder_send_time": "16:30",
                "sales_bot_appointment_reminder_timezone": "America/Chicago",
                "sales_bot_appointment_reminder_channels": ["sms"],
                "sales_bot_appointment_reminder_template": "Appointment reminder for {client_name}",
                "sales_bot_appointment_reminder_respect_client_channel": "1",
                "sales_bot_transcript_export": "1",
                "sales_bot_meta_lead_forms": "1",
                "sales_bot_call_logging": "1",
                "sales_bot_auto_push_crm": "1",
                "sales_bot_nurture_enabled": "1",
                "sales_bot_nurture_hot_hours": "3",
                "sales_bot_nurture_hot_max": "4",
                "sales_bot_nurture_warm_hours": "26",
                "sales_bot_nurture_warm_max": "2",
                "sales_bot_nurture_cold_hours": "52",
                "sales_bot_nurture_cold_max": "2",
                "sales_bot_nurture_ghost_hours": "96",
                "sales_bot_dnd_enabled": "1",
                "sales_bot_dnd_start": "20:30",
                "sales_bot_dnd_end": "08:30",
                "sales_bot_dnd_weekends": "1",
                "sales_bot_dnd_timezone": "America/Los_Angeles",
                "sales_bot_sms_opt_out_footer": "Reply STOP to opt out.",
                "sales_bot_crm_event_alert_emails": "owner@example.com, manager@example.com",
                "crm_rule_failed_payment_enabled": "1",
                "crm_rule_failed_payment_channels": ["sms", "email"],
                "crm_rule_failed_payment_delay_minutes": "0",
                "crm_rule_failed_payment_retry_days": "2",
                "crm_rule_failed_payment_max_attempts": "3",
                "crm_rule_failed_payment_owner_alert": "1",
                "crm_rule_failed_payment_respect_dnd": "1",
                "crm_rule_failed_payment_template": "Payment issue for {client_name}",
                "crm_rule_invoice_finalized_enabled": "1",
                "crm_rule_invoice_finalized_channels": ["email"],
                "crm_rule_invoice_finalized_delay_minutes": "15",
                "crm_rule_invoice_finalized_retry_days": "0",
                "crm_rule_invoice_finalized_max_attempts": "1",
                "crm_rule_invoice_finalized_owner_alert": "1",
                "crm_rule_invoice_finalized_respect_dnd": "1",
                "crm_rule_invoice_finalized_template": "Invoice ready for {client_name}",
                "crm_rule_subscription_canceled_enabled": "1",
                "crm_rule_subscription_canceled_channels": ["email"],
                "crm_rule_subscription_canceled_delay_minutes": "10",
                "crm_rule_subscription_canceled_retry_days": "1",
                "crm_rule_subscription_canceled_max_attempts": "2",
                "crm_rule_subscription_canceled_owner_alert": "1",
                "crm_rule_subscription_canceled_respect_dnd": "1",
                "crm_rule_subscription_canceled_template": "Canceled subscription for {client_name}",
                "crm_rule_subscription_paused_enabled": "1",
                "crm_rule_subscription_paused_channels": ["email"],
                "crm_rule_subscription_paused_delay_minutes": "10",
                "crm_rule_subscription_paused_retry_days": "2",
                "crm_rule_subscription_paused_max_attempts": "2",
                "crm_rule_subscription_paused_owner_alert": "1",
                "crm_rule_subscription_paused_respect_dnd": "1",
                "crm_rule_subscription_paused_template": "Paused subscription for {client_name}",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/automations"))

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)

        self.assertEqual(brand["sales_bot_enabled"], 1.0)
        self.assertEqual(brand["sales_bot_reply_delay_seconds"], 11.0)
        self.assertEqual(brand["sales_bot_payment_reminder_days_before"], 4.0)
        self.assertEqual(brand["sales_bot_payment_reminder_billing_day"], 2.0)
        self.assertEqual(brand["sales_bot_appointment_reminder_send_time"], "16:30")
        self.assertEqual(brand["sales_bot_appointment_reminder_timezone"], "America/Chicago")
        self.assertEqual(brand["sales_bot_nurture_hot_hours"], 3.0)
        self.assertEqual(brand["sales_bot_dnd_start"], "20:30")
        self.assertEqual(brand["sales_bot_sms_opt_out_footer"], "Reply STOP to opt out.")
        self.assertEqual(brand["sales_bot_crm_event_alert_emails"], "owner@example.com, manager@example.com")
        crm_rules = json.loads(brand["sales_bot_crm_event_rules"])
        self.assertTrue(crm_rules["failed_payment"]["enabled"])
        self.assertEqual(crm_rules["failed_payment"]["channels"], ["sms", "email"])
        self.assertEqual(crm_rules["failed_payment"]["delay_minutes"], 0)
        self.assertEqual(crm_rules["failed_payment"]["retry_days"], 2)
        self.assertEqual(crm_rules["failed_payment"]["max_attempts"], 3)
        self.assertEqual(crm_rules["failed_payment"]["template"], "Payment issue for {client_name}")
        self.assertTrue(crm_rules["invoice_finalized"]["enabled"])
        self.assertTrue(crm_rules["subscription_canceled"]["enabled"])
        self.assertTrue(crm_rules["subscription_paused"]["enabled"])

    def test_client_can_save_warren_channel_settings(self):
        response = self.client.post(
            "/client/settings/warren-channels",
            data={
                "quo_api_key": "quo_test_key_abc",
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

        self.assertEqual(brand["quo_api_key"], "quo_test_key_abc")
        self.assertEqual(brand["quo_phone_number"], "+15555550123")
        self.assertEqual(brand["sales_bot_quo_webhook_secret"], "quo-secret")
        self.assertEqual(brand["sales_bot_incoming_webhook_secret"], "incoming-secret")

    def test_settings_page_shows_generic_lead_webhook_url(self):
        response = self.client.get("/client/settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Generic Incoming Lead Webhook URL", response.data)
        self.assertIn(b"/webhooks/leads/", response.data)

    def test_settings_page_shows_connection_workspace_and_automations_link(self):
        response = self.client.get("/client/settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Connection Workspace", response.data)
        self.assertIn(b"Automations", response.data)

    def test_settings_page_shows_sng_webhook_url_and_generates_secret(self):
        response = self.client.get("/client/settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Sweep and Go Webhook URL", response.data)
        self.assertIn(b"/webhooks/sng/", response.data)

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)
            self.assertTrue((brand.get("sales_bot_sng_webhook_secret") or "").strip())

    def test_settings_page_shows_partial_warren_status_when_only_one_channel_is_ready(self):
        with self.app.app_context():
            self.app.db.update_brand_text_field(
                self.brand_id,
                "sales_bot_incoming_webhook_secret",
                "incoming-secret",
            )

        response = self.client.get("/client/settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"1 of 3 ready", response.data)

    def test_connections_and_help_pages_include_crm_setup_guides(self):
        response = self.client.get("/client/settings")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"#crm-sng", response.data)
        self.assertIn(b"#crm-ghl", response.data)

        help_response = self.client.get("/client/help?guide=connections")
        self.assertEqual(help_response.status_code, 200)
        self.assertIn(b"Sweep and Go CRM", help_response.data)
        self.assertIn(b"GoHighLevel CRM", help_response.data)

    def test_connections_page_includes_google_maps_and_place_id_setup(self):
        response = self.client.get("/client/settings")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Google Maps", response.data)
        self.assertIn(b"Google Maps API Key", response.data)
        self.assertIn(b"Test Google Maps Key", response.data)
        self.assertIn(b"Find Place ID", response.data)
        self.assertIn(b"#google-maps", response.data)

    def test_automations_page_shows_appointment_reminder_reports(self):
        with self.app.app_context():
            self.app.db.record_appointment_reminder_run(
                self.brand_id,
                "2026-04-18",
                status="completed",
                reason="Processed 1 appointment candidate(s): 1 sent, 0 failed, 0 skipped.",
                candidates=1,
                sent=1,
                summary={"local_time": "2026-04-17 17:00", "send_after": "17:00", "timezone": "America/New_York"},
            )
            self.app.db.record_client_billing_reminder(
                self.brand_id,
                "job:123",
                "2026-04-18",
                "sms",
                recipient="+15555550123",
                status="sent",
                detail='{"result":{"id":"msg_123","status":"queued"}}',
                reminder_type="appointment_day_ahead",
            )

        response = self.client.get("/client/automations")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Recent appointment reminder runs", response.data)
        self.assertIn(b"Processed 1 appointment candidate", response.data)
        self.assertIn(b"Recent delivery attempts", response.data)
        self.assertIn(b"+15555550123", response.data)
        self.assertIn(b"queued - msg_123", response.data)

    def test_automations_page_shows_crm_event_section(self):
        response = self.client.get("/client/automations")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"CRM Event Messages", response.data)
        self.assertIn(b"Failed Payments", response.data)
        self.assertIn(b"Internal alert recipients", response.data)
        self.assertIn(b"Recent CRM action history", response.data)

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

    @patch("webapp.warren_appointments.process_appointment_reminders")
    def test_client_can_run_appointment_reminder_check_now(self, mock_process_appointment_reminders):
        with self.app.app_context():
            self.app.db.record_appointment_reminder_run(
                self.brand_id,
                "2026-04-18",
                status="completed",
                reason="Processed 1 appointment candidate(s): 1 sent, 0 failed, 0 skipped.",
                candidates=1,
                sent=1,
            )

        mock_process_appointment_reminders.return_value = {
            "brands": 1,
            "candidates": 1,
            "sent": 1,
            "failed": 0,
            "skipped": 0,
            "errors": [],
        }

        response = self.client.post("/client/api/warren/run-appointment-reminders", json={})

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["ok"])
        self.assertIn("Processed 1 appointment candidate", data["message"])
        mock_process_appointment_reminders.assert_called_once()
        kwargs = mock_process_appointment_reminders.call_args.kwargs
        self.assertEqual(kwargs["brand_ids"], [self.brand_id])
        self.assertTrue(kwargs["ignore_send_time"])
        self.assertTrue(kwargs["include_disabled"])

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
                "sales_bot_crm_event_alert_emails": "owner@example.com, manager@example.com",
                "sales_bot_handoff_alert_phones": "+15555550124, +15555550125",
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
        self.assertEqual(brand["sales_bot_crm_event_alert_emails"], "owner@example.com, manager@example.com")
        self.assertEqual(brand["sales_bot_handoff_alert_phones"], "+15555550124, +15555550125")

    def test_client_can_save_warren_hosted_lead_form_config(self):
        response = self.client.post(
            "/client/lead-assistant",
            data={
                "lead_form_enabled": "1",
                "lead_form_auto_text_enabled": "1",
                "lead_form_require_sms_consent": "1",
                "lead_form_show_service": "1",
                "lead_form_show_email": "1",
                "lead_form_show_company": "1",
                "lead_form_show_address": "1",
                "lead_form_show_message": "1",
                "lead_form_headline": "Get a pet waste quote",
                "lead_form_intro": "Tell us about the property and Warren will open the lead instantly.",
                "lead_form_cta_label": "Text me pricing",
                "lead_form_success_title": "We got it",
                "lead_form_success_message": "Watch your phone for pricing.",
                "lead_form_service_label": "Service type",
                "lead_form_details_label": "Property notes",
                "lead_form_service_options": "Weekly pet waste\nTwice weekly pet waste",
                "lead_form_consent_label": "I agree to receive text messages about my quote.",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/lead-assistant"))

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)

        config = json.loads(brand["sales_bot_lead_form_config"])
        self.assertTrue(config["enabled"])
        self.assertTrue(config["auto_text_enabled"])
        self.assertTrue(config["require_sms_consent"])
        self.assertTrue(config["show_service"])
        self.assertEqual(config["headline"], "Get a pet waste quote")
        self.assertEqual(config["cta_label"], "Text me pricing")
        self.assertTrue(config["show_company"])
        self.assertEqual(config["service_options"], ["Weekly pet waste", "Twice weekly pet waste"])

    @patch("webapp.warren_sender.send_reply")
    @patch("webapp.warren_brain.process_and_respond")
    def test_public_warren_lead_form_creates_thread_and_can_auto_text(self, mock_process_and_respond, mock_send_reply):
        mock_process_and_respond.return_value = {
            "reply": "We can help with that. I just sent pricing.",
            "action": "quote",
            "thread_id": 1,
            "should_send": True,
            "handoff_reason": "",
        }
        mock_send_reply.return_value = (True, "queued")

        with self.app.app_context():
            self.app.db.update_brand_number_field(self.brand_id, "sales_bot_enabled", 1)
            self.app.db.update_brand_text_field(
                self.brand_id,
                "sales_bot_lead_form_config",
                json.dumps({
                    "enabled": True,
                    "headline": "Fast quote form",
                    "show_service": True,
                    "service_options": ["Weekly service", "Initial cleanup"],
                    "show_email": True,
                    "show_company": True,
                    "show_address": True,
                    "show_message": True,
                    "auto_text_enabled": True,
                    "require_sms_consent": True,
                }),
            )
            brand = self.app.db.get_brand(self.brand_id)

        response = self.client.get(f"/warren/form/{brand['slug']}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Fast quote form", response.data)

        submit_response = self.client.post(
            f"/warren/form/{brand['slug']}",
            data={
                "name": "Taylor Prospect",
                "phone": "5208672540",
                "email": "taylor@example.com",
                "company": "Taylor Property Services",
                "service_needed": "Weekly service",
                "address": "123 Main St",
                "message": "Need service for a dog run and side yard.",
                "sms_consent": "1",
            },
            follow_redirects=False,
        )

        self.assertEqual(submit_response.status_code, 302)
        self.assertIn("submitted=1", submit_response.headers["Location"])
        self.assertTrue(mock_process_and_respond.called)
        self.assertTrue(mock_process_and_respond.call_args.kwargs["allow_auto_send"])
        mock_send_reply.assert_called_once()

        with self.app.app_context():
            threads = self.app.db.get_lead_threads(self.brand_id)
            self.assertEqual(len(threads), 1)
            thread = threads[0]
            self.assertEqual(thread["lead_name"], "Taylor Prospect")
            self.assertEqual(thread["lead_phone"], "+15208672540")
            self.assertEqual(thread["source"], "warren_hosted_form")

            messages = self.app.db.get_lead_messages(thread["id"])
            self.assertEqual(len(messages), 1)
            self.assertIn("Warren Hosted Lead Form", messages[0]["content"])
            self.assertIn("Company Name: Taylor Property Services", messages[0]["content"])

    @patch("webapp.warren_sender.send_reply")
    @patch("webapp.warren_brain.process_and_respond")
    def test_public_warren_lead_form_does_not_auto_text_without_consent(self, mock_process_and_respond, mock_send_reply):
        mock_process_and_respond.return_value = {
            "reply": "Drafted reply only.",
            "action": "reply",
            "thread_id": 1,
            "should_send": False,
            "handoff_reason": "",
        }

        with self.app.app_context():
            self.app.db.update_brand_number_field(self.brand_id, "sales_bot_enabled", 1)
            self.app.db.update_brand_text_field(
                self.brand_id,
                "sales_bot_lead_form_config",
                json.dumps({
                    "enabled": True,
                    "headline": "Fast quote form",
                    "show_service": False,
                    "service_options": ["Weekly service"],
                    "show_email": True,
                    "show_address": True,
                    "show_message": True,
                    "auto_text_enabled": True,
                    "require_sms_consent": False,
                }),
            )
            brand = self.app.db.get_brand(self.brand_id)

        submit_response = self.client.post(
            f"/warren/form/{brand['slug']}",
            data={
                "name": "No Consent Lead",
                "phone": "5208672540",
                "email": "noconsent@example.com",
                "address": "123 Main St",
                "message": "Need help weekly.",
            },
            follow_redirects=False,
        )

        self.assertEqual(submit_response.status_code, 302)
        self.assertTrue(mock_process_and_respond.called)
        self.assertFalse(mock_process_and_respond.call_args.kwargs["allow_auto_send"])
        mock_send_reply.assert_not_called()

        with self.app.app_context():
            threads = self.app.db.get_lead_threads(self.brand_id)
            self.assertEqual(len(threads), 1)
            self.assertEqual(threads[0]["lead_name"], "No Consent Lead")

    def test_public_warren_lead_form_hides_unchecked_fields(self):
        with self.app.app_context():
            self.app.db.update_brand_text_field(
                self.brand_id,
                "sales_bot_lead_form_config",
                json.dumps({
                    "enabled": True,
                    "headline": "Fast quote form",
                    "show_service": False,
                    "show_email": False,
                    "show_company": False,
                    "show_address": False,
                    "show_message": False,
                }),
            )
            brand = self.app.db.get_brand(self.brand_id)

        response = self.client.get(f"/warren/form/{brand['slug']}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Full name", response.data)
        self.assertIn(b"Mobile number", response.data)
        self.assertNotIn(b"Email", response.data)
        self.assertNotIn(b"Company name", response.data)
        self.assertNotIn(b"Service address", response.data)
        self.assertNotIn(b"Tell us about the job", response.data)


if __name__ == "__main__":
    unittest.main()
