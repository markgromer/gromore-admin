import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-warren-handoff-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class WarrenHandoffAlertTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"warren-handoff-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"warren-handoff-{uuid.uuid4().hex[:8]}",
                "display_name": "Warren Handoff Brand",
                "industry": "plumbing",
            })
            self.app.db.update_brand_number_field(self.brand_id, "sales_bot_enabled", 1)
            self.app.db.update_brand_text_field(self.brand_id, "sales_bot_crm_event_alert_emails", "owner@example.com")
            self.app.db.update_brand_text_field(self.brand_id, "sales_bot_handoff_alert_phones", "+15555550124")

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    @patch("webapp.warren_sender.send_transactional_sms")
    @patch("webapp.email_sender.send_simple_email")
    @patch("webapp.warren_brain.generate_response")
    def test_handoff_alerts_owner_and_assigns_thread_to_human(self, mock_generate_response, mock_send_email, mock_send_sms):
        mock_generate_response.return_value = {
            "action": "handoff",
            "reply": "",
            "confidence": 0.92,
            "handoff_reason": "Lead is angry and asked for the owner.",
        }
        mock_send_email.return_value = None
        mock_send_sms.return_value = (True, "sent")

        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Taylor Prospect",
                    "lead_phone": "+15555550199",
                    "lead_email": "taylor@example.com",
                    "channel": "sms",
                    "summary": "Asked about emergency service.",
                },
            )
            self.app.db.add_lead_message(
                thread_id,
                direction="inbound",
                role="lead",
                content="I need the owner right now. I am upset.",
                channel="sms",
            )

            from webapp.warren_brain import process_and_respond

            result = process_and_respond(self.app.db, self.brand_id, thread_id, channel="sms")

            self.assertIsNotNone(result)
            self.assertEqual(result["action"], "handoff")
            self.assertFalse(result["should_send"])

            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            self.assertEqual(thread["assigned_to"], "human")

            handoff_events = self.app.db.get_lead_events(thread_id, event_type="handoff_triggered")
            self.assertEqual(len(handoff_events), 1)
            self.assertIn("asked for the owner", handoff_events[0]["event_value"])

            owner_alert_events = self.app.db.get_lead_events(thread_id, event_type="owner_handoff_alert")
            self.assertEqual(len(owner_alert_events), 1)
            self.assertIn("email x1", owner_alert_events[0]["event_value"])
            self.assertIn("sms x1", owner_alert_events[0]["event_value"])

        self.assertEqual(mock_send_email.call_count, 1)
        email_args = mock_send_email.call_args.args
        self.assertEqual(email_args[1], "owner@example.com")
        self.assertIn("Taylor Prospect", email_args[2])
        self.assertIn("interrupt a live lead conversation", email_args[3])

        self.assertEqual(mock_send_sms.call_count, 1)
        sms_args = mock_send_sms.call_args.args
        self.assertEqual(sms_args[2], "+15555550124")
        self.assertIn("interrupt the conversation", sms_args[3])
        self.assertFalse(mock_send_sms.call_args.kwargs["append_opt_out_footer"])

    @patch("webapp.warren_brain.generate_response")
    def test_human_assigned_threads_skip_future_warren_responses(self, mock_generate_response):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Casey Prospect",
                    "channel": "sms",
                    "assigned_to": "human",
                },
            )

            from webapp.warren_brain import process_and_respond

            result = process_and_respond(self.app.db, self.brand_id, thread_id, channel="sms")

        self.assertIsNone(result)
        mock_generate_response.assert_not_called()


if __name__ == "__main__":
    unittest.main()