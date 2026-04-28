import json
import unittest
from unittest.mock import patch

from webapp.app import create_app


class WarrenCrmEventAutomationTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()
        self.db = self.app.db

        with self.app.app_context():
            conn = self.db._conn()
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE slug = 'warren_crm_events_test'"
            ).fetchone()
            if brand_row:
                self.brand_id = brand_row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO brands (slug, display_name, industry, sales_bot_enabled) VALUES (?, ?, ?, ?)",
                    ("warren_crm_events_test", "CRM Events Test Co", "plumbing", 1),
                )
                self.brand_id = cur.lastrowid
            conn.commit()
            conn.execute("DELETE FROM crm_event_actions WHERE brand_id = ?", (self.brand_id,))
            conn.execute("DELETE FROM sng_webhook_events WHERE brand_id = ?", (self.brand_id,))
            conn.execute("DELETE FROM lead_messages WHERE thread_id IN (SELECT id FROM lead_threads WHERE brand_id = ?)", (self.brand_id,))
            conn.execute("DELETE FROM lead_events WHERE brand_id = ?", (self.brand_id,))
            conn.execute("DELETE FROM lead_threads WHERE brand_id = ?", (self.brand_id,))
            conn.commit()
            conn.close()

            self.db.update_brand_text_field(self.brand_id, "crm_type", "sweepandgo")
            self.db.update_brand_text_field(self.brand_id, "crm_api_key", "sng-test-token")
            self.db.update_brand_text_field(self.brand_id, "quo_api_key", "quo-test-key")
            self.db.update_brand_text_field(self.brand_id, "quo_phone_number", "+15550001111")
            self.db.update_brand_text_field(self.brand_id, "sales_bot_crm_event_alert_emails", "owner@example.com")
            self.db.update_brand_text_field(
                self.brand_id,
                "sales_bot_crm_event_rules",
                json.dumps(
                    {
                        "failed_payment": {
                            "enabled": True,
                            "channels": ["sms", "email"],
                            "delay_minutes": 0,
                            "retry_days": 2,
                            "max_attempts": 2,
                            "respect_dnd": True,
                            "owner_alert": True,
                            "template": "Payment issue for {client_name}",
                        },
                        "invoice_finalized": {
                            "enabled": False,
                            "channels": ["email"],
                            "delay_minutes": 15,
                            "retry_days": 0,
                            "max_attempts": 1,
                            "respect_dnd": True,
                            "owner_alert": True,
                            "template": "Invoice ready for {client_name}",
                        },
                        "subscription_canceled": {
                            "enabled": False,
                            "channels": ["email"],
                            "delay_minutes": 10,
                            "retry_days": 1,
                            "max_attempts": 2,
                            "respect_dnd": True,
                            "owner_alert": True,
                            "template": "Canceled subscription for {client_name}",
                        },
                        "subscription_paused": {
                            "enabled": False,
                            "channels": ["email"],
                            "delay_minutes": 10,
                            "retry_days": 2,
                            "max_attempts": 2,
                            "respect_dnd": True,
                            "owner_alert": True,
                            "template": "Paused subscription for {client_name}",
                        },
                    },
                    separators=(",", ":"),
                ),
            )
            self.sng_secret = self.db.ensure_brand_sng_webhook_secret(self.brand_id)

    @patch("webapp.warren_crm_events.send_simple_email")
    @patch("webapp.warren_crm_events.send_transactional_sms", return_value=(True, "sent"))
    def test_failed_payment_webhook_queues_immediate_and_retry_actions(self, mock_send_sms, mock_send_email):
        payload = {
            "id": "evt_declined_001",
            "event": {"type": "client:client_payment_declined"},
            "client": {
                "id": "client_123",
                "name": "Taylor Prospect",
                "email": "taylor@example.com",
                "phone": "+15551234567",
            },
            "payment": {"id": "pay_123", "status": "failed"},
        }

        response = self.client.post(
            f"/webhooks/sng/warren_crm_events_test/{self.sng_secret}",
            json=payload,
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        mock_send_sms.assert_called_once()
        self.assertEqual(mock_send_email.call_count, 2)

        with self.app.app_context():
            actions = self.db.get_crm_event_actions(self.brand_id, limit=10)
            event = self.db.get_sng_webhook_event_by_external_id(self.brand_id, "evt_declined_001")

        self.assertEqual(len(actions), 5)
        statuses = [action["status"] for action in actions]
        self.assertEqual(statuses.count("sent"), 3)
        self.assertEqual(statuses.count("queued"), 2)
        self.assertEqual(event["status"], "processed")
        self.assertIn("queued 5 crm action", event["detail"].lower())

    @patch("webapp.warren_crm_events.send_simple_email")
    @patch("webapp.warren_crm_events.send_transactional_sms", return_value=(True, "sent"))
    def test_payment_recovered_event_resolves_future_failed_payment_actions(self, mock_send_sms, mock_send_email):
        declined_payload = {
            "id": "evt_declined_002",
            "event": {"type": "client:client_payment_declined"},
            "client": {
                "id": "client_456",
                "name": "Jordan Client",
                "email": "jordan@example.com",
                "phone": "+15557654321",
            },
            "payment": {"id": "pay_456", "status": "failed"},
        }
        recovered_payload = {
            "id": "evt_paid_002",
            "event": {"type": "client:client_payment_accepted"},
            "client": {
                "id": "client_456",
                "name": "Jordan Client",
                "email": "jordan@example.com",
                "phone": "+15557654321",
            },
            "payment": {"id": "pay_456", "status": "paid"},
        }

        first_response = self.client.post(
            f"/webhooks/sng/warren_crm_events_test/{self.sng_secret}",
            json=declined_payload,
            content_type="application/json",
        )
        second_response = self.client.post(
            f"/webhooks/sng/warren_crm_events_test/{self.sng_secret}",
            json=recovered_payload,
            content_type="application/json",
        )

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)

        with self.app.app_context():
            actions = self.db.get_crm_event_actions(self.brand_id, limit=10)
            recovered_event = self.db.get_sng_webhook_event_by_external_id(self.brand_id, "evt_paid_002")

        queued_followups = [action for action in actions if action["attempt_number"] == 2]
        self.assertTrue(queued_followups)
        self.assertTrue(all(action["status"] == "resolved" for action in queued_followups))
        self.assertEqual(recovered_event["status"], "processed")
        self.assertIn("resolved", recovered_event["detail"].lower())

    @patch("webapp.warren_crm_events.send_reply", return_value=(True, "sent"))
    @patch("webapp.warren_crm_events.lookup_contact_policy", return_value={"suppress_marketing": False, "is_active_client": False})
    def test_quote_event_waits_then_imports_lead_and_sends_sms_if_not_active(self, mock_policy, mock_send_reply):
        self.db.update_brand_text_field(
            self.brand_id,
            "sales_bot_crm_event_rules",
            json.dumps(
                {
                    "quote_not_signed_up": {
                        "enabled": True,
                        "channels": ["sms"],
                        "delay_minutes": 0,
                        "retry_days": 0,
                        "max_attempts": 1,
                        "respect_dnd": True,
                        "owner_alert": False,
                        "template": "Hi {client_name}, want to start from quote {quote_id}?",
                    }
                },
                separators=(",", ":"),
            ),
        )

        payload = {
            "id": "evt_quote_001",
            "event": {"type": "client:free_quote_created"},
            "client": {
                "id": "prospect_123",
                "name": "Quinn Quote",
                "email": "quinn@example.com",
                "phone": "+15552223333",
            },
            "quote": {"id": "quote_123", "amount": "89.00", "service": "Weekly cleanup"},
        }

        response = self.client.post(
            f"/webhooks/sng/warren_crm_events_test/{self.sng_secret}",
            json=payload,
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        mock_send_reply.assert_called_once()

        with self.app.app_context():
            actions = self.db.get_crm_event_actions(self.brand_id, limit=10)
            threads = self.db.get_lead_threads(self.brand_id, limit=10)
            messages = self.db.get_lead_messages(threads[0]["id"]) if threads else []

        self.assertEqual(actions[0]["rule_key"], "quote_not_signed_up")
        self.assertEqual(actions[0]["status"], "sent")
        self.assertEqual(len(threads), 1)
        self.assertEqual(threads[0]["source"], "sweepandgo_quote")
        self.assertEqual(threads[0]["status"], "quoted")
        self.assertEqual(threads[0]["quote_status"], "sent")
        self.assertTrue(any("Sweep and Go Quote" in msg["content"] for msg in messages))
        self.assertTrue(any("want to start" in msg["content"] for msg in messages))
        self.assertTrue(mock_policy.called)

    @patch("webapp.warren_crm_events.send_reply")
    @patch("webapp.warren_crm_events.lookup_contact_policy", return_value={"suppress_marketing": True, "is_active_client": True, "reason": "active_client"})
    def test_quote_event_does_not_sms_when_contact_is_active_at_send_time(self, mock_policy, mock_send_reply):
        self.db.update_brand_text_field(
            self.brand_id,
            "sales_bot_crm_event_rules",
            json.dumps(
                {
                    "quote_not_signed_up": {
                        "enabled": True,
                        "channels": ["sms"],
                        "delay_minutes": 0,
                        "retry_days": 0,
                        "max_attempts": 1,
                        "respect_dnd": True,
                        "owner_alert": False,
                        "template": "Checking in on your quote.",
                    }
                },
                separators=(",", ":"),
            ),
        )

        payload = {
            "id": "evt_quote_002",
            "event": {"type": "quote:created"},
            "client": {
                "id": "client_active_123",
                "name": "Avery Active",
                "phone": "+15554445555",
            },
            "quote": {"id": "quote_active_123"},
        }

        response = self.client.post(
            f"/webhooks/sng/warren_crm_events_test/{self.sng_secret}",
            json=payload,
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        mock_send_reply.assert_not_called()

        with self.app.app_context():
            actions = self.db.get_crm_event_actions(self.brand_id, limit=10)
            threads = self.db.get_lead_threads(self.brand_id, limit=10)

        self.assertEqual(actions[0]["rule_key"], "quote_not_signed_up")
        self.assertEqual(actions[0]["status"], "resolved")
        self.assertEqual(len(threads), 0)
        self.assertTrue(mock_policy.called)


if __name__ == "__main__":
    unittest.main()
