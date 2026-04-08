"""
Tests for the Warren Brain system:
  - Pipeline engine (stages, auto-advance, metrics)
  - Webhook endpoints (Quo SMS, Meta leadgen, Meta Messenger)
  - Inbox routes (thread list, messages, reply, stage change, warren draft)
  - Nurture engine (stale thread detection, ghost detection)
"""
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from webapp.app import create_app


class WarrenPipelineTests(unittest.TestCase):
    """Test pipeline stage logic."""

    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.db = self.app.db

        with self.app.app_context():
            conn = self.db._conn()
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE slug = 'warren_test_brand'"
            ).fetchone()
            if brand_row:
                self.brand_id = brand_row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO brands (slug, display_name, industry) VALUES (?, ?, ?)",
                    ("warren_test_brand", "Warren Test Co", "plumbing"),
                )
                self.brand_id = cur.lastrowid

            # Enable sales bot
            conn.execute(
                "UPDATE brands SET sales_bot_enabled = 1 WHERE id = ?",
                (self.brand_id,),
            )
            conn.commit()

            # Ensure beta tester access
            existing = conn.execute(
                "SELECT id FROM beta_testers WHERE brand_id = ?",
                (self.brand_id,),
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO beta_testers (brand_id, name, email, status) VALUES (?, ?, ?, ?)",
                    (self.brand_id, "Test", "test@warren.com", "activated"),
                )
                conn.commit()
            conn.close()

    def test_pipeline_stages_defined(self):
        from webapp.warren_pipeline import PIPELINE_STAGES, STAGE_INDEX
        self.assertEqual(len(PIPELINE_STAGES), 7)
        self.assertIn("new", PIPELINE_STAGES)
        self.assertIn("won", PIPELINE_STAGES)
        self.assertIn("lost", PIPELINE_STAGES)
        # Index order is correct
        self.assertTrue(STAGE_INDEX["won"] > STAGE_INDEX["new"])

    def test_can_advance_forward(self):
        from webapp.warren_pipeline import can_advance
        self.assertTrue(can_advance("new", "engaged"))
        self.assertTrue(can_advance("engaged", "quoted"))
        self.assertTrue(can_advance("quoted", "won"))

    def test_cannot_advance_backward(self):
        from webapp.warren_pipeline import can_advance
        self.assertFalse(can_advance("quoted", "new"))
        self.assertFalse(can_advance("won", "engaged"))

    def test_can_always_move_to_lost(self):
        from webapp.warren_pipeline import can_advance
        self.assertTrue(can_advance("new", "lost"))
        self.assertTrue(can_advance("quoted", "lost"))
        self.assertTrue(can_advance("booked", "lost"))

    def test_advance_stage_auto(self):
        from webapp.warren_pipeline import advance_stage

        with self.app.app_context():
            thread_id = self.db.create_lead_thread(self.brand_id, {
                "lead_name": "Pipeline Test",
                "lead_phone": "+15551234567",
                "channel": "sms",
            })

            # Should advance from new -> engaged
            new_stage, event_id = advance_stage(
                self.db, thread_id, self.brand_id, "warren_replied"
            )
            self.assertEqual(new_stage, "engaged")
            self.assertIsNotNone(event_id)

            # Verify thread was updated
            thread = self.db.get_lead_thread(thread_id)
            self.assertEqual(thread["status"], "engaged")

    def test_manual_stage_change(self):
        from webapp.warren_pipeline import manual_stage_change

        with self.app.app_context():
            thread_id = self.db.create_lead_thread(self.brand_id, {
                "lead_name": "Manual Test",
                "channel": "sms",
            })

            success, event_id = manual_stage_change(
                self.db, thread_id, self.brand_id, "qualified", changed_by="test_user"
            )
            self.assertTrue(success)

            thread = self.db.get_lead_thread(thread_id)
            self.assertEqual(thread["status"], "qualified")

    def test_pipeline_summary(self):
        from webapp.warren_pipeline import get_pipeline_summary

        with self.app.app_context():
            summary = get_pipeline_summary(self.db, self.brand_id)
            self.assertIn("new", summary)
            self.assertIn("won", summary)
            self.assertIn("lost", summary)

    def test_pipeline_metrics(self):
        from webapp.warren_pipeline import get_pipeline_metrics

        with self.app.app_context():
            metrics = get_pipeline_metrics(self.db, self.brand_id)
            self.assertIn("total_leads", metrics)
            self.assertIn("conversion_rate", metrics)
            self.assertIn("stage_counts", metrics)
            self.assertIn("channels", metrics)


class WarrenWebhookTests(unittest.TestCase):
    """Test webhook endpoints."""

    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()
        self.db = self.app.db

        with self.app.app_context():
            conn = self.db._conn()
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE slug = 'warren_webhook_test'"
            ).fetchone()
            if brand_row:
                self.brand_id = brand_row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO brands (slug, display_name, industry, sales_bot_enabled) VALUES (?, ?, ?, ?)",
                    ("warren_webhook_test", "Webhook Test Co", "plumbing", 1),
                )
                self.brand_id = cur.lastrowid

            conn.execute(
                "UPDATE brands SET sales_bot_enabled = 1, facebook_page_id = '123456789' WHERE id = ?",
                (self.brand_id,),
            )
            conn.commit()

            # Ensure beta tester
            existing = conn.execute(
                "SELECT id FROM beta_testers WHERE brand_id = ?",
                (self.brand_id,),
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO beta_testers (brand_id, name, email, status) VALUES (?, ?, ?, ?)",
                    (self.brand_id, "Webhook Test", "webhook@test.com", "activated"),
                )
                conn.commit()
            conn.close()

    @patch("webapp.warren_webhooks.threading.Thread")
    def test_quo_sms_webhook_creates_thread(self, mock_thread):
        """Inbound SMS creates a thread and message."""
        mock_thread.return_value = MagicMock()

        resp = self.client.post(
            "/webhooks/quo/sms/warren_webhook_test",
            json={
                "type": "message.received",
                "data": {
                    "object": {
                        "id": "msg_123",
                        "conversationId": "conv_456",
                        "from": "+15551234567",
                        "to": "+15559876543",
                        "body": "Hi, I need a quote for lawn service",
                        "direction": "incoming",
                    }
                }
            },
            content_type="application/json",
        )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertIn("thread_id", data)

        # Verify thread was created
        with self.app.app_context():
            thread = self.db.get_lead_thread(data["thread_id"])
            self.assertIsNotNone(thread)
            self.assertEqual(thread["lead_phone"], "+15551234567")
            self.assertEqual(thread["channel"], "sms")

            # Verify message was stored
            messages = self.db.get_lead_messages(data["thread_id"])
            self.assertGreaterEqual(len(messages), 1)
            inbound_msgs = [m for m in messages if m["direction"] == "inbound"]
            self.assertGreaterEqual(len(inbound_msgs), 1)
            self.assertTrue(any("lawn service" in m["content"] for m in inbound_msgs))

    def test_quo_webhook_unknown_brand(self):
        resp = self.client.post(
            "/webhooks/quo/sms/nonexistent_brand_xyz",
            json={"type": "message.received", "data": {"object": {"body": "test", "from": "+1555"}}},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 404)

    def test_quo_webhook_skips_non_message_events(self):
        resp = self.client.post(
            "/webhooks/quo/sms/warren_webhook_test",
            json={"type": "call.completed", "data": {}},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data.get("skipped"), "call.completed")

    @patch("webapp.warren_webhooks.threading.Thread")
    def test_meta_messenger_webhook(self, mock_thread):
        """Inbound Messenger message creates a thread."""
        mock_thread.return_value = MagicMock()

        with self.app.app_context():
            self.db.update_brand_number_field(self.brand_id, "sales_bot_messenger_enabled", 1)

        resp = self.client.post(
            "/webhooks/meta/messenger",
            json={
                "object": "page",
                "entry": [{
                    "id": "123456789",
                    "time": 1234567890,
                    "messaging": [{
                        "sender": {"id": "user_psid_001"},
                        "recipient": {"id": "123456789"},
                        "timestamp": 1234567890,
                        "message": {
                            "mid": "msg_mid_001",
                            "text": "Do you service my area?"
                        }
                    }]
                }]
            },
            content_type="application/json",
        )

        self.assertEqual(resp.status_code, 200)

        # Verify thread was created
        with self.app.app_context():
            threads = self.db.get_lead_threads(self.brand_id)
            messenger_threads = [t for t in threads if t["channel"] == "messenger"]
            self.assertTrue(len(messenger_threads) >= 1)

    def test_meta_messenger_verify(self):
        """Meta webhook GET verification challenge."""
        with self.app.app_context():
            self.db.save_setting("meta_webhook_verify_token", "test_verify_123")

        resp = self.client.get(
            "/webhooks/meta/messenger",
            query_string={
                "hub.mode": "subscribe",
                "hub.verify_token": "test_verify_123",
                "hub.challenge": "challenge_12345",
            },
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data.decode(), "challenge_12345")


class WarrenInboxTests(unittest.TestCase):
    """Test inbox UI routes."""

    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.app.config["WTF_CSRF_ENABLED"] = False
        self.client = self.app.test_client()
        self.db = self.app.db

        with self.app.app_context():
            conn = self.db._conn()
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE slug = 'warren_inbox_test'"
            ).fetchone()
            if brand_row:
                self.brand_id = brand_row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO brands (slug, display_name, industry, sales_bot_enabled) VALUES (?, ?, ?, ?)",
                    ("warren_inbox_test", "Inbox Test Co", "plumbing", 1),
                )
                self.brand_id = cur.lastrowid

            conn.execute(
                "UPDATE brands SET sales_bot_enabled = 1 WHERE id = ?",
                (self.brand_id,),
            )
            conn.commit()

            # Ensure beta tester
            existing = conn.execute(
                "SELECT id FROM beta_testers WHERE brand_id = ?",
                (self.brand_id,),
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO beta_testers (brand_id, name, email, status) VALUES (?, ?, ?, ?)",
                    (self.brand_id, "Inbox Test", "inbox@test.com", "activated"),
                )
                conn.commit()

            # Create client user
            from werkzeug.security import generate_password_hash
            cu_row = conn.execute(
                "SELECT id FROM client_users WHERE email = 'inbox@test.com'"
            ).fetchone()
            if not cu_row:
                conn.execute(
                    "INSERT INTO client_users (brand_id, display_name, email, password_hash) VALUES (?, ?, ?, ?)",
                    (self.brand_id, "Inbox Tester", "inbox@test.com", generate_password_hash("test123")),
                )
            conn.commit()
            conn.close()

            # Create a test thread
            self.thread_id = self.db.create_lead_thread(self.brand_id, {
                "lead_name": "Test Lead",
                "lead_phone": "+15551112222",
                "channel": "sms",
                "source": "openphone",
            })
            self.db.add_lead_message(
                self.thread_id, "inbound", "lead", "Hi, I need help",
                channel="sms",
            )

    def _login(self):
        return self.client.post("/client/login", data={
            "email": "inbox@test.com",
            "password": "test123",
        }, follow_redirects=True)

    def test_inbox_page_loads(self):
        self._login()
        resp = self.client.get("/client/inbox")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Pipeline", resp.data)
        self.assertIn(b"Test Lead", resp.data)

    def test_inbox_thread_detail(self):
        self._login()
        resp = self.client.get(f"/client/inbox/thread/{self.thread_id}")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["thread"]["lead_name"], "Test Lead")
        self.assertEqual(len(data["messages"]), 1)
        self.assertEqual(data["messages"][0]["content"], "Hi, I need help")

    def test_inbox_stage_change(self):
        self._login()
        resp = self.client.post(
            f"/client/inbox/thread/{self.thread_id}/stage",
            json={"stage": "engaged"},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])

        with self.app.app_context():
            thread = self.db.get_lead_thread(self.thread_id)
            self.assertEqual(thread["status"], "engaged")


class WarrenBrainTests(unittest.TestCase):
    """Test the Warren brain system prompt builder."""

    def test_system_prompt_builds(self):
        from webapp.warren_brain import _build_system_prompt

        brand = {
            "display_name": "Ace Plumbing",
            "industry": "plumbing",
            "service_area": "Portland, OR",
            "primary_services": "drain cleaning, pipe repair",
            "sales_bot_reply_tone": "friendly and professional",
            "sales_bot_service_menu": "Drain cleaning: $150-250\nPipe repair: $200-500",
            "sales_bot_pricing_notes": "Premium pricing, no discounts",
            "sales_bot_guardrails": "Never promise same-day",
            "sales_bot_example_language": "Hey, thanks for reaching out!",
            "sales_bot_disallowed_language": "No em dashes",
            "sales_bot_handoff_rules": "Complaints go to human",
            "sales_bot_quote_mode": "hybrid",
            "crm_avg_service_price": 275,
            "sales_bot_business_hours": "Mon-Fri 8am-5pm",
        }

        prompt = _build_system_prompt(brand)
        self.assertIn("Ace Plumbing", prompt)
        self.assertIn("plumbing", prompt)
        self.assertIn("Portland, OR", prompt)
        self.assertIn("$150-250", prompt)
        self.assertIn("hybrid", prompt)
        self.assertIn("GUARDRAILS", prompt)
        self.assertIn("HUMAN HANDOFF", prompt)
        self.assertIn("EXAMPLE LANGUAGE", prompt)
        self.assertIn("NEVER SAY", prompt)

    def test_conversation_context_truncation(self):
        from webapp.warren_brain import _build_conversation_context

        messages = [{"role": "lead", "content": f"msg {i}"} for i in range(50)]
        context = _build_conversation_context(messages, max_messages=10)
        self.assertEqual(len(context), 10)


if __name__ == "__main__":
    unittest.main()
