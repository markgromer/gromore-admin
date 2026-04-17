"""
Tests for the Warren Brain system:
  - Pipeline engine (stages, auto-advance, metrics)
  - Webhook endpoints (Quo SMS, Meta leadgen, Meta Messenger)
  - Inbox routes (thread list, messages, reply, stage change, warren draft)
  - Nurture engine (stale thread detection, ghost detection)
"""
import json
import os
import re
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
            self.sng_secret = self.db.ensure_brand_sng_webhook_secret(self.brand_id)

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

    def test_sng_webhook_logs_event(self):
        payload = {
            "id": "evt_sng_001",
            "event": {"type": "client:client_payment_declined"},
            "client": {
                "id": "client_123",
                "name": "Taylor Prospect",
                "email": "taylor@example.com",
                "phone": "+15551234567",
            },
            "payment": {"id": "pay_123", "status": "failed"},
        }

        resp = self.client.post(
            f"/webhooks/sng/warren_webhook_test/{self.sng_secret}",
            json=payload,
            content_type="application/json",
        )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["event_type"], "client:client_payment_declined")
        self.assertEqual(data["event_id"], "evt_sng_001")

        with self.app.app_context():
            events = self.db.get_sng_webhook_events(self.brand_id, limit=5)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["external_event_id"], "evt_sng_001")
            self.assertEqual(events[0]["event_type"], "client:client_payment_declined")
            self.assertIn("Taylor Prospect", events[0]["detail"])

    def test_sng_webhook_rejects_invalid_secret(self):
        resp = self.client.post(
            "/webhooks/sng/warren_webhook_test/not_the_secret",
            json={"id": "evt_bad", "event": {"type": "client:invoice_finalized"}},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 401)

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

    def test_meta_leadgen_verify_uses_global_verify_token(self):
        """Leadgen verification uses the platform-level verify token."""
        with self.app.app_context():
            self.db.save_setting("meta_webhook_verify_token", "leadgen_verify_789")

        resp = self.client.get(
            "/webhooks/meta/leadgen",
            query_string={
                "hub.mode": "subscribe",
                "hub.verify_token": "leadgen_verify_789",
                "hub.challenge": "challenge_leadgen",
            },
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data.decode(), "challenge_leadgen")

    def test_meta_messenger_verify_rejects_brand_only_token(self):
        """Messenger verification should not depend on a brand-level token."""
        with self.app.app_context():
            self.db.update_brand_text_field(self.brand_id, "sales_bot_meta_webhook_secret", "brand_verify_456")

        resp = self.client.get(
            "/webhooks/meta/messenger",
            query_string={
                "hub.mode": "subscribe",
                "hub.verify_token": "brand_verify_456",
                "hub.challenge": "challenge_brand_token",
            },
        )
        self.assertEqual(resp.status_code, 403)

    def test_generic_lead_webhook_creates_thread(self):
        with self.app.app_context():
            self.db.update_brand_number_field(self.brand_id, "sales_bot_enabled", 0)
            self.db.update_brand_text_field(self.brand_id, "sales_bot_incoming_webhook_secret", "incoming-secret-123")

        resp = self.client.post(
            "/webhooks/leads/warren_webhook_test",
            json={
                "name": "Jordan Prospect",
                "email": "Jordan@example.com",
                "phone": "+15550001111",
                "message": "Need a fast quote for a drain issue.",
                "source": "website_contact_form",
                "submission_id": "lead-submission-001",
            },
            headers={"X-GroMore-Webhook-Secret": "incoming-secret-123"},
        )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])

        with self.app.app_context():
            thread = self.db.get_lead_thread(data["thread_id"])
            self.assertEqual(thread["channel"], "lead_form")
            self.assertEqual(thread["lead_name"], "Jordan Prospect")
            self.assertEqual(thread["lead_email"], "jordan@example.com")
            self.assertEqual(thread["source"], "incoming_webhook:website_contact_form")

            messages = self.db.get_lead_messages(data["thread_id"])
            self.assertGreaterEqual(len(messages), 1)
            self.assertTrue(any("Inbound Lead Submission" in message["content"] for message in messages))

    def test_generic_lead_webhook_rejects_invalid_secret(self):
        with self.app.app_context():
            self.db.update_brand_text_field(self.brand_id, "sales_bot_incoming_webhook_secret", "expected-secret")

        resp = self.client.post(
            "/webhooks/leads/warren_webhook_test",
            json={"name": "Blocked Lead", "message": "hello"},
            headers={"X-GroMore-Webhook-Secret": "wrong-secret"},
        )
        self.assertEqual(resp.status_code, 401)


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

            # Enable the warren_inbox feature flag for tests
            self.db.update_feature_flag("warren_inbox", "all", True)

            # Create a test thread
            self.thread_id = self.db.create_lead_thread(self.brand_id, {
                "lead_name": "Test Lead",
                "lead_email": "lead@test.com",
                "lead_phone": "+15551112222",
                "channel": "sms",
                "source": "openphone",
                "status": "quoted",
            })
            self.db.add_lead_message(
                self.thread_id, "inbound", "lead", "Hi, I need help. We have 3 dogs, but the price seems high and I need to check with my wife.",
                channel="sms",
                metadata={"fields": {"dogs": "3", "service_type": "Weekly cleanup"}},
            )
            self.db.add_lead_message(
                self.thread_id, "outbound", "assistant", "We can likely handle this for between $180 and $220.",
                channel="sms",
            )
            self.db.upsert_lead_quote(
                self.brand_id,
                self.thread_id,
                status="sent",
                amount_low=180,
                amount_high=220,
                summary="Dog cleanup and weekly maintenance",
                follow_up_text="Let us know if you want to book.",
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
        self.assertIn(b"Active Lead Profiles", resp.data)
        self.assertIn(b"Test Lead", resp.data)
        self.assertIn(b"closeability", resp.data.lower())
        self.assertIn(b"Waiting on quote approval", resp.data)
        self.assertIn(b"$180-$220", resp.data)
        self.assertIn(b"Budget", resp.data)

    def test_inbox_thread_detail(self):
        self._login()
        resp = self.client.get(f"/client/inbox/thread/{self.thread_id}")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["thread"]["lead_name"], "Test Lead")
        self.assertEqual(len(data["messages"]), 2)
        self.assertEqual(data["profile"]["dog_count"], 3)
        self.assertEqual(data["profile"]["quoted_amount"], "$180-$220")
        self.assertIn("budget", data["profile"]["objections"])
        self.assertIn("needs partner approval", data["profile"]["objections"])
        self.assertEqual(data["profile"]["waiting_on"], "Waiting on quote approval")
        self.assertGreaterEqual(data["profile"]["closeability_pct"], 40)
        self.assertTrue(len(data["profile"]["closeability_drivers"]) >= 1)

    def test_inbox_profile_override_can_be_saved(self):
        self._login()
        resp = self.client.post(
            f"/client/inbox/thread/{self.thread_id}/profile",
            json={
                "lead_name": "Updated Lead",
                "lead_phone": "+15559990000",
                "lead_email": "updated@example.com",
                "dog_count": 5,
                "closeability_pct": 83,
                "waiting_on": "Waiting on spouse approval",
                "objections_text": "budget, timing",
                "profile_notes": "Asked for a Friday follow-up call.",
            },
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["profile"]["lead_name"], "Updated Lead")
        self.assertEqual(data["profile"]["dog_count"], 5)
        self.assertEqual(data["profile"]["closeability_pct"], 83)
        self.assertEqual(data["profile"]["waiting_on"], "Waiting on spouse approval")
        self.assertEqual(data["profile"]["objections"], ["budget", "timing"])
        self.assertEqual(data["profile"]["profile_notes"], "Asked for a Friday follow-up call.")

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

    def test_conversation_context_includes_image_parts(self):
        from webapp.warren_brain import _build_conversation_context

        messages = [{
            "role": "lead",
            "content": "Can you tell how bad this looks?",
            "metadata_json": json.dumps({"image_urls": ["https://example.com/test-photo.jpg"]}),
        }]
        context = _build_conversation_context(messages)
        self.assertEqual(len(context), 1)
        self.assertEqual(context[0]["role"], "user")
        self.assertIsInstance(context[0]["content"], list)
        self.assertEqual(context[0]["content"][0]["type"], "text")
        self.assertEqual(context[0]["content"][1]["type"], "image_url")
        self.assertEqual(context[0]["content"][1]["image_url"]["url"], "https://example.com/test-photo.jpg")


class WarrenNurtureCadenceTests(unittest.TestCase):
    """Test per-brand nurture cadence and DND logic."""

    def test_brand_nurture_rules_uses_settings(self):
        from webapp.warren_nurture import _brand_nurture_rules

        brand = {
            "sales_bot_nurture_hot_hours": 1,
            "sales_bot_nurture_hot_max": 5,
            "sales_bot_nurture_warm_hours": 12,
            "sales_bot_nurture_warm_max": 4,
            "sales_bot_nurture_cold_hours": 96,
            "sales_bot_nurture_cold_max": 1,
        }
        rules = _brand_nurture_rules(brand)
        self.assertEqual(len(rules), 4)

        by_stage = {r["stage"]: r for r in rules}
        self.assertEqual(by_stage["new"]["hours_since_last"], 1.0)
        self.assertEqual(by_stage["new"]["max_attempts"], 5)
        self.assertEqual(by_stage["engaged"]["hours_since_last"], 1.0)
        self.assertEqual(by_stage["quoted"]["hours_since_last"], 12.0)
        self.assertEqual(by_stage["quoted"]["max_attempts"], 4)
        self.assertEqual(by_stage["qualified"]["hours_since_last"], 96.0)

    def test_brand_nurture_rules_uses_defaults(self):
        from webapp.warren_nurture import _brand_nurture_rules

        rules = _brand_nurture_rules({})
        by_stage = {r["stage"]: r for r in rules}
        self.assertEqual(by_stage["new"]["hours_since_last"], 2.0)
        self.assertEqual(by_stage["new"]["max_attempts"], 3)
        self.assertEqual(by_stage["quoted"]["hours_since_last"], 24.0)

    def test_detect_contextual_nudge_plan_for_spouse_check(self):
        from webapp.warren_nurture import _detect_contextual_nudge_plan

        thread = {"status": "quoted"}
        messages = [
            {"role": "lead", "direction": "inbound", "content": "Looks good."},
            {"role": "assistant", "direction": "outbound", "content": "Want me to lock it in?"},
            {"role": "lead", "direction": "inbound", "content": "I need to check with my wife tonight first."},
        ]

        plan = _detect_contextual_nudge_plan(thread, messages)
        self.assertIsNotNone(plan)
        self.assertEqual(plan["event"], "nurture_spouse_followup")
        self.assertEqual(plan["wait_hours"], 4.0)
        self.assertIn("spouse", plan["prompt"].lower())

    def test_detect_contextual_nudge_plan_for_mid_conversation_ghost(self):
        from webapp.warren_nurture import _detect_contextual_nudge_plan

        thread = {"status": "engaged"}
        messages = [
            {"role": "lead", "direction": "inbound", "content": "Hey, do you service my area?"},
            {"role": "assistant", "direction": "outbound", "content": "Yes, we do. What do you need help with?"},
            {"role": "lead", "direction": "inbound", "content": "Dog poop cleanup for two dogs."},
            {"role": "assistant", "direction": "outbound", "content": "We can help with that. Weekly is our most popular option."},
        ]

        plan = _detect_contextual_nudge_plan(thread, messages)
        self.assertIsNotNone(plan)
        self.assertEqual(plan["event"], "nurture_soft_close")
        self.assertEqual(plan["wait_hours"], 0.25)
        self.assertIn("close it out", plan["prompt"].lower())

    def test_dnd_disabled_returns_false(self):
        from webapp.warren_nurture import _is_dnd
        self.assertFalse(_is_dnd({"sales_bot_dnd_enabled": 0}))
        self.assertFalse(_is_dnd({}))

    def test_dnd_overnight_window(self):
        from webapp.warren_nurture import _is_dnd
        from unittest.mock import patch
        from datetime import datetime

        brand = {
            "sales_bot_dnd_enabled": 1,
            "sales_bot_dnd_start": "21:00",
            "sales_bot_dnd_end": "08:00",
            "sales_bot_dnd_timezone": "America/New_York",
            "sales_bot_dnd_weekends": 0,
        }

        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo

        tz = ZoneInfo("America/New_York")

        # 23:00 ET - should be in DND
        with patch("webapp.warren_nurture.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 7, 23, 0, tzinfo=tz)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            self.assertTrue(_is_dnd(brand))

        # 06:00 ET - should be in DND
        with patch("webapp.warren_nurture.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 7, 6, 0, tzinfo=tz)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            self.assertTrue(_is_dnd(brand))

        # 10:00 ET - should NOT be in DND
        with patch("webapp.warren_nurture.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 7, 10, 0, tzinfo=tz)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            self.assertFalse(_is_dnd(brand))

    def test_dnd_weekends(self):
        from webapp.warren_nurture import _is_dnd
        from unittest.mock import patch
        from datetime import datetime

        brand = {
            "sales_bot_dnd_enabled": 1,
            "sales_bot_dnd_start": "21:00",
            "sales_bot_dnd_end": "08:00",
            "sales_bot_dnd_timezone": "America/New_York",
            "sales_bot_dnd_weekends": 1,
        }

        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo

        tz = ZoneInfo("America/New_York")

        # Saturday 10:00 ET - outside time window but weekend DND is on
        with patch("webapp.warren_nurture.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 11, 10, 0, tzinfo=tz)  # Saturday
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            self.assertTrue(_is_dnd(brand))

        # Monday 10:00 ET - should NOT be in DND
        with patch("webapp.warren_nurture.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 6, 10, 0, tzinfo=tz)  # Monday
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            self.assertFalse(_is_dnd(brand))


class WarrenA2PConsentTests(unittest.TestCase):
    """Test A2P opt-out/opt-in consent tracking."""

    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.db = self.app.db

        with self.app.app_context():
            conn = self.db._conn()
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE slug = 'warren_a2p_test'"
            ).fetchone()
            if brand_row:
                self.brand_id = brand_row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO brands (slug, display_name, industry, sales_bot_enabled) VALUES (?, ?, ?, ?)",
                    ("warren_a2p_test", "A2P Test Co", "plumbing", 1),
                )
                self.brand_id = cur.lastrowid
            conn.commit()

            # Clean up old consent records
            conn.execute("DELETE FROM sms_consent WHERE brand_id = ?", (self.brand_id,))
            conn.commit()
            conn.close()

    def test_no_consent_record_not_opted_out(self):
        with self.app.app_context():
            self.assertFalse(self.db.is_opted_out(self.brand_id, "+15551234567"))

    def test_opt_out_then_check(self):
        with self.app.app_context():
            self.db.record_opt_out(self.brand_id, "+15551234567", keyword="STOP")
            self.assertTrue(self.db.is_opted_out(self.brand_id, "+15551234567"))

    def test_opt_out_then_opt_in(self):
        with self.app.app_context():
            self.db.record_opt_out(self.brand_id, "+15559999999", keyword="STOP")
            self.assertTrue(self.db.is_opted_out(self.brand_id, "+15559999999"))

            self.db.record_opt_in(self.brand_id, "+15559999999", source="START")
            self.assertFalse(self.db.is_opted_out(self.brand_id, "+15559999999"))

    def test_opt_out_per_brand(self):
        with self.app.app_context():
            self.db.record_opt_out(self.brand_id, "+15550001111", keyword="STOP")
            self.assertTrue(self.db.is_opted_out(self.brand_id, "+15550001111"))
            # Different brand should not be opted out
            self.assertFalse(self.db.is_opted_out(self.brand_id + 9999, "+15550001111"))

    def test_get_opted_out_phones(self):
        with self.app.app_context():
            self.db.record_opt_out(self.brand_id, "+15551111111", keyword="STOP")
            self.db.record_opt_out(self.brand_id, "+15552222222", keyword="UNSUBSCRIBE")
            phones = self.db.get_opted_out_phones(self.brand_id)
            opted_numbers = [p["phone"] for p in phones]
            self.assertIn("+15551111111", opted_numbers)
            self.assertIn("+15552222222", opted_numbers)

    def test_consent_record_details(self):
        with self.app.app_context():
            self.db.record_opt_out(self.brand_id, "+15553333333", keyword="CANCEL")
            record = self.db.get_sms_consent(self.brand_id, "+15553333333")
            self.assertIsNotNone(record)
            self.assertEqual(record["status"], "opted_out")
            self.assertEqual(record["opted_out_keyword"], "CANCEL")


class WarrenWebhookSTOPTests(unittest.TestCase):
    """Test that STOP/START keywords are handled at the webhook level."""

    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.test_client = self.app.test_client()
        self.db = self.app.db

        with self.app.app_context():
            conn = self.db._conn()
            brand_row = conn.execute(
                "SELECT id FROM brands WHERE slug = 'warren_stop_test'"
            ).fetchone()
            if brand_row:
                self.brand_id = brand_row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO brands (slug, display_name, industry, sales_bot_enabled) VALUES (?, ?, ?, ?)",
                    ("warren_stop_test", "STOP Test Co", "plumbing", 1),
                )
                self.brand_id = cur.lastrowid
            conn.commit()

            # Clean up
            conn.execute("DELETE FROM sms_consent WHERE brand_id = ?", (self.brand_id,))
            conn.commit()
            conn.close()

    @patch("webapp.quo_sms.send_sms")
    def test_stop_keyword_opts_out(self, mock_send):
        mock_send.return_value = (True, {})
        with self.app.app_context():
            payload = {
                "type": "message.received",
                "data": {
                    "object": {
                        "id": "msg_stop1",
                        "from": "+15557778888",
                        "body": "STOP",
                        "direction": "incoming",
                    }
                },
            }
            resp = self.test_client.post(
                "/webhooks/quo/sms/warren_stop_test",
                data=json.dumps(payload),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(data.get("action"), "opted_out")

            # Verify consent record
            self.assertTrue(self.db.is_opted_out(self.brand_id, "+15557778888"))

    @patch("webapp.quo_sms.send_sms")
    def test_start_keyword_opts_in(self, mock_send):
        mock_send.return_value = (True, {})
        with self.app.app_context():
            # First opt out
            self.db.record_opt_out(self.brand_id, "+15556667777", keyword="STOP")
            self.assertTrue(self.db.is_opted_out(self.brand_id, "+15556667777"))

            # Then send START
            payload = {
                "type": "message.received",
                "data": {
                    "object": {
                        "id": "msg_start1",
                        "from": "+15556667777",
                        "body": "start",
                        "direction": "incoming",
                    }
                },
            }
            resp = self.test_client.post(
                "/webhooks/quo/sms/warren_stop_test",
                data=json.dumps(payload),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(data.get("action"), "opted_in")

            # Verify re-subscribed
            self.assertFalse(self.db.is_opted_out(self.brand_id, "+15556667777"))


class WarrenBrainPromptTests(unittest.TestCase):
    """Test that objection playbook and templates are wired into the system prompt."""

    def test_objection_playbook_in_prompt(self):
        from webapp.warren_brain import _build_system_prompt

        brand = {
            "display_name": "Test Co",
            "industry": "plumbing",
            "sales_bot_reply_tone": "friendly",
            "sales_bot_objection_playbook": "TOO EXPENSIVE: We offer financing options.",
        }
        prompt = _build_system_prompt(brand)
        self.assertIn("OBJECTION HANDLING", prompt)
        self.assertIn("TOO EXPENSIVE", prompt)

    def test_message_templates_in_prompt(self):
        from webapp.warren_brain import _build_system_prompt

        brand = {
            "display_name": "Test Co",
            "industry": "plumbing",
            "sales_bot_reply_tone": "friendly",
            "sales_bot_message_templates": "FIRST CONTACT: Hey, thanks for reaching out!",
        }
        prompt = _build_system_prompt(brand)
        self.assertIn("MESSAGE TEMPLATES", prompt)
        self.assertIn("FIRST CONTACT", prompt)

    def test_no_objection_no_section(self):
        from webapp.warren_brain import _build_system_prompt

        brand = {
            "display_name": "Test Co",
            "industry": "plumbing",
            "sales_bot_reply_tone": "friendly",
        }
        prompt = _build_system_prompt(brand)
        self.assertNotIn("OBJECTION HANDLING", prompt)
        self.assertNotIn("MESSAGE TEMPLATES", prompt)


if __name__ == "__main__":
    unittest.main()
