import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-feedback-ai-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from webapp.app import create_app


class _FakeChatResponse:
    def __init__(self, content):
        self.choices = [type("Choice", (), {"message": type("Message", (), {"content": content})()})()]


class FeedbackAiWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"feedback-ai-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"
        os.environ["OPENAI_API_KEY"] = "test-openai-key"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.admin_id = self.app.db.create_user(f"admin-{uuid.uuid4().hex[:8]}@example.com", "Password123", "Admin User")
            self.brand_id = self.app.db.create_brand({
                "slug": f"feedback-ai-{uuid.uuid4().hex[:8]}",
                "display_name": "Feedback AI Brand",
            })
            self.client_user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            self.app.db.create_beta_feedback(self.brand_id, self.client_user_id, "feature_request", 5, "Please add a faster way to summarize feedback.", "/client/feedback")
            self.app.db.create_beta_feedback(self.brand_id, self.client_user_id, "bug", 2, "The feedback page feels slow when the list gets large.", "/client/feedback")
            created_feedback = self.app.db.get_beta_feedback(limit=10)
            self.feedback_ids = [item["id"] for item in created_feedback]
            self.feedback_ids.sort()

        with self.client.session_transaction() as session:
            session["user_id"] = self.admin_id
            session["user_name"] = "Admin User"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL", "OPENAI_API_KEY"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    @patch("openai.OpenAI")
    def test_ai_review_generates_run_and_reply_drafts(self, mock_openai):
        payload = {
            "summary": {
                "executive_summary": "Customers want faster feedback triage and cleaner admin handling.",
                "counts": {
                    "total_feedback": 2,
                    "bugs": 1,
                    "feature_requests": 1,
                    "ui_ux": 0,
                    "likes": 0,
                    "dislikes": 0,
                    "general": 0,
                },
                "top_themes": [
                    {
                        "title": "Feedback triage speed",
                        "category": "feature_request",
                        "frequency": 1,
                        "why_it_matters": "Admins are spending too long reading repetitive notes.",
                        "evidence_ids": [self.feedback_ids[0]],
                    }
                ],
                "priority_recommendations": [
                    {"title": "Add AI summaries", "reason": "Repeated asks for faster triage.", "urgency": "high"}
                ],
            },
            "dev_plan": {
                "title": "Feedback AI rollout",
                "objective": "Turn recurring feedback into actionable internal plans and reply drafts.",
                "likely_areas": ["webapp/app.py", "webapp/database.py"],
                "implementation_steps": ["Add AI summary storage", "Render draft replies in admin UI"],
                "qa_checks": ["Confirm drafts save correctly", "Confirm send is one-time"],
                "rollout_order": ["Backend", "UI", "Email sending"],
                "customer_comms": ["Acknowledge the request", "Do not promise dates"],
            },
            "reply_drafts": [
                {
                    "feedback_id": self.feedback_ids[0],
                    "reply_subject": "Thanks for the feedback",
                    "reply_draft": "Thanks for flagging this. We agree the feedback workflow needs to move faster, and we are reviewing improvements now.",
                    "internal_note": "Good candidate for AI summary work.",
                    "recommended_status": "reviewed",
                    "confidence": 0.92,
                    "needs_manual_review": False,
                },
                {
                    "feedback_id": self.feedback_ids[1],
                    "reply_subject": "Thanks for reporting this issue",
                    "reply_draft": "Thanks for reporting the slowdown. We are reviewing the feedback page performance and will use this note in the fix plan.",
                    "internal_note": "Check query performance on feedback list.",
                    "recommended_status": "reviewed",
                    "confidence": 0.88,
                    "needs_manual_review": False,
                },
            ],
        }
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse(json.dumps(payload))

        response = self.client.post(
            "/beta/feedback/ai/generate",
            data={"scope": "new"},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            run = self.app.db.get_latest_feedback_ai_run()
            drafts = self.app.db.get_feedback_ai_drafts(self.feedback_ids)

        self.assertIsNotNone(run)
        self.assertEqual(run["summary"]["counts"]["total_feedback"], 2)
        self.assertEqual(run["dev_plan"]["title"], "Feedback AI rollout")
        self.assertEqual(len(drafts), 2)
        self.assertIn("workflow needs to move faster", drafts[0]["reply_draft"] + drafts[1]["reply_draft"])

    @patch("openai.OpenAI")
    def test_ai_review_ajax_returns_json_result(self, mock_openai):
        payload = {
            "summary": {
                "executive_summary": "Recent feedback points to faster triage needs.",
                "counts": {
                    "total_feedback": 2,
                    "bugs": 1,
                    "feature_requests": 1,
                    "ui_ux": 0,
                    "likes": 0,
                    "dislikes": 0,
                    "general": 0,
                },
                "top_themes": [],
                "priority_recommendations": [],
            },
            "dev_plan": {
                "title": "Feedback AI rollout",
                "objective": "Speed up beta feedback handling.",
                "likely_areas": ["webapp/app.py"],
                "implementation_steps": ["Return inline status for review actions"],
                "qa_checks": ["Verify success and error states render"],
                "rollout_order": ["Backend", "Frontend"],
                "customer_comms": ["Acknowledge input promptly"],
            },
            "reply_drafts": [
                {
                    "feedback_id": self.feedback_ids[0],
                    "reply_subject": "Thanks for the feedback",
                    "reply_draft": "We are reviewing ways to speed this up.",
                    "internal_note": "Handle with AI workflow.",
                    "recommended_status": "reviewed",
                    "confidence": 0.9,
                    "needs_manual_review": False,
                },
                {
                    "feedback_id": self.feedback_ids[1],
                    "reply_subject": "Thanks for reporting this issue",
                    "reply_draft": "We are checking the slowdown now.",
                    "internal_note": "Review performance path.",
                    "recommended_status": "reviewed",
                    "confidence": 0.85,
                    "needs_manual_review": False,
                },
            ],
        }
        mock_client = mock_openai.return_value
        mock_client.chat.completions.create.return_value = _FakeChatResponse(json.dumps(payload))

        response = self.client.post(
            "/beta/feedback/ai/generate",
            data={"scope": "new"},
            headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["item_count"], 2)
        self.assertEqual(body["draft_count"], 2)
        self.assertIn("#tab-feedback", body["redirect_url"])

    @patch("webapp.email_sender.send_simple_email")
    def test_send_draft_reply_uses_client_user_email_and_marks_sent(self, mock_send_simple_email):
        with self.app.app_context():
            self.app.db.save_feedback_ai_draft({
                "feedback_id": self.feedback_ids[0],
                "reply_subject": "Thanks for the feedback",
                "reply_draft": "We reviewed your note and added it to the active improvement queue.",
                "internal_note": "Route to AI summary work.",
                "recommended_status": "reviewed",
                "confidence": 0.95,
                "needs_manual_review": False,
            })

        response = self.client.post(
            f"/beta/feedback/{self.feedback_ids[0]}/draft/send",
            data={},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(mock_send_simple_email.call_count, 1)
        args = mock_send_simple_email.call_args[0]
        self.assertIn("owner-", args[1])
        self.assertEqual(args[2], "Thanks for the feedback")

        with self.app.app_context():
            draft = self.app.db.get_feedback_ai_draft(self.feedback_ids[0])
            feedback = next(item for item in self.app.db.get_beta_feedback(limit=10) if item["id"] == self.feedback_ids[0])

        self.assertTrue(bool(draft["sent_at"]))
        self.assertEqual(feedback["status"], "reviewed")
        self.assertIn("active improvement queue", feedback["admin_response"])


if __name__ == "__main__":
    unittest.main()