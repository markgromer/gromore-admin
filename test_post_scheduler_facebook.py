import os
import json
import io
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-post-scheduler-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app


class _FakeChatResponse:
    def __init__(self, content):
        self.choices = [type("Choice", (), {"message": type("Message", (), {"content": content})()})()]


class FacebookPostSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"post-scheduler-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            db = self.app.db
            self.brand_id = db.create_brand({
                "slug": f"post-scheduler-{uuid.uuid4().hex[:8]}",
                "display_name": "Scheduler Test Brand",
                "industry": "plumbing",
                "primary_services": "Drain cleaning, water heater repair",
                "service_area": "Columbus, OH",
            })
            self.user_id = db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )
            db.update_brand_text_field(self.brand_id, "openai_api_key", "sk-test-openai")
            db.update_brand_api_field(self.brand_id, "facebook_page_id", "123456789")
            db.upsert_connection(
                self.brand_id,
                "meta",
                {
                    "access_token": "meta-user-token",
                    "token_expiry": "2099-01-01T00:00:00",
                    "scopes": "pages_manage_posts,pages_show_list",
                    "account_id": "12345",
                    "account_name": "Test Ad Account",
                },
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Scheduler Test Brand"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    @patch("openai.OpenAI")
    def test_generate_facebook_post_returns_ai_draft(self, mock_openai):
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            '{"message":"Here is a testimonial-style post about a customer win in Columbus.","image_hint":"Tech with a happy customer after the job","link_url":"https://example.com"}'
        )
        mock_openai.return_value = mock_client

        response = self.client.post(
            "/client/post-scheduler/generate",
            json={"post_type": "testimonial", "brief": "Talk about weekend emergency calls"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["post_type"], "testimonial")
        self.assertEqual(payload["post_type_label"], "Testimonial Post")
        self.assertIn("customer win", payload["message"])
        self.assertIn("happy customer", payload["image_hint"])

    @patch("openai.OpenAI")
    def test_generate_facebook_calendar_returns_typed_posts(self, mock_openai):
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            json.dumps(
                {
                    "posts": [
                        {
                            "post_type": "value",
                            "message": "A practical spring plumbing tip for Columbus homeowners.",
                            "image_hint": "Technician pointing at a shutoff valve",
                            "link_url": "https://example.com/tips",
                        },
                        {
                            "post_type": "faq",
                            "message": "One common question we hear about slow drains and how we handle it.",
                            "image_hint": "Sink drain close-up",
                            "link_url": "",
                        },
                        {
                            "post_type": "team_intro",
                            "message": "Meet one of the team members customers see on water heater calls.",
                            "image_hint": "Technician smiling next to service van",
                            "link_url": "",
                        },
                        {
                            "post_type": "special_offer",
                            "message": "A specific seasonal inspection offer with a clear reason to book now.",
                            "image_hint": "Service checklist on clipboard",
                            "link_url": "https://example.com/offer",
                        },
                        {
                            "post_type": "community_spotlight",
                            "message": "A local community spotlight post tied to a nearby neighborhood.",
                            "image_hint": "Neighborhood street and service van",
                            "link_url": "",
                        },
                        {
                            "post_type": "seasonal_reminder",
                            "message": "A seasonal reminder about checking outdoor plumbing before the weather shifts.",
                            "image_hint": "Outdoor faucet prep",
                            "link_url": "",
                        },
                    ]
                }
            )
        )
        mock_openai.return_value = mock_client

        response = self.client.post(
            "/client/post-scheduler/generate-calendar",
            json={
                "content_mix": "trust_and_proof",
                "weeks": 2,
                "posts_per_week": 3,
                "start_date": (datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat(),
                "post_types": ["value", "faq", "team_intro", "special_offer", "community_spotlight", "seasonal_reminder"],
                "brief": "Focus on spring service demand and practical homeowner education.",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["content_mix"], "trust_and_proof")
        self.assertEqual(payload["content_mix_label"], "Trust And Proof")
        self.assertEqual(payload["total_posts"], 6)
        self.assertEqual(len(payload["posts"]), 6)
        self.assertEqual(payload["posts"][1]["post_type"], "faq")
        self.assertEqual(payload["posts"][2]["post_type"], "team_intro")
        self.assertTrue(all(post["scheduled_at"] for post in payload["posts"]))
        self.assertIn("example.com", payload["posts"][0]["link_url"])

    @patch("webapp.api_bridge._get_page_access_token", return_value="page-token")
    @patch("webapp.api_bridge._get_meta_token", return_value="meta-user-token")
    @patch("requests.post")
    def test_schedule_post_persists_post_type(self, mock_post, _mock_meta_token, _mock_page_token):
        mock_post.return_value = Mock(
            status_code=200,
            json=lambda: {"id": "fb-post-123"},
            text="",
        )

        response = self.client.post(
            "/client/post-scheduler/schedule",
            json={
                "message": "A testimonial post about how we fixed a leak fast.",
                "scheduled_at": (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S"),
                "post_type": "testimonial",
                "link_url": "https://example.com/testimonial",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["post_type"], "testimonial")

        with self.app.app_context():
            posts = self.app.db.get_scheduled_posts(self.brand_id)

        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["post_type"], "testimonial")
        self.assertEqual(posts[0]["status"], "scheduled")

    @patch("webapp.google_drive.upload_file")
    def test_drive_upload_returns_scheduler_ready_urls(self, mock_upload_file):
        mock_upload_file.return_value = {
            "id": "drive-file-123",
            "name": "promo.jpg",
            "webViewLink": "https://drive.google.com/file/d/drive-file-123/view",
        }

        with self.app.app_context():
            db = self.app.db
            db.update_brand_text_field(self.brand_id, "google_drive_folder_id", "https://drive.google.com/drive/folders/root-folder-123")
            db.upsert_connection(
                self.brand_id,
                "google",
                {
                    "access_token": "google-access-token",
                    "refresh_token": "google-refresh-token",
                    "token_expiry": "2099-01-01T00:00:00",
                    "scopes": "openid email profile https://www.googleapis.com/auth/drive.file",
                    "account_id": "google-account-1",
                    "account_name": "Test Google",
                },
            )

        response = self.client.post(
            "/client/api/drive/upload",
            data={
                "subfolder": "Images",
                "file": (io.BytesIO(b"fake-image-bytes"), "promo.jpg"),
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["file"]["id"], "drive-file-123")
        self.assertEqual(payload["file"]["download_url"], "/client/api/drive/download/drive-file-123")
        self.assertEqual(payload["file"]["thumbnail_url"], "/client/api/drive/thumbnail/drive-file-123")


if __name__ == "__main__":
    unittest.main()