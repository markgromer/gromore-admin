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
    def test_generate_facebook_post_prompt_includes_storytelling_controls(self, mock_openai):
        with self.app.app_context():
            db = self.app.db
            db.update_brand_text_field(self.brand_id, "facebook_storytelling_strategy", "Let people watch us grow the business in public.")
            db.update_brand_text_field(self.brand_id, "facebook_content_personality", "playful_funny")
            db.update_brand_text_field(self.brand_id, "facebook_cta_style", "subtle")
            db.update_brand_text_field(self.brand_id, "facebook_post_length", "story_time")
            db.update_brand_text_field(self.brand_id, "facebook_storytelling_guardrails", "No cheesy motivation. Keep the humor dry.")
            db.update_brand_text_field(
                self.brand_id,
                "facebook_recurring_characters",
                json.dumps(
                    [
                        {
                            "name": "Marty",
                            "role": "Dispatcher",
                            "description": "Keeps the schedule tight and the jokes dry.",
                            "voice": "Short, sharp, practical",
                            "json_profile": {"favorite_service": "Emergency dispatch", "camera_ready": True},
                        }
                    ]
                ),
            )

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            '{"message":"Marty gives a quick update on how the team is handling spring demand.","image_hint":"Dispatcher at the service board","link_url":""}'
        )
        mock_openai.return_value = mock_client

        response = self.client.post(
            "/client/post-scheduler/generate",
            json={"post_type": "character_spotlight", "brief": "Show the business momentum.", "character_name": "Marty"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

        prompt = mock_client.chat.completions.create.call_args.kwargs["messages"][1]["content"]
        self.assertIn("Organic storytelling controls", prompt)
        self.assertIn("Let people watch us grow the business in public.", prompt)
        self.assertIn("Playful And Funny", prompt)
        self.assertIn("Subtle", prompt)
        self.assertIn("Story Time", prompt)
        self.assertIn("Requested recurring character: Marty", prompt)
        self.assertIn("No cheesy motivation. Keep the humor dry.", prompt)
        self.assertIn("JSON Profile:", prompt)
        self.assertIn("favorite_service", prompt)
        self.assertIn("Keep the post between 220 and 320 words.", prompt)
        self.assertIn("Use intentional line breaks between the hook, the body, and the CTA when it helps clarity.", prompt)
        self.assertIn("Prefer this flow when it fits the idea: opening hook, supporting body, closing CTA.", prompt)
        self.assertIn("Do not invent fake statistics, awards, customer details, staff members, vehicles, or growth milestones.", prompt)
        self.assertIn("Do not mention staff members by name in automated posts.", prompt)

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
        calendar_prompt = mock_client.chat.completions.create.call_args.kwargs["messages"][1]["content"]
        self.assertIn("Do not invent fake reviews, fake names, fake awards, fake metrics, fake staff members, fake vehicles, or fake growth events.", calendar_prompt)
        self.assertIn("Do not mention staff members by name in automated posts.", calendar_prompt)

    @patch("openai.OpenAI")
    def test_generate_facebook_calendar_respects_character_cadence(self, mock_openai):
        with self.app.app_context():
            self.app.db.update_brand_text_field(
                self.brand_id,
                "facebook_recurring_characters",
                json.dumps(
                    [
                        {
                            "name": "Marty",
                            "role": "Dispatcher",
                            "description": "Dry and practical.",
                            "cadence": "every_3_posts",
                        },
                        {
                            "name": "Alex",
                            "role": "Owner",
                            "description": "Shows up for milestone moments.",
                            "cadence": "once_per_calendar",
                        },
                    ]
                ),
            )

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = _FakeChatResponse(
            json.dumps(
                {
                    "posts": [
                        {"post_type": "business_growth", "message": f"Calendar post {index}", "image_hint": "Photo idea", "link_url": ""}
                        for index in range(1, 9)
                    ]
                }
            )
        )
        mock_openai.return_value = mock_client

        response = self.client.post(
            "/client/post-scheduler/generate-calendar",
            json={
                "content_mix": "brand_story_engine",
                "weeks": 2,
                "posts_per_week": 4,
                "start_date": (datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat(),
                "post_types": ["business_growth", "behind_the_scenes", "team_intro", "community_spotlight"],
                "brief": "Make the story feel like a living business.",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        character_names = [post.get("character_name") for post in payload["posts"] if post.get("character_name")]
        self.assertGreaterEqual(character_names.count("Marty"), 2)
        self.assertLessEqual(character_names.count("Alex"), 1)
        self.assertGreater(character_names.count("Marty"), character_names.count("Alex"))

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

    def test_post_scheduler_story_settings_save_storytelling_fields(self):
        recurring_characters = json.dumps([
            {
                "name": "Alex",
                "role": "Owner-operator",
                "description": "Calm authority",
                "cadence": "every_4_posts",
            }
        ])
        response = self.client.post(
            "/client/post-scheduler",
            data={
                "section": "facebook_storytelling_profile",
                "facebook_storytelling_strategy": "Make the feed feel like an ongoing story about disciplined growth.",
                "facebook_content_personality": "warm_professional",
                "facebook_cta_style": "consultative",
                "facebook_post_length": "long",
                "facebook_storytelling_guardrails": "No forced jokes and no fake urgency.",
                "facebook_recurring_characters": recurring_characters,
            },
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)

        self.assertEqual(brand["facebook_storytelling_strategy"], "Make the feed feel like an ongoing story about disciplined growth.")
        self.assertEqual(brand["facebook_content_personality"], "warm_professional")
        self.assertEqual(brand["facebook_cta_style"], "consultative")
        self.assertEqual(brand["facebook_post_length"], "long")
        self.assertEqual(brand["facebook_storytelling_guardrails"], "No forced jokes and no fake urgency.")
        self.assertEqual(brand["facebook_recurring_characters"], recurring_characters)

    def test_my_business_voice_section_preserves_storytelling_fields(self):
        with self.app.app_context():
            db = self.app.db
            db.update_brand_text_field(self.brand_id, "facebook_storytelling_strategy", "Do not clear this strategy.")
            db.update_brand_text_field(self.brand_id, "facebook_content_personality", "playful_funny")
            db.update_brand_text_field(self.brand_id, "facebook_cta_style", "subtle")
            db.update_brand_text_field(self.brand_id, "facebook_post_length", "short")
            db.update_brand_text_field(self.brand_id, "facebook_storytelling_guardrails", "Keep this guardrail.")
            db.update_brand_text_field(self.brand_id, "facebook_recurring_characters", json.dumps([{"name": "Marty", "cadence": "every_3_posts"}]))

        response = self.client.post(
            "/client/my-business",
            data={
                "section": "voice",
                "brand_voice": "Direct and local.",
                "active_offers": "Free estimate.",
                "target_audience": "Homeowners who want reliability.",
                "reporting_notes": "Keep reports plain English.",
                "website_url": "https://example.com",
            },
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            brand = self.app.db.get_brand(self.brand_id)

        self.assertEqual(brand["brand_voice"], "Direct and local.")
        self.assertEqual(brand["facebook_storytelling_strategy"], "Do not clear this strategy.")
        self.assertEqual(brand["facebook_content_personality"], "playful_funny")
        self.assertEqual(brand["facebook_cta_style"], "subtle")
        self.assertEqual(brand["facebook_post_length"], "short")
        self.assertEqual(brand["facebook_storytelling_guardrails"], "Keep this guardrail.")
        self.assertIn("Marty", brand["facebook_recurring_characters"])

    def test_storytelling_pages_render(self):
        with self.app.app_context():
            db = self.app.db
            db.update_brand_text_field(self.brand_id, "facebook_storytelling_strategy", "Make the feed feel like an unfolding brand story.")
            db.update_brand_text_field(self.brand_id, "facebook_content_personality", "warm_professional")
            db.update_brand_text_field(self.brand_id, "facebook_cta_style", "subtle")
            db.update_brand_text_field(self.brand_id, "facebook_post_length", "story_time")
            db.update_brand_text_field(self.brand_id, "facebook_recurring_characters", json.dumps([{"name": "Marty", "role": "Dispatcher"}]))

        my_business_response = self.client.get("/client/my-business")
        scheduler_response = self.client.get("/client/post-scheduler")

        self.assertEqual(my_business_response.status_code, 200)
        self.assertEqual(scheduler_response.status_code, 200)
        self.assertNotIn(b"Facebook Storytelling Strategy", my_business_response.data)
        self.assertIn(b"Facebook Story Settings", scheduler_response.data)
        self.assertIn(b"Post Length", scheduler_response.data)
        self.assertIn(b"Story Time", scheduler_response.data)
        self.assertIn(b"Add Character", scheduler_response.data)
        self.assertIn(b"How often should they show up?", scheduler_response.data)
        self.assertIn(b"Optional JSON Profile", scheduler_response.data)
        self.assertIn(b"Brand storytelling profile is active", scheduler_response.data)
        self.assertIn(b"The preview keeps line breaks and surfaces the hook, body, and CTA", scheduler_response.data)
        self.assertIn(b"Schedule Selected", scheduler_response.data)
        self.assertIn(b"Select All", scheduler_response.data)
        self.assertIn(b"Select None", scheduler_response.data)


if __name__ == "__main__":
    unittest.main()