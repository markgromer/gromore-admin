import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

from webapp.client_portal import _default_client_commercial_proposal_builder
from webapp.commercial_prospector import extract_commercial_public_intel, extract_commercial_site_intel, search_commercial_prospects

_TEST_ROOT = Path(__file__).resolve().parent / ".tmp-test-artifacts"
_TEST_ROOT.mkdir(exist_ok=True)
_BOOTSTRAP_DB = str(_TEST_ROOT / "gromore-client-commercial-bootstrap.db")
os.environ.setdefault("DATABASE_PATH", _BOOTSTRAP_DB)
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("APP_URL", "http://localhost:5000")

from webapp.app import create_app
from webapp.commercial_strategy import build_commercial_outreach_brief


class ClientCommercialProspectingTests(unittest.TestCase):
    def setUp(self):
        self.db_file = _TEST_ROOT / f"client-commercial-{uuid.uuid4().hex}.db"
        os.environ["DATABASE_PATH"] = str(self.db_file)
        os.environ["SECRET_KEY"] = "test-secret"
        os.environ["APP_URL"] = "http://localhost:5000"

        self.app = create_app()
        self.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = self.app.test_client()

        with self.app.app_context():
            self.brand_id = self.app.db.create_brand({
                "slug": f"client-commercial-{uuid.uuid4().hex[:8]}",
                "display_name": "Scoopy Yard Care",
                "service_area": "Phoenix, AZ",
            })
            self.user_id = self.app.db.create_client_user(
                self.brand_id,
                f"owner-{uuid.uuid4().hex[:8]}@example.com",
                "Password123",
                "Owner User",
            )

        with self.client.session_transaction() as session:
            session["client_user_id"] = self.user_id
            session["client_brand_id"] = self.brand_id
            session["client_role"] = "owner"
            session["client_brand_name"] = "Scoopy Yard Care"

    def tearDown(self):
        for key in ("DATABASE_PATH", "SECRET_KEY", "APP_URL"):
            os.environ.pop(key, None)
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_file) + suffix)
            if path.exists():
                path.unlink()

    def test_client_commercial_page_loads(self):
        response = self.client.get("/client/commercial")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Commercial Lead Engine", response.data)
        self.assertIn(b"Search commercial targets", response.data)

    def test_client_commercial_upgrade_state_redirects_to_upgrade_page(self):
        with self.app.app_context():
            self.app.db.update_brand_feature_access(self.brand_id, {"commercial": "upgrade"})

        response = self.client.get("/client/commercial", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertIn("/client/upgrade/commercial", response.headers["Location"])

        upgrade_response = self.client.get("/client/upgrade/commercial")
        html = upgrade_response.get_data(as_text=True)
        self.assertEqual(upgrade_response.status_code, 200)
        self.assertIn("Upgrade Available", html)
        self.assertIn("Commercial", html)

    @patch("webapp.commercial_prospector.search_commercial_prospects")
    def test_client_commercial_search_renders_results(self, search_mock):
        search_mock.return_value = [
            {
                "business_name": "Mesa Property Group",
                "contact_name": "Mesa Property Group",
                "website": "https://mesaproperty.example.com",
                "address": "123 Main St, Mesa, AZ",
                "phone": "+14805551234",
                "rating": 4.6,
                "review_count": 31,
                "emails": ["leasing@mesaproperty.example.com"],
                "prospect_type": "property_manager",
                "prospect_type_label": "Property Managers",
                "service_area": "Mesa, AZ",
                "source_query": "property management companies in Mesa, AZ",
                "score": 85,
            }
        ]

        response = self.client.post(
            "/client/commercial/search",
            data={
                "location": "Mesa, AZ",
                "prospect_types": ["property_manager"],
                "max_results": "5",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Mesa Property Group", response.data)
        self.assertIn(b"Import selected targets into WARREN", response.data)

    @patch("webapp.commercial_prospector.search_commercial_prospects")
    def test_client_commercial_search_uses_brand_google_maps_api_key(self, search_mock):
        search_mock.return_value = []

        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "google_maps_api_key", "brand-maps-key")

        response = self.client.post(
            "/client/commercial/search",
            data={
                "location": "Mesa, AZ",
                "prospect_types": ["property_manager"],
                "max_results": "5",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(search_mock.call_args.kwargs["api_key"], "brand-maps-key")

    @patch("webapp.commercial_prospector.search_commercial_prospects")
    def test_client_commercial_search_passes_search_criteria(self, search_mock):
        search_mock.return_value = []

        response = self.client.post(
            "/client/commercial/search",
            data={
                "location": "Mesa, AZ",
                "prospect_types": ["apartment"],
                "max_results": "5",
                "search_criteria": "dog friendly properties",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(search_mock.call_args.kwargs["search_criteria"], "dog friendly properties")

    def test_client_commercial_brief_uses_brand_service_context(self):
        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "primary_services", "Pet waste removal, waste station refill, deodorizer treatment")
            self.app.db.update_brand_text_field(self.brand_id, "active_offers", "Apartment dog park cleanup, HOA common-area service")
            brand = self.app.db.get_brand(self.brand_id)

        brief = build_commercial_outreach_brief(
            {
                "business_name": "Palm Vista Apartments",
                "industry": "Apartment Complexes",
                "account_type": "apartment",
                "service_area": "Mesa, AZ",
                "source_details_json": json.dumps({"emails": ["manager@palmvista.example.com"]}),
                "audit_snapshot_json": json.dumps({}),
                "qualification_answers_json": json.dumps({}),
            },
            brand=brand,
        )

        self.assertIn("pet waste removal", brief["email_body"].lower())
        self.assertIn("site cleanliness", brief["outreach_angle"].lower())
        self.assertNotIn("lead flow", brief["email_body"].lower())
        self.assertNotIn("website conversion", brief["call_opener"].lower())

    def test_client_commercial_import_creates_brand_thread(self):
        payload = {
            "business_name": "Skyline HOA Services",
            "contact_name": "Skyline HOA Services",
            "website": "https://skyline-hoa.example.com",
            "address": "456 Camelback Rd, Phoenix, AZ",
            "phone": "+16025550123",
            "emails": ["board@skyline-hoa.example.com"],
            "prospect_type": "hoa",
            "prospect_type_label": "HOAs",
            "service_area": "Phoenix, AZ",
            "source_query": "HOA management companies in Phoenix, AZ",
            "score": 81,
            "property_count": "214 units across 3 buildings",
            "walkthrough_common_area_count": 2,
            "walkthrough_relief_area_count": 1,
            "pet_traffic_estimate": "Moderate to high around shared pet areas",
            "site_condition": "214 units footprint with pet-friendly common areas likely need recurring cleanup and visible service proof",
            "site_signals": ["214 units", "3 buildings", "Dog park", "Pet-friendly"],
            "audit_snapshot": {
                "title": "Skyline HOA Services",
                "description": "Community management and board support for Phoenix HOAs.",
                "h1": ["HOA management with faster response times"],
            },
        }

        response = self.client.post(
            "/client/commercial/import",
            data={"selected_results": json.dumps(payload)},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            threads = self.app.db.get_lead_threads(self.brand_id)
            self.assertEqual(len(threads), 1)
            thread = threads[0]
            self.assertEqual(thread["source"], "commercial_prospecting")
            self.assertEqual(thread["channel"], "commercial")
            self.assertIn("Skyline HOA Services", thread.get("commercial_data_json") or "")
            self.assertIn("214 units across 3 buildings", thread.get("commercial_data_json") or "")
            self.assertIn("Moderate to high around shared pet areas", thread.get("commercial_data_json") or "")

            messages = self.app.db.get_lead_messages(thread["id"])
            self.assertEqual(len(messages), 1)
            self.assertIn("Commercial target imported", messages[0]["content"])

    @patch("requests.Session.get")
    def test_extract_commercial_site_intel_fills_units_and_pet_signals(self, session_get_mock):
        homepage = MagicMock()
        homepage.status_code = 200
        homepage.url = "https://palmvista.example.com"
        homepage.headers = {"Content-Type": "text/html; charset=utf-8"}
        homepage.text = """
            <html>
                <head><title>Palm Vista Apartments</title></head>
                <body>
                    <h1>Palm Vista Apartments</h1>
                    <p>Pet-friendly apartment living with 214 apartment homes across 3 buildings.</p>
                    <p>Residents enjoy a fenced dog park and 2 courtyards.</p>
                    <a href=\"/amenities\">Amenities</a>
                </body>
            </html>
        """
        amenities = MagicMock()
        amenities.status_code = 200
        amenities.url = "https://palmvista.example.com/amenities"
        amenities.headers = {"Content-Type": "text/html; charset=utf-8"}
        amenities.text = """
            <html>
                <head><title>Amenities</title></head>
                <body>
                    <p>Our bark park and pet spa keep dog owners happy. Residents also have access to 4 pet waste stations.</p>
                    <p>Service is handled every other week with onsite dumpster enclosure disposal.</p>
                </body>
            </html>
        """
        not_found = MagicMock()
        not_found.status_code = 404
        not_found.url = "https://palmvista.example.com/about"
        not_found.headers = {"Content-Type": "text/html; charset=utf-8"}
        not_found.text = ""

        def _get(url, *args, **kwargs):
            if url.endswith("/amenities"):
                return amenities
            if url.endswith("/community") or url.endswith("/property") or url.endswith("/properties") or url.endswith("/pet-friendly") or url.endswith("/leasing") or url.endswith("/about") or url.endswith("/faq"):
                return not_found
            return homepage

        session_get_mock.side_effect = _get

        intel = extract_commercial_site_intel(
            "https://palmvista.example.com",
            business_name="Palm Vista Apartments",
            prospect_type="apartment",
        )

        self.assertEqual(intel["property_count"], "214 units across 3 buildings")
        self.assertEqual(intel["walkthrough_common_area_count"], 2)
        self.assertEqual(intel["walkthrough_relief_area_count"], 1)
        self.assertEqual(intel["walkthrough_waste_station_count"], 4)
        self.assertIn("High", intel["pet_traffic_estimate"])
        self.assertIn("Pet-friendly", intel["site_signals"])
        self.assertIn("Dog park", intel["site_signals"])
        self.assertEqual(intel["service_frequency_hint"], "every_2_weeks")
        self.assertEqual(intel["service_days_hint"], "Every other week")
        self.assertIn("onsite dumpster", intel["disposal_notes"].lower())

    @patch("requests.get")
    @patch("requests.Session.get")
    def test_extract_commercial_public_intel_finds_contacts_and_mentions(self, session_get_mock, get_mock):
        ddg_response = MagicMock()
        ddg_response.status_code = 200
        ddg_response.text = """
            <html>
                <body>
                    <div class="result">
                        <a class="result__a" href="https://www.apartments.example.com/palm-vista">Palm Vista Apartments - Pet Friendly Community</a>
                        <div class="result__snippet">Pet-friendly community with fenced dog park. Professionally managed by Desert Residential Management.</div>
                    </div>
                    <div class="result">
                        <a class="result__a" href="https://palmvista.example.com/contact">Palm Vista Apartments Contact</a>
                        <div class="result__snippet">Call the leasing office or community manager for tours and resident support.</div>
                    </div>
                </body>
            </html>
        """
        get_mock.return_value = ddg_response

        homepage = MagicMock()
        homepage.status_code = 200
        homepage.url = "https://palmvista.example.com"
        homepage.headers = {"Content-Type": "text/html; charset=utf-8"}
        homepage.text = """
            <html>
                <body>
                    <a href="/contact">Contact</a>
                </body>
            </html>
        """
        contact_page = MagicMock()
        contact_page.status_code = 200
        contact_page.url = "https://palmvista.example.com/contact"
        contact_page.headers = {"Content-Type": "text/html; charset=utf-8"}
        contact_page.text = """
            <html>
                <body>
                    <p>Jane Doe - Community Manager</p>
                    <p>manager@palmvista.example.com</p>
                    <p>(480) 555-9898</p>
                </body>
            </html>
        """
        not_found = MagicMock()
        not_found.status_code = 404
        not_found.url = "https://palmvista.example.com/team"
        not_found.headers = {"Content-Type": "text/html; charset=utf-8"}
        not_found.text = ""

        def _session_get(url, *args, **kwargs):
            if url.endswith("/contact"):
                return contact_page
            if url.endswith("/contact-us") or url.endswith("/team") or url.endswith("/staff") or url.endswith("/management") or url.endswith("/leasing") or url.endswith("/about"):
                return not_found
            return homepage

        session_get_mock.side_effect = _session_get

        intel = extract_commercial_public_intel(
            "Palm Vista Apartments",
            website="https://palmvista.example.com",
            address="123 Main St, Mesa, AZ",
            service_area="Mesa, AZ",
            prospect_type="apartment",
        )

        self.assertEqual(intel["decision_maker_role_hint"], "Community Manager")
        self.assertEqual(intel["primary_contact_name"], "Jane Doe")
        self.assertEqual(intel["management_company"], "Desert Residential Management")
        self.assertIn("manager@palmvista.example.com", intel["emails"])
        self.assertIn("+14805559898", intel["phones"])
        self.assertGreaterEqual(intel["decision_maker_contacts"][0]["priority_score"], 80)
        self.assertEqual(intel["decision_maker_contacts"][0]["priority_label"], "Best")
        self.assertTrue(any(item["category"] == "dog_area" for item in intel["complaint_signals"]))
        self.assertTrue(any(item["snippet"] for item in intel["public_mentions"]))

    def test_default_client_commercial_proposal_builder_uses_cadence_hint(self):
        builder = _default_client_commercial_proposal_builder(
            brand={"crm_avg_service_price": 72},
            prospect={
                "property_count": "214 units across 3 buildings",
                "walkthrough_waste_station_count": 4,
                "walkthrough_common_area_count": 2,
                "walkthrough_relief_area_count": 1,
                "service_frequency_hint": "every_2_weeks",
                "service_days_hint": "Every other week",
            },
        )

        self.assertEqual(builder["service_frequency"], "every_2_weeks")
        self.assertEqual(builder["service_days"], "Every other week")

    def test_client_commercial_research_promote_fills_blank_fields(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Palm Vista Apartments",
                    "lead_email": "manager@palmvista.example.com",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Apartments.",
                    "commercial_data_json": json.dumps({
                        "name": "Palm Vista Apartments",
                        "email": "manager@palmvista.example.com",
                        "business_name": "Palm Vista Apartments",
                        "industry": "Apartment Complexes",
                        "account_type": "apartment",
                        "property_count": "214 units across 3 buildings",
                        "walkthrough_waste_station_count": 4,
                        "walkthrough_common_area_count": 2,
                        "walkthrough_relief_area_count": 1,
                        "disposal_notes": "Waste appears to require removal from the property",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({
                            "decision_maker_role_hint": "Community Manager",
                            "management_company": "Desert Residential Management",
                            "decision_maker_contacts": [
                                {
                                    "name": "Jane Doe",
                                    "role": "Community Manager",
                                    "email": "manager@palmvista.example.com",
                                    "phone": "+14805559898",
                                    "source_url": "https://palmvista.example.com/contact",
                                    "evidence": "Jane Doe - Community Manager",
                                    "priority_score": 92,
                                    "priority_label": "Best",
                                }
                            ],
                            "complaint_signals": [
                                {
                                    "category": "dog_area",
                                    "label": "Dog area complaints",
                                    "title": "Palm Vista Reviews",
                                    "url": "https://reviews.example.com/palm-vista",
                                    "snippet": "Residents mention the dog park stays messy after weekends.",
                                },
                                {
                                    "category": "cleanliness",
                                    "label": "Cleanliness or odor complaints",
                                    "title": "Palm Vista Reviews",
                                    "url": "https://reviews.example.com/palm-vista",
                                    "snippet": "Multiple reviews mention odor near pet areas.",
                                }
                            ],
                        }),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/worksheet/research",
            data={},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            payload = json.loads(thread["commercial_data_json"])
            self.assertEqual(payload["decision_maker_role"], "Community Manager")
            self.assertIn("Managed by Desert Residential Management", payload["current_vendor_status"])
            self.assertIn("Public complaint clues", payload["walkthrough_notes"])
            self.assertIn("dog-area", json.loads(payload["qualification_answers_json"])["commercial_goal"].lower())
            self.assertIn("Bag refill", payload["required_add_ons_json"])
            self.assertIn("Off-site waste haul", payload["required_add_ons_json"])

    def test_client_commercial_detail_and_qualification_save(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target imported - property manager.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "phone": "+14805551234",
                        "business_name": "Mesa Property Group",
                        "website": "https://mesaproperty.example.com",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "service_area": "Mesa, AZ",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({
                            "emails": ["leasing@mesaproperty.example.com"],
                            "service_area": "Mesa, AZ",
                        }),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.get(f"/client/commercial/thread/{thread_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Lead Worksheet", response.data)
        self.assertIn(b"Commercial Strategy Brief", response.data)

        save_response = self.client.post(
            f"/client/commercial/thread/{thread_id}/qualification",
            data={
                "property_count": "6 properties / 420 units",
                "decision_maker_role": "regional manager",
                "current_vendor_status": "replacing incumbent agency",
                "service_scope": "paid ads, landing page cleanup, reporting",
                "buying_timeline": "needs recommendation before next month",
                "decision_process": "regional manager recommends, ownership approves",
                "commercial_goal": "increase occupancy on two underperforming sites",
                "budget_range": "$4k-$6k monthly",
            },
            follow_redirects=False,
        )

        self.assertEqual(save_response.status_code, 302)
        self.assertTrue(save_response.headers["Location"].endswith(f"/client/commercial/thread/{thread_id}"))

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            self.assertIn("ownership approves", thread.get("commercial_data_json") or "")
            self.assertIn("Proposal-ready", thread.get("summary") or "")

    def test_client_commercial_worksheet_save_updates_buyer_and_site(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Palm Vista Apartments",
                    "lead_email": "manager@palmvista.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Apartments.",
                    "commercial_data_json": json.dumps({
                        "name": "Palm Vista Apartments",
                        "email": "manager@palmvista.example.com",
                        "business_name": "Palm Vista Apartments",
                        "industry": "Apartment Complexes",
                        "account_type": "apartment",
                        "service_area": "Mesa, AZ",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["manager@palmvista.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/worksheet",
            data={
                "property_count": "214 units across 3 buildings",
                "decision_maker_role": "regional manager",
                "current_vendor_status": "reviewing current vendor performance",
                "service_scope": "Pet waste removal, bag refill, deodorizer treatment",
                "buying_timeline": "Needs options before next quarter",
                "decision_process": "Regional manager recommends, ownership approves",
                "commercial_goal": "Reduce complaints and tighten reporting",
                "budget_range": "$2k-$4k monthly",
                "walkthrough_property_label": "North dog run + courtyard loop",
                "walkthrough_waste_station_count": "8",
                "walkthrough_common_area_count": "4",
                "walkthrough_relief_area_count": "2",
                "pet_traffic_estimate": "High after work hours",
                "site_condition": "Complaint-prone around the dog run",
                "access_notes": "Leasing opens the gate before 8am",
                "gate_notes": "Photograph latch after each visit",
                "disposal_notes": "Rear dumpster enclosure",
                "walkthrough_notes": "Manager wants proof tied to complaints",
                "required_add_ons": "Bag refill\nDeodorizer",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            payload = thread.get("commercial_data_json") or ""
            self.assertIn("North dog run + courtyard loop", payload)
            self.assertIn("regional manager", payload)
            self.assertIn("Bag refill", payload)

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_worksheet_saved")
            self.assertEqual(len(events), 1)

    @patch("requests.post")
    def test_client_commercial_worksheet_ai_assist_fills_missing_fields(self, post_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "decision_maker_role": "regional manager",
                                "service_scope": "Pet waste removal, bag refill, deodorizer treatment",
                                "commercial_goal": "Reduce complaints and improve service proof",
                                "site_condition": "Likely complaint-prone around pet-heavy common areas",
                                "required_add_ons": ["Bag refill", "Deodorizer"],
                            }
                        )
                    }
                }
            ]
        }
        post_mock.return_value = mock_response

        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "openai_api_key", "sk-test-key")
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Palm Vista Apartments",
                    "lead_email": "manager@palmvista.example.com",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Apartments.",
                    "commercial_data_json": json.dumps({
                        "name": "Palm Vista Apartments",
                        "email": "manager@palmvista.example.com",
                        "business_name": "Palm Vista Apartments",
                        "industry": "Apartment Complexes",
                        "account_type": "apartment",
                        "service_area": "Mesa, AZ",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["manager@palmvista.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/worksheet/ai",
            data={},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        post_mock.assert_called_once()

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            payload = json.loads(thread["commercial_data_json"])
            self.assertEqual(payload["decision_maker_role"], "regional manager")
            answers = json.loads(payload["qualification_answers_json"])
            self.assertIn("Pet waste removal", answers["service_scope"])
            self.assertIn("Reduce complaints", answers["commercial_goal"])
            self.assertIn("complaint-prone", payload["site_condition"])

    def test_client_commercial_walkthrough_save_updates_payload(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Palm Vista Apartments",
                    "lead_email": "manager@palmvista.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "qualified",
                    "summary": "Commercial target - Apartments.",
                    "commercial_data_json": json.dumps({
                        "name": "Palm Vista Apartments",
                        "email": "manager@palmvista.example.com",
                        "phone": "+14805551234",
                        "business_name": "Palm Vista Apartments",
                        "website": "https://palmvista.example.com",
                        "industry": "Apartment Complexes",
                        "account_type": "apartment",
                        "service_area": "Mesa, AZ",
                        "source": "commercial_prospecting",
                        "stage": "qualified",
                        "source_details_json": json.dumps({"emails": ["manager@palmvista.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/walkthrough",
            data={
                "property_count": "214 units across 3 buildings",
                "walkthrough_property_label": "North dog run + courtyard loop",
                "walkthrough_waste_station_count": "8",
                "walkthrough_common_area_count": "4",
                "walkthrough_relief_area_count": "2",
                "pet_traffic_estimate": "High after work hours",
                "site_condition": "Moderate complaint risk around dog run",
                "access_notes": "Leasing office opens the service gate before 8am.",
                "gate_notes": "Latch must be photographed after each visit.",
                "disposal_notes": "Bag waste goes to rear dumpster enclosure.",
                "required_add_ons": "Bag refill\nDeodorizer",
                "walkthrough_photo_urls": "https://example.com/photo-1.jpg",
                "walkthrough_notes": "Manager wants recap tied to tenant complaints.",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            payload = thread.get("commercial_data_json") or ""
            self.assertIn("North dog run + courtyard loop", payload)
            self.assertIn("walkthrough_waste_station_count", payload)
            self.assertIn("Bag refill", payload)

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_walkthrough_saved")
            self.assertEqual(len(events), 1)
            self.assertIn("North dog run", events[0]["event_value"])

    @patch("webapp.email_sender.send_simple_email")
    def test_client_commercial_send_email_logs_outreach(self, send_mock):
        send_mock.return_value = None

        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Property Managers.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "business_name": "Mesa Property Group",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["leasing@mesaproperty.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/send-email",
            data={
                "subject": "Quick idea for Mesa Property Group",
                "message": "Hi there,\n\nWe found a few clear improvements.",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        send_mock.assert_called_once()

        with self.app.app_context():
            messages = self.app.db.get_lead_messages(thread_id)
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["channel"], "email")
            self.assertIn("few clear improvements", messages[0]["content"])

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_email_sent")
            self.assertEqual(len(events), 1)
            self.assertIn("Quick idea", events[0]["event_value"])

    def test_client_commercial_outreach_save_persists_custom_assets(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Property Managers.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "business_name": "Mesa Property Group",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["leasing@mesaproperty.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/outreach",
            data={
                "subject": "Cleanup idea for Mesa Property Group",
                "message": "Hi team,\n\nWe can tighten station upkeep and reporting without adding manager overhead.",
                "call_opener": "We help commercial properties tighten station upkeep and service reporting.",
                "rewrite_prompt": "Make it more direct for a regional manager.",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            payload = json.loads(thread["commercial_data_json"])
            self.assertEqual(payload["outreach_subject_override"], "Cleanup idea for Mesa Property Group")
            self.assertIn("station upkeep", payload["outreach_email_body_override"])
            self.assertIn("regional manager", payload["outreach_rewrite_prompt"])

    @patch("requests.post")
    def test_client_commercial_outreach_rewrite_uses_ai_prompt(self, post_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "subject": "Mesa property cleanup idea",
                                "email_body": "Hi team,\n\nWe can tighten station upkeep, proof-of-service, and manager reporting.",
                                "call_opener": "We help properties tighten station upkeep and reporting.",
                            }
                        )
                    }
                }
            ]
        }
        post_mock.return_value = mock_response

        with self.app.app_context():
            self.app.db.update_brand_text_field(self.brand_id, "openai_api_key", "sk-test-key")
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Property Managers.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "business_name": "Mesa Property Group",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "service_area": "Mesa, AZ",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["leasing@mesaproperty.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/outreach/rewrite",
            data={"rewrite_prompt": "Make this more direct for a regional property manager."},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        post_mock.assert_called_once()

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            payload = json.loads(thread["commercial_data_json"])
            self.assertEqual(payload["outreach_subject_override"], "Mesa property cleanup idea")
            self.assertIn("manager reporting", payload["outreach_email_body_override"])
            self.assertIn("regional property manager", payload["outreach_rewrite_prompt"])

    def test_client_commercial_target_can_be_deleted(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Property Managers.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "business_name": "Mesa Property Group",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["leasing@mesaproperty.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/delete",
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/client/commercial"))

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            self.assertIsNone(thread)

    def test_client_commercial_can_enroll_in_drip(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Property Managers.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "business_name": "Mesa Property Group",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["leasing@mesaproperty.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )
            sequence_id = self.app.db.create_drip_sequence("Commercial nurture", "Follow-up flow", "commercial")

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/enroll-drip",
            data={"sequence_id": str(sequence_id)},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            enrollments = self.app.db.get_lead_drip_enrollments("client_commercial", thread_id)
            self.assertEqual(len(enrollments), 1)
            self.assertEqual(enrollments[0]["sequence_id"], sequence_id)
            self.assertEqual(enrollments[0]["email"], "leasing@mesaproperty.example.com")

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_drip_enrolled")
            self.assertEqual(len(events), 1)
            self.assertIn("Commercial nurture", events[0]["event_value"])

    def test_client_commercial_builds_structured_proposal_quote(self):
        with self.app.app_context():
            self.app.db.update_brand_number_field(self.brand_id, "crm_avg_service_price", 72)
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Palm Vista Apartments",
                    "lead_email": "manager@palmvista.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "qualified",
                    "summary": "Commercial target - Apartments.",
                    "commercial_data_json": json.dumps({
                        "name": "Palm Vista Apartments",
                        "email": "manager@palmvista.example.com",
                        "business_name": "Palm Vista Apartments",
                        "industry": "Apartment Complexes",
                        "account_type": "apartment",
                        "source": "commercial_prospecting",
                        "stage": "qualified",
                        "property_count": "214 units across 3 buildings",
                        "source_details_json": json.dumps({"emails": ["manager@palmvista.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/proposal",
            data={
                "selected_package": "premium",
                "service_frequency": "5x_week",
                "service_days": "Monday-Friday",
                "property_count": "214 units across 3 buildings",
                "waste_station_count": "8",
                "waste_station_rate": "15",
                "common_area_count": "4",
                "common_area_rate": "28",
                "relief_area_count": "2",
                "relief_area_rate": "35",
                "bag_refill_included": "1",
                "bag_refill_fee": "45",
                "deodorizer_included": "1",
                "deodorizer_fee": "25",
                "initial_cleanup_required": "1",
                "initial_cleanup_fee": "180",
                "monthly_management_fee": "60",
                "scope_summary": "Stations, common lawns, dog relief areas, and refill coverage.",
                "notes": "Start with building access map and porter contact.",
                "quote_status": "approved",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            quote = self.app.db.get_lead_quote_for_thread(thread_id)
            self.assertIsNotNone(quote)
            self.assertEqual(quote["quote_mode"], "structured")
            self.assertEqual(quote["status"], "approved")
            self.assertGreater(float(quote["amount_low"]), 0)
            self.assertIn("Waste station servicing", quote.get("line_items_json") or "")
            self.assertIn("Palm Vista Apartments", quote.get("summary") or "")
            self.assertIn("Premium", quote.get("summary") or "")

            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            self.assertEqual(thread["quote_status"], "approved")
            self.assertIn("waste_station_count", thread.get("commercial_data_json") or "")

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_proposal_built")
            self.assertEqual(len(events), 1)
            self.assertIn("Premium", events[0]["event_value"])

    def test_client_commercial_service_visit_logging_persists_visit(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Palm Vista Apartments",
                    "lead_email": "manager@palmvista.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "qualified",
                    "summary": "Commercial target - Apartments.",
                    "commercial_data_json": json.dumps({
                        "name": "Palm Vista Apartments",
                        "email": "manager@palmvista.example.com",
                        "business_name": "Palm Vista Apartments",
                        "walkthrough_property_label": "North dog park",
                        "walkthrough_waste_station_count": 8,
                        "source": "commercial_prospecting",
                        "stage": "qualified",
                        "source_details_json": json.dumps({"emails": ["manager@palmvista.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/service-visit",
            data={
                "service_date": "2026-04-14",
                "completed_by": "Scoopy Yard Care Ops",
                "property_label": "North dog park",
                "waste_station_count_serviced": "8",
                "bags_restocked": "1",
                "gate_secured": "1",
                "summary": "Completed full cleanup, restocked stations, and secured gate after exit.",
                "issues": "One broken bag dispenser\nFence latch loose",
                "photo_urls": "https://example.com/service-photo.jpg",
                "client_note": "Recommend dispenser replacement this week.",
                "internal_note": "Bring spare latch clip next visit.",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            visits = self.app.db.get_commercial_service_visits(thread_id)
            self.assertEqual(len(visits), 1)
            self.assertEqual(visits[0]["property_label"], "North dog park")
            self.assertEqual(visits[0]["waste_station_count_serviced"], 8)
            self.assertEqual(visits[0]["bags_restocked"], 1)
            self.assertIn("broken bag dispenser", visits[0]["issues_json"])

            messages = self.app.db.get_lead_messages(thread_id)
            self.assertEqual(len(messages), 1)
            self.assertIn("Service visit logged", messages[0]["content"])

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_service_visit_logged")
            self.assertEqual(len(events), 1)
            self.assertIn("Completed full cleanup", events[0]["event_value"])

        detail_response = self.client.get(f"/client/commercial/thread/{thread_id}")
        self.assertEqual(detail_response.status_code, 200)
        self.assertIn(b"Manager Recap Preview", detail_response.data)
        self.assertIn(b"North dog park", detail_response.data)

    @patch("webapp.drip_engine.smtplib.SMTP")
    def test_client_commercial_drip_processing_logs_to_thread(self, smtp_mock):
        smtp_server = MagicMock()
        smtp_mock.return_value = smtp_server

        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Property Managers.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "business_name": "Mesa Property Group",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({"emails": ["leasing@mesaproperty.example.com"]}),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )
            sequence_id = self.app.db.create_drip_sequence("Commercial nurture", "Follow-up flow", "commercial")
            self.app.db.create_drip_step(sequence_id, 1, 0, "Follow up", "<p>Checking in</p>", "Checking in")
            enrollment_id = self.app.db.enroll_in_drip(
                sequence_id,
                "leasing@mesaproperty.example.com",
                "Mesa Property Group",
                lead_source="client_commercial",
                lead_id=thread_id,
            )

            from webapp.drip_engine import process_pending_drips

            sent, failed = process_pending_drips(
                {
                    "SMTP_HOST": "smtp.example.com",
                    "SMTP_PORT": 587,
                    "SMTP_USER": "test@example.com",
                    "SMTP_PASSWORD": "secret",
                    "SMTP_FROM_NAME": "Warren",
                    "SMTP_FROM_EMAIL": "test@example.com",
                    "APP_URL": "http://localhost:5000",
                },
                self.app.db,
            )

            self.assertEqual(sent, 1)
            self.assertEqual(failed, 0)

            sends = self.app.db.get_drip_sends(enrollment_id=enrollment_id)
            self.assertEqual(len(sends), 1)
            self.assertEqual(sends[0]["status"], "sent")

            messages = self.app.db.get_lead_messages(thread_id)
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["channel"], "email")
            self.assertIn("Checking in", messages[0]["content"])

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_drip_step_sent")
            self.assertEqual(len(events), 1)
            self.assertIn("Follow up", events[0]["event_value"])

    def test_client_commercial_import_updates_existing_thread_by_website(self):
        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Skyline HOA",
                    "lead_email": "",
                    "lead_phone": "+16025550123",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "qualified",
                    "summary": "Commercial target - HOAs. Proposal: Needs qualification.",
                    "commercial_data_json": json.dumps({
                        "name": "Skyline HOA",
                        "business_name": "Skyline HOA",
                        "website": "https://skyline-hoa.example.com",
                        "industry": "HOAs",
                        "account_type": "hoa",
                        "service_area": "Phoenix, AZ",
                        "source": "commercial_prospecting",
                        "stage": "qualified",
                        "source_details_json": json.dumps({
                            "emails": [],
                            "website": "https://skyline-hoa.example.com",
                            "service_area": "Phoenix, AZ",
                        }),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": json.dumps({
                            "decision_process": "Board approves after manager review",
                        }),
                    }),
                },
            )

        payload = {
            "business_name": "Skyline HOA Services",
            "website": "https://skyline-hoa.example.com/",
            "address": "456 Camelback Rd, Phoenix, AZ",
            "phone": "+16025550123",
            "emails": [],
            "prospect_type": "hoa",
            "prospect_type_label": "HOAs",
            "service_area": "Phoenix, AZ",
            "source_query": "HOA management companies in Phoenix, AZ",
            "audit_snapshot": {
                "title": "Skyline HOA Services",
            },
        }

        response = self.client.post(
            "/client/commercial/import",
            data={"selected_results": json.dumps(payload)},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            threads = self.app.db.get_lead_threads(self.brand_id)
            self.assertEqual(len(threads), 1)
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            self.assertIn("Skyline HOA Services", thread.get("commercial_data_json") or "")
            self.assertIn("Board approves after manager review", thread.get("commercial_data_json") or "")
            self.assertIn("456 Camelback Rd", thread.get("commercial_data_json") or "")

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_imported")
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event_value"], "updated")

    @patch("webapp.commercial_prospector._extract_public_emails")
    @patch("webapp.competitor_intel._scrape_website")
    def test_client_commercial_refresh_preserves_existing_contact_data(self, scrape_mock, email_mock):
        email_mock.return_value = []
        scrape_mock.return_value = {
            "title": "Mesa Property Group",
            "description": "Regional apartment marketing support.",
        }

        with self.app.app_context():
            thread_id = self.app.db.create_lead_thread(
                self.brand_id,
                {
                    "lead_name": "Mesa Property Group",
                    "lead_email": "leasing@mesaproperty.example.com",
                    "lead_phone": "+14805551234",
                    "source": "commercial_prospecting",
                    "channel": "commercial",
                    "status": "new",
                    "summary": "Commercial target - Property Managers. Proposal: Needs qualification.",
                    "commercial_data_json": json.dumps({
                        "name": "Mesa Property Group",
                        "email": "leasing@mesaproperty.example.com",
                        "phone": "+14805551234",
                        "business_name": "Mesa Property Group",
                        "website": "https://mesaproperty.example.com",
                        "industry": "Property Managers",
                        "account_type": "property_manager",
                        "service_area": "Mesa, AZ",
                        "source": "commercial_prospecting",
                        "stage": "new",
                        "source_details_json": json.dumps({
                            "emails": ["leasing@mesaproperty.example.com"],
                            "website": "https://mesaproperty.example.com",
                            "service_area": "Mesa, AZ",
                        }),
                        "audit_snapshot_json": json.dumps({}),
                        "qualification_answers_json": "{}",
                    }),
                },
            )

        response = self.client.post(
            f"/client/commercial/thread/{thread_id}/refresh",
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            thread = self.app.db.get_lead_thread(thread_id, brand_id=self.brand_id)
            self.assertIn("leasing@mesaproperty.example.com", thread.get("commercial_data_json") or "")
            self.assertIn("Regional apartment marketing support.", thread.get("commercial_data_json") or "")

            events = self.app.db.get_lead_events(thread_id, event_type="commercial_refreshed")
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event_value"], "needs_qualification")

    @patch("webapp.commercial_prospector._extract_public_emails")
    @patch("webapp.commercial_prospector._search_google_places")
    @patch("webapp.commercial_prospector.extract_commercial_site_intel")
    @patch("webapp.commercial_prospector.extract_commercial_public_intel")
    def test_search_commercial_prospects_skips_deep_enrichment(self, public_intel_mock, site_intel_mock, search_mock, email_mock):
        search_mock.return_value = [
            {
                "id": f"place-{index}",
                "displayName": {"text": f"Property {index}"},
                "websiteUri": f"https://property{index}.example.com",
                "formattedAddress": f"{index} Main St, Mesa, AZ",
                "nationalPhoneNumber": f"+14805550{index:03d}",
            }
            for index in range(7)
        ]
        email_mock.return_value = ["team@example.com"]
        site_intel_mock.return_value = {"property_count": "120 units", "site_signals": ["Pet-friendly"]}
        public_intel_mock.return_value = {"decision_maker_role_hint": "Property Manager", "emails": ["manager@example.com"]}

        results = search_commercial_prospects(
            "Mesa, AZ",
            ["property_manager"],
            api_key="maps-key",
            max_results_per_type=8,
        )

        self.assertEqual(len(results), 7)
        self.assertEqual(email_mock.call_count, 7)
        self.assertEqual(site_intel_mock.call_count, 0)
        self.assertEqual(public_intel_mock.call_count, 0)

    @patch("webapp.commercial_prospector._extract_public_emails")
    @patch("webapp.commercial_prospector._search_google_places")
    def test_search_commercial_prospects_appends_search_criteria(self, search_mock, email_mock):
        search_mock.return_value = [
            {
                "id": "place-1",
                "displayName": {"text": "Dogwood Apartments"},
                "websiteUri": "https://dogwood.example.com",
                "formattedAddress": "1 Main St, Mesa, AZ",
            }
        ]
        email_mock.return_value = []

        results = search_commercial_prospects(
            "Mesa, AZ",
            ["apartment"],
            api_key="maps-key",
            max_results_per_type=8,
            search_criteria="dog friendly properties",
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["search_criteria"], "dog friendly properties")
        self.assertIn("dog friendly properties", results[0]["source_query"])
        self.assertIn("dog friendly properties", search_mock.call_args.args[0])


if __name__ == "__main__":
    unittest.main()