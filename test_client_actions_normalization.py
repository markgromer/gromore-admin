import unittest

from webapp.client_portal import _normalize_client_actions


class ClientActionsNormalizationTests(unittest.TestCase):
    def test_normalize_handles_string_and_partial_dict_actions(self):
        actions = _normalize_client_actions(
            [
                "Call more leads",
                {"mission_name": "Fix CTR", "difficulty": "2", "xp": "150", "steps": "Open Ads Manager"},
                {"title": "", "key": "", "xp": None, "steps": ["", "Review campaign", None]},
            ]
        )

        self.assertEqual(len(actions), 3)
        self.assertEqual(actions[0]["title"], "Call more leads")
        self.assertEqual(actions[0]["mission_name"], "Call more leads")
        self.assertEqual(actions[0]["xp"], 100)
        self.assertEqual(actions[1]["difficulty"], 2)
        self.assertEqual(actions[1]["xp"], 150)
        self.assertEqual(actions[1]["steps"], ["Open Ads Manager"])
        self.assertTrue(actions[2]["key"].startswith("mission_"))
        self.assertEqual(actions[2]["steps"], ["Review campaign"])

    def test_normalize_skips_non_supported_items(self):
        actions = _normalize_client_actions([None, 123, {"title": "Valid mission"}])
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["title"], "Valid mission")


if __name__ == "__main__":
    unittest.main()