import unittest

from webapp.client_advisor import infer_mission_profile


class MissionSkillProfileTests(unittest.TestCase):
    def test_auto_profile_defaults_to_beginner_for_new_owners(self):
        profile = infer_mission_profile(completed_count=1, requested_level="auto")

        self.assertEqual(profile["skill_level"], "beginner")
        self.assertEqual(profile["source"], "auto")
        self.assertEqual(profile["max_active"], 3)

    def test_auto_profile_advances_with_completion_volume(self):
        profile = infer_mission_profile(completed_count=5, requested_level="auto")

        self.assertEqual(profile["skill_level"], "intermediate")
        self.assertEqual(profile["label"], "Builder Track")

    def test_manual_override_wins(self):
        profile = infer_mission_profile(completed_count=0, requested_level="advanced")

        self.assertEqual(profile["skill_level"], "advanced")
        self.assertEqual(profile["source"], "manual")
        self.assertEqual(profile["max_active"], 6)


if __name__ == "__main__":
    unittest.main()