import unittest

from webapp.ai_assistant import _build_niche_benchmark_prompt


class WarrenBenchmarkPromptTests(unittest.TestCase):
    def test_pet_waste_prompt_uses_niche_calibration(self):
        prompt = _build_niche_benchmark_prompt({"industry": "pet_waste_removal"})

        self.assertIn("pet waste removal", prompt.lower())
        self.assertIn("1.5%", prompt)
        self.assertIn("do not grade it against higher-intent home service funnels", prompt.lower())

    def test_unknown_industry_returns_empty_prompt(self):
        prompt = _build_niche_benchmark_prompt({"industry": "unknown_new_vertical"})
        self.assertEqual(prompt, "")


if __name__ == "__main__":
    unittest.main()