import unittest

from app.agents.workflow_agents.policy_agent import PolicyAgent
from app.services.llm_service import LLMServiceError


class SequencedLLMService:
    def __init__(self, responses):
        self._responses = list(responses)

    def count_generate_input_tokens(self, prompt, system_prompt, max_tokens=None):  # noqa: ANN001, D401
        return 321

    def generate(self, prompt, system_prompt, temperature=0.2, max_tokens=400):  # noqa: ANN001, D401
        next_item = self._responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


class PolicyAgentResilienceTests(unittest.TestCase):
    def test_policy_agent_falls_back_after_empty_or_malformed_llm_output(self) -> None:
        llm_service = SequencedLLMService(
            [
                LLMServiceError("LLM response did not include text content. done=True done_reason=stop"),
                LLMServiceError("LLM response did not include valid JSON."),
            ]
        )
        agent = PolicyAgent(llm_service)
        policy_input = {
            "user_id": 7,
            "question": "Provide a financial assessment and a simple action plan.",
            "analysis_summary": "Observed monthly spending is elevated relative to user policy thresholds.",
            "planning": {
                "goal": "Reduce spend and resolve warnings.",
                "actions": [
                    {"title": "Review recurring payments", "details": "Audit subscriptions.", "priority": "high"},
                ],
                "checkpoints": ["Recheck after 30 days"],
            },
            "user_policy": {
                "overall_status": "warning",
                "score": 48,
                "rules": [
                    {
                        "name": "Spending-to-Income Ratio",
                        "status": "warning",
                        "detail": "Observed monthly average spend is above the preferred threshold.",
                    }
                ],
            },
        }

        output = agent.run(policy_input=policy_input)

        self.assertEqual(output["input_tokens"], 321)
        self.assertTrue(output["approved"])
        self.assertTrue(output["requires_human_review"])
        self.assertEqual(output["user_policy_status"], "warning")
        self.assertIn("generation_warning", output)
        self.assertEqual(len(output["ai_risk_checks"]), 5)
        self.assertGreaterEqual(len(output["guardrails"]), 4)
        self.assertIn("Policy review passed with caution.", output["summary"])


if __name__ == "__main__":
    unittest.main()
