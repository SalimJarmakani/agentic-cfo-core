import json
from typing import Any, Dict

from app.agents.workflow_agents.base import LLMJsonAgent


class ExplanationAgent(LLMJsonAgent):
    SYSTEM_PROMPT = (
        "You are an explanation agent for Agent CFO. "
        "Explain the completed workflow to the user in plain language using only the provided data. "
        "Keep the tone clear, direct, and non-technical. "
        "Do not introduce new metrics or reinterpret historical totals as monthly values. "
        "Return valid JSON only."
    )

    def run(self, explanation_input: Dict[str, Any]) -> Dict[str, Any]:
        prompt = self._build_prompt(explanation_input=explanation_input)
        input_tokens = self.count_input_tokens(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            max_tokens=900,
        )
        parsed_output = self.generate_json(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.2,
            max_tokens=900,
        )
        output_payload = self._normalize_output(parsed_output)
        output_payload["input_tokens"] = input_tokens
        output_payload["explanation"] = self.render_output(output_payload)
        return output_payload

    @staticmethod
    def _build_prompt(explanation_input: Dict[str, Any]) -> str:
        return (
            "Explain the workflow result to the user in plain language and respond with JSON only.\n\n"
            f"{json.dumps(explanation_input, indent=2, default=str)}\n\n"
            "Explanation rules:\n"
            "- Use only the supplied analysis, planning, and policy outputs.\n"
            "- Do not add new numbers or recalculate ratios.\n"
            "- Keep historical totals and monthly or policy-normalized metrics distinct.\n"
            "- Make the policy note match the supplied approval and review status.\n\n"
            "Return this schema:\n"
            "{\n"
            '  "headline": "short title",\n'
            '  "summary": "short paragraph for the user",\n'
            '  "key_points": ["point 1", "point 2"],\n'
            '  "recommended_next_steps": ["next step 1", "next step 2"],\n'
            '  "policy_note": "short note about approval or review status"\n'
            "}\n"
            "Use simple wording, keep it concise, and explain what the user should understand from the analysis."
        )

    @staticmethod
    def _normalize_output(payload: Dict[str, Any]) -> Dict[str, Any]:
        headline = str(payload.get("headline", "")).strip() or "Workflow explanation"
        summary = str(payload.get("summary", "")).strip() or "The workflow completed, but no explanation summary was returned."
        key_points = ExplanationAgent.normalize_string_list(
            payload.get("key_points"),
            default_item="Review the workflow outputs before taking action.",
        )[:4]
        recommended_next_steps = ExplanationAgent.normalize_string_list(
            payload.get("recommended_next_steps"),
            default_item="Discuss the recommendations with the user before acting on them.",
        )[:4]
        policy_note = (
            str(payload.get("policy_note", "")).strip()
            or "Policy guidance should be reviewed before any action is taken."
        )
        return {
            "headline": headline,
            "summary": summary,
            "key_points": key_points,
            "recommended_next_steps": recommended_next_steps,
            "policy_note": policy_note,
        }

    @staticmethod
    def render_output(payload: Dict[str, Any]) -> str:
        key_points = "\n".join(f"- {item}" for item in payload["key_points"])
        next_steps = "\n".join(f"- {item}" for item in payload["recommended_next_steps"])
        return (
            f"Headline: {payload['headline']}\n\n"
            f"Summary: {payload['summary']}\n\n"
            "Key points:\n"
            f"{key_points}\n\n"
            "Recommended next steps:\n"
            f"{next_steps}\n\n"
            f"Policy note: {payload['policy_note']}"
        )
