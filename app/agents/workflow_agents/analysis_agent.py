import json
from typing import Any, Dict

from app.agents.workflow_agents.base import LLMJsonAgent


class AnalysisAgent(LLMJsonAgent):
    SYSTEM_PROMPT = (
        "You are a financial analysis agent for Agent CFO. "
        "Use only the provided data. "
        "Return valid JSON only."
    )

    def run(self, input_payload: Dict[str, Any]) -> Dict[str, Any]:
        prompt = self._build_prompt(input_payload=input_payload)
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
        output_payload["analysis"] = self.render_output(output_payload)
        return output_payload

    @staticmethod
    def _build_prompt(input_payload: Dict[str, Any]) -> str:
        return (
            "Analyze the user data below and respond with JSON only.\n\n"
            f"{json.dumps(input_payload, indent=2, default=str)}\n\n"
            "Return this schema:\n"
            "{\n"
            '  "summary": "short paragraph",\n'
            '  "risks": ["risk 1", "risk 2"],\n'
            '  "opportunities": ["opportunity 1", "opportunity 2"],\n'
            '  "next_actions": ["action 1", "action 2"]\n'
            "}\n"
            "Keep it concise and grounded in the provided data."
        )

    @staticmethod
    def _normalize_output(payload: Dict[str, Any]) -> Dict[str, Any]:
        summary = str(payload.get("summary", "")).strip()
        risks = AnalysisAgent.normalize_string_list(
            payload.get("risks"),
            default_item="No material risks identified.",
        )
        opportunities = AnalysisAgent.normalize_string_list(
            payload.get("opportunities"),
            default_item="No clear opportunities identified.",
        )
        next_actions = AnalysisAgent.normalize_string_list(
            payload.get("next_actions"),
            default_item="Review the analysis with the user.",
        )
        return {
            "summary": summary or "No summary returned.",
            "risks": risks[:4],
            "opportunities": opportunities[:4],
            "next_actions": next_actions[:4],
        }

    @staticmethod
    def render_output(payload: Dict[str, Any]) -> str:
        return (
            f"Summary: {payload['summary']}\n\n"
            "Risks:\n"
            + "\n".join(f"- {item}" for item in payload["risks"])
            + "\n\nOpportunities:\n"
            + "\n".join(f"- {item}" for item in payload["opportunities"])
            + "\n\nNext actions:\n"
            + "\n".join(f"- {item}" for item in payload["next_actions"])
        )
