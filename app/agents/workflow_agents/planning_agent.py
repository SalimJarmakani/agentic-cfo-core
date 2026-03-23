import json
from typing import Any, Dict

from app.agents.workflow_agents.base import LLMJsonAgent


class PlanningAgent(LLMJsonAgent):
    SYSTEM_PROMPT = (
        "You are a financial planning agent for Agent CFO. "
        "Create a short, actionable plan based only on the analysis findings and policy constraints. "
        "Do not introduce unsupported ratios, savings, or time claims. "
        "Return valid JSON only."
    )

    def run(self, planning_input: Dict[str, Any]) -> Dict[str, Any]:
        prompt = self._build_prompt(planning_input=planning_input)
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
        output_payload["plan"] = self.render_output(output_payload)
        return output_payload

    @staticmethod
    def _build_prompt(planning_input: Dict[str, Any]) -> str:
        return (
            "Create a short actionable plan from the analysis findings and policy context below "
            "and respond with JSON only.\n\n"
            f"{json.dumps(planning_input, indent=2, default=str)}\n\n"
            "Planning rules:\n"
            "- Base the plan on the supplied summary, risks, opportunities, next actions, and user policy findings.\n"
            "- If user_policy_status is warning or violation, prioritize remediation before discretionary optimization.\n"
            "- Keep actions concrete and recommendation-only.\n"
            "- Do not invent numerical savings, deadlines, or metrics that are not in the input.\n\n"
            "Return this schema:\n"
            "{\n"
            '  "goal": "single sentence goal",\n'
            '  "actions": [\n'
            '    {"title": "action", "details": "what to do", "priority": "high"}\n'
            "  ],\n"
            '  "checkpoints": ["checkpoint 1", "checkpoint 2"]\n'
            "}\n"
            "Use 3 to 5 actions, keep the wording practical, and account for any policy issues or guardrails."
        )

    @staticmethod
    def _normalize_output(payload: Dict[str, Any]) -> Dict[str, Any]:
        goal = str(payload.get("goal", "")).strip() or "Create a practical financial improvement plan."
        checkpoints = PlanningAgent.normalize_string_list(
            payload.get("checkpoints"),
            default_item="Review progress after the first action is completed.",
        )[:4]

        actions: list[Dict[str, str]] = []
        for item in payload.get("actions") or []:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            details = str(item.get("details", "")).strip()
            priority = PlanningAgent.normalize_priority(item.get("priority"))
            if title and details:
                actions.append({"title": title, "details": details, "priority": priority})

        if not actions:
            actions = [
                {
                    "title": "Review analysis findings",
                    "details": "Validate the biggest risk and the clearest opportunity before taking action.",
                    "priority": "high",
                }
            ]

        return {
            "goal": goal,
            "actions": actions[:5],
            "checkpoints": checkpoints,
        }

    @staticmethod
    def render_output(payload: Dict[str, Any]) -> str:
        actions = "\n".join(
            f"- [{item['priority']}] {item['title']}: {item['details']}"
            for item in payload["actions"]
        )
        checkpoints = "\n".join(f"- {item}" for item in payload["checkpoints"])
        return f"Goal: {payload['goal']}\n\nActions:\n{actions}\n\nCheckpoints:\n{checkpoints}"
