import json
import logging
from typing import Any, Dict

from app.agents.workflow_agents.base import LLMJsonAgent
from app.services.llm_service import LLMServiceError


logger = logging.getLogger(__name__)


class PolicyAgent(LLMJsonAgent):
    MAX_TOKENS = 1200
    RETRY_MAX_TOKENS = 800
    SYSTEM_PROMPT = (
        "You are a policy review agent for Agent CFO. "
        "Review the proposed plan using only the provided workflow and user-policy data. "
        "Apply simplified Singapore-style AI governance checks with cautious judgment. "
        "Return valid JSON only."
    )

    AUTONOMY_KEYWORDS = {"automatically", "autonomous", "auto-approve", "without review", "bypass", "override"}
    PRIVACY_KEYWORDS = {"cvv", "password", "pin", "secret", "ssn", "social security", "card number"}
    PROTECTED_ATTRIBUTE_KEYWORDS = {"gender", "age", "race", "religion", "ethnicity", "marital status"}
    AI_RISK_CHECK_ORDER = (
        ("sg-1", "Human oversight and accountability"),
        ("sg-2", "Transparency and explainability"),
        ("sg-3", "Data minimization and privacy"),
        ("sg-4", "Fairness and non-discrimination"),
        ("sg-5", "Safety and harm prevention"),
    )

    def run(self, policy_input: Dict[str, Any]) -> Dict[str, Any]:
        prompt = self._build_prompt(policy_input=policy_input)
        input_tokens = self.count_input_tokens(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            max_tokens=self.MAX_TOKENS,
        )
        generation_warning = ""

        try:
            parsed_output = self.generate_json(
                prompt=prompt,
                system_prompt=self.SYSTEM_PROMPT,
                temperature=0.2,
                max_tokens=self.MAX_TOKENS,
            )
        except LLMServiceError as exc:
            if not self._is_recoverable_generation_error(exc):
                raise
            generation_warning = str(exc)
            logger.warning("PolicyAgent primary generation failed; retrying with compact prompt. error=%s", exc)
            parsed_output = self._retry_or_fallback(policy_input=policy_input, initial_error=exc)

        output_payload = self._normalize_output(parsed_output=parsed_output, policy_input=policy_input)
        output_payload["input_tokens"] = input_tokens
        if generation_warning:
            output_payload["generation_warning"] = generation_warning
        output_payload["policy_review"] = self.render_output(output_payload)
        return self.normalize_json_value(output_payload)

    @staticmethod
    def _build_prompt(policy_input: Dict[str, Any]) -> str:
        return (
            "Review the workflow and proposed plan below for policy compliance and respond with JSON only.\n\n"
            f"{json.dumps(policy_input, indent=2, default=str)}\n\n"
            "Policy review rules:\n"
            "- Treat user_policy rules and metrics as the source of truth for financial policy status.\n"
            "- Do not recompute financial ratios or invent additional user-policy violations.\n"
            "- Focus on governance checks for the proposed plan and how it should be constrained.\n\n"
            "- Return exactly 5 ai_risk_checks using ids sg-1 through sg-5.\n"
            "- Keep each detail short and concrete.\n"
            "- Include only the most material user_policy findings, prioritizing warnings or violations.\n"
            "- Keep the whole response compact so it fits comfortably within the token limit.\n\n"
            "Return this schema:\n"
            "{\n"
            '  "summary": "short review summary",\n'
            '  "ai_risk_checks": [\n'
            '    {"id": "sg-1", "name": "Human oversight and accountability", "status": "pass", "detail": "reason"}\n'
            "  ],\n"
            '  "user_policy_findings": [\n'
            '    {"name": "rule name", "status": "warning", "detail": "reason"}\n'
            "  ],\n"
            '  "guardrails": ["guardrail 1", "guardrail 2"],\n'
            '  "blocked_actions": ["blocked action 1", "blocked action 2"]\n'
            "}\n"
            "Rules:\n"
            "- Use only statuses pass, review, or block for ai_risk_checks.\n"
            "- Use only statuses compliant, warning, or violation for user_policy_findings.\n"
            "- Evaluate recommendation-only behavior, explainability, privacy, fairness, and harm prevention.\n"
            "- If the user policy context contains warnings or violations, reflect that in the findings and caution level.\n"
            "- Keep the review concise and grounded in the supplied data."
        )

    @classmethod
    def _build_retry_prompt(cls, policy_input: Dict[str, Any]) -> str:
        planning = policy_input.get("planning") or {}
        user_policy = policy_input.get("user_policy") or {}
        compact_payload = {
            "question": policy_input.get("question"),
            "analysis_summary": policy_input.get("analysis_summary"),
            "planning_goal": planning.get("goal"),
            "planning_actions": [
                {
                    "title": item.get("title"),
                    "priority": item.get("priority"),
                }
                for item in (planning.get("actions") or [])[:3]
                if isinstance(item, dict)
            ],
            "user_policy_status": user_policy.get("overall_status"),
            "user_policy_rules": [
                {
                    "name": rule.get("name"),
                    "status": rule.get("status"),
                    "detail": rule.get("detail"),
                }
                for rule in (user_policy.get("rules") or [])[:4]
            ],
        }
        return (
            "Return a compact JSON-only policy review for this workflow.\n\n"
            f"{json.dumps(compact_payload, indent=2, default=str)}\n\n"
            "Constraints:\n"
            "- Output exactly 5 ai_risk_checks with ids sg-1 to sg-5.\n"
            "- Keep each detail under 16 words.\n"
            "- Include at most 3 user_policy_findings.\n"
            "- Include at most 4 guardrails.\n"
            "- Include at most 3 blocked_actions.\n"
            "- Return valid JSON only.\n\n"
            "Schema:\n"
            "{\n"
            '  "summary": "short review summary",\n'
            '  "ai_risk_checks": [\n'
            '    {"id": "sg-1", "name": "Human oversight and accountability", "status": "pass", "detail": "reason"}\n'
            "  ],\n"
            '  "user_policy_findings": [\n'
            '    {"name": "rule name", "status": "warning", "detail": "reason"}\n'
            "  ],\n"
            '  "guardrails": ["guardrail 1"],\n'
            '  "blocked_actions": ["blocked action 1"]\n'
            "}"
        )

    def _retry_or_fallback(self, policy_input: Dict[str, Any], initial_error: Exception) -> Dict[str, Any]:
        retry_prompt = self._build_retry_prompt(policy_input=policy_input)
        try:
            return self.generate_json(
                prompt=retry_prompt,
                system_prompt=self.SYSTEM_PROMPT,
                temperature=0.0,
                max_tokens=self.RETRY_MAX_TOKENS,
            )
        except Exception as retry_exc:  # noqa: BLE001
            logger.warning(
                "PolicyAgent retry generation failed; using deterministic fallback. initial_error=%s retry_error=%s",
                initial_error,
                retry_exc,
            )
            return {}

    @staticmethod
    def _is_recoverable_generation_error(error: Exception) -> bool:
        message = str(error).lower()
        return (
            "did not include text content" in message
            or "did not include valid json" in message
            or "json must be an object" in message
        )

    @classmethod
    def _normalize_output(cls, parsed_output: Dict[str, Any], policy_input: Dict[str, Any]) -> Dict[str, Any]:
        user_policy = policy_input.get("user_policy") or {}
        user_policy_status = cls._normalize_user_policy_status(user_policy.get("overall_status"))
        user_policy_score = cls._coerce_int(user_policy.get("score"))
        default_checks = cls._default_ai_risk_checks(policy_input=policy_input, user_policy_status=user_policy_status)
        ai_risk_checks = cls._normalize_ai_risk_checks(parsed_output.get("ai_risk_checks"), default_checks)
        user_policy_findings = cls._normalize_user_policy_findings(
            parsed_output.get("user_policy_findings"),
            user_policy=user_policy,
        )
        guardrails = cls._normalize_guardrails(parsed_output.get("guardrails"), user_policy_status=user_policy_status)
        blocked_actions = cls._normalize_blocked_actions(parsed_output.get("blocked_actions"), user_policy_status)

        severe_user_policy = user_policy_status == "violation"
        approved = not any(item["status"] == "block" for item in ai_risk_checks) and not severe_user_policy
        requires_human_review = severe_user_policy or any(item["status"] != "pass" for item in ai_risk_checks)
        summary = str(parsed_output.get("summary", "")).strip() or cls._build_summary(
            approved=approved,
            requires_human_review=requires_human_review,
            user_policy_status=user_policy_status,
            user_policy_findings=user_policy_findings,
        )

        return {
            "approved": approved,
            "requires_human_review": requires_human_review,
            "summary": summary,
            "ai_risk_checks": ai_risk_checks,
            "user_policy_status": user_policy_status,
            "user_policy_score": user_policy_score,
            "user_policy_findings": user_policy_findings,
            "guardrails": guardrails,
            "blocked_actions": blocked_actions,
        }

    @classmethod
    def _default_ai_risk_checks(cls, policy_input: Dict[str, Any], user_policy_status: str) -> list[Dict[str, str]]:
        planning_output = policy_input.get("planning") or {}
        combined_text = cls._combined_text(policy_input=policy_input)
        autonomy_flag = cls._contains_keyword(combined_text, cls.AUTONOMY_KEYWORDS)
        privacy_flag = cls._contains_keyword(combined_text, cls.PRIVACY_KEYWORDS)
        fairness_flag = cls._contains_keyword(combined_text, cls.PROTECTED_ATTRIBUTE_KEYWORDS)
        weak_user_policy = user_policy_status in {"warning", "violation"}

        return [
            {
                "id": "sg-1",
                "name": "Human oversight and accountability",
                "status": "block" if autonomy_flag else "pass",
                "detail": (
                    "Potential autonomous or review-bypassing language was detected in the plan."
                    if autonomy_flag
                    else "The workflow remains recommendation-only and keeps humans in control."
                ),
            },
            {
                "id": "sg-2",
                "name": "Transparency and explainability",
                "status": "pass" if planning_output.get("actions") else "review",
                "detail": (
                    "The plan includes explicit actions that can be reviewed by the user."
                    if planning_output.get("actions")
                    else "The plan needs clearer actions before it should be used."
                ),
            },
            {
                "id": "sg-3",
                "name": "Data minimization and privacy",
                "status": "block" if privacy_flag else "pass",
                "detail": (
                    "Sensitive data terms were detected and should not appear in workflow output."
                    if privacy_flag
                    else "The workflow avoids unnecessary card, credential, and secret data."
                ),
            },
            {
                "id": "sg-4",
                "name": "Fairness and non-discrimination",
                "status": "review" if fairness_flag else "pass",
                "detail": (
                    "Protected-attribute language was detected and should be removed from the plan."
                    if fairness_flag
                    else "No protected attributes were used as recommendation drivers."
                ),
            },
            {
                "id": "sg-5",
                "name": "Safety and harm prevention",
                "status": "review" if weak_user_policy else "pass",
                "detail": (
                    "User policy already shows warnings or violations, so the plan should focus on remediation first."
                    if weak_user_policy
                    else "No elevated harm signal was detected from user policy compliance."
                ),
            },
        ]

    @classmethod
    def _normalize_ai_risk_checks(cls, value: Any, defaults: list[Dict[str, str]]) -> list[Dict[str, str]]:
        normalized_by_id = {item["id"]: dict(item) for item in defaults}
        fallback_ids = [item["id"] for item in defaults]

        if isinstance(value, list):
            for index, item in enumerate(value):
                if not isinstance(item, dict):
                    continue
                raw_id = str(item.get("id", "")).strip().lower()
                item_id = raw_id if raw_id in normalized_by_id else fallback_ids[index] if index < len(fallback_ids) else ""
                if not item_id:
                    continue
                default_item = normalized_by_id[item_id]
                name = str(item.get("name", "")).strip() or default_item["name"]
                status = cls._normalize_ai_risk_status(item.get("status")) or default_item["status"]
                detail = str(item.get("detail", "")).strip() or default_item["detail"]
                normalized_by_id[item_id] = {
                    "id": item_id,
                    "name": name,
                    "status": status,
                    "detail": detail,
                }

        return [normalized_by_id[item_id] for item_id, _ in cls.AI_RISK_CHECK_ORDER]

    @classmethod
    def _normalize_user_policy_findings(cls, value: Any, user_policy: Dict[str, Any]) -> list[Dict[str, str]]:
        findings: list[Dict[str, str]] = []
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip()
                status = cls._normalize_user_policy_status(item.get("status"))
                detail = str(item.get("detail", "")).strip()
                if name and detail:
                    findings.append({"name": name, "status": status, "detail": detail})

        if findings:
            return findings[:4]

        fallback_findings = [
            {
                "name": str(rule.get("name", "Unnamed rule")).strip() or "Unnamed rule",
                "status": cls._normalize_user_policy_status(rule.get("status")),
                "detail": str(rule.get("detail", "No detail provided.")).strip() or "No detail provided.",
            }
            for rule in (user_policy.get("rules") or [])
            if cls._normalize_user_policy_status(rule.get("status")) != "compliant"
        ][:4]

        if fallback_findings:
            return fallback_findings

        return [
            {
                "name": "User financial policy",
                "status": "compliant",
                "detail": "No user policy issues were detected in the current data snapshot.",
            }
        ]

    @classmethod
    def _normalize_guardrails(cls, value: Any, user_policy_status: str) -> list[str]:
        defaults = [
            "Keep the system recommendation-only. Do not imply autonomous execution of payments, credit actions, or account changes.",
            "Ground every recommendation in the supplied analysis, plan, and user policy data.",
            "Do not include unnecessary card, credential, or personally sensitive details in outputs.",
            "Do not use protected attributes such as age or gender as decision criteria.",
        ]
        if user_policy_status in {"warning", "violation"}:
            defaults.append(
                "Prioritize remediation of user-policy warnings or violations before discretionary spend growth or new debt."
            )
        return cls._merge_string_list(value, defaults, limit=6)

    @classmethod
    def _normalize_blocked_actions(cls, value: Any, user_policy_status: str) -> list[str]:
        defaults = [
            "Do not execute or suggest autonomous financial actions without human approval.",
            "Do not recommend bypassing an existing risk, fraud, or policy warning.",
        ]
        if user_policy_status == "violation":
            defaults.append(
                "Do not recommend higher debt, higher discretionary spend, or other risk-increasing actions until violations are resolved."
            )
        return cls._merge_string_list(value, defaults, limit=6)

    @classmethod
    def _merge_string_list(cls, value: Any, defaults: list[str], limit: int) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for item in cls.normalize_string_list(value, default_item=defaults[0]):
            normalized = item.strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
        for item in defaults:
            if item not in seen:
                seen.add(item)
                merged.append(item)
        return merged[:limit]

    @staticmethod
    def _combined_text(policy_input: Dict[str, Any]) -> str:
        planning_output = policy_input.get("planning") or {}
        action_text = " ".join(
            f"{item.get('title', '')} {item.get('details', '')}"
            for item in (planning_output.get("actions") or [])
            if isinstance(item, dict)
        )
        parts = [
            str(policy_input.get("question", "")),
            str(policy_input.get("analysis_summary", "")),
            str(planning_output.get("goal", "")),
            action_text,
            " ".join(str(item) for item in (planning_output.get("checkpoints") or [])),
        ]
        return " ".join(part.lower() for part in parts if part)

    @staticmethod
    def _contains_keyword(text: str, keywords: set[str]) -> bool:
        return any(keyword in text for keyword in keywords)

    @staticmethod
    def _normalize_ai_risk_status(value: Any) -> str:
        status = str(value or "").strip().lower()
        if status in {"pass", "review", "block"}:
            return status
        return "review"

    @staticmethod
    def _normalize_user_policy_status(value: Any) -> str:
        status = str(value or "").strip().lower()
        if status in {"compliant", "warning", "violation"}:
            return status
        return "warning"

    @staticmethod
    def _coerce_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _build_summary(
        approved: bool,
        requires_human_review: bool,
        user_policy_status: str,
        user_policy_findings: list[Dict[str, str]],
    ) -> str:
        if approved and not requires_human_review:
            return (
                "Policy review passed. The plan aligns with simplified Singapore-style AI risk guardrails "
                f"and the user's policy status is '{user_policy_status}'."
            )
        if approved:
            return (
                "Policy review passed with caution. The plan can be shown to the user, but human review is "
                f"required because the user policy status is '{user_policy_status}' or a guardrail needs attention."
            )
        primary_issue = user_policy_findings[0]["detail"] if user_policy_findings else "A blocking policy issue was detected."
        return f"Policy review found a blocking issue. Primary issue: {primary_issue}"

    @staticmethod
    def render_output(payload: Dict[str, Any]) -> str:
        ai_checks = "\n".join(
            f"- [{item['status']}] {item['name']}: {item['detail']}"
            for item in payload["ai_risk_checks"]
        )
        policy_findings = "\n".join(
            f"- [{item['status']}] {item['name']}: {item['detail']}"
            for item in payload["user_policy_findings"]
        )
        guardrails = "\n".join(f"- {item}" for item in payload["guardrails"])
        blocked_actions = "\n".join(f"- {item}" for item in payload["blocked_actions"])
        return (
            f"Summary: {payload['summary']}\n\n"
            "Simplified Singapore AI risk checks:\n"
            f"{ai_checks}\n\n"
            "User policy findings:\n"
            f"{policy_findings}\n\n"
            "Guardrails:\n"
            f"{guardrails}\n\n"
            "Blocked actions:\n"
            f"{blocked_actions}"
        )
