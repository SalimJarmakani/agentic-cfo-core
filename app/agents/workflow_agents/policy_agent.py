from typing import Any, Dict

from app.agents.workflow_agents.base import LLMJsonAgent


class PolicyAgent:
    AUTONOMY_KEYWORDS = {"automatically", "autonomous", "auto-approve", "without review", "bypass", "override"}
    PRIVACY_KEYWORDS = {"cvv", "password", "pin", "secret", "ssn", "social security", "card number"}
    PROTECTED_ATTRIBUTE_KEYWORDS = {"gender", "age", "race", "religion", "ethnicity", "marital status"}

    def run(self, policy_input: Dict[str, Any]) -> Dict[str, Any]:
        planning_output = policy_input.get("planning") or {}
        user_policy = policy_input.get("user_policy") or {}
        combined_text = self._combined_text(policy_input=policy_input)

        autonomy_flag = self._contains_keyword(combined_text, self.AUTONOMY_KEYWORDS)
        privacy_flag = self._contains_keyword(combined_text, self.PRIVACY_KEYWORDS)
        fairness_flag = self._contains_keyword(combined_text, self.PROTECTED_ATTRIBUTE_KEYWORDS)
        weak_user_policy = str(user_policy.get("overall_status", "")).strip().lower() in {"warning", "violation"}
        severe_user_policy = str(user_policy.get("overall_status", "")).strip().lower() == "violation"

        ai_risk_checks = [
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

        user_policy_findings = [
            {
                "name": str(rule.get("name", "Unnamed rule")).strip(),
                "status": str(rule.get("status", "warning")).strip().lower() or "warning",
                "detail": str(rule.get("detail", "No detail provided.")).strip() or "No detail provided.",
            }
            for rule in (user_policy.get("rules") or [])
            if str(rule.get("status", "")).strip().lower() != "compliant"
        ][:4]

        if not user_policy_findings:
            user_policy_findings = [
                {
                    "name": "User financial policy",
                    "status": "compliant",
                    "detail": "No user policy issues were detected in the current data snapshot.",
                }
            ]

        guardrails = [
            "Keep the system recommendation-only. Do not imply autonomous execution of payments, credit actions, or account changes.",
            "Ground every recommendation in the supplied analysis, plan, and user policy data.",
            "Do not include unnecessary card, credential, or personally sensitive details in outputs.",
            "Do not use protected attributes such as age or gender as decision criteria.",
        ]
        if weak_user_policy:
            guardrails.append(
                "Prioritize remediation of user-policy warnings or violations before discretionary spend growth or new debt."
            )

        blocked_actions = [
            "Do not execute or suggest autonomous financial actions without human approval.",
            "Do not recommend bypassing an existing risk, fraud, or policy warning.",
        ]
        if privacy_flag:
            blocked_actions.append("Do not output card numbers, PINs, CVVs, passwords, or similar secrets.")
        if severe_user_policy:
            blocked_actions.append(
                "Do not recommend higher debt, higher discretionary spend, or other risk-increasing actions until violations are resolved."
            )

        approved = not any(item["status"] == "block" for item in ai_risk_checks) and not severe_user_policy
        requires_human_review = severe_user_policy or any(item["status"] != "pass" for item in ai_risk_checks)

        summary = self._build_summary(
            approved=approved,
            requires_human_review=requires_human_review,
            user_policy=user_policy,
            user_policy_findings=user_policy_findings,
        )

        output = {
            "approved": approved,
            "requires_human_review": requires_human_review,
            "summary": summary,
            "ai_risk_checks": ai_risk_checks,
            "user_policy_status": str(user_policy.get("overall_status", "warning")).strip().lower() or "warning",
            "user_policy_score": int(user_policy.get("score", 0) or 0),
            "user_policy_findings": user_policy_findings,
            "guardrails": guardrails,
            "blocked_actions": blocked_actions,
        }
        output["policy_review"] = self.render_output(output)
        return LLMJsonAgent.normalize_json_value(output)

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
    def _build_summary(
        approved: bool,
        requires_human_review: bool,
        user_policy: Dict[str, Any],
        user_policy_findings: list[Dict[str, str]],
    ) -> str:
        status = str(user_policy.get("overall_status", "warning")).strip().lower() or "warning"
        if approved and not requires_human_review:
            return (
                "Policy review passed. The plan aligns with simplified Singapore-style AI risk guardrails "
                f"and the user's policy status is '{status}'."
            )
        if approved:
            return (
                "Policy review passed with caution. The plan can be shown to the user, but human review is "
                f"required because the user policy status is '{status}' or a guardrail needs attention."
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
