import json
from typing import Any, Dict

from app.agents.planner import QueryPlanner
from app.models.schemas import AnalysisAgentResponse, AgentQueryResponse, MidStageWorkflowResponse
from app.services.analytics_service import AnalyticsService
from app.services.llm_service import LLMService


class AgentOrchestrator:
    def __init__(self):
        self.planner = QueryPlanner()
        self.service = AnalyticsService()
        self.llm_service = LLMService()

    def run(self, question: str, top_k: int) -> AgentQueryResponse:
        plan = self.planner.build_plan(question)

        data: Dict[str, Any] = {}
        for step in plan:
            if step.action == "graph_merchant_risk":
                data["risky_merchants"] = self.service.get_risky_merchants(limit=top_k)
            elif step.action == "tabular_user_or_txn_analytics":
                data["recent_transactions"] = self.service.get_recent_transactions(limit=top_k)
            else:
                data["recent_transactions"] = self.service.get_recent_transactions(limit=top_k)
                data["risky_merchants"] = self.service.get_risky_merchants(limit=min(top_k, 10))

        answer = self._build_answer(question=question, data=data)
        return AgentQueryResponse(answer=answer, plan=plan, data=data)

    def run_mid_stage_workflow(self, user_id: int, question: str, top_k: int) -> MidStageWorkflowResponse:
        # Analysis Agent
        analysis = {
            "recent_transactions": self.service.get_recent_transactions(limit=top_k),
            "risky_merchants": self.service.get_risky_merchants(limit=min(top_k, 20)),
            "user_spending_graph": self.service.get_user_spending_graph(user_id=user_id),
            "optimization": self.service.get_user_optimization(user_id=user_id),
        }

        # Planning Agent
        planning = self.planner.build_plan(question)

        # Policy Agent
        policy = self.service.get_user_policy_compliance(user_id=user_id)

        # Explanation Agent
        explanation = self._build_mid_stage_explanation(
            question=question,
            planning=planning,
            policy=policy,
            optimization=analysis["optimization"],
        )

        return MidStageWorkflowResponse(
            user_id=user_id,
            analysis=analysis,
            planning=planning,
            policy=policy,
            explanation=explanation,
        )

    def run_analysis_agent(
        self,
        user_id: int,
    ) -> AnalysisAgentResponse:
        system_prompt = (
            "You are a financial analysis agent for Agent CFO. "
            "Use only the provided user transaction and graph data. "
            "Keep the analysis simple, specific, and actionable."
        )
        supporting_data: Dict[str, Any] = {
            "user_spending_summary": self.service.get_user_spending_summary(user_id=user_id),
            "user_spending_graph": self.service.get_user_spending_graph(user_id=user_id),
            "optimization": self.service.get_user_optimization(user_id=user_id),
            "policy": self.service.get_user_policy_compliance(user_id=user_id),
            "recent_transactions": self.service.get_user_recent_transactions(user_id=user_id, limit=10),
        }
        prompt = self._build_analysis_prompt(supporting_data=supporting_data, user_id=user_id)
        input_tokens = self.llm_service.count_generate_input_tokens(
            prompt=prompt,
            system_prompt=system_prompt,
        )

        print(f"[analysis-agent] user_id={user_id} input_tokens={input_tokens}")

        analysis = self.llm_service.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.2,
            max_tokens=1500,
        )

        return AnalysisAgentResponse(
            user_id=user_id,
            input_tokens=input_tokens,
            analysis=analysis,
            supporting_data=supporting_data,
        )

    @staticmethod
    def _build_answer(question: str, data: Dict[str, Any]) -> str:
        parts = [f"Query: {question}"]
        if "recent_transactions" in data:
            parts.append(f"Recent transactions fetched: {len(data['recent_transactions'])}")
        if "risky_merchants" in data:
            parts.append(f"Risk-ranked merchants fetched: {len(data['risky_merchants'])}")
        return " | ".join(parts)

    @staticmethod
    def _build_mid_stage_explanation(
        question: str,
        planning: list[Any],
        policy: Dict[str, Any],
        optimization: Dict[str, Any],
    ) -> str:
        total_suggestions = len(optimization.get("suggestions", []))
        plan_actions = ", ".join(step.action for step in planning) if planning else "none"
        return (
            f"Workflow executed for question: {question}. "
            f"Planned actions: {plan_actions}. "
            f"Policy status: {policy.get('overall_status')} (score {policy.get('score')}). "
            f"Optimization suggestions generated: {total_suggestions}."
        )

    @staticmethod
    def _build_analysis_prompt(supporting_data: Dict[str, Any], user_id: int) -> str:
        summary = supporting_data.get("user_spending_summary", {})
        graph = supporting_data.get("user_spending_graph", {})
        optimization = supporting_data.get("optimization", {})
        policy = supporting_data.get("policy", {})
        recent_transactions = supporting_data.get("recent_transactions", [])

        top_categories = (graph.get("categories") or [])[:5]
        top_suggestions = (optimization.get("suggestions") or [])[:5]
        policy_rules = [
            rule
            for rule in (policy.get("rules") or [])
            if rule.get("status") != "compliant"
        ][:5]
        recent_tx_lines = [
            (
                f"- {tx.get('txn_ts')}: amount={tx.get('amount')}, "
                f"merchant_id={tx.get('merchant_id')}, mcc={tx.get('mcc')}"
            )
            for tx in recent_transactions[:5]
        ]
        category_lines = [
            (
                f"- {cat.get('category')}: amount={cat.get('amount')}, "
                f"share={cat.get('percentage')}%, txns={cat.get('transaction_count')}"
            )
            for cat in top_categories
        ]
        suggestion_lines = [
            (
                f"- {item.get('title')}: priority={item.get('priority')}, "
                f"estimated_savings={item.get('estimated_savings')}"
            )
            for item in top_suggestions
        ]
        policy_lines = [
            f"- {rule.get('name')}: status={rule.get('status')}, detail={rule.get('detail')}"
            for rule in policy_rules
        ]

        return (
            f"Analyze user {user_id} based on the summary below.\n\n"
            "User summary:\n"
            f"- total_spend={summary.get('total_spend')}\n"
            f"- txn_count={summary.get('txn_count')}\n"
            f"- avg_ticket={summary.get('avg_ticket')}\n"
            f"- first_txn_ts={summary.get('first_txn_ts')}\n"
            f"- last_txn_ts={summary.get('last_txn_ts')}\n\n"
            "Graph summary:\n"
            f"- graph_total_spend={graph.get('total_spend')}\n"
            f"- recurring_payments={graph.get('recurring_payments')}\n"
            f"- subscriptions={graph.get('subscriptions')}\n"
            "Top categories:\n"
            f"{chr(10).join(category_lines) if category_lines else '- none'}\n\n"
            "Recent transactions:\n"
            f"{chr(10).join(recent_tx_lines) if recent_tx_lines else '- none'}\n\n"
            "Optimization suggestions:\n"
            f"{chr(10).join(suggestion_lines) if suggestion_lines else '- none'}\n\n"
            "Policy summary:\n"
            f"- overall_status={policy.get('overall_status')}\n"
            f"- score={policy.get('score')}\n"
            "Policy issues:\n"
            f"{chr(10).join(policy_lines) if policy_lines else '- none'}\n\n"
            "Write a short analysis with these sections:\n"
            "1. Summary\n"
            "2. Risks\n"
            "3. Opportunities\n"
            "4. Next actions\n"
            "Keep each section brief and grounded in the data."
        )
