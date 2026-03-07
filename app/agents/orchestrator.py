from typing import Any, Dict

from app.agents.planner import QueryPlanner
from app.models.schemas import AgentQueryResponse, MidStageWorkflowResponse
from app.services.analytics_service import AnalyticsService


class AgentOrchestrator:
    def __init__(self):
        self.planner = QueryPlanner()
        self.service = AnalyticsService()

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
