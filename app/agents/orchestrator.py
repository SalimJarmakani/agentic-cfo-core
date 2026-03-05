from typing import Any, Dict

from app.agents.planner import QueryPlanner
from app.models.schemas import AgentQueryResponse
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

    @staticmethod
    def _build_answer(question: str, data: Dict[str, Any]) -> str:
        parts = [f"Query: {question}"]
        if "recent_transactions" in data:
            parts.append(f"Recent transactions fetched: {len(data['recent_transactions'])}")
        if "risky_merchants" in data:
            parts.append(f"Risk-ranked merchants fetched: {len(data['risky_merchants'])}")
        return " | ".join(parts)
