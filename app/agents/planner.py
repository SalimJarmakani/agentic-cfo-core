from app.models.schemas import PlanStep


class QueryPlanner:
    def build_plan(self, question: str) -> list[PlanStep]:
        q = question.lower()
        steps: list[PlanStep] = []

        if any(token in q for token in ["merchant", "network", "graph", "relationship"]):
            steps.append(
                PlanStep(
                    datasource="neo4j",
                    action="graph_merchant_risk",
                    details="Use graph traversal and fraud labels to rank risky merchants.",
                )
            )

        if any(token in q for token in ["user", "spend", "transaction", "amount", "history"]):
            steps.append(
                PlanStep(
                    datasource="postgres",
                    action="tabular_user_or_txn_analytics",
                    details="Use relational aggregation for transaction summaries and recent activity.",
                )
            )

        if not steps:
            steps.append(
                PlanStep(
                    datasource="hybrid",
                    action="fallback_hybrid_summary",
                    details="Combine default transaction summary with graph risk snapshot.",
                )
            )

        return steps

