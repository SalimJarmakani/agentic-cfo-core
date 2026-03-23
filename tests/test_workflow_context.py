import unittest
from unittest.mock import Mock

from app.agents.orchestrator import AgentOrchestrator
from app.agents.workflow_agents.analysis_agent import AnalysisAgent
from app.services.analytics_service import AnalyticsService


class AnalyticsServiceTests(unittest.TestCase):
    def test_normalize_spending_summary_row_adds_window_metrics(self) -> None:
        row = {
            "user_id": 42,
            "txn_count": 120,
            "total_spend": 24000.0,
            "avg_ticket": 200.0,
            "active_months": 36,
            "observed_span_months": 120,
            "first_txn_ts": "2015-01-01T00:00:00+00:00",
            "last_txn_ts": "2024-12-31T00:00:00+00:00",
        }

        summary = AnalyticsService._normalize_spending_summary_row(row=row, user_id=42)

        self.assertEqual(summary["observed_span_months"], 120)
        self.assertEqual(summary["active_months"], 36)
        self.assertEqual(summary["observed_monthly_avg_spend"], 200.0)
        self.assertEqual(summary["observed_monthly_avg_txn_count"], 1.0)
        self.assertEqual(summary["analysis_period"], "all-time")

    def test_build_spend_window_detail_mentions_range(self) -> None:
        detail = AnalyticsService._build_spend_window_detail(
            observed_span_months=120,
            first_txn_ts="2015-01-01T00:00:00+00:00",
            last_txn_ts="2024-12-31T00:00:00+00:00",
        )

        self.assertIn("120-month history", detail)
        self.assertIn("2015-01-01T00:00:00+00:00", detail)
        self.assertIn("2024-12-31T00:00:00+00:00", detail)


class WorkflowPayloadTests(unittest.TestCase):
    def test_analysis_prompt_has_time_horizon_rules(self) -> None:
        prompt = AnalysisAgent._build_prompt(
            {
                "time_context": {"analysis_period": "all-time"},
                "summary_metrics": {"total_spend": 1000},
                "policy_snapshot": {},
            }
        )

        self.assertIn("Use observed_monthly_avg_spend for any monthly framing.", prompt)
        self.assertIn("Do not restate a spending-to-income or debt-to-income ratio", prompt)

    def test_orchestrator_builds_richer_analysis_payload(self) -> None:
        orchestrator = AgentOrchestrator.__new__(AgentOrchestrator)
        orchestrator.service = Mock()
        orchestrator.service.get_user_spending_summary.return_value = {
            "user_id": 7,
            "txn_count": 1200,
            "total_spend": 291534.0,
            "avg_ticket": 27.47,
            "observed_monthly_avg_spend": 2429.45,
            "observed_monthly_avg_txn_count": 10.0,
            "first_txn_ts": "2015-01-01T00:00:00+00:00",
            "last_txn_ts": "2024-12-31T00:00:00+00:00",
            "observed_span_months": 120,
            "active_months": 118,
            "analysis_period": "all-time",
        }
        orchestrator.service.get_user_spending_graph.return_value = {
            "categories": [{"category": f"cat-{i}", "amount": i, "percentage": i, "transaction_count": i} for i in range(1, 7)]
        }
        orchestrator.service.get_user_optimization.return_value = {
            "suggestions": [
                {
                    "title": f"opt-{i}",
                    "priority": "medium",
                    "estimated_savings": float(i),
                    "description": f"desc-{i}",
                }
                for i in range(1, 6)
            ]
        }
        orchestrator.service.get_user_policy_compliance.return_value = {
            "overall_status": "warning",
            "score": 48,
            "metrics": {
                "yearly_income": 60000.0,
                "monthly_income": 5000.0,
                "total_debt": 12000.0,
                "credit_score": 710,
                "spend_to_income_ratio_pct": 48.6,
                "debt_to_income_ratio_pct": 20.0,
                "fraud_exposure_pct": 3.0,
                "top_category_concentration_pct": 22.0,
            },
            "rules": [
                {"name": "Spending-to-Income Ratio", "status": "warning", "detail": "Observed monthly average spend is 49% of monthly income."}
            ],
        }
        orchestrator.service.get_user_recent_transactions.return_value = [
            {
                "txn_ts": f"2024-12-{day:02d}T00:00:00+00:00",
                "amount": float(day),
                "merchant_id": 1000 + day,
                "mcc": 5411,
            }
            for day in range(1, 21)
        ]

        payload = orchestrator._build_analysis_input_payload(user_id=7, question="Assess the account.")

        self.assertEqual(payload["time_context"]["observed_span_months"], 120)
        self.assertEqual(payload["summary_metrics"]["observed_monthly_avg_spend"], 2429.45)
        self.assertEqual(payload["financial_profile"]["monthly_income"], 5000.0)
        self.assertEqual(payload["policy_snapshot"]["metrics"]["spend_to_income_ratio_pct"], 48.6)
        self.assertEqual(len(payload["top_categories"]), 5)
        self.assertEqual(len(payload["optimization_suggestions"]), 4)
        self.assertEqual(len(payload["recent_transactions"]), 12)


if __name__ == "__main__":
    unittest.main()
