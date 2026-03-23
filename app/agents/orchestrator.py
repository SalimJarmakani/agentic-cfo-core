import json
import logging
from typing import Any, Dict

from app.agents.planner import QueryPlanner
from app.agents.workflow_agents import AnalysisAgent, ExplanationAgent, PlanningAgent, PolicyAgent
from app.models.schemas import AnalysisAgentResponse, AgentQueryResponse, MidStageWorkflowResponse
from app.services.analytics_service import AnalyticsService
from app.services.llm_service import LLMService
from app.services.workflow_service import WorkflowService, WorkflowServiceError

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    def __init__(self):
        self.planner = QueryPlanner()
        self.service = AnalyticsService()
        self.llm_service = LLMService()
        self.workflow_service = WorkflowService()
        self.analysis_agent = AnalysisAgent(self.llm_service)
        self.planning_agent = PlanningAgent(self.llm_service)
        self.policy_agent = PolicyAgent(self.llm_service)
        self.explanation_agent = ExplanationAgent(self.llm_service)

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

    def start_agent_workflow(self, user_id: int, question: str) -> Dict[str, Any]:
        logger.info("Starting agent workflow for user_id=%s", user_id)
        workflow = self.workflow_service.create_workflow_run(user_id=user_id, question=question)
        workflow_run_id = int(workflow["workflow_run_id"])
        logger.info("Workflow run %s created; starting analysis step", workflow_run_id)

        try:
            analysis_input = self._build_analysis_input_payload(user_id=user_id, question=question)
            self.workflow_service.start_step(
                workflow_run_id=workflow_run_id,
                step_name="analysis",
                input_payload=analysis_input,
            )
            analysis_result = self._run_analysis_step(
                user_id=user_id,
                question=question,
                input_payload=analysis_input,
            )
            self.workflow_service.complete_step(
                workflow_run_id=workflow_run_id,
                step_name="analysis",
                output_payload=analysis_result["output_payload"],
            )
            self.workflow_service.update_workflow_status(
                workflow_run_id=workflow_run_id,
                status="waiting_for_user",
                current_stage="analysis",
            )
            logger.info("Workflow run %s moved to waiting_for_user after analysis", workflow_run_id)
        except Exception as exc:
            logger.exception("Workflow run %s failed during analysis", workflow_run_id)
            self.workflow_service.fail_step(
                workflow_run_id=workflow_run_id,
                step_name="analysis",
                error_message=str(exc),
            )
            self.workflow_service.update_workflow_status(
                workflow_run_id=workflow_run_id,
                status="failed",
                current_stage="failed",
            )
            raise

        return self.workflow_service.get_workflow(workflow_run_id)

    def list_agent_workflows(self, user_id: int, limit: int = 20) -> Dict[str, Any]:
        return {
            "user_id": user_id,
            "items": self.workflow_service.list_workflows(user_id=user_id, limit=limit),
        }

    def continue_agent_workflow(self, workflow_run_id: int) -> Dict[str, Any]:
        logger.info("Continuing workflow run %s", workflow_run_id)
        workflow = self.workflow_service.get_workflow(workflow_run_id)
        analysis_step = self.workflow_service.get_step(workflow_run_id=workflow_run_id, step_name="analysis")
        planning_step = self.workflow_service.get_step(workflow_run_id=workflow_run_id, step_name="planning")
        policy_step = self.workflow_service.get_step(workflow_run_id=workflow_run_id, step_name="policy")
        explanation_step = self.workflow_service.get_step(workflow_run_id=workflow_run_id, step_name="explanation")

        if explanation_step["status"] == "completed":
            return workflow

        if analysis_step["status"] != "completed" or not analysis_step.get("output_payload"):
            raise WorkflowServiceError("Analysis must complete before planning can run.")

        planning_output = planning_step.get("output_payload")
        if planning_step["status"] != "completed" or not planning_output:
            planning_input = self._build_planning_input(
                user_id=int(workflow["user_id"]),
                question=workflow["question"],
                analysis_output=analysis_step["output_payload"],
            )
            try:
                self.workflow_service.update_workflow_status(
                    workflow_run_id=workflow_run_id,
                    status="running",
                    current_stage="planning",
                )
                self.workflow_service.start_step(
                    workflow_run_id=workflow_run_id,
                    step_name="planning",
                    input_payload=planning_input,
                )
                planning_output = self._run_planning_step(planning_input=planning_input)
                self.workflow_service.complete_step(
                    workflow_run_id=workflow_run_id,
                    step_name="planning",
                    output_payload=planning_output,
                )
                logger.info("Workflow run %s completed planning step", workflow_run_id)
                self.workflow_service.update_workflow_status(
                    workflow_run_id=workflow_run_id,
                    status="waiting_for_user",
                    current_stage="planning",
                )
                logger.info("Workflow run %s moved to waiting_for_user after planning", workflow_run_id)
                return self.workflow_service.get_workflow(workflow_run_id)
            except Exception as exc:
                logger.exception("Workflow run %s failed during planning", workflow_run_id)
                self.workflow_service.fail_step(
                    workflow_run_id=workflow_run_id,
                    step_name="planning",
                    error_message=str(exc),
                )
                self.workflow_service.update_workflow_status(
                    workflow_run_id=workflow_run_id,
                    status="failed",
                    current_stage="failed",
                )
                raise

        policy_output = policy_step.get("output_payload")
        if policy_step["status"] != "completed" or not policy_output:
            policy_input = self._build_policy_input_payload(
                user_id=int(workflow["user_id"]),
                question=workflow["question"],
                analysis_output=analysis_step["output_payload"],
                planning_output=planning_output,
            )

            try:
                self.workflow_service.update_workflow_status(
                    workflow_run_id=workflow_run_id,
                    status="running",
                    current_stage="policy",
                )
                self.workflow_service.start_step(
                    workflow_run_id=workflow_run_id,
                    step_name="policy",
                    input_payload=policy_input,
                )
                policy_output = self._run_policy_step(policy_input=policy_input)
                self.workflow_service.complete_step(
                    workflow_run_id=workflow_run_id,
                    step_name="policy",
                    output_payload=policy_output,
                )
                logger.info("Workflow run %s completed policy step", workflow_run_id)
                self.workflow_service.update_workflow_status(
                    workflow_run_id=workflow_run_id,
                    status="waiting_for_user",
                    current_stage="policy",
                )
                logger.info("Workflow run %s moved to waiting_for_user after policy", workflow_run_id)
                return self.workflow_service.get_workflow(workflow_run_id)
            except Exception as exc:
                logger.exception("Workflow run %s failed during policy", workflow_run_id)
                self.workflow_service.fail_step(
                    workflow_run_id=workflow_run_id,
                    step_name="policy",
                    error_message=str(exc),
                )
                self.workflow_service.update_workflow_status(
                    workflow_run_id=workflow_run_id,
                    status="failed",
                    current_stage="failed",
                )
                raise

        explanation_input = self._build_explanation_input_payload(
            question=workflow["question"],
            analysis_output=analysis_step["output_payload"],
            planning_output=planning_output,
            policy_output=policy_output,
        )

        try:
            self.workflow_service.update_workflow_status(
                workflow_run_id=workflow_run_id,
                status="running",
                current_stage="explanation",
            )
            self.workflow_service.start_step(
                workflow_run_id=workflow_run_id,
                step_name="explanation",
                input_payload=explanation_input,
            )
            explanation_output = self._run_explanation_step(explanation_input=explanation_input)
            self.workflow_service.complete_step(
                workflow_run_id=workflow_run_id,
                step_name="explanation",
                output_payload=explanation_output,
            )
            self.workflow_service.update_workflow_status(
                workflow_run_id=workflow_run_id,
                status="completed",
                current_stage="done",
            )
            logger.info("Workflow run %s completed explanation step and workflow", workflow_run_id)
        except Exception as exc:
            logger.exception("Workflow run %s failed during explanation", workflow_run_id)
            self.workflow_service.fail_step(
                workflow_run_id=workflow_run_id,
                step_name="explanation",
                error_message=str(exc),
            )
            self.workflow_service.update_workflow_status(
                workflow_run_id=workflow_run_id,
                status="failed",
                current_stage="failed",
            )
            raise

        return self.workflow_service.get_workflow(workflow_run_id)

    def get_agent_workflow(self, workflow_run_id: int) -> Dict[str, Any]:
        return self.workflow_service.get_workflow(workflow_run_id)

    def run_mid_stage_workflow(self, user_id: int, question: str, top_k: int) -> MidStageWorkflowResponse:
        analysis = {
            "recent_transactions": self.service.get_recent_transactions(limit=top_k),
            "risky_merchants": self.service.get_risky_merchants(limit=min(top_k, 20)),
            "user_spending_graph": self.service.get_user_spending_graph(user_id=user_id),
            "optimization": self.service.get_user_optimization(user_id=user_id),
        }

        planning = self.planner.build_plan(question)
        policy = self.service.get_user_policy_compliance(user_id=user_id)
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

    def run_analysis_agent(self, user_id: int) -> AnalysisAgentResponse:
        result = self._run_analysis_step(
            user_id=user_id,
            question="Provide a financial assessment and next actions.",
        )
        return AnalysisAgentResponse(
            user_id=user_id,
            input_tokens=int(result["output_payload"]["input_tokens"]),
            analysis=str(result["output_payload"]["analysis"]),
            supporting_data=result["input_payload"],
        )

    def _run_analysis_step(
        self,
        user_id: int,
        question: str,
        input_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        input_payload = input_payload or self._build_analysis_input_payload(user_id=user_id, question=question)
        output_payload = self.analysis_agent.run(input_payload=input_payload)
        return {
            "input_payload": input_payload,
            "output_payload": output_payload,
        }

    def _run_planning_step(self, planning_input: Dict[str, Any]) -> Dict[str, Any]:
        return self.planning_agent.run(planning_input=planning_input)

    def _run_policy_step(self, policy_input: Dict[str, Any]) -> Dict[str, Any]:
        return self.policy_agent.run(policy_input=policy_input)

    def _run_explanation_step(self, explanation_input: Dict[str, Any]) -> Dict[str, Any]:
        return self.explanation_agent.run(explanation_input=explanation_input)

    def _build_analysis_input_payload(self, user_id: int, question: str) -> Dict[str, Any]:
        supporting_data: Dict[str, Any] = {
            "user_spending_summary": self.service.get_user_spending_summary(user_id=user_id),
            "user_spending_graph": self.service.get_user_spending_graph(user_id=user_id),
            "optimization": self.service.get_user_optimization(user_id=user_id),
            "policy": self.service.get_user_policy_compliance(user_id=user_id),
            "recent_transactions": self.service.get_user_recent_transactions(user_id=user_id, limit=100),
        }
        summary = supporting_data["user_spending_summary"]
        graph = supporting_data["user_spending_graph"]
        optimization = supporting_data["optimization"]
        policy = supporting_data["policy"]
        policy_metrics = policy.get("metrics") or {}
        recent_transactions = supporting_data.get("recent_transactions") or []

        payload = {
            "user_id": user_id,
            "question": question,
            "time_context": {
                "analysis_period": summary.get("analysis_period", "all-time"),
                "first_txn_ts": summary.get("first_txn_ts"),
                "last_txn_ts": summary.get("last_txn_ts"),
                "observed_span_months": summary.get("observed_span_months"),
                "active_months": summary.get("active_months"),
                "normalization_note": (
                    "Treat total_spend as all-time historical spend. Use observed_monthly_avg_spend for monthly framing. "
                    "Do not compare all-time totals directly against monthly income."
                ),
            },
            "summary_metrics": {
                "total_spend": summary.get("total_spend"),
                "txn_count": summary.get("txn_count"),
                "avg_ticket": summary.get("avg_ticket"),
                "observed_monthly_avg_spend": summary.get("observed_monthly_avg_spend"),
                "observed_monthly_avg_txn_count": summary.get("observed_monthly_avg_txn_count"),
            },
            "financial_profile": {
                "yearly_income": policy_metrics.get("yearly_income"),
                "monthly_income": policy_metrics.get("monthly_income"),
                "total_debt": policy_metrics.get("total_debt"),
                "credit_score": policy_metrics.get("credit_score"),
            },
            "policy_snapshot": {
                "overall_status": policy.get("overall_status"),
                "score": policy.get("score"),
                "metrics": {
                    "spend_to_income_ratio_pct": policy_metrics.get("spend_to_income_ratio_pct"),
                    "debt_to_income_ratio_pct": policy_metrics.get("debt_to_income_ratio_pct"),
                    "fraud_exposure_pct": policy_metrics.get("fraud_exposure_pct"),
                    "top_category_concentration_pct": policy_metrics.get("top_category_concentration_pct"),
                },
                "rules": [
                    {
                        "name": rule.get("name"),
                        "status": rule.get("status"),
                        "detail": rule.get("detail"),
                    }
                    for rule in (policy.get("rules") or [])[:4]
                ],
            },
            "top_categories": (graph.get("categories") or [])[:5],
            "optimization_suggestions": [
                {
                    "title": item.get("title"),
                    "priority": item.get("priority"),
                    "estimated_savings": item.get("estimated_savings"),
                    "description": item.get("description"),
                }
                for item in (optimization.get("suggestions") or [])[:4]
            ],
            "recent_transactions": [
                {
                    "txn_ts": tx.get("txn_ts"),
                    "amount": tx.get("amount"),
                    "merchant_id": tx.get("merchant_id"),
                    "mcc": tx.get("mcc"),
                }
                for tx in recent_transactions[:12]
            ],
        }
        return self._normalize_json_value(payload)

    def _build_planning_input(
        self,
        user_id: int,
        question: str,
        analysis_output: Dict[str, Any],
    ) -> Dict[str, Any]:
        user_policy = self.service.get_user_policy_compliance(user_id=user_id)
        return self._normalize_json_value(
            {
                "question": question,
                "summary": analysis_output.get("summary", ""),
                "risks": (analysis_output.get("risks") or [])[:3],
                "opportunities": (analysis_output.get("opportunities") or [])[:3],
                "next_actions": (analysis_output.get("next_actions") or [])[:3],
                "user_policy_status": user_policy.get("overall_status"),
                "user_policy_score": user_policy.get("score"),
                "user_policy_findings": [
                    {
                        "name": rule.get("name"),
                        "status": rule.get("status"),
                        "detail": rule.get("detail"),
                    }
                    for rule in (user_policy.get("rules") or [])[:4]
                    if rule.get("status") != "compliant"
                ],
            }
        )

    def _build_policy_input_payload(
        self,
        user_id: int,
        question: str,
        analysis_output: Dict[str, Any],
        planning_output: Dict[str, Any],
    ) -> Dict[str, Any]:
        user_policy = self.service.get_user_policy_compliance(user_id=user_id)
        payload = {
            "user_id": user_id,
            "question": question,
            "analysis_summary": analysis_output.get("summary", ""),
            "planning": {
                "goal": planning_output.get("goal", ""),
                "actions": planning_output.get("actions") or [],
                "checkpoints": planning_output.get("checkpoints") or [],
            },
            "user_policy": {
                "overall_status": user_policy.get("overall_status"),
                "score": user_policy.get("score"),
                "rules": [
                    {
                        "name": rule.get("name"),
                        "status": rule.get("status"),
                        "detail": rule.get("detail"),
                    }
                    for rule in (user_policy.get("rules") or [])[:4]
                ],
            },
        }
        return self._normalize_json_value(payload)

    def _build_explanation_input_payload(
        self,
        question: str,
        analysis_output: Dict[str, Any],
        planning_output: Dict[str, Any],
        policy_output: Dict[str, Any],
    ) -> Dict[str, Any]:
        payload = {
            "question": question,
            "analysis": {
                "summary": analysis_output.get("summary", ""),
                "risks": (analysis_output.get("risks") or [])[:3],
                "opportunities": (analysis_output.get("opportunities") or [])[:3],
                "next_actions": (analysis_output.get("next_actions") or [])[:3],
            },
            "planning": {
                "goal": planning_output.get("goal", ""),
                "actions": (planning_output.get("actions") or [])[:3],
                "checkpoints": (planning_output.get("checkpoints") or [])[:3],
            },
            "policy": {
                "approved": policy_output.get("approved"),
                "requires_human_review": policy_output.get("requires_human_review"),
                "summary": policy_output.get("summary", ""),
                "user_policy_status": policy_output.get("user_policy_status", ""),
                "user_policy_findings": (policy_output.get("user_policy_findings") or [])[:3],
                "guardrails": (policy_output.get("guardrails") or [])[:3],
                "blocked_actions": (policy_output.get("blocked_actions") or [])[:3],
            },
        }
        return self._normalize_json_value(payload)

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
    def _normalize_json_value(value: Dict[str, Any]) -> Dict[str, Any]:
        return json.loads(json.dumps(value, default=str))
