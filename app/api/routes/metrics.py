import logging

from fastapi import APIRouter, HTTPException

from app.agents.orchestrator import AgentOrchestrator
from app.models.schemas import (
    AgentTokenUsageResponse,
    MetricsSummaryResponse,
    RecommendationFeedbackRequest,
    RecommendationFeedbackResponse,
    ValidatedQueryEvaluationRequest,
    ValidatedQueryEvaluationResponse,
)
from app.services.metrics_service import MetricsService
from app.services.workflow_service import WorkflowNotFoundError, WorkflowService

logger = logging.getLogger(__name__)

router = APIRouter()
metrics_service = MetricsService()
orchestrator = AgentOrchestrator()
workflow_service = WorkflowService()


@router.get("/metrics/summary", response_model=MetricsSummaryResponse)
def get_metrics_summary() -> MetricsSummaryResponse:
    payload = metrics_service.get_summary()
    return MetricsSummaryResponse(**payload)


@router.get("/metrics/agent-token-usage", response_model=AgentTokenUsageResponse)
def get_agent_token_usage() -> AgentTokenUsageResponse:
    payload = metrics_service.get_agent_token_usage()
    return AgentTokenUsageResponse(**payload)


@router.post("/metrics/validated-queries/evaluations", response_model=ValidatedQueryEvaluationResponse)
def create_validated_query_evaluation(
    payload: ValidatedQueryEvaluationRequest,
) -> ValidatedQueryEvaluationResponse:
    actual_answer = payload.actual_answer
    if not actual_answer:
        logger.info("Running validated query evaluation for question=%s", payload.question)
        result = orchestrator.run(question=payload.question, top_k=payload.top_k)
        actual_answer = result.answer

    stored = metrics_service.create_validated_query_evaluation(
        question=payload.question,
        expected_answer=payload.expected_answer,
        actual_answer=actual_answer,
        is_correct=payload.is_correct,
        evaluator=payload.evaluator,
        notes=payload.notes,
    )
    return ValidatedQueryEvaluationResponse(**stored)


@router.post("/metrics/recommendation-feedback", response_model=RecommendationFeedbackResponse)
def create_recommendation_feedback(
    payload: RecommendationFeedbackRequest,
) -> RecommendationFeedbackResponse:
    try:
        workflow_service.get_workflow(payload.workflow_run_id)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    stored = metrics_service.create_recommendation_feedback(
        workflow_run_id=payload.workflow_run_id,
        recommendation_stage=payload.recommendation_stage,
        usefulness_rating=payload.usefulness_rating,
        clarity_rating=payload.clarity_rating,
        adopted=payload.adopted,
        evaluator=payload.evaluator,
        comments=payload.comments,
    )
    return RecommendationFeedbackResponse(**stored)
