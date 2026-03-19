import logging

from fastapi import APIRouter, HTTPException, Query

from app.agents.orchestrator import AgentOrchestrator
from app.models.schemas import (
    AnalysisAgentRequest,
    AnalysisAgentResponse,
    AgentQueryRequest,
    AgentQueryResponse,
    AgentWorkflowListResponse,
    AgentWorkflowResponse,
    AgentWorkflowStartRequest,
    MidStageWorkflowRequest,
    MidStageWorkflowResponse,
)
from app.services.llm_service import LLMServiceError
from app.services.workflow_service import WorkflowNotFoundError, WorkflowServiceError

logger = logging.getLogger(__name__)

router = APIRouter()
orchestrator = AgentOrchestrator()


@router.post("/agent/query", response_model=AgentQueryResponse)
def run_agent_query(payload: AgentQueryRequest) -> AgentQueryResponse:
    return orchestrator.run(question=payload.question, top_k=payload.top_k)


@router.post("/agent/analysis", response_model=AnalysisAgentResponse)
def run_analysis_agent(payload: AnalysisAgentRequest) -> AnalysisAgentResponse:
    try:
        return orchestrator.run_analysis_agent(user_id=payload.user_id)
    except LLMServiceError as exc:
        logger.exception("LLMServiceError in run_analysis_agent for user_id=%s", payload.user_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/agent/workflows", response_model=AgentWorkflowResponse)
def start_agent_workflow(payload: AgentWorkflowStartRequest) -> AgentWorkflowResponse:
    logger.info("Received workflow start request for user_id=%s", payload.user_id)
    try:
        workflow = orchestrator.start_agent_workflow(user_id=payload.user_id, question=payload.question)
        logger.info(
            "Completed workflow start request for user_id=%s workflow_run_id=%s status=%s",
            payload.user_id,
            workflow["workflow_run_id"],
            workflow["status"],
        )
        return workflow
    except LLMServiceError as exc:
        logger.exception("LLMServiceError while starting workflow for user_id=%s", payload.user_id)
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/agent/workflows", response_model=AgentWorkflowListResponse)
def list_agent_workflows(user_id: int = Query(ge=1), limit: int = Query(default=20, ge=1, le=100)) -> AgentWorkflowListResponse:
    return orchestrator.list_agent_workflows(user_id=user_id, limit=limit)


@router.get("/agent/workflows/{workflow_run_id}", response_model=AgentWorkflowResponse)
def get_agent_workflow(workflow_run_id: int) -> AgentWorkflowResponse:
    try:
        return orchestrator.get_agent_workflow(workflow_run_id=workflow_run_id)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/agent/workflows/{workflow_run_id}/continue", response_model=AgentWorkflowResponse)
def continue_agent_workflow(workflow_run_id: int) -> AgentWorkflowResponse:
    try:
        return orchestrator.continue_agent_workflow(workflow_run_id=workflow_run_id)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except WorkflowServiceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LLMServiceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/agent/workflow/mid-stage/start", response_model=MidStageWorkflowResponse)
def start_mid_stage_workflow(payload: MidStageWorkflowRequest) -> MidStageWorkflowResponse:
    return orchestrator.run_mid_stage_workflow(
        user_id=payload.user_id,
        question=payload.question,
        top_k=payload.top_k,
    )
