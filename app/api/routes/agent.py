from fastapi import APIRouter, HTTPException

from app.agents.orchestrator import AgentOrchestrator
from app.models.schemas import (
    AnalysisAgentRequest,
    AnalysisAgentResponse,
    AgentQueryRequest,
    AgentQueryResponse,
    MidStageWorkflowRequest,
    MidStageWorkflowResponse,
)
from app.services.llm_service import LLMServiceError

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
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/agent/workflow/mid-stage/start", response_model=MidStageWorkflowResponse)
def start_mid_stage_workflow(payload: MidStageWorkflowRequest) -> MidStageWorkflowResponse:
    return orchestrator.run_mid_stage_workflow(
        user_id=payload.user_id,
        question=payload.question,
        top_k=payload.top_k,
    )
