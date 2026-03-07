from fastapi import APIRouter

from app.agents.orchestrator import AgentOrchestrator
from app.models.schemas import (
    AgentQueryRequest,
    AgentQueryResponse,
    MidStageWorkflowRequest,
    MidStageWorkflowResponse,
)

router = APIRouter()
orchestrator = AgentOrchestrator()


@router.post("/agent/query", response_model=AgentQueryResponse)
def run_agent_query(payload: AgentQueryRequest) -> AgentQueryResponse:
    return orchestrator.run(question=payload.question, top_k=payload.top_k)


@router.post("/agent/workflow/mid-stage/start", response_model=MidStageWorkflowResponse)
def start_mid_stage_workflow(payload: MidStageWorkflowRequest) -> MidStageWorkflowResponse:
    return orchestrator.run_mid_stage_workflow(
        user_id=payload.user_id,
        question=payload.question,
        top_k=payload.top_k,
    )
