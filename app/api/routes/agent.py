from fastapi import APIRouter

from app.agents.orchestrator import AgentOrchestrator
from app.models.schemas import AgentQueryRequest, AgentQueryResponse

router = APIRouter()
orchestrator = AgentOrchestrator()


@router.post("/agent/query", response_model=AgentQueryResponse)
def run_agent_query(payload: AgentQueryRequest) -> AgentQueryResponse:
    return orchestrator.run(question=payload.question, top_k=payload.top_k)

