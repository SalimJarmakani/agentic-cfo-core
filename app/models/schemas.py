from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str = "agent-cfo-core"


class RecentTransactionsResponse(BaseModel):
    items: List[Dict[str, Any]]


class UserSpendingSummaryResponse(BaseModel):
    user_id: int
    txn_count: int
    total_spend: float
    avg_ticket: float
    first_txn_ts: Optional[str] = None
    last_txn_ts: Optional[str] = None


class RiskyMerchantsResponse(BaseModel):
    items: List[Dict[str, Any]]


class PaginatedUsersResponse(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[Dict[str, Any]]


class AgentQueryRequest(BaseModel):
    question: str = Field(min_length=3, description="Natural language prompt for the CFO agent")
    top_k: int = Field(default=10, ge=1, le=100)


class PlanStep(BaseModel):
    datasource: Literal["postgres", "neo4j", "hybrid"]
    action: str
    details: str


class AgentQueryResponse(BaseModel):
    answer: str
    plan: List[PlanStep]
    data: Dict[str, Any]
