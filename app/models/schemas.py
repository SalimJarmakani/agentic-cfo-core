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




class SpendingCategoryItem(BaseModel):
    category: str
    amount: float
    percentage: float
    transaction_count: int

class UserSpendingGraphResponse(BaseModel):
    user_id: int
    total_spend: float
    period: str = "all-time"
    categories: List[SpendingCategoryItem]
    recurring_payments: int
    subscriptions: int


class OptimizationSuggestionItem(BaseModel):
    id: str
    title: str
    description: str
    estimated_savings: float
    category: str
    priority: Literal["high", "medium", "low"]

class UserOptimizationResponse(BaseModel):
    user_id: int
    suggestions: List[OptimizationSuggestionItem]
    total_estimated_savings: float


class PolicyRuleItem(BaseModel):
    id: str
    name: str
    description: str
    status: Literal["compliant", "warning", "violation"]
    detail: str


class UserPolicyComplianceResponse(BaseModel):
    user_id: int
    overall_status: Literal["compliant", "warning", "violation"]
    score: int
    rules: List[PolicyRuleItem]
