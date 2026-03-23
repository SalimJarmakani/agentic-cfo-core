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


class UserSpendingOverviewItem(BaseModel):
    user_id: int
    txn_count: int
    total_spend: float
    avg_ticket: float
    first_txn_ts: Optional[str] = None
    last_txn_ts: Optional[str] = None


class UsersSpendingOverviewResponse(BaseModel):
    items: List[UserSpendingOverviewItem]


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


class AnalysisAgentRequest(BaseModel):
    user_id: int = Field(
        ge=1,
        description="User ID to analyze using transaction and graph data.",
    )


class AgentWorkflowStartRequest(BaseModel):
    user_id: int = Field(ge=1)
    question: str = Field(
        default="Provide a financial assessment and a simple action plan.",
        min_length=3,
    )


class PlanStep(BaseModel):
    datasource: Literal["postgres", "neo4j", "hybrid"]
    action: str
    details: str


class AgentQueryResponse(BaseModel):
    answer: str
    plan: List[PlanStep]
    data: Dict[str, Any]


class AnalysisAgentResponse(BaseModel):
    user_id: int
    input_tokens: int
    analysis: str
    supporting_data: Dict[str, Any]


class WorkflowStepResponse(BaseModel):
    workflow_step_id: int
    workflow_run_id: int
    step_name: Literal["analysis", "planning", "policy", "explanation"]
    status: Literal["pending", "running", "completed", "failed"]
    input_payload: Optional[Dict[str, Any]] = None
    output_payload: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class AgentWorkflowResponse(BaseModel):
    workflow_run_id: int
    user_id: int
    question: str
    status: Literal["running", "waiting_for_user", "completed", "failed"]
    current_stage: Literal["analysis", "planning", "policy", "explanation", "done", "failed"]
    created_at: str
    updated_at: str
    steps: List[WorkflowStepResponse]


class AgentWorkflowSummaryResponse(BaseModel):
    workflow_run_id: int
    user_id: int
    question: str
    status: Literal["running", "waiting_for_user", "completed", "failed"]
    current_stage: Literal["analysis", "planning", "policy", "explanation", "done", "failed"]
    created_at: str
    updated_at: str


class AgentWorkflowListResponse(BaseModel):
    user_id: int
    items: List[AgentWorkflowSummaryResponse]


class MidStageWorkflowRequest(BaseModel):
    user_id: int = Field(ge=1)
    question: str = Field(
        default="Provide a financial assessment and next best actions.",
        min_length=3,
    )
    top_k: int = Field(default=10, ge=1, le=100)


class MidStageWorkflowResponse(BaseModel):
    user_id: int
    analysis: Dict[str, Any]
    planning: List[PlanStep]
    policy: Dict[str, Any]
    explanation: str




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


class ValidatedQueryEvaluationRequest(BaseModel):
    question: str = Field(min_length=3)
    expected_answer: str = Field(min_length=1)
    actual_answer: Optional[str] = None
    top_k: int = Field(default=10, ge=1, le=100)
    is_correct: Optional[bool] = None
    evaluator: Optional[str] = Field(default=None, max_length=100)
    notes: Optional[str] = None


class ValidatedQueryEvaluationResponse(BaseModel):
    evaluation_id: int
    question: str
    expected_answer: str
    actual_answer: str
    is_correct: Optional[bool] = None
    evaluator: Optional[str] = None
    notes: Optional[str] = None
    created_at: str


class RecommendationFeedbackRequest(BaseModel):
    workflow_run_id: int = Field(ge=1)
    recommendation_stage: Literal["analysis", "planning", "policy", "explanation"] = "planning"
    usefulness_rating: int = Field(ge=1, le=5)
    clarity_rating: Optional[int] = Field(default=None, ge=1, le=5)
    adopted: Optional[bool] = None
    evaluator: Optional[str] = Field(default=None, max_length=100)
    comments: Optional[str] = None


class RecommendationFeedbackResponse(BaseModel):
    feedback_id: int
    workflow_run_id: int
    recommendation_stage: Literal["analysis", "planning", "policy", "explanation"]
    usefulness_rating: int
    clarity_rating: Optional[int] = None
    adopted: Optional[bool] = None
    evaluator: Optional[str] = None
    comments: Optional[str] = None
    created_at: str


class AgentTokenUsageResponse(BaseModel):
    average_input_tokens_by_agent: Dict[str, Optional[float]]
    runs_with_input_tokens_by_agent: Dict[str, int]


class MetricsSummaryResponse(BaseModel):
    analytics_accuracy_rate: Optional[float] = None
    analytics_accuracy_total_evaluations: int
    workflow_completion_rate: Optional[float] = None
    workflow_total_runs: int
    workflow_completed_runs: int
    workflow_failed_runs: int
    workflow_in_progress_runs: int
    policy_intervention_rate: Optional[float] = None
    policy_intervention_total_reviews: int
    policy_intervention_count: int
    policy_compliance_rate: Optional[float] = None
    policy_compliance_total_reviews: int
    policy_compliant_count: int
    average_api_response_time_ms: Optional[float] = None
    average_workflow_completion_time_ms: Optional[float] = None
    average_stage_response_time_ms: Dict[str, float]
    human_recommendation_usefulness_avg: Optional[float] = None
    human_recommendation_usefulness_total_reviews: int
