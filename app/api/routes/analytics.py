from fastapi import APIRouter, Query

from app.models.schemas import (
    PaginatedUsersResponse,
    RecentTransactionsResponse,
    RiskyMerchantsResponse,
    UserSpendingSummaryResponse,
    UserSpendingGraphResponse,
    UserOptimizationResponse,
    UserPolicyComplianceResponse,
)
from app.services.analytics_service import AnalyticsService

router = APIRouter()
service = AnalyticsService()


@router.get("/analytics/users", response_model=PaginatedUsersResponse)
def list_users(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
) -> PaginatedUsersResponse:
    payload = service.get_users_paginated(page=page, page_size=page_size)
    return PaginatedUsersResponse(**payload)


@router.get("/analytics/transactions/recent", response_model=RecentTransactionsResponse)
def recent_transactions(limit: int = Query(default=25, ge=1, le=500)) -> RecentTransactionsResponse:
    rows = service.get_recent_transactions(limit=limit)
    return RecentTransactionsResponse(items=rows)


@router.get("/analytics/users/{user_id}/spending", response_model=UserSpendingSummaryResponse)
def user_spending_summary(user_id: int) -> UserSpendingSummaryResponse:
    payload = service.get_user_spending_summary(user_id=user_id)
    return UserSpendingSummaryResponse(**payload)


@router.get("/graph/merchants/risky", response_model=RiskyMerchantsResponse)
def risky_merchants(limit: int = Query(default=20, ge=1, le=200)) -> RiskyMerchantsResponse:
    rows = service.get_risky_merchants(limit=limit)
    return RiskyMerchantsResponse(items=rows)


@router.get("/graph/users/{user_id}/spending", response_model=UserSpendingGraphResponse)
def user_spending_graph(user_id: int) -> UserSpendingGraphResponse:
    payload = service.get_user_spending_graph(user_id=user_id)
    return UserSpendingGraphResponse(**payload)


@router.get("/graph/users/{user_id}/optimization", response_model=UserOptimizationResponse)
def user_optimization(user_id: int) -> UserOptimizationResponse:
    payload = service.get_user_optimization(user_id=user_id)
    return UserOptimizationResponse(**payload)


@router.get("/graph/users/{user_id}/policy", response_model=UserPolicyComplianceResponse)
def user_policy_compliance(user_id: int) -> UserPolicyComplianceResponse:
    payload = service.get_user_policy_compliance(user_id=user_id)
    return UserPolicyComplianceResponse(**payload)
