from fastapi import APIRouter

from app.api.routes.agent import router as agent_router
from app.api.routes.analytics import router as analytics_router
from app.api.routes.health import router as health_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(analytics_router, prefix="/api/v1", tags=["analytics"])
api_router.include_router(agent_router, prefix="/api/v1", tags=["agent"])

