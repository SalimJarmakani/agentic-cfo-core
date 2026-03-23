from contextlib import asynccontextmanager
import logging
from time import perf_counter

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import get_settings
from app.db.neo4j import close_neo4j_driver, get_neo4j_driver
from app.services.metrics_service import MetricsService

logger = logging.getLogger(__name__)
metrics_service = MetricsService()


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Initialize graph driver once at startup so first request is not penalized.
    get_neo4j_driver()
    yield
    close_neo4j_driver()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in settings.cors_origins.split(",") if o.strip()],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def record_request_timing(request, call_next):
        start = perf_counter()
        status_code = 500
        path = request.url.path

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = (perf_counter() - start) * 1000.0
            if path.startswith("/api/v1/") and not path.startswith("/api/v1/metrics"):
                try:
                    metrics_service.record_api_request(
                        method=request.method,
                        path=path,
                        status_code=status_code,
                        duration_ms=duration_ms,
                    )
                except Exception:
                    logger.exception("Failed to record API request timing for %s %s", request.method, path)

    app.include_router(api_router)
    return app


app = create_app()
