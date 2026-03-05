from fastapi import APIRouter

from app.models.schemas import HealthResponse

router = APIRouter()


@router.get("/", response_model=HealthResponse)
def root() -> HealthResponse:
    return HealthResponse()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()

