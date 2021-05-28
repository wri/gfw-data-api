"""Assets are replicas of the original source files."""

from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

from ..models.pydantic.responses import Response

router = APIRouter()


@router.get(
    "/ping",
    response_class=ORJSONResponse,
    tags=["Health"],
    response_model=Response,
)
async def ping():
    """Simple uptime check."""

    return Response(data="pong")
