from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import ORJSONResponse

from app.routes import dataset_dependency, version_dependency

router = APIRouter()
description = """Explore data entries for a given dataset version using standard SQL"""


@router.get("/{dataset}/{version}/query", response_class=ORJSONResponse, tags=["Query"])
async def query_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    sql: str = Query(None, title="SQL query"),
    geostore_id: UUID = Query(None, title="Geostore ID")
):
    """Execute a read ONLY SQL query on the given dataset version (if implemented)."""

    raise NotImplementedError
