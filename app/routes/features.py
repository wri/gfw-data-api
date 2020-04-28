from fastapi import APIRouter, Path, Query, Depends
from fastapi.responses import ORJSONResponse

from app.routes import dataset_dependency, version_dependency

router = APIRouter()
VERSION_REGEX = r"^v\d{1,8}\.?\d{1,3}\.?\d{1,3}$|^latest$"


@router.get(
    "/{dataset}/{version}/features", response_class=ORJSONResponse, tags=["Features"],
)
async def get_features(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    lat: float = Query(None, title="Latitude", ge=-90, le=90),
    lng: float = Query(None, title="Longitude", ge=-180, le=180),
    z: int = Query(None, title="Zoom level", ge=0, le=22)
):
    """
    Retrieve list of features
    Add optional spatial filter using a point buffer (for info tool).
    """
    pass


@router.get(
    "/{dataset}/{version}/feature/{feature_id}",
    response_class=ORJSONResponse,
    tags=["Features"],
)
async def get_feature(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    feature_id: int = Path(..., title="Feature ID", ge=0)
):
    """
    Retrieve attribute values for a given feature
    """
    pass
