from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Response
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.schema import CreateSchema, DropSchema

from ..application import db
from ..crud import datasets, versions
from ..models.orm.datasets import Dataset as ORMDataset
from ..models.pydantic.datasets import Dataset, DatasetCreateIn, DatasetUpdateIn
from ..routes import is_admin
from ..settings.globals import READER_USERNAME
from . import dataset_dependency

router = APIRouter()


class AnalysisType(str, Enum):
    summary = "summary"
    change = "change"


class TreeCoverLossSummary(BaseModel):
    area__ha: float
    emission__Mg_ha_1: Optional[float] = Field(None, alias="emission__Mg_ha-1")


@router.get("/analysis", tags=["Analysis"])
def get_general_analysis(
    *,
    group_by: List[str] = Query(None),
    filter: List[str] = Query(None),
    geostore: UUID = Query(None)
):
    pass


@router.get(
    "/umd_tree_cover_loss/analysis/{analysis_type}",
    tags=["Analysis"],
    response_model=TreeCoverLossSummary,
)
def get_analysis_by_type(
    *,
    dataset: str = Depends(dataset_dependency),
    analysis_type: AnalysisType = Path(...),
    tree_cover_density__threshold: int = Query(..., ge=0, le=100),
    geostore: UUID = Query(None)
):
    pass
