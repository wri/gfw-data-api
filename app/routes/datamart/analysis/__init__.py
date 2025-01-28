from fastapi import APIRouter

from app.routes.datamart.analysis.forest_change import forest_change_router, \
    datamart_analysis_forest_change_metadata_tags

datamart_analysis_metadata_tags = [
    {"name": "Data Mart Analysis", "description": __doc__},
]

datamart_analysis_metadata_tags.extend(datamart_analysis_forest_change_metadata_tags)

analysis_router = APIRouter(
    prefix="/analysis"
)

analysis_router.include_router(forest_change_router)