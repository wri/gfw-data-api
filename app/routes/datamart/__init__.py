"""
Data Mart APIs for Global Forest Watch (GFW) backend consumption.

These APIs provide coarse-grained, tailored data services specifically designed to meet the needs of WRI frontend applications.
The endpoints abstract away the complexities of querying datasets related to tree cover change, allowing applications to integrate and consume
data efficiently and reliably.

### Key Features:
- Tailored queries for retrieving net tree cover change data from the GFW database.
- Efficient data retrieval for ISO country codes and administrative regions.
- Abstracts the SQL query generation process to simplify integration with applications.
"""
from fastapi import APIRouter

from app.routes.datamart.analysis import analysis_router, datamart_analysis_metadata_tags

datamart_metadata_tags = [
    {"name": "Data Mart", "description": __doc__},
]

datamart_metadata_tags.extend(datamart_analysis_metadata_tags)

data_mart_router = APIRouter(
    prefix="/datamart"
)

data_mart_router.include_router(analysis_router)