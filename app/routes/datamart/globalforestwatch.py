"""
Data Mart APIs for Global Forest Watch (GFW) backend consumption.

These APIs provide granular, tailored data services specifically designed to meet the needs of the **Global Forest Watch (GFW)** backend infrastructure.
The endpoints abstract away the complexities of querying datasets related to net tree cover change, allowing the GFW backend to integrate and consume
data efficiently and reliably.

### Intended Audience:
This API is not designed for general public use. It is purpose-built for internal use by the GFW backend services and systems. The consumers of this
API are expected to have an in-depth understanding of GFW's data models and query requirements.

### Key Features:
- Tailored queries for retrieving net tree cover change data from the GFW database.
- Efficient data retrieval for ISO country codes and administrative regions.
- Abstracts the SQL query generation process to simplify integration with the backend.

### Usage:
These endpoints are intended to be consumed programmatically by the GFW backend and are not optimized for external client-facing use. The data
retrieved is intended to support GFW's internal applications and services.

Specifically, it supports the [Net change in tree cover](https://www.globalforestwatch.org/map/country/BRA/14/?mainMap=eyJzaG93QW5hbHlzaXMiOnRydWV9&map=eyJjZW50ZXIiOnsibGF0IjotMy42MjgwNjcwOTUyMDc3NDc2LCJsbmciOi01Mi40NzQ4OTk5OTk5OTczMzR9LCJ6b29tIjo2LjA1NTQ1ODQ3NjM4NDE1LCJjYW5Cb3VuZCI6ZmFsc2UsImRhdGFzZXRzIjpbeyJkYXRhc2V0IjoiTmV0LUNoYW5nZS1TVEFHSU5HIiwib3BhY2l0eSI6MSwidmlzaWJpbGl0eSI6dHJ1ZSwibGF5ZXJzIjpbImZvcmVzdC1uZXQtY2hhbmdlIl19LHsiZGF0YXNldCI6InBvbGl0aWNhbC1ib3VuZGFyaWVzIiwibGF5ZXJzIjpbImRpc3B1dGVkLXBvbGl0aWNhbC1ib3VuZGFyaWVzIiwicG9saXRpY2FsLWJvdW5kYXJpZXMiXSwib3BhY2l0eSI6MSwidmlzaWJpbGl0eSI6dHJ1ZX1dfQ%3D%3D&mapMenu=eyJtZW51U2VjdGlvbiI6ImRhdGFzZXRzIiwiZGF0YXNldENhdGVnb3J5IjoiZm9yZXN0Q2hhbmdlIn0%3D) widget
"""
from enum import Enum
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.logger import logger
from fastapi.openapi.models import APIKey
from fastapi.responses import ORJSONResponse
from httpx import AsyncClient
from pydantic import Field, root_validator, ValidationError

from app.authentication.api_keys import get_api_key
from app.models.pydantic.base import StrictBaseModel

router = APIRouter()


class Gadm(str, Enum):
    ISO = "iso"
    ADM0 = "adm0"
    ADM1 = "adm1"
    ADM2 = "adm2"


class GadmSpecification(StrictBaseModel):
    iso: str = Field(..., description="ISO code of the country or region (e.g., 'BRA' for Brazil).")
    adm1: Optional[int] = Field(None, description="Admin level 1 ID (e.g., a state or province).")
    adm2: Optional[int] = Field(None, description="Admin level 2 ID (e.g., a municipality). ⚠️ **Must be provided with adm1.**")

    @root_validator
    def check_adm1_adm2_dependency(cls, values):
        """
        Validates that adm2 is only provided if adm1 is also present.
        Raises a validation error if adm2 is given without adm1.
        """
        adm1, adm2 = values.get('adm1'), values.get('adm2')
        if adm2 is not None and adm1 is None:
            raise ValueError("If 'adm2' is provided, 'adm1' must also be present.")
        return values

    def get_specified_admin_level(self):
        """
        Determines the appropriate level ('adm0', 'adm1', or 'adm2') based on the presence of adm1 and adm2.
        """
        if self.adm2 is not None:
            return Gadm.ADM2.value  # Return the Enum value 'adm2'
        if self.adm1 is not None:
            return Gadm.ADM1.value  # Return the Enum value 'adm1'
        return Gadm.ADM0.value  # Default to 'adm0'


class TreeCoverData(StrictBaseModel):
    """
    Model representing individual tree cover change data from the API.
    """
    iso: str = Field(..., description="ISO code of the country or region (e.g., 'BRA' for Brazil).")
    adm1: Optional[int] = Field(None, description="Admin level 1 ID (e.g., a state or province).")
    adm2: Optional[int] = Field(None, description="Admin level 2 ID (e.g., a municipality).")
    stable: float = Field(..., description="The area of stable forest in hectares.")
    loss: float = Field(..., description="The area of forest loss in hectares.")
    gain: float = Field(..., description="The area of forest gain in hectares.")
    disturb: float = Field(..., description="The area of forest disturbance in hectares.")
    net: float = Field(..., description="The net change in forest cover in hectares (gain - loss).")
    change: float = Field(..., description="The percentage change in forest cover.")
    gfw_area__ha: float = Field(..., description="The total forest area in hectares.")


class NetTreeCoverChangeResponse(StrictBaseModel):
    data: TreeCoverData = Field(..., description="A list of tree cover change data records.")
    status: str = Field(..., description="Status of the request (e.g., 'success').")

    class Config:
        schema_extra = {
            "example": {
                "data":
                    {
                        "iso": "BRA",
                        "stable": 413722809.3,
                        "loss": 36141245.77,
                        "gain": 8062324.946,
                        "disturb": 23421628.86,
                        "net": -28078920.83,
                        "change": -5.932759761810303,
                        "gfw_area__ha": 850036547.481532,
                        "adm1": 12,
                        "adm2": 34
                    },
                "status": "success"
            }
        }


def _build_sql_query(request):
    select_fields = [Gadm.ISO.value]
    where_conditions = [f"{Gadm.ISO.value} = '{request.iso}'"]

    _append_field_and_condition(select_fields, where_conditions, Gadm.ADM1.value, request.adm1)
    _append_field_and_condition(select_fields, where_conditions, Gadm.ADM2.value, request.adm2)

    select_fields += ["stable", "loss", "gain", "disturb", "net", "change", "gfw_area__ha"]

    select_fields_str = ", ".join(select_fields)
    where_filter_str = " AND ".join(where_conditions)

    sql = f"SELECT {select_fields_str} FROM data WHERE {where_filter_str}"

    return sql

def _append_field_and_condition(select_fields, where_conditions, field_name, field_value):
    if field_value is not None:
        select_fields.append(field_name)
        where_conditions.append(f"{field_name} = '{field_value}'")


async def _fetch_tree_cover_data(sql_query: str, level: str, api_key: str) -> TreeCoverData:
    """
    Fetches tree cover data from the external API using the SQL query and level.
    Handles the HTTP request, response status check, and data extraction.
    Adds a custom header for tracking the service name for NewRelic/AWS monitoring.
    """
    production_service_uri = "https://data-api.globalforestwatch.org"
    net_change_version = "v202209"
    url = f"{production_service_uri}/dataset/umd_{level}_net_tree_cover_change_from_height/{net_change_version}/query/json?sql={sql_query}"

    # Custom header for identifying the service for monitoring
    service_name = "globalforestwatch-datamart"

    async with AsyncClient() as client:
        # Add the 'x-api-key' and custom 'X-Service-Name' headers
        headers = {
            "x-api-key": api_key,
            "x-service-name": service_name
        }
        response = await client.get(url, headers=headers)

        if response.status_code != 200:
            logger.error(f"API responded with status code {response.status_code}: {response.content}")
            raise Exception("Failed to fetch tree cover data.")

        # Parse and validate the response data into TreeCoverData models
        response_data = response.json().get("data", [])[0]
        return TreeCoverData(**response_data)


@router.get(
    "/v1/net_tree_cover_change",
    response_class=ORJSONResponse,
    response_model=NetTreeCoverChangeResponse,
    tags=["Data Mart"],
    summary="Retrieve net tree cover change data",
    description="This endpoint provides data on net tree cover change by querying the Global Forest Watch (GFW) database.",
)
async def net_tree_cover_change(
        iso: str = Query(..., description="ISO code of the country or region (e.g., 'BRA' for Brazil).", example="BRA"),
        adm1: Optional[int] = Query(None, description="Admin level 1 ID (e.g., a state or province).", example="12"),
        adm2: Optional[int] = Query(None, description="Admin level 2 ID (e.g., a municipality). ⚠️ **Must provide `adm1` also.**", example="34"),
        api_key: APIKey = Depends(get_api_key)
):
    """
    Retrieves net tree cover change data.
    """
    try:
        gadm_specifier = GadmSpecification(iso=iso, adm1=adm1, adm2=adm2)
        sql_query: str = _build_sql_query(gadm_specifier)
        admin_level: str = gadm_specifier.get_specified_admin_level()
        tree_cover_data: TreeCoverData = await _fetch_tree_cover_data(sql_query, admin_level, api_key)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return NetTreeCoverChangeResponse(data=tree_cover_data, status="success")
