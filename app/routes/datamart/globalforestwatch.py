"""APIs for Global Forest Watch data."""
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


class NetTreeCoverChangeRequest(StrictBaseModel):
    iso: str = Field(..., description="ISO code of the country or region (e.g., 'BRA' for Brazil).")
    adm1: Optional[int] = Field(None, description="Admin level 1 ID (e.g., a state or province).")
    adm2: Optional[int] = Field(None, description="Admin level 2 ID (e.g., a municipality). ⚠️ **Must be provided with adm1.**")

    @root_validator
    def check_adm1_adm2_dependency(cls, values):
        """
        Validates that adm2 is only provided if adm1 is also present.
        Raises a validation error if adm2 is given without adm1.
        """
        print(values.keys())
        adm1, adm2 = values.get('adm1'), values.get('adm2')
        if adm2 is not None and adm1 is None:
            raise ValueError("If 'adm2' is provided, 'adm1' must also be present.")
        return values

    def get_admin_level(self):
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


def build_sql_query(request):
    select_fields = [Gadm.ISO.value]
    where_conditions = [f"{Gadm.ISO.value} = '{request.iso}'"]

    append_field_and_condition(select_fields, where_conditions, Gadm.ADM1.value, request.adm1)
    append_field_and_condition(select_fields, where_conditions, Gadm.ADM2.value, request.adm2)

    select_fields += ["stable", "loss", "gain", "disturb", "net", "change", "gfw_area__ha"]

    select_fields_str = ", ".join(select_fields)
    where_filter_str = " AND ".join(where_conditions)

    sql = f"SELECT {select_fields_str} FROM data WHERE {where_filter_str}"

    return sql

def append_field_and_condition(select_fields, where_conditions, field_name, field_value):
    if field_value is not None:
        select_fields.append(field_name)
        where_conditions.append(f"{field_name} = '{field_value}'")


async def fetch_tree_cover_data(sql_query: str, level: str, api_key: str) -> TreeCoverData:
    """
    Fetches tree cover data from the external API using the SQL query and level.
    Handles the HTTP request, response status check, and data extraction.
    """
    production_service_uri = "https://data-api.globalforestwatch.org"
    net_change_version = "v202209"
    url = f"{production_service_uri}/dataset/umd_{level}_net_tree_cover_change_from_height/{net_change_version}/query/json?sql={sql_query}"

    async with AsyncClient() as client:
        response = await client.get(url, headers={"x-api-key": api_key})
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
        request = NetTreeCoverChangeRequest(iso=iso, adm1=adm1, adm2=adm2)
        sql_query: str = build_sql_query(request)
        admin_level: str = request.get_admin_level()
        tree_cover_data: TreeCoverData = await fetch_tree_cover_data(sql_query, admin_level, api_key)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return NetTreeCoverChangeResponse(data=tree_cover_data, status="success")
