from fastapi import APIRouter, Request as FastApiRequest


router = APIRouter()


@router.get(
    "/geoencoder/{country}/{region}/{subregion}",
    # response_class=RedirectResponse,
    status_code=200,
)
async def geoencoder(
    *,
    # dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: FastApiRequest,
):
    """

    """
    return {
        "adminVersion": "4.1",
        "matches": [
            {
                "country": {
                    "id": "HND",
                    "name": "Honduras"
                },
                "region": {
                    "id": 1,
                    "name": "Atlántida"
                },
                "subregion": {
                    "id": 4,
                    "name": "Jutiapa"
                }
            }
        ]
    }