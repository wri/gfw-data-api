from typing import List, Dict, Any, Optional, Tuple, Union
from typing.io import IO

from asyncpg.exceptions import UniqueViolationError
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    File,
    UploadFile,
    BackgroundTasks,
    Response,
)
from fastapi.responses import ORJSONResponse

from ..crud import update_data, versions
from ..models.orm.asset import Asset as ORMAsset
from ..models.orm.version import Version as ORMVersion
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.version import Version, VersionCreateIn, VersionUpdateIn
from ..routes import dataset_dependency, is_admin, version_dependency
from ..settings.globals import BUCKET
from ..tasks.default_assets import create_default_asset
from ..utils import true_xor

router = APIRouter()


# TODO:
#  - inherit/ override default asset type for new versions


@router.get(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Version"],
    response_model=Version,
)
async def get_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
):
    """
    Get basic metadata for a given version
    """
    row: ORMVersion = await versions.get_version(dataset, version)

    return await _version_response(dataset, version, row)


@router.put(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Version"],
    response_model=Version,
    status_code=201,
)
async def add_new_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: VersionCreateIn,
    files: Optional[UploadFile] = File(None),
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """
    Create or update a version for a given dataset
    """

    async def callback(message: Dict[str, Any]) -> None:
        pass
        # await _version_history(message, dataset, version)

    input_data, file_obj, uri = _prepare_sources(dataset, version, request, files)

    # Register version with DB
    new_version: ORMVersion = await versions.create_version(
        dataset, version, **input_data
    )

    # Everything else happens in the background task asynchronously
    background_tasks.add_task(create_default_asset, input_data, file_obj, callback)

    response.headers["Location"] = f"/{dataset}/{version}"
    return await _version_response(dataset, version, new_version)


@router.patch(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Version"],
    response_model=Version,
)
async def update_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: Optional[VersionUpdateIn],
    files: Optional[List[UploadFile]] = File(None),
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
):
    """
    Partially update a version of a given dataset.
    When using PATCH and uploading files,
    this will overwrite the existing source(s) and trigger a complete update of all managed assets
    """

    input_data, file_obj, uri = _prepare_sources(dataset, version, request, files)

    row: ORMVersion = await versions.get_version(dataset, version)
    row = await update_data(row, input_data)

    # TODO: If files is not None, delete all files in RAW folder

    # if file_obj is not None:
    #     background_tasks.add_task(inject_file, file_obj, uri)

    # TODO: If files is not None, delete and recreate all Assets based on new input files

    return await _version_response(dataset, version, row)


@router.delete(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Version"],
    response_model=Version,
)
async def delete_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    is_authorized: bool = Depends(is_admin),
):
    """
    Delete a version
    """
    row: ORMVersion = await versions.delete_version(dataset, version)

    # TODO:
    #  Delete all managed assets and raw data

    return await _version_response(dataset, version, row)


@router.post("/{dataset}/{version}/change_log", tags=["Version"])
async def version_history(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: ChangeLog,
    is_authorized: bool = Depends(is_admin),
):
    """
    Log changes for given dataset version
    """
    message = [request.dict()]

    return await versions.update_version(dataset, version, change_log=message)


async def _version_response(
    dataset: str, version: str, data: ORMVersion
) -> Dict[str, Any]:
    """
    Assure that version responses are parsed correctly and include associated assets
    """

    assets: List[ORMAsset] = await ORMAsset.select("asset_type", "asset_uri").where(
        ORMAsset.dataset == dataset
    ).where(ORMAsset.version == version).gino.all()
    response = Version.from_orm(data).dict(by_alias=True)
    response["assets"] = [(asset[0], asset[1]) for asset in assets]

    return response


def _prepare_sources(
    dataset: str,
    version: str,
    request: Union[VersionCreateIn, Optional[VersionUpdateIn]],
    uploaded_file: Optional[UploadFile],
) -> Tuple[Dict[str, Any], Optional[IO], Optional[str]]:

    if request is None:
        input_data: Dict[str, Any] = {}
    else:
        # Check if either files or source_uri are set, but not both
        if not true_xor(bool(uploaded_file), bool(request.source_uri)):
            raise HTTPException(
                status_code=400,
                detail="Either source_uri must be set, or a file need to be attached",
            )
        input_data = request.dict()

    if uploaded_file:
        file_obj: Optional[IO] = uploaded_file.file
        uri: Optional[str] = f"{dataset}/{version}/raw/{uploaded_file.filename}"
        input_data["source_uri"] = [f"s3://{BUCKET}/{uri}"]

    else:
        file_obj = None
        uri = None

    return input_data, file_obj, uri
