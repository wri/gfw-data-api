import json
from typing import Any, Dict, List, Optional, Tuple, Union
from typing.io import IO

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    HTTPException,
    Response,
    UploadFile,
    logger,
)
from fastapi.responses import ORJSONResponse

from ..crud import update_data, versions
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.versions import Version as ORMVersion
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.metadata import VersionMetadata
from ..models.pydantic.sources import SourceType
from ..models.pydantic.versions import Version, VersionCreateIn, VersionUpdateIn
from ..routes import (
    dataset_dependency,
    is_admin,
    version_dependency,
    version_dependency_form,
)
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
    status_code=202,
)
async def add_new_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: VersionCreateIn,
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """
    Create or update a version for a given dataset
    """

    async def callback(message: Dict[str, Any]) -> None:
        pass

    input_data = request.dict()
    # Register version with DB
    new_version: ORMVersion = await versions.create_version(
        dataset, version, **input_data
    )

    # Everything else happens in the background task asynchronously
    background_tasks.add_task(
        create_default_asset, dataset, version, input_data, None, callback
    )

    response.headers["Location"] = f"/{dataset}/{version}"
    return await _version_response(dataset, version, new_version)


@router.post(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Version"],
    response_model=Version,
    status_code=202,
)
async def add_new_version_with_attached_file(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency_form),
    is_latest: bool = Form(False),
    source_type: SourceType = Form(...),
    metadata: str = Form(
        ...,
        description="Version Metadata. Add data as JSON object, converted to String",
    ),
    creation_options: str = Form(
        ...,
        description="Creation Options. Add data as JSON object, converted to String",
    ),
    file_upload: UploadFile = File(...),
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """
    Create or update a version for a given dataset. When using this path operation,
    you all parameter must be encoded as multipart/form-data, not application/json.
    """

    async def callback(message: Dict[str, Any]) -> None:
        pass

    file_obj: IO = file_upload.file
    uri: str = f"{dataset}/{version}/raw/{file_upload.filename}"

    version_metadata = VersionMetadata(**json.loads(metadata))
    request = VersionCreateIn(
        is_latest=is_latest,
        source_type=source_type,
        source_uri=[f"s3://{BUCKET}/{uri}"],
        metadata=version_metadata,
        creation_options=json.loads(creation_options),
    )

    input_data = request.dict()
    # Register version with DB
    new_version: ORMVersion = await versions.create_version(
        dataset, version, **input_data
    )

    # Everything else happens in the background task asynchronously
    background_tasks.add_task(
        create_default_asset, dataset, version, input_data, file_obj, callback
    )

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
    request: VersionUpdateIn,
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
):
    """
    Partially update a version of a given dataset.
    When using PATCH and uploading files,
    this will overwrite the existing source(s) and trigger a complete update of all managed assets
    """

    input_data = request.dict()

    row: ORMVersion = await versions.update_version(dataset, version, **input_data)
    # TODO: Need to clarify routine for when source_uri has changed. Append/ overwrite

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


#
# def _prepare_sources(
#     dataset: str,
#     version: str,
#     request: Union[VersionCreateIn, Optional[VersionUpdateIn]],
#     uploaded_file: Optional[UploadFile],
# ) -> Tuple[Dict[str, Any], Optional[IO], Optional[str]]:
#
#     if request is None:
#         input_data: Dict[str, Any] = {}
#     else:
#         # Check if either files or source_uri are set, but not both
#         if not true_xor(bool(uploaded_file), bool(request.source_uri)):
#             raise HTTPException(
#                 status_code=400,
#                 detail="Either source_uri must be set, or a file need to be attached",
#             )
#         input_data = request.dict()
#
#     if uploaded_file:
#         file_obj: Optional[IO] = uploaded_file.file
#         uri: Optional[str] = f"{dataset}/{version}/raw/{uploaded_file.filename}"
#         input_data["source_uri"] = [f"s3://{BUCKET}/{uri}"]
#
#     else:
#         file_obj = None
#         uri = None
#
#     return input_data, file_obj, uri
