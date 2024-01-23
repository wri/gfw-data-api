from asyncio import Task, create_task, gather
from typing import List, Sequence, Tuple
from urllib.parse import urlparse

from aiobotocore.session import get_session
from fastapi import Depends, HTTPException, Path
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordBearer

from ..crud.versions import get_version
from ..errors import RecordNotFoundError
from ..settings.globals import AWS_REGION, S3_ENTRYPOINT_URL
from ..utils.aws import get_aws_files
from ..utils.google import get_gs_files

DATASET_REGEX = r"^[a-z][a-z0-9_-]{2,}$"
VERSION_REGEX = r"^v\d{1,8}(\.\d{1,3}){0,2}?$|^latest$"
DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

SUPPORTED_FILE_EXTENSIONS: Sequence[str] = (
    ".csv",
    ".geojson",
    ".gpkg",
    ".ndjson",
    ".shp",
    ".tif",
    ".tsv",
    ".zip",
)

source_uri_lister_constructor = {"gs": get_gs_files, "s3": get_aws_files}


async def dataset_dependency(
    dataset: str = Path(..., title="Dataset", regex=DATASET_REGEX)
) -> str:
    if dataset == "latest":
        raise HTTPException(
            status_code=400,
            detail="Name `latest` is reserved for versions only.",
        )
    return dataset


async def version_dependency(
    version: str = Path(..., title="Dataset version", regex=VERSION_REGEX),
) -> str:
    # Middleware should have redirected GET requests to latest version already.
    # Any other request method should not use `latest` keyword.
    if version == "latest":
        raise HTTPException(
            status_code=400,
            detail="You must list version name explicitly for this operation.",
        )
    return version


async def dataset_version_dependency(
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
) -> Tuple[str, str]:
    # make sure version exists
    try:
        await get_version(dataset, version)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=(str(e)))

    return dataset, version


async def verify_source_file_access(sources: List[str]) -> None:
    """For each source URI, verify that it points to an existing object
    or a bucket and prefix which contain one or more objects. Returns
    nothing on success, but raises an HTTPException if one or more
    sources are invalid"""
    # TODO:
    # 1. It would be nice if the acceptable file extensions were passed
    # into this function so we could say, for example, that there must be
    # TIFFs found for a new raster tile set, but a CSV is required for a new
    # vector tile set version. Even better would be to specify whether
    # paths to individual files or "folders" (prefixes) are allowed.

    invalid_sources: List[str] = list()

    tasks: List[Task] = list()

    for source in sources:
        url_parts = urlparse(source, allow_fragments=False)
        try:
            list_func = source_uri_lister_constructor[url_parts.scheme.lower()]
        except KeyError:
            invalid_sources.append(source)
            continue
        bucket = url_parts.netloc
        prefix = url_parts.path.lstrip("/")

        # Allow pseudo-globbing: Tolerate a "*" at the end of a
        # src_uri entry to allow partial prefixes (for example
        # /bucket/prefix_part_1/prefix_fragment* will match
        # /bucket/prefix_part_1/prefix_fragment_1.tif and
        # /bucket/prefix_part_1/prefix_fragment_2.tif, etc.)
        # If the prefix doesn't end in "*" or an acceptable file extension
        # add a "/" to the end of the prefix to enforce it being a "folder".
        new_prefix: str = prefix
        if new_prefix.endswith("*"):
            new_prefix = new_prefix[:-1]
        elif not new_prefix.endswith("/") and not any(
            [new_prefix.endswith(suffix) for suffix in SUPPORTED_FILE_EXTENSIONS]
        ):
            new_prefix += "/"

        session = get_session()
        async with session.create_client(
            "s3", region_name=AWS_REGION, endpoint_url=S3_ENTRYPOINT_URL
        ) as s3_client:
            tasks.append(
                create_task(
                    get_aws_files(
                        s3_client,
                        bucket,
                        new_prefix,
                        limit=10,
                        exit_after_max=1,
                        extensions=SUPPORTED_FILE_EXTENSIONS,
                    )
                )
            )

    results = await gather(*tasks, return_exceptions=True)
    for uri, result in zip(sources, results):
        if isinstance(result, Exception):
            logger.error(f"Encountered exception checking src_uri {uri}: {result}")
            invalid_sources.append(uri)
        elif not result:
            invalid_sources.append(uri)

    if invalid_sources:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot access all of the source files. "
                f"Invalid sources: {invalid_sources}"
            ),
        )
