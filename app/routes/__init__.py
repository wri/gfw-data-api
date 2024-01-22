from typing import Tuple, List, Sequence
from urllib.parse import urlparse

from fastapi import Depends, HTTPException, Path
from fastapi.security import OAuth2PasswordBearer

from ..crud.versions import get_version
from ..errors import RecordNotFoundError

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

# I cannot seem to satisfy mypy WRT the type of this default dict. Last thing I tried:
# DefaultDict[str, Callable[[str, str, int, int, ...], List[str]]]
source_uri_lister_constructor = defaultdict((lambda: lambda w, x, limit=None, exit_after_max=None, extensions=None: list()))  # type: ignore
source_uri_lister_constructor.update(**{"gs": get_gs_files, "s3": get_aws_files})  # type: ignore


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

    # TODO:
    # 1. Making the list functions asynchronous and using asyncio.gather
    # to check for valid sources in a non-blocking fashion would be good.
    # Perhaps use the aioboto3 package for aws, gcloud-aio-storage for gcs.
    # 2. It would be nice if the acceptable file extensions were passed
    # into this function so we could say, for example, that there must be
    # TIFFs found for a new raster tile set, but a CSV is required for a new
    # vector tile set version. Even better would be to specify whether
    # paths to individual files or "folders" (prefixes) are allowed.

    invalid_sources: List[str] = list()

    for source in sources:
        url_parts = urlparse(source, allow_fragments=False)
        list_func = source_uri_lister_constructor[url_parts.scheme.lower()]
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

        if not list_func(
            bucket,
            new_prefix,
            limit=10,
            exit_after_max=1,
            extensions=SUPPORTED_FILE_EXTENSIONS,
        ):
            invalid_sources.append(source)

    if invalid_sources:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot access all of the source files. "
                f"Invalid sources: {invalid_sources}"
            ),
        )
