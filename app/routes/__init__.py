from fastapi import Path


VERSION_REGEX = r"^v\d{1,8}\.?\d{1,3}\.?\d{1,3}$|^latest$"


async def dataset_dependency(dataset: str = Path(..., title="Dataset")):
    return dataset


async def version_dependency(
    version: str = Path(..., title="Dataset version", regex=VERSION_REGEX)
):

    # if version == "latest":
    #     version = ...

    return version
