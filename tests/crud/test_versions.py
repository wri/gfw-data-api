import json
from datetime import datetime

import asyncpg
import pytest

from app.application import ContextEngine
from app.crud.datasets import create_dataset
from app.crud.versions import (
    create_version,
    delete_version,
    get_latest_version,
    get_version,
    get_version_names,
    get_versions,
    update_version,
)
from app.errors import RecordAlreadyExistsError, RecordNotFoundError
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.metadata import VersionMetadata

from ..utils import version_metadata, dataset_metadata


@pytest.mark.asyncio
async def test_versions():
    """Testing all CRUD operations on dataset in one go."""

    dataset_name = "test"
    version_name = "v1.1.1"

    # Add a dataset
    async with ContextEngine("WRITE"):
        new_row = await create_dataset(dataset_name, metadata=dataset_metadata)
    assert new_row.dataset == dataset_name

    # There should be no versions for new datasets
    rows = await get_versions(dataset_name)
    assert isinstance(rows, list)
    assert len(rows) == 0

    # Writing to DB using context engine with "READ" shouldn't work
    async with ContextEngine("READ"):
        result = ""
        try:
            await create_version(dataset_name, version_name)
        except asyncpg.exceptions.InsufficientPrivilegeError as e:
            result = str(e)

        assert result == "permission denied for table versions"

    # Using context engine with "PUT" should work
    async with ContextEngine("WRITE"):
        new_row = await create_version(dataset_name, version_name)
    assert new_row.dataset == dataset_name
    assert new_row.version == version_name
    assert new_row.is_latest is False
    assert new_row.is_mutable is False
    assert new_row.status == "pending"
    assert new_row.metadata == {}
    assert new_row.change_log == []

    # This shouldn't work a second time
    async with ContextEngine("WRITE"):
        result = ""
        try:
            await create_version(dataset_name, version_name)
        except RecordAlreadyExistsError as e:
            result = str(e)

        assert (
            result == f"Version with name {dataset_name}.{version_name} already exists."
        )

    # There should be an entry now
    rows = await get_versions(dataset_name)
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0].dataset == dataset_name
    assert rows[0].version == version_name

    # Version names should only have a single column
    names = await get_version_names(dataset_name)
    assert isinstance(names, list)
    assert len(names) == 1
    assert names[0].version == version_name
    result = ""
    try:
        _ = names[0].dataset
    except AttributeError as e:
        result = str(e)
    assert result == "Could not locate column in row for column 'dataset'"

    # It should be possible to access the dataset by dataset name
    row = await get_version(dataset_name, version_name)
    assert row.dataset == dataset_name
    assert row.version == version_name

    # But only if the dataset exists
    result = ""
    try:
        await get_version("test2", version_name)
    except RecordNotFoundError as e:
        result = str(e)

    assert result == f"Version with name test2.{version_name} does not exist"

    # It should be possible to update a version using a context engine
    metadata = VersionMetadata(**version_metadata)
    logs = ChangeLog(date_time=datetime.now(), status="pending", message="all good")
    async with ContextEngine("WRITE"):
        row = await update_version(
            dataset_name,
            version_name,
            metadata=metadata.dict(by_alias=True),
            change_log=[logs.dict(by_alias=True)],
        )
    assert row.metadata.resolution == version_metadata["resolution"]
    assert row.change_log[0]["date_time"] == json.loads(logs.json())["date_time"]
    assert row.change_log[0]["status"] == logs.dict(by_alias=True)["status"]
    assert row.change_log[0]["message"] == logs.dict(by_alias=True)["message"]

    # When deleting a dataset, method should return the deleted object
    async with ContextEngine("WRITE"):
        row = await delete_version(dataset_name, version_name)
    assert row.dataset == dataset_name
    assert row.version == version_name

    # After deleting the dataset, there should be an empty DB
    rows = await get_versions(dataset_name)
    assert isinstance(rows, list)
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_latest_versions():
    """Test if trigger function on versions table work It is suppose to reset
    is_latest field to False for all versions of a dataset Once a version's
    is_latest field is set to True Get Latest Version function should always
    return the latest version number."""

    dataset_name = "test"

    # Add a dataset
    async with ContextEngine("WRITE"):
        await create_dataset(dataset_name)
        await create_version(dataset_name, "v1.1.1", is_latest=True)
        await create_version(dataset_name, "v1.1.2", is_latest=True)
        latest = await get_latest_version(dataset_name)
        first_row = await get_version(dataset_name, "v1.1.1")
        second_row = await get_version(dataset_name, "v1.1.2")

    assert first_row.is_latest is False
    assert second_row.is_latest is True
    assert latest == "v1.1.2"
