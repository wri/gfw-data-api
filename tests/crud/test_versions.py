import json
from datetime import datetime

import asyncpg
import pytest
from fastapi import HTTPException

from app.application import ContextEngine
from app.crud.datasets import create_dataset
from app.crud.versions import (
    create_version,
    delete_version,
    get_version,
    get_version_names,
    get_versions,
    update_version,
)
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.metadata import VersionMetadata


@pytest.mark.asyncio
async def test_versions():
    """
    Testing all CRUD operations on dataset in one go
    """

    dataset_name = "test"
    version_name = "v1.1.1"

    # Add a dataset
    async with ContextEngine("PUT"):
        new_row = await create_dataset(dataset_name)
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
    async with ContextEngine("PUT"):
        new_row = await create_version(dataset_name, version_name, source_type="table")
    assert new_row.dataset == dataset_name
    assert new_row.version == version_name
    assert new_row.is_latest is False
    assert new_row.is_mutable is False
    assert new_row.source_type == "table"
    assert new_row.source_uri == []
    assert new_row.status == "pending"
    assert new_row.has_geostore is False
    assert new_row.metadata == {}
    assert new_row.change_log == []

    # This shouldn't work a second time
    async with ContextEngine("PUT"):
        result = ""
        status_code = 200
        try:
            await create_version(dataset_name, version_name, source_type="table")
        except HTTPException as e:
            result = e.detail
            status_code = e.status_code

        assert (
            result == f"Version with name {dataset_name}.{version_name} already exists"
        )
        assert status_code == 400

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
    status_code = 200
    try:
        await get_version("test2", version_name)
    except HTTPException as e:
        result = e.detail
        status_code = e.status_code

    assert result == f"Version with name test2.{version_name} does not exist"
    assert status_code == 404

    # It should be possible to update a dataset using a context engine
    metadata = VersionMetadata(title="Test Title", tags=["tag1", "tag2"])
    logs = ChangeLog(date_time=datetime.now(), status="saved", message="all good")
    async with ContextEngine("PUT"):
        row = await update_version(
            dataset_name,
            version_name,
            metadata=metadata.dict(),
            change_log=[logs.dict()],
        )
    assert row.metadata["title"] == "Test Title"
    assert row.metadata["tags"] == ["tag1", "tag2"]
    assert row.change_log[0]["date_time"] == json.loads(logs.json())["date_time"]
    assert row.change_log[0]["status"] == logs.dict()["status"]
    assert row.change_log[0]["message"] == logs.dict()["message"]

    # When deleting a dataset, method should return the deleted object
    async with ContextEngine("DELETE"):
        row = await delete_version(dataset_name, version_name)
    assert row.dataset == dataset_name
    assert row.version == version_name

    # After deleting the dataset, there should be an empty DB
    rows = await get_versions(dataset_name)
    assert isinstance(rows, list)
    assert len(rows) == 0
