import asyncpg
import pytest
from fastapi import HTTPException

from app.application import ContextEngine
from app.crud.datasets import (
    create_dataset,
    delete_dataset,
    get_dataset,
    get_datasets,
    update_dataset,
)
from app.models.pydantic.datasets import DatasetUpdateIn
from app.models.pydantic.metadata import DatasetMetadata


@pytest.mark.asyncio
async def test_dataset():
    """
    Testing all CRUD operations on dataset in one go
    """

    # There should be an empty DB
    rows = await get_datasets()
    assert isinstance(rows, list)
    assert len(rows) == 0

    # Writing to DB using context engine with "READ" shouldn't work
    async with ContextEngine("READ"):
        result = ""
        try:
            await create_dataset("test")
        except asyncpg.exceptions.InsufficientPrivilegeError as e:
            result = str(e)

        assert result == "permission denied for table datasets"

    # Using context engine with "PUT" should work
    async with ContextEngine("PUT"):
        new_row = await create_dataset("test")
    assert new_row.dataset == "test"

    # This shouldn't work a second time
    async with ContextEngine("PUT"):
        result = ""
        status_code = 200
        try:
            await create_dataset("test")
        except HTTPException as e:
            result = e.detail
            status_code = e.status_code

        assert result == "Dataset with name test already exists"
        assert status_code == 400

    # Trying to write without context shouldn't work
    result = ""
    try:
        await create_dataset("test2")
    except asyncpg.exceptions.InsufficientPrivilegeError as e:
        result = str(e)

    assert result == "permission denied for table datasets"

    # There should be an entry now
    rows = await get_datasets()
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0].dataset == "test"

    # It should be possible to access the dataset by dataset name
    row = await get_dataset("test")
    assert row.dataset == "test"
    assert row.metadata == {}

    # But only if the dataset exists
    result = ""
    status_code = 200
    try:
        await get_dataset("test2")
    except HTTPException as e:
        result = e.detail
        status_code = e.status_code

    assert result == "Dataset with name test2 does not exist"
    assert status_code == 404

    # It should be possible to update a dataset using a context engine
    metadata = DatasetMetadata(title="Test Title", tags=["tag1", "tag2"])
    data = DatasetUpdateIn(metadata=metadata)
    async with ContextEngine("PUT"):
        row = await update_dataset("test", data)
    assert row.metadata["title"] == "Test Title"
    assert row.metadata["tags"] == ["tag1", "tag2"]

    # When deleting a dataset, method should return the deleted object
    async with ContextEngine("DELETE"):
        row = await delete_dataset("test")
    assert row.dataset == "test"

    # After deleting the dataset, there should be an empty DB
    rows = await get_datasets()
    assert isinstance(rows, list)
    assert len(rows) == 0
