import asyncpg
import pytest

from app.application import ContextEngine
from app.crud.datasets import (
    create_dataset,
    delete_dataset,
    get_dataset,
    get_datasets,
    update_dataset,
)
from app.errors import RecordAlreadyExistsError, RecordNotFoundError
from app.models.pydantic.datasets import DatasetUpdateIn
from app.models.pydantic.metadata import DatasetMetadata

from ..utils import dataset_metadata


@pytest.mark.asyncio
async def test_dataset():
    """Testing all CRUD operations on dataset in one go."""

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
    async with ContextEngine("WRITE"):
        new_row = await create_dataset("test")
    assert new_row.dataset == "test"

    # This shouldn't work a second time
    async with ContextEngine("WRITE"):
        result = ""
        try:
            await create_dataset("test")
        except RecordAlreadyExistsError as e:
            result = str(e)

        assert result == "Dataset with name test already exists"

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
    try:
        await get_dataset("test2")
    except RecordNotFoundError as e:
        result = str(e)

    assert result == "Dataset with name test2 does not exist"

    # It should be possible to update a dataset using a context engine
    metadata = DatasetMetadata(**dataset_metadata)
    data = DatasetUpdateIn(metadata=metadata)
    async with ContextEngine("WRITE"):
        row = await update_dataset("test", **data.dict(exclude_unset=True))
    assert row.metadata.title == "test metadata"
    assert row.metadata.data_language == "en"

    # When deleting a dataset, method should return the deleted object
    async with ContextEngine("WRITE"):
        row = await delete_dataset("test")
    assert row.dataset == "test"

    # After deleting the dataset, there should be an empty DB
    rows = await get_datasets()
    assert isinstance(rows, list)
    assert len(rows) == 0
