import json
from datetime import datetime
from uuid import UUID, uuid4

import asyncpg
import pytest

from app.application import ContextEngine
from app.crud.assets import (
    create_asset,
    delete_asset,
    get_asset,
    get_assets,
    get_assets_by_filter,
    update_asset,
)
from app.crud.datasets import create_dataset
from app.crud.versions import create_version
from app.errors import RecordAlreadyExistsError, RecordNotFoundError
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.asset_metadata import DatabaseTableMetadata
from app.models.pydantic.metadata import VersionMetadata, DatasetMetadata

from ..utils import dataset_metadata, version_metadata, asset_metadata


@pytest.mark.asyncio
async def test_assets():
    """Testing all CRUD operations on assets in one go."""

    dataset_name = "test"
    version_name = "v1.1.1"

    # Add a dataset
    async with ContextEngine("WRITE"):
        new_dataset = await create_dataset(dataset_name)
        new_version = await create_version(dataset_name, version_name)
    assert new_dataset.dataset == dataset_name
    assert new_version.dataset == dataset_name
    assert new_version.version == version_name

    # There should be no assert for current version
    # This will throw an error b/c when initialized correctly,
    # there will be always a default asset
    assets = await get_assets_by_filter(dataset=dataset_name, version=version_name)

    assert len(assets) == 0

    # Writing to DB using context engine with "READ" shouldn't work
    async with ContextEngine("READ"):
        result = ""
        try:
            await create_asset(
                dataset_name,
                version_name,
                asset_type="Database table",
                asset_uri="s3://path/to/file",
            )
        except asyncpg.exceptions.InsufficientPrivilegeError as e:
            result = str(e)

        assert result == "permission denied for table assets"

    # Using context engine with "WRITE" should work
    async with ContextEngine("WRITE"):
        new_row = await create_asset(
            dataset_name,
            version_name,
            asset_type="Database table",
            asset_uri="s3://path/to/file",
        )
    assert isinstance(new_row.asset_id, UUID)
    assert new_row.dataset == dataset_name
    assert new_row.version == version_name
    assert new_row.asset_type == "Database table"
    assert new_row.asset_uri == "s3://path/to/file"
    assert new_row.status == "pending"
    assert new_row.is_managed is True
    assert new_row.creation_options == {}
    assert new_row.metadata == None
    assert new_row.change_log == []

    # This shouldn't work a second time
    async with ContextEngine("WRITE"):
        result = ""
        try:
            await create_asset(
                dataset_name,
                version_name,
                asset_type="Database table",
                asset_uri="s3://path/to/file",
            )
        except RecordAlreadyExistsError as e:
            result = str(e)

        assert result == (
            "Cannot create asset of type Database table. "
            "Asset uri must be unique. An asset with uri s3://path/to/file already exists"
        )

    # There should be an entry now
    rows = await get_assets_by_filter(dataset=dataset_name, version=version_name)
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0].dataset == dataset_name
    assert rows[0].version == version_name
    assert isinstance(rows[0].asset_id, UUID)
    asset_id = rows[0].asset_id

    # There should be an entry now
    rows = await get_assets_by_filter()
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0].dataset == dataset_name
    assert rows[0].version == version_name

    # There should be an entry now
    rows = await get_assets_by_filter(asset_types=["Database table"])
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0].dataset == dataset_name
    assert rows[0].version == version_name

    # There should be no such entry
    rows = await get_assets_by_filter(asset_types=["Vector tile cache"])
    assert isinstance(rows, list)
    assert len(rows) == 0

    # It should be possible to access the asset by asset id
    row = await get_asset(asset_id)
    assert row.dataset == dataset_name
    assert row.version == version_name

    # But only if the asset exists
    result = ""
    _asset_id = uuid4()
    try:
        await get_asset(_asset_id)
    except RecordNotFoundError as e:
        result = str(e)

    assert result == f"Could not find requested asset {_asset_id}"

    # It should be possible to update a dataset using a context engine
    fields = asset_metadata["fields"]
    metadata = DatabaseTableMetadata(**asset_metadata)
    logs = ChangeLog(date_time=datetime.now(), status="pending", message="all good")
    async with ContextEngine("WRITE"):
        row = await update_asset(
            asset_id,
            metadata=metadata.dict(by_alias=True),
            change_log=[logs.dict(by_alias=True)],
        )
    assert row.metadata.fields[0].name == fields[0]["name"]
    assert row.metadata.fields[0].data_type == fields[0]["data_type"]

    assert row.change_log[0]["date_time"] == json.loads(logs.json())["date_time"]
    assert row.change_log[0]["status"] == logs.dict(by_alias=True)["status"]
    assert row.change_log[0]["message"] == logs.dict(by_alias=True)["message"]

    # When deleting a dataset, method should return the deleted object
    async with ContextEngine("WRITE"):
        row = await delete_asset(asset_id)
    assert row.dataset == dataset_name
    assert row.version == version_name

    # After deleting the dataset, there should be an empty DB
    rows = await get_assets_by_filter()
    assert isinstance(rows, list)
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_assets_metadata():
    """Testing all CRUD operations on dataset in one go."""

    dataset = "test"
    version = "v1.1.1"

    # Add a dataset
    async with ContextEngine("WRITE"):
        await create_dataset(
            dataset, metadata=DatasetMetadata(**dataset_metadata).dict(by_alias=False)
        )
        await create_version(
            dataset,
            version,
            metadata=VersionMetadata(**version_metadata).dict(by_alias=True),
        )
        new_asset = await create_asset(
            dataset,
            version,
            asset_type="Database table",
            asset_uri="s3://path/to/file",
            metadata=asset_metadata,
        )

    asset_id = new_asset.asset_id
    assert new_asset.metadata.fields[0].name == asset_metadata["fields"][0]["name"]
    assert (
        new_asset.metadata.fields[0].data_type
        == asset_metadata["fields"][0]["data_type"]
    )

    async with ContextEngine("READ"):
        asset = await get_asset(asset_id)
    assert asset.metadata.fields[0].name == asset_metadata["fields"][0]["name"]
    assert (
        asset.metadata.fields[0].data_type == asset_metadata["fields"][0]["data_type"]
    )

    async with ContextEngine("READ"):
        assets = await get_assets_by_filter(dataset=dataset, version=version)
    assert assets[0].metadata.fields[0].name == asset_metadata["fields"][0]["name"]
    assert (
        assets[0].metadata.fields[0].data_type
        == asset_metadata["fields"][0]["data_type"]
    )

    async with ContextEngine("READ"):
        assets = await get_assets_by_filter(asset_types=["Database table"])
    assert assets[0].metadata.fields[0].name == asset_metadata["fields"][0]["name"]
    assert (
        assets[0].metadata.fields[0].data_type
        == asset_metadata["fields"][0]["data_type"]
    )

    async with ContextEngine("READ"):
        assets = await get_assets_by_filter()
    assert assets[0].metadata.fields[0].name == asset_metadata["fields"][0]["name"]
    assert (
        assets[0].metadata.fields[0].data_type
        == asset_metadata["fields"][0]["data_type"]
    )

    async with ContextEngine("WRITE"):
        asset = await update_asset(asset_id, metadata={"resolution": 10})
    assert asset.metadata.resolution == 10

    async with ContextEngine("WRITE"):
        asset = await delete_asset(asset_id)
    assert asset.metadata.fields[0].name == asset_metadata["fields"][0]["name"]
    assert (
        asset.metadata.fields[0].data_type == asset_metadata["fields"][0]["data_type"]
    )
