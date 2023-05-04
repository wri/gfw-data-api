from typing import Any, Dict, Tuple

import pytest
from httpx import AsyncClient

from app.models.pydantic.datasets import DatasetResponse, Dataset
from app.models.pydantic.metadata import DatasetMetadata
from tests_v2.unit.app.routes.utils import assert_jsend
from tests_v2.fixtures.metadata.dataset import DATASET_METADATA


@pytest.mark.asyncio
async def test_get_dataset(
    async_client: AsyncClient, generic_dataset: Tuple[str, str]
) -> None:
    dataset_name, _ = generic_dataset
    resp = await async_client.get(f"/dataset/{dataset_name}")
    assert resp.status_code == 200
    _validate_dataset_response(resp.json(), dataset_name)


# TODO: Use mark.parameterize to test variations
@pytest.mark.asyncio
async def test_create_dataset(async_client: AsyncClient) -> None:
    dataset_name = "my_first_dataset"

    resp = await async_client.put(
        "/dataset/my_first_dataset", json={"metadata": DATASET_METADATA}
    )
    assert resp.status_code == 201
    _validate_dataset_response(resp.json(), dataset_name)


def test_update_dataset():
    pass


def test_delete_dataset():
    pass


def test__dataset_response():
    pass


def _validate_dataset_response(data, dataset_name: str) -> None:
    assert_jsend(data)
    model = DatasetResponse(**data)

    assert model.data.dataset == dataset_name
    assert model.data.metadata.data_language == DATASET_METADATA["data_language"]
    assert model.data.metadata.source == DATASET_METADATA["source"]
    assert model.data.metadata.title == DATASET_METADATA["title"]
    assert model.data.metadata.overview == DATASET_METADATA["overview"]

    assert model.data.versions == list()
