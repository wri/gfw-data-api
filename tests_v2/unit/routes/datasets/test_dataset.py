from typing import Any, Dict, Tuple

from requests import Session

from app.models.pydantic.datasets import DatasetResponse
from app.models.pydantic.metadata import DatasetMetadata
from tests_v2.unit.routes.utils import assert_jsend


def test_get_dataset(client: Session, generic_dataset: Tuple[str, str]) -> None:
    dataset_name, _ = generic_dataset
    resp = client.get(f"/dataset/{dataset_name}")
    assert resp.status_code == 200
    _validate_dataset_response(resp.json(), dataset_name)


# TODO: Use mark.paramterize to test variations
def test_create_dataset(client: Session) -> None:
    dataset_name = "my_first_dataset"
    metadata: Dict[str, Any] = {}

    resp = client.put("/dataset/my_first_dataset", json={"metadata": metadata})
    assert resp.status_code == 201
    _validate_dataset_response(resp.json(), dataset_name)


def test_update_dataset():
    pass


def test_delete_dataset():
    pass


def test__dataset_response():
    pass


def _validate_dataset_response(data: Dict[str, Any], dataset_name: str) -> None:
    assert_jsend(data)
    model = DatasetResponse(**data)

    expected_metadata = DatasetMetadata()

    assert model.data.dataset == dataset_name
    assert model.data.metadata == expected_metadata.dict()
    assert model.data.versions == list()
