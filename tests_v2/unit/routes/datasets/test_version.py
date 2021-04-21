from requests import Session

from tests_v2.unit.routes.utils import assert_jsend


def test_get_version(client: Session, generic_vector_source_version):
    dataset_name, version_name, version_metadata = generic_vector_source_version
    resp = client.get(f"/dataset/{dataset_name}/{version_name}")
    assert resp.status_code == 200
    data = resp.json()
    assert_jsend(data)
