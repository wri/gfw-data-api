from app.utils.path import get_layer_name, is_zipped
from tests import BUCKET, GEOJSON_NAME, SHP_NAME


def test_zipped():
    s3_uri = f"s3://{BUCKET}/{GEOJSON_NAME}"
    zipped = is_zipped(s3_uri)
    assert zipped is False

    s3_uri = f"s3://{BUCKET}/{SHP_NAME}"
    zipped = is_zipped(s3_uri)
    assert zipped is True

    found = True
    s3_uri = f"s3://{BUCKET}/doesntexist"
    try:
        is_zipped(s3_uri)
    except FileNotFoundError:
        found = False

    assert not found


def test_get_layer_name():
    s3_uri = f"s3://{BUCKET}/{SHP_NAME}"
    layer = get_layer_name(s3_uri)
    assert layer == "test"

    s3_uri = f"s3://{BUCKET}/{GEOJSON_NAME}"
    layer = get_layer_name(s3_uri)
    assert layer == "test"
