import pytest
from moto import mock_s3

from app.utils.aws import get_s3_client


@mock_s3
@pytest.mark.asyncio
async def test_get_aws_files():
    good_bucket = "good_bucket"
    good_prefix = "good_prefix"

    s3_client = get_s3_client()

    s3_client.create_bucket(Bucket=good_bucket)
    s3_client.put_object(
        Bucket=good_bucket, Key=f"{good_prefix}/world.tif", Body="booga booga!"
    )

    # Import this inside the test function so we're covered
    # by the mock_s3 decorator
    from app.utils.aws import get_aws_files

    keys = get_aws_files(good_bucket, good_prefix)
    assert len(keys) == 1
    assert keys[0] == f"/vsis3/{good_bucket}/{good_prefix}/world.tif"

    keys = get_aws_files(good_bucket, good_prefix, extensions=[".pdf"])
    assert len(keys) == 0

    keys = get_aws_files(good_bucket, "bad_prefix")
    assert len(keys) == 0

    keys = get_aws_files("bad_bucket", "doesnt_matter")
    assert len(keys) == 0

    s3_client.put_object(
        Bucket=good_bucket, Key=f"{good_prefix}/another_world.csv", Body="booga booga!"
    )

    keys = get_aws_files(good_bucket, good_prefix)
    assert len(keys) == 2
    assert f"/vsis3/{good_bucket}/{good_prefix}/another_world.csv" in keys
    assert f"/vsis3/{good_bucket}/{good_prefix}/world.tif" in keys

    keys = get_aws_files(good_bucket, good_prefix, extensions=[".csv"])
    assert len(keys) == 1
    assert keys[0] == f"/vsis3/{good_bucket}/{good_prefix}/another_world.csv"

    keys = get_aws_files(good_bucket, good_prefix, limit=1)
    assert len(keys) == 1
    assert (
        f"/vsis3/{good_bucket}/{good_prefix}/another_world.csv" in keys
        or f"/vsis3/{good_bucket}/{good_prefix}/world.tif" in keys
    )

    s3_client.put_object(
        Bucket=good_bucket, Key=f"{good_prefix}/coverage_layer.tif", Body="booga booga!"
    )
    keys = get_aws_files(good_bucket, good_prefix)
    assert len(keys) == 3
    assert f"/vsis3/{good_bucket}/{good_prefix}/another_world.csv" in keys
    assert f"/vsis3/{good_bucket}/{good_prefix}/coverage_layer.tif" in keys
    assert f"/vsis3/{good_bucket}/{good_prefix}/world.tif" in keys

    keys = get_aws_files(
        good_bucket, good_prefix, exit_after_max=1, extensions=[".tif"]
    )
    assert len(keys) == 1
    assert (
        f"/vsis3/{good_bucket}/{good_prefix}/coverage_layer.tif" in keys
        or f"/vsis3/{good_bucket}/{good_prefix}/world.tif" in keys
    )
