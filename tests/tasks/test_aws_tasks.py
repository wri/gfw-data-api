from unittest import mock

from moto import mock_s3, mock_cloudfront

from app.tasks.aws_tasks import (
    delete_s3_objects,
    expire_s3_objects,
    flush_cloudfront_cache,
    update_ecs_service,
)
from app.utils.aws import get_s3_client, get_cloudfront_client

from .. import BUCKET, TSV_NAME, TSV_PATH
from . import KEY, VALUE, MockECSClient, example_distribution_config


@mock_s3
def test_delete_s3_objects():
    """ Make sure we can delete more than 1000 items."""
    s3_client = get_s3_client()

    for i in range(1001):
        s3_client.upload_file(TSV_PATH, BUCKET, "TEST_DELETE_S3_OBJECTS" + str(i))

    count = delete_s3_objects(BUCKET, "TEST_DELETE_S3_OBJECTS")
    assert count == 1001


@mock_s3
def test_expire_s3_objects():
    prefix = TSV_NAME

    s3_client = get_s3_client()
    s3_client.put_object(Bucket=BUCKET, Key=prefix, Body="booga booga booga!")

    expire_s3_objects(BUCKET, prefix=prefix)
    response = s3_client.get_bucket_lifecycle_configuration(Bucket=BUCKET)
    assert len(response["Rules"]) == 1
    assert response["Rules"][0]["ID"] == f"delete_{prefix}_None".replace(
        "/", "_"
    ).replace(".", "_")
    assert response["Rules"][0]["Filter"] == {"Prefix": prefix}

    expire_s3_objects(BUCKET, key=KEY, value=VALUE)
    response = s3_client.get_bucket_lifecycle_configuration(Bucket=BUCKET)
    assert len(response["Rules"]) == 2
    assert response["Rules"][1]["ID"] == f"delete_None_{VALUE}".replace(
        "/", "_"
    ).replace(".", "_")
    assert response["Rules"][1]["Filter"] == {"Tag": {"Key": KEY, "Value": VALUE}}

    expire_s3_objects(BUCKET, prefix, KEY, VALUE)
    response = s3_client.get_bucket_lifecycle_configuration(Bucket=BUCKET)
    assert len(response["Rules"]) == 3
    assert response["Rules"][2]["ID"] == f"delete_{prefix}_{VALUE}".replace(
        "/", "_"
    ).replace(".", "_")
    assert response["Rules"][2]["Filter"] == {
        "And": {"Prefix": prefix, "Tags": [{"Key": KEY, "Value": VALUE}]}
    }

    message = ""
    try:
        expire_s3_objects(BUCKET, prefix, KEY)
    except ValueError as e:
        message = str(e)

    assert message == "Cannot create filter using input data"


@mock_cloudfront
def test_flush_cloudfront_cache():
    config = example_distribution_config("foo")

    cloudfront_client = get_cloudfront_client()
    create_response = cloudfront_client.create_distribution(
        DistributionConfig=config
    )
    distribution_name = create_response["Distribution"]["Id"]

    invalid_response = flush_cloudfront_cache(distribution_name, [TSV_NAME])

    assert invalid_response["Invalidation"]["InvalidationBatch"]["Paths"]["Quantity"] == 1
    assert invalid_response["Invalidation"]["InvalidationBatch"]["Paths"]["Items"] == [TSV_NAME]


@mock.patch("app.tasks.aws_tasks.get_ecs_client")  # TODO use moto client
def test_update_ecs_service(mock_client):

    mock_client.return_value = MockECSClient()
    response = update_ecs_service("cluster", "service")

    assert response["service"]["serviceName"] == "service"
