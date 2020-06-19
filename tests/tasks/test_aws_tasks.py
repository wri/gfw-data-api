import os
from datetime import datetime
from typing import Any, Dict, List
from unittest import mock

from app.tasks.aws_tasks import (
    delete_s3_objects,
    expire_s3_objects,
    flush_cloudfront_cache,
)
from app.utils.aws import get_s3_client

from moto import mock_s3  # isort:skip


TSV_NAME = "test.tsv"
TSV_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", TSV_NAME)

BUCKET = "test-bucket"
KEY = "KEY"
VALUE = "VALUE"


class MockS3Client(object):
    rules: List[Dict[str, Any]] = []

    def get_bucket_lifecycle_configuration(self, Bucket):
        return {"Rules": self.rules}

    def put_bucket_lifecycle_configuration(self, Bucket, LifecycleConfiguration):
        self.rules = LifecycleConfiguration["Rules"]
        return {
            "ResponseMetadata": {"...": "..."},
        }


class MockCloudfrontClient(object):
    def create_invalidation(self, DistributionId, InvalidationBatch):
        return {
            "Location": "string",
            "Invalidation": {
                "Id": "string",
                "Status": "string",
                "CreateTime": datetime.now(),
                "InvalidationBatch": InvalidationBatch,
            },
        }


@mock_s3
def test_delete_s3_objects():
    """"
    Make sure we can delete more than 1000 items
    """

    s3_client = get_s3_client()

    s3_client.create_bucket(Bucket=BUCKET)
    for i in range(1001):
        s3_client.upload_file(TSV_PATH, BUCKET, TSV_NAME + str(i))

    count = delete_s3_objects(BUCKET, TSV_NAME)
    assert count == 1001


@mock.patch("app.tasks.aws_tasks.get_s3_client")
def test_expire_s3_objects(mock_client):
    """
    Updating lifecycle policies in Moto doesn't seem to work correctly
    Hence I created a custom mock
    """

    mock_client.return_value = MockS3Client()
    s3_client = mock_client()
    prefix = TSV_NAME

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
    assert response["Rules"][1]["Filter"] == {"Tags": {"Key": KEY, "Value": VALUE}}

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


@mock.patch("app.tasks.aws_tasks.get_cloudfront_client")
def test_flush_cloudfront_cache(mock_client):
    """
    Moto doesn't cover cloudfront, hence my onw mock
    """
    mock_client.return_value = MockCloudfrontClient()
    # cloudfront_client = mock_client()

    response = flush_cloudfront_cache("ID", TSV_NAME)

    assert response["Invalidation"]["InvalidationBatch"]["Paths"]["Quantity"] == 1
    assert response["Invalidation"]["InvalidationBatch"]["Paths"]["Items"] == [TSV_NAME]
