import os

import boto3
import pytest

from app.application import ContextEngine
from app.crud import datasets, versions
from app.settings.globals import AWS_REGION
from app.tasks.default_assets import create_default_asset

GEOJSON_NAME = "test.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", GEOJSON_NAME)
BUCKET = "test-bucket"


@pytest.mark.asyncio
async def test_vector_source_asset(batch_client, moto_s3):
    # TODO: define what a callback should do
    async def callback(message):
        pass

    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)

    dataset = "test"
    version = "v1.1.1"
    input_data = {
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME}"],
        "creation_options": {"src_driver": "GeoJSON", "zipped": False},
        "metadata": {},
    }

    file_obj = None

    async with ContextEngine("PUT"):
        await datasets.create_dataset(dataset)
        await versions.create_version(dataset, version, **input_data)

    await create_default_asset(
        dataset, version, input_data, file_obj, callback,
    )

    row = await versions.get_version(dataset, version)
    assert row.status == "failed"
    assert row.change_log == ""
