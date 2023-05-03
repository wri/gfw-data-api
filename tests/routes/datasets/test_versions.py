from unittest.mock import patch
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError
from httpx import AsyncClient

from app.settings.globals import S3_ENTRYPOINT_URL
from app.utils.aws import get_s3_client
from tests import BUCKET, DATA_LAKE_BUCKET, SHP_NAME
from tests.conftest import FAKE_INT_DATA_PARAMS
from tests.tasks import MockCloudfrontClient
from tests.utils import (
    create_dataset,
    create_default_asset,
    dataset_metadata,
    version_metadata,
)

dataset_payload = {"metadata": dataset_metadata}

version_payload = {
    "creation_options": {
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
        "source_driver": "ESRI Shapefile",
        "timeout": 42,
    },
    "metadata": version_metadata,
}


@pytest.mark.asyncio
async def test_versions(async_client: AsyncClient):
    """Test version path operations.

    We patch/disable background tasks here, as they run asynchronously.
    Such tasks are tested separately in a different module
    """
    dataset = "test"
    version = "v1.1.1"

    await create_default_asset(
        dataset,
        version,
        dataset_payload=dataset_payload,
        version_payload=version_payload,
        async_client=async_client,
        execute_batch_jobs=False,
    )

    response = await async_client.get(f"/dataset/{dataset}/{version}")
    version_data = response.json()

    assert version_data["data"]["is_latest"] is False
    assert version_data["data"]["dataset"] == dataset
    assert version_data["data"]["version"] == version
    assert (
        version_data["data"]["metadata"]["resolution"] == version_metadata["resolution"]
    )
    assert (
        version_data["data"]["metadata"]["content_date_range"]["start_date"]
        == version_metadata["content_date_range"]["start_date"]
    )

    assert version_data["data"]["version"] == "v1.1.1"

    ###############
    # Latest Tag
    ###############

    response = await async_client.patch(
        f"/dataset/{dataset}/{version}", json={"is_latest": True}
    )
    assert response.status_code == 200

    # Check if the latest endpoint redirects us to v1.1.1
    response = await async_client.get(
        f"/dataset/{dataset}/latest?test=test&test1=test1", follow_redirects=True
    )
    assert response.json()["data"]["version"] == "v1.1.1"

    ##################################################
    # additional attributes coming from default asset
    ##################################################

    # Creation Options

    version_creation_options = {
        "source_driver": "ESRI Shapefile",
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
        "layers": None,
        "indices": [
            {"column_names": ["geom"], "index_type": "gist"},
            {"column_names": ["geom_wm"], "index_type": "gist"},
            {"column_names": ["gfw_geostore_id"], "index_type": "hash"},
        ],
        "create_dynamic_vector_tile_cache": True,
        "add_to_geostore": True,
        "timeout": 42,
        "cluster": None,
        "table_schema": None,
    }

    response = await async_client.get(f"/dataset/{dataset}/{version}/creation_options")
    assert response.status_code == 200
    assert response.json()["data"] == version_creation_options

    # Change Log

    response = await async_client.get(f"/dataset/{dataset}/{version}/change_log")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1

    # Query

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=SELECT%20%2A%20from%20version%3B%20DELETE%20FROM%20version%3B",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == "Must use exactly one SQL statement."

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=DELETE FROM version;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == "Must use SELECT statements only."

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=WITH t as (select 1) SELECT * FROM version;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == "Must not have WITH clause."

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=SELECT * FROM version, version2;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == "Must list exactly one table in FROM clause."

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=SELECT * FROM (select * from a) as b;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == "Must not use sub queries."

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=SELECT PostGIS_Full_Version() FROM data;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio
@pytest.mark.skip("Deprecated. We don't inherit metadata by default any longer.")
async def test_version_metadata(async_client: AsyncClient):
    """Test if Version inherits metadata from Dataset.

    Version should be able to overwrite any metadata attribute
    """
    dataset = "test"
    version = "v1.1.1"

    response = await async_client.put(
        f"/dataset/{dataset}",
        json={"metadata": dataset_metadata},
        follow_redirects=True,
    )

    assert response.status_code == 201

    assert (
        response.json()["data"]["metadata"]["resolution"]
        == version_metadata["resolution"]
    )
    assert (
        response.json()["data"]["metadata"]["content_date_range"]
        == version_metadata["content_date_range"]
    )

    new_metadata = {"title": "New title"}

    new_payload = {
        "metadata": new_metadata,
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
            "source_driver": "ESRI Shapefile",
        },
    }

    with patch("app.tasks.default_assets.create_default_asset", return_value=True):
        response = await async_client.put(
            f"/dataset/{dataset}/{version}", json=new_payload
        )

    assert response.status_code == 202
    assert (
        response.json()["data"]["metadata"]["resolution"] == version_metadata["title"]
    )
    assert response.json()["data"]["metadata"]["title"] == new_metadata["title"]

    response = await async_client.get(f"/dataset/{dataset}/{version}")
    assert response.json()["data"]["metadata"]["title"] == version_metadata["title"]

    with patch("fastapi.BackgroundTasks.add_task", return_value=None) as mocked_task:
        response = await async_client.delete(f"/dataset/{dataset}/{version}")
        assert response.json()["data"]["metadata"] == version_metadata
        assert mocked_task.called


@pytest.mark.asyncio
@patch("app.tasks.aws_tasks.get_cloudfront_client")
async def test_version_delete_protection(
    mocked_cloudfront_client, async_client: AsyncClient
):
    dataset = "test"
    version1 = "v1.1.1"
    version2 = "v1.1.2"

    mocked_cloudfront_client.return_value = MockCloudfrontClient()

    await create_default_asset(dataset, version1, async_client=async_client)
    await create_default_asset(
        dataset, version2, async_client=async_client, skip_dataset=True
    )

    response = await async_client.patch(
        f"/dataset/{dataset}/{version2}", json={"is_latest": True}
    )
    assert response.status_code == 200

    response = await async_client.delete(f"/dataset/{dataset}/{version2}")

    assert response.status_code == 409

    await async_client.delete(f"/dataset/{dataset}/{version1}")
    response = await async_client.delete(f"/dataset/{dataset}/{version2}")
    assert response.status_code == 200
    assert mocked_cloudfront_client.called


@pytest.mark.asyncio
async def test_latest_middleware(async_client: AsyncClient):
    """Test if middleware redirects to correct version when using `latest`
    version identifier."""

    dataset = "test"
    version = "v1.1.1"

    await create_default_asset(dataset, version, async_client=async_client)

    response = await async_client.get(f"/dataset/{dataset}/{version}")
    assert response.status_code == 200

    response = await async_client.get(f"/dataset/{dataset}/latest")
    assert response.status_code == 404

    response = await async_client.patch(
        f"/dataset/{dataset}/{version}", json={"is_latest": True}
    )
    assert response.status_code == 200
    assert response.json()["data"]["is_latest"] is True

    response = await async_client.get(
        f"/dataset/{dataset}/latest", follow_redirects=True
    )
    assert response.status_code == 200
    assert response.json()["data"]["version"] == version


@pytest.mark.asyncio
async def test_invalid_source_uri(async_client: AsyncClient):
    """Test version path operations.

    We patch/ disable background tasks here, as they run asynchronously.
    Such tasks are tested separately in a different module
    """
    dataset = "test"
    version = "v1.1.1"

    source_uri = [
        "s3://doesnotexist",
    ]
    new_payload = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": source_uri,
            "source_driver": "ESRI Shapefile",
        },
        "metadata": version_metadata,
    }

    await create_dataset(dataset, async_client, dataset_payload)

    # Test creating a version with (some) bad source URIs
    bad_uri = "s3://doesnotexist"
    response = await async_client.put(f"/dataset/{dataset}/{version}", json=new_payload)
    assert response.status_code == 400
    assert response.json()["status"] == "failed"
    assert (
        response.json()["message"]
        == f"Cannot access all of the source files. Invalid sources: ['{bad_uri}']"
    )

    # Create a version with a valid source_uri so we have something to append to
    version_payload["creation_options"]["source_uri"] = [f"s3://{BUCKET}/{SHP_NAME}"]
    await create_default_asset(
        dataset,
        version,
        skip_dataset=True,
        version_payload=version_payload,
        async_client=async_client,
        execute_batch_jobs=False,
    )

    # Test appending to a version that exists
    response = await async_client.post(
        f"/dataset/{dataset}/{version}/append", json={"source_uri": source_uri}
    )
    assert response.status_code == 400
    assert response.json()["status"] == "failed"
    assert (
        response.json()["message"]
        == f"Cannot access all of the source files. Invalid sources: ['{bad_uri}']"
    )

    # Test appending to a version that DOESN'T exist
    # Really this tests dataset_version_dependency, but that isn't done elsewhere yet
    bad_version = "v1.42"
    response = await async_client.post(
        f"/dataset/{dataset}/{bad_version}/append", json={"source_uri": source_uri}
    )
    assert response.status_code == 404
    assert response.json()["status"] == "failed"
    assert (
        response.json()["message"]
        == f"Version with name {dataset}.{bad_version} does not exist"
    )


@pytest.mark.asyncio
async def test_put_latest(async_client: AsyncClient):

    dataset = "test"
    response = await async_client.put(f"/dataset/{dataset}", json=dataset_payload)
    assert response.status_code == 201

    response = await async_client.put(
        f"/dataset/{dataset}/latest", json=version_payload
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "You must list version name explicitly for this operation."
    )


@pytest.mark.asyncio
async def test_version_put_raster(async_client: AsyncClient):
    """Test raster source version operations."""

    dataset = "test_version_put_raster"
    version = "v1.0.0"

    s3_client = get_s3_client()

    pixetl_output_files = [
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/gdal-geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/gdal-geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/gdal-geotiff/90N_000E.tif",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/geotiff/90N_000E.tif",
    ]

    for key in pixetl_output_files:
        s3_client.delete_object(Bucket="gfw-data-lake-test", Key=key)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "subset": "90N_000E",
        },
        "metadata": version_metadata,
    }

    await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )

    for key in pixetl_output_files:
        try:
            s3_client.head_object(Bucket="gfw-data-lake-test", Key=key)
        except ClientError:
            raise AssertionError(f"Key {key} doesn't exist!")

    # test to download assets
    response = await async_client.get(
        f"/dataset/{dataset}/{version}/download/geotiff",
        params={"grid": "90/27008", "tile_id": "90N_000E", "pixel_meaning": "percent"},
        follow_redirects=False,
    )
    assert response.status_code == 307
    url = urlparse(response.headers["Location"])
    assert url.scheme == "http"
    assert url.netloc == urlparse(S3_ENTRYPOINT_URL).netloc
    assert (
        url.path
        == f"/gfw-data-lake-test/{dataset}/{version}/raster/epsg-4326/90/27008/percent/geotiff/90N_000E.tif"
    )
    assert "AWSAccessKeyId" in url.query
    assert "Signature" in url.query
    assert "Expires" in url.query

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/download/geotiff",
        params={"grid": "10/40000", "tile_id": "90N_000E", "pixel_meaning": "percent"},
        follow_redirects=False,
    )
    assert response.status_code == 404


@pytest.mark.hanging
@pytest.mark.asyncio
async def test_version_put_raster_bug_fixes(async_client: AsyncClient):
    """Test bug fixes for raster source version operations."""

    dataset = "test_version_put_raster_minimal_args"
    version = "v1.0.0"

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            # "overwrite": True,  # Leave these out to test bug fix
            # "subset": "90N_000E",  # Leave these out to test bug fix
        },
        # "metadata": payload["metadata"],  # Leave these out to test bug fix
    }

    await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )
