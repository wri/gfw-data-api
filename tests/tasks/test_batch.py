import pytest

from app.application import ContextEngine
from app.crud import assets, datasets, versions
from app.errors import TooManyRetriesError
from app.models.pydantic.jobs import PostgresqlClientJob
from app.tasks import callback_constructor, writer_secrets
from app.tasks.batch import execute
from tests import BUCKET, GEOJSON_NAME, PORT


@pytest.mark.skip(reason="Still working on it")
@pytest.mark.asyncio
async def test_batch_failure():
    dataset = "test"
    version = "v1.1.1"
    creation_options = {
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME}"],
        "source_driver": "GeoJSON",
        "zipped": False,
    }

    async with ContextEngine("WRITE"):
        await datasets.create_dataset(dataset)
        await versions.create_version(dataset, version)
        new_asset = await assets.create_asset(
            dataset,
            version,
            asset_type="Database table",
            asset_uri="s3://path/to/file",
            creation_options=creation_options,
        )

    job_env = writer_secrets + [
        {"name": "STATUS_URL", "value": f"http://app_test:{PORT}/tasks"}
    ]
    callback = callback_constructor(new_asset.asset_id)

    # Can't have two parents with same name

    job1 = PostgresqlClientJob(
        dataset=dataset,
        job_name="job1",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=job_env,
        callback=callback,
    )
    job2 = PostgresqlClientJob(
        dataset=dataset,
        job_name="job1",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=job_env,
        callback=callback,
    )

    job3 = PostgresqlClientJob(
        dataset=dataset,
        job_name="job3",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=job_env,
        callback=callback,
        parents=[job1.job_name, job2.job_name],
    )
    message = ""

    try:
        await execute([job1, job2, job3])
    except TooManyRetriesError as e:
        message = str(e)

    assert message == ""
