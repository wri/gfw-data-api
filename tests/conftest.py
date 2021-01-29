import csv
import io
import os
import shutil
import threading
import zipfile
from http.server import HTTPServer

import boto3
import httpx
import pytest
import rasterio
from alembic.config import main
from docker.models.containers import ContainerCollection
from fastapi.testclient import TestClient
from httpx import AsyncClient

from app.routes import is_admin, is_service_account
from app.settings.globals import (
    AURORA_JOB_QUEUE,
    AURORA_JOB_QUEUE_FAST,
    AWS_GCS_KEY_SECRET_ARN,
    AWS_REGION,
    AWS_SECRETSMANAGER_URL,
    DATA_LAKE_BUCKET,
    DATA_LAKE_JOB_QUEUE,
    GDAL_PYTHON_JOB_DEFINITION,
    PIXETL_JOB_DEFINITION,
    PIXETL_JOB_QUEUE,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
    RASTER_ANALYSIS_LAMBDA_NAME,
    TILE_CACHE_BUCKET,
    TILE_CACHE_JOB_DEFINITION,
    TILE_CACHE_JOB_QUEUE,
)

from . import (
    APPEND_TSV_NAME,
    APPEND_TSV_PATH,
    BUCKET,
    GEOJSON_NAME,
    GEOJSON_PATH,
    PORT,
    SHP_NAME,
    SHP_PATH,
    TSV_NAME,
    TSV_PATH,
    AWSMock,
    MemoryServer,
    is_admin_mocked,
    is_service_account_mocked,
    setup_clients,
)
from .utils import upload_fake_data

FAKE_INT_DATA_PARAMS = {
    "dtype": rasterio.uint16,
    "no_data": 0,
    "dtype_name": "uint16",
    "prefix": "test/v1.1.1/raw/uint16",
}
FAKE_FLOAT_DATA_PARAMS = {
    "dtype": rasterio.float32,
    "no_data": float("nan"),
    "dtype_name": "float32",
    "prefix": "test/v1.1.1/raw/float32",
}


# TODO Fixme
# @pytest.fixture(scope="session", autouse=True)
# def ecs_client():
#     with mock_ecs():
#         client = boto3.client("ecs", region_name="us-east-1")
#         task_definition = client.register_task_definition(
#             family="test_task",
#             networkMode='host',
#             containerDefinitions=[{
#             'name': 'test_container',
#             'image': 'test_image',}]
#         )
#         cluster = client.create_cluster(clusterName=TILE_CACHE_CLUSTER)
#         service = client.create_service(
#             cluster=cluster["cluster"]["clusterArn"],
#             serviceName=TILE_CACHE_SERVICE,
#             taskDefinition=task_definition['taskDefinition']['taskDefinitionArn'])
#         yield client
#
#         # client.stop()


@pytest.fixture(scope="session", autouse=True)
def batch_client():
    services = ["ec2", "ecs", "logs", "iam", "batch"]
    aws_mock = AWSMock(*services)

    original_run = ContainerCollection.run

    def patch_run(self, *k, **kwargs):
        kwargs["network"] = "gfw-data-api_test_default"
        return original_run(self, *k, **kwargs)

    ContainerCollection.run = patch_run

    vpc_id, subnet_id, sg_id, iam_arn = setup_clients(
        aws_mock.mocked_services["ec2"]["client"],
        aws_mock.mocked_services["iam"]["client"],
    )

    aurora_writer_env = aws_mock.add_compute_environment(
        "aurora_writer", subnet_id, sg_id, iam_arn
    )
    s3_writer_env = aws_mock.add_compute_environment(
        "s3_writer", subnet_id, sg_id, iam_arn
    )
    pixetl_env = aws_mock.add_compute_environment("pixetl", subnet_id, sg_id, iam_arn)

    aws_mock.add_job_queue(AURORA_JOB_QUEUE, aurora_writer_env["computeEnvironmentArn"])
    aws_mock.add_job_queue(
        AURORA_JOB_QUEUE_FAST, aurora_writer_env["computeEnvironmentArn"]
    )
    aws_mock.add_job_queue(DATA_LAKE_JOB_QUEUE, s3_writer_env["computeEnvironmentArn"])
    aws_mock.add_job_queue(TILE_CACHE_JOB_QUEUE, s3_writer_env["computeEnvironmentArn"])
    aws_mock.add_job_queue(PIXETL_JOB_QUEUE, pixetl_env["computeEnvironmentArn"])

    aws_mock.add_job_definition(GDAL_PYTHON_JOB_DEFINITION, "batch_gdal-python_test")
    aws_mock.add_job_definition(
        POSTGRESQL_CLIENT_JOB_DEFINITION, "batch_postgresql-client_test"
    )
    aws_mock.add_job_definition(TILE_CACHE_JOB_DEFINITION, "batch_tile_cache_test")
    aws_mock.add_job_definition(PIXETL_JOB_DEFINITION, "pixetl_test", mount_tmp=True)

    yield aws_mock.mocked_services["batch"]["client"], aws_mock.mocked_services["logs"][
        "client"
    ]

    # aws_mock.print_logs()
    aws_mock.stop_services()


# @pytest.fixture(scope="session", autouse=True)
# def db():
#     """Acquire a database session for a test and make sure the connection gets
#     properly closed, even if test fails.
#
#     This is a synchronous connection using psycopg2.
#     """
#     with contextlib.ExitStack() as stack:
#         yield stack.enter_context(session())


@pytest.fixture(autouse=True)
def client():
    """Set up a clean database before running a test Run all migrations before
    test and downgrade afterwards."""
    from app.main import app

    main(["--raiseerr", "upgrade", "head"])
    app.dependency_overrides[is_admin] = is_admin_mocked
    app.dependency_overrides[is_service_account] = is_service_account_mocked

    with TestClient(app) as client:
        yield client

        # Clean up created assets/versions/datasets so teardown doesn't break
        datasets_resp = client.get("/datasets")
        for ds in datasets_resp.json()["data"]:
            ds_id = ds["dataset"]
            if ds.get("versions") is not None:
                for version in ds["versions"]:
                    assets_resp = client.get(f"/dataset/{ds_id}/{version}/assets")
                    for asset in assets_resp.json()["data"]:
                        print(f"DELETING ASSET {asset['asset_id']}")
                        try:
                            _ = client.delete(
                                f"/dataset/{ds_id}/{version}/{asset['asset_id']}"
                            )
                        except Exception as ex:
                            print(f"Exception deleting asset {asset['asset_id']}: {ex}")
                    try:
                        _ = client.delete(f"/dataset/{ds_id}/{version}")
                    except Exception as ex:
                        print(f"Exception deleting version {version}: {ex}")
            try:
                _ = client.delete(f"/dataset/{ds_id}")
            except Exception as ex:
                print(f"Exception deleting dataset {ds_id}: {ex}")

    app.dependency_overrides = {}
    main(["--raiseerr", "downgrade", "base"])


@pytest.fixture(autouse=True)
@pytest.mark.asyncio
async def async_client():
    """Async Test Client."""
    from app.main import app

    # main(["--raiseerr", "upgrade", "head"])
    app.dependency_overrides[is_admin] = is_admin_mocked
    app.dependency_overrides[is_service_account] = is_service_account_mocked

    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as client:
        yield client

    # app.dependency_overrides = {}
    # main(["--raiseerr", "downgrade", "base"])


@pytest.fixture(scope="session")
def httpd():

    server_class = HTTPServer
    handler_class = MemoryServer

    port = PORT

    httpd = server_class(("0.0.0.0", port), handler_class)

    t = threading.Thread(target=httpd.serve_forever)  # , daemon=True)
    t.start()

    yield httpd

    httpd.shutdown()
    t.join()


@pytest.fixture(autouse=True)
def flush_request_list(httpd):
    """Delete request cache before every test."""
    httpx.delete(f"http://localhost:{httpd.server_port}")


@pytest.fixture(autouse=True)
def copy_fixtures():
    # Upload file to mocked S3 bucket
    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver-s3:5000"
    )

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.create_bucket(Bucket=DATA_LAKE_BUCKET)
    s3_client.create_bucket(Bucket=TILE_CACHE_BUCKET)

    upload_fake_data(**FAKE_INT_DATA_PARAMS)
    upload_fake_data(**FAKE_FLOAT_DATA_PARAMS)

    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)
    s3_client.upload_file(TSV_PATH, BUCKET, TSV_NAME)
    s3_client.upload_file(SHP_PATH, BUCKET, SHP_NAME)
    s3_client.upload_file(APPEND_TSV_PATH, BUCKET, APPEND_TSV_NAME)

    # upload a separate for each row so we can test running large numbers of sources in parallel
    reader = csv.DictReader(open(TSV_PATH, newline=""), delimiter="\t")
    for row in reader:
        out = io.StringIO(newline="")
        writer = csv.DictWriter(out, delimiter="\t", fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerow(row)

        s3_client.put_object(
            Body=str.encode(out.getvalue()),
            Bucket=BUCKET,
            Key=f"test_{reader.line_num}.tsv",
        )
        out.close()


@pytest.fixture(autouse=True)
async def tmp_folder():
    """Create TMP dir."""

    curr_dir = os.path.dirname(__file__)
    tmp_dir = os.path.join(curr_dir, "fixtures", "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    ready = os.path.join(tmp_dir, "READY")

    # Create zero bytes READY file
    with open(ready, "w"):
        pass
    yield

    # clean up
    shutil.rmtree(tmp_dir)


@pytest.fixture(scope="session", autouse=True)
def lambda_client():
    services = ["lambda", "iam", "logs"]
    aws_mock = AWSMock(*services)

    resp = aws_mock.mocked_services["iam"]["client"].create_role(
        RoleName="TestRole", AssumeRolePolicyDocument="some_policy"
    )
    iam_arn = resp["Role"]["Arn"]

    def create_lambda(func_str):
        zip_output = io.BytesIO()
        zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
        zip_file.writestr("lambda_function.py", func_str)
        zip_file.close()
        zip_output.seek(0)

        return aws_mock.mocked_services["lambda"]["client"].create_function(
            Code={"ZipFile": zip_output.read()},
            FunctionName=RASTER_ANALYSIS_LAMBDA_NAME,
            Handler="lambda_function.lambda_handler",
            Runtime="python3.7",
            Role=iam_arn,
        )

    yield create_lambda

    aws_mock.stop_services()


@pytest.fixture(scope="session", autouse=True)
def secrets():

    secret_client = boto3.client(
        "secretsmanager", region_name=AWS_REGION, endpoint_url=AWS_SECRETSMANAGER_URL
    )
    secret_client.create_secret(
        Name=AWS_GCS_KEY_SECRET_ARN,
        SecretString="foosecret",  # pragma: allowlist secret
    )
    yield

    secret_client.delete_secret(SecretId=AWS_GCS_KEY_SECRET_ARN)
