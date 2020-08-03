import csv
import io
import threading
from http.server import HTTPServer

import boto3
import pytest
import requests
from alembic.config import main
from docker.models.containers import ContainerCollection
from fastapi.testclient import TestClient
from httpx import AsyncClient

from app.routes import is_admin, is_service_account
from app.settings.globals import (
    AURORA_JOB_QUEUE,
    AURORA_JOB_QUEUE_FAST,
    AWS_REGION,
    DATA_LAKE_BUCKET,
    DATA_LAKE_JOB_QUEUE,
    GDAL_PYTHON_JOB_DEFINITION,
    PIXETL_JOB_DEFINITION,
    PIXETL_JOB_QUEUE,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
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
    aws_mock.add_job_definition(PIXETL_JOB_DEFINITION, "pixetl_test")

    yield aws_mock.mocked_services["batch"]["client"], aws_mock.mocked_services["logs"][
        "client"
    ]

    # aws_mock.print_logs()
    aws_mock.stop_services()


#
#
# @pytest.fixture(scope="session", autouse=True)
# def db():
#     """Acquire a database session for a test and make sure the connection gets
#     properly closed, even if test fails.
#
#     This is a synchronous connection using psycopg2.
#     """
#     with contextlib.ExitStack() as stack:
#         yield stack.enter_context(session())
#
#
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
    requests.delete(f"http://localhost:{httpd.server_port}")


@pytest.fixture(autouse=True)
def copy_fixtures():
    # Upload file to mocked S3 bucket
    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.create_bucket(Bucket=DATA_LAKE_BUCKET)
    s3_client.create_bucket(Bucket=TILE_CACHE_BUCKET)
    s3_client.upload_file(GEOJSON_PATH, TILE_CACHE_BUCKET, "tiles.geojson")  # FIXME
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
