import contextlib
import threading
import csv
import io
from http.server import HTTPServer

import boto3
import pytest
import requests
from alembic.config import main
from docker.models.containers import ContainerCollection
from fastapi.testclient import TestClient

from app.routes import is_admin, is_service_account
from app.settings.globals import (
    AURORA_JOB_QUEUE,
    AWS_REGION,
    DATA_LAKE_JOB_QUEUE,
    GDAL_PYTHON_JOB_DEFINITION,
    PIXETL_JOB_DEFINITION,
    PIXETL_JOB_QUEUE,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
    TILE_CACHE_JOB_DEFINITION,
    TILE_CACHE_JOB_QUEUE,
)

from . import (
    BUCKET,
    GEOJSON_NAME,
    GEOJSON_PATH,
    SHP_NAME,
    SHP_PATH,
    TSV_NAME,
    TSV_PATH,
    APPEND_TSV_NAME,
    APPEND_TSV_PATH,
    AWSMock,
    MemoryServer,
    is_admin_mocked,
    is_service_account_mocked,
    session,
    setup_clients,
)

# We overwrite endpoint_url directly in the app.
# Keeping this around for now, just in case we want to revert back to fixtures.
# @pytest.fixture(autouse=True)
# def moto_s3():
#     with patch(
#         "app.utils.aws.get_s3_client",
#         return_value=boto3.client(
#             "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
#         ),
#     ) as moto_s3:
#         yield moto_s3


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


@pytest.fixture(scope="session", autouse=True)
def db():
    """Acquire a database session for a test and make sure the connection gets
    properly closed, even if test fails.

    This is a synchronous connection using psycopg2.
    """
    with contextlib.ExitStack() as stack:
        yield stack.enter_context(session())


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


@pytest.fixture(scope="session")
def httpd():
    server_class = HTTPServer
    handler_class = MemoryServer
    port = 9000

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
    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)
    s3_client.upload_file(TSV_PATH, BUCKET, TSV_NAME)
    s3_client.upload_file(SHP_PATH, BUCKET, SHP_NAME)
    s3_client.upload_file(APPEND_TSV_PATH, BUCKET, APPEND_TSV_NAME)

    # upload a separate for each row so we can test running large numbers of sources in parallel
    reader = csv.DictReader(open(TSV_PATH, newline=''), delimiter='\t')
    for row in reader:
        out = io.StringIO()
        writer = csv.writer(out, delimiter='\t')
        writer.writerow(reader.fieldnames)
        writer.writerow(row.values())

        s3_client.upload_fileobj(out, BUCKET, f"test_{reader.line_num}.tsv")
        out.close()

