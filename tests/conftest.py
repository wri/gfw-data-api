import contextlib
import os
from typing import Optional
from unittest.mock import patch

import boto3
import pytest
from alembic.config import main
from docker.models.containers import ContainerCollection
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.routes import is_admin
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
    WRITER_DBNAME,
    WRITER_HOST,
    WRITER_PASSWORD,
    WRITER_PORT,
    WRITER_USERNAME,
)

from moto import mock_batch, mock_iam, mock_ecs, mock_ec2, mock_logs  # isort:skip

SessionLocal: Optional[Session] = None
LOG_GROUP = "/aws/batch/job"
ROOT = os.environ["ROOT"]


class AWSMock(object):
    mocks = {
        "batch": mock_batch,
        "iam": mock_iam,
        "ecs": mock_ecs,
        "ec2": mock_ec2,
        "logs": mock_logs,
    }

    def __init__(self, *services):
        self.mocked_services = dict()
        for service in services:
            mocked_service = self.mocks[service]()
            mocked_service.start()
            client = boto3.client(service, region_name=AWS_REGION)
            self.mocked_services[service] = {
                "client": client,
                "mock": mocked_service,
            }
        self.add_log_group(LOG_GROUP)

    def stop_services(self):
        for service in self.mocked_services.keys():
            self.mocked_services[service]["mock"].stop()

    def add_log_group(self, log_group_name):
        self.mocked_services["logs"]["client"].create_log_group(
            logGroupName=log_group_name
        )

    def add_compute_environment(self, compute_name, subnet_id, sg_id, iam_arn):
        return self.mocked_services["batch"]["client"].create_compute_environment(
            computeEnvironmentName=compute_name,
            type="MANAGED",
            state="ENABLED",
            computeResources={
                "type": "EC2",
                "minvCpus": 0,
                "maxvCpus": 2,
                "desiredvCpus": 2,
                "instanceTypes": ["t2.small", "t2.medium"],
                "imageId": "some_image_id",
                "subnets": [subnet_id],
                "securityGroupIds": [sg_id],
                "ec2KeyPair": "string",
                "instanceRole": iam_arn.replace("role", "instance-profile"),
                "tags": {"string": "string"},
                "bidPercentage": 100,
                "spotIamFleetRole": "string",
            },
            serviceRole=iam_arn,
        )

    def add_job_queue(self, job_queue_name, env_arn):
        return self.mocked_services["batch"]["client"].create_job_queue(
            jobQueueName=job_queue_name,
            state="ENABLED",
            priority=123,
            computeEnvironmentOrder=[{"order": 123, "computeEnvironment": env_arn}],
        )

    def add_job_definition(self, job_definition_name, docker_image):

        return self.mocked_services["batch"]["client"].register_job_definition(
            jobDefinitionName=job_definition_name,
            type="container",
            containerProperties={
                "image": f"{docker_image}:latest",
                "vcpus": 1,
                "memory": 128,
                "environment": [
                    {"name": "AWS_ACCESS_KEY_ID", "value": "testing"},
                    {"name": "AWS_SECRET_ACCESS_KEY", "value": "testing"},
                    {"name": "DEBUG", "value": "1"},
                ],
                "volumes": [
                    {
                        "host": {"sourcePath": f"{ROOT}/tests/fixtures/aws"},
                        "name": "aws",
                    }
                ],
                "mountPoints": [
                    {
                        "sourceVolume": "aws",
                        "containerPath": "/root/.aws",
                        "readOnly": True,
                    }
                ],
            },
        )

    def print_logs(self):
        resp = self.mocked_services["logs"]["client"].describe_log_streams(
            logGroupName=LOG_GROUP
        )

        for stream in resp["logStreams"]:
            ls_name = stream["logStreamName"]

            stream_resp = self.mocked_services["logs"]["client"].get_log_events(
                logGroupName=LOG_GROUP, logStreamName=ls_name
            )

            print(f"-------- LOGS FROM {ls_name} --------")
            for event in stream_resp["events"]:
                print(event["message"])


@pytest.fixture(autouse=True)
def moto_s3():
    with patch(
        "app.utils.aws.get_s3_client",
        return_value=boto3.client(
            "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
        ),
    ) as moto_s3:
        yield moto_s3


@pytest.fixture(scope="session", autouse=True)
def batch_client():
    services = ["ec2", "ecs", "logs", "iam", "batch"]
    aws_mock = AWSMock(*services)

    original_run = ContainerCollection.run

    def patch_run(self, *k, **kwargs):
        kwargs["network"] = "gfw-data-api_test_default"
        return original_run(self, *k, **kwargs)

    ContainerCollection.run = patch_run

    vpc_id, subnet_id, sg_id, iam_arn = _setup(
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


def _setup(ec2_client, iam_client):
    """
    Do prerequisite setup
    :return: VPC ID, Subnet ID, Security group ID, IAM Role ARN
    :rtype: tuple
    """
    resp = ec2_client.create_vpc(CidrBlock="172.30.0.0/24")
    vpc_id = resp["Vpc"]["VpcId"]
    resp = ec2_client.create_subnet(
        AvailabilityZone="us-east-1a", CidrBlock="172.30.0.0/25", VpcId=vpc_id
    )
    subnet_id = resp["Subnet"]["SubnetId"]
    resp = ec2_client.create_security_group(
        Description="test_sg_desc", GroupName="test_sg", VpcId=vpc_id
    )
    sg_id = resp["GroupId"]

    resp = iam_client.create_role(
        RoleName="TestRole", AssumeRolePolicyDocument="some_policy"
    )
    iam_arn = resp["Role"]["Arn"]
    iam_client.create_instance_profile(InstanceProfileName="TestRole")
    iam_client.add_role_to_instance_profile(
        InstanceProfileName="TestRole", RoleName="TestRole"
    )

    return vpc_id, subnet_id, sg_id, iam_arn


@contextlib.contextmanager
def session():
    global SessionLocal

    if SessionLocal is None:
        db_conn = f"postgresql://{WRITER_USERNAME}:{WRITER_PASSWORD}@{WRITER_HOST}:{WRITER_PORT}/{WRITER_DBNAME}"  # pragma: allowlist secret
        engine = create_engine(db_conn, pool_size=1, max_overflow=0)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db: Optional[Session] = None
    try:
        db = SessionLocal()
        yield db
    finally:
        if db is not None:
            db.close()


@pytest.fixture(scope="session", autouse=True)
def db():
    """
    Aquire a database session for a test and make sure the connection gets
    properly closed, even if test fails.
    This is a synchronous connection using psycopg2.
    """
    with contextlib.ExitStack() as stack:
        yield stack.enter_context(session())


async def is_admin_mocked():
    return True


@pytest.fixture(autouse=True)
def meta_client():
    """
    Set up a clean database before running a test
    Run all migrations before test and downgrade afterwards
    """
    from app.main import meta_api

    main(["--raiseerr", "upgrade", "head"])
    meta_api.dependency_overrides[is_admin] = is_admin_mocked

    with TestClient(meta_api) as client:
        yield client

    meta_api.dependency_overrides = {}
    main(["--raiseerr", "downgrade", "base"])
