from typing import Optional

from moto import mock_batch, mock_iam, mock_ecs, mock_ec2, mock_logs
import boto3
import pytest
import os
from docker.models.containers import ContainerCollection

from boto3_type_annotations.ec2 import Client as EC2Client
from boto3_type_annotations.iam import Client as IAMClient
from boto3_type_annotations.ecs import Client as ECSClient
from boto3_type_annotations.logs import Client as LOGSClient
from boto3_type_annotations.batch import Client as BATCHClient

from app.settings.globals import (
    AURORA_JOB_QUEUE,
    DATA_LAKE_JOB_QUEUE,
    TILE_CACHE_JOB_QUEUE,
    PIXETL_JOB_QUEUE,
    GDAL_PYTHON_JOB_DEFINITION,
    PIXETL_JOB_DEFINITION,
    TILE_CACHE_JOB_DEFINITION,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
)

JOB_QUEUE = "test_job_queue"
JOB_DEF = "test_job_def"

os.environ["JOB_QUEUE"] = JOB_QUEUE
os.environ["JOB_DEFINITION"] = JOB_DEF

DEFAULT_REGION = "us-east-1"
BATCH_TEST = "batch_test"

ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
SECURITY_TOKEN = os.environ.get("AWS_SECURITY_TOKEN", "")
SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")

EC2_CLIENT: Optional[EC2Client] = None
IAM_CLIENT: Optional[IAMClient] = None
ECS_CLIENT: Optional[ECSClient] = None
LOGS_CLIENT: Optional[LOGSClient] = None
BATCH_CLIENT: Optional[BATCHClient] = None


@pytest.fixture(autouse=True)
def batch_client():
    set_aws_credentials()

    global EC2_CLIENT, IAM_CLIENT, ECS_CLIENT, LOGS_CLIENT, BATCH_CLIENT

    mockec2 = mock_ec2()
    mockecs = mock_ecs()
    mocklogs = mock_logs()
    mockiam = mock_iam()
    mockbatch = mock_batch()

    mockec2.start()
    mockecs.start()
    mocklogs.start()
    mockiam.start()
    mockbatch.start()

    original_run = ContainerCollection.run

    def patch_run(self, *k, **kwargs):
        kwargs["network"] = "host"
        return original_run(self, *k, **kwargs)

    ContainerCollection.run = patch_run

    EC2_CLIENT, IAM_CLIENT, ECS_CLIENT, LOGS_CLIENT, BATCH_CLIENT = _get_clients()
    vpc_id, subnet_id, sg_id, iam_arn = _setup(EC2_CLIENT, IAM_CLIENT)

    aurora_writer_env = compute_environment("aurora_writer", subnet_id, sg_id, iam_arn)
    s3_writer_env = compute_environment("s3_writer", subnet_id, sg_id, iam_arn)
    # pixetl_env = compute_environment("pixetl", subnet_id, sg_id, iam_arn)

    job_queue(AURORA_JOB_QUEUE, aurora_writer_env["computeEnvironmentArn"])
    job_queue(DATA_LAKE_JOB_QUEUE, s3_writer_env["computeEnvironmentArn"])
    job_queue(TILE_CACHE_JOB_QUEUE, s3_writer_env["computeEnvironmentArn"])
    # job_queue(PIXETL_JOB_QUEUE, pixetl_env["computeEnvironmentArn"])

    job_definition(GDAL_PYTHON_JOB_DEFINITION, "batch_gdal-python_test")
    job_definition(POSTGRESQL_CLIENT_JOB_DEFINITION, "batch_postgresql-client_test")
    job_definition(TILE_CACHE_JOB_DEFINITION, "batch_tile_cache_test")
    # job_definition(PIXETL_JOB_DEFINITION, "")

    yield BATCH_CLIENT

    resp = LOGS_CLIENT.describe_log_streams(logGroupName="/aws/batch/job")
    ls_name = resp["logStreams"][0]["logStreamName"]

    resp = LOGS_CLIENT.get_log_events(
        logGroupName="/aws/batch/job", logStreamName=ls_name
    )

    for event in resp["events"]:
        print(event["message"])

    mockec2.stop()
    mockecs.stop()
    mocklogs.stop()
    mockiam.stop()
    mockbatch.stop()

    reset_aws_credentials()


def _get_clients():
    return (
        boto3.client("ec2", region_name=DEFAULT_REGION),
        boto3.client("iam", region_name=DEFAULT_REGION),
        boto3.client("ecs", region_name=DEFAULT_REGION),
        boto3.client("logs", region_name=DEFAULT_REGION),
        boto3.client("batch", region_name=DEFAULT_REGION),
    )


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


def set_aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # pragma: allowlist secret
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


def reset_aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY
    os.environ["AWS_SECURITY_TOKEN"] = SECURITY_TOKEN
    os.environ["AWS_SESSION_TOKEN"] = SESSION_TOKEN


def compute_environment(compute_name, subnet_id, sg_id, iam_arn):
    if BATCH_CLIENT:
        return BATCH_CLIENT.create_compute_environment(
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


def job_queue(job_queue_name, env_arn):
    if BATCH_CLIENT:
        return BATCH_CLIENT.create_job_queue(
            jobQueueName=job_queue_name,
            state="ENABLED",
            priority=123,
            computeEnvironmentOrder=[{"order": 123, "computeEnvironment": env_arn}],
        )


def job_definition(job_definition_name, docker_image):
    if BATCH_CLIENT:
        return BATCH_CLIENT.register_job_definition(
            jobDefinitionName=job_definition_name,
            type="container",
            containerProperties={
                "image": f"{docker_image}:latest",
                "vcpus": 1,
                "memory": 128,
                # 'volumes': [
                #      {
                #          "host": {
                #              'sourcePath': f"{os.environ['HOME']}/.aws"
                #          },
                #          "name": "aws"
                #      }
                #  ],
                #  "mountPoints": [
                #      {
                #          "containerPath": "/root/.aws",
                #          "readOnly": True,
                #          "sourceVolume": "aws"
                #      }
                #  ]
            },
        )
