from moto import mock_batch, mock_iam, mock_ecs, mock_ec2, mock_logs
import boto3
import pytest
import os
from docker.models.containers import ContainerCollection

from app.settings.globals import (
    AURORA_JOB_QUEUE,
    DATA_LAKE_JOB_QUEUE,
    TILE_CACHE_JOB_QUEUE,
    PIXETL_JOB_QUEUE,
    GDAL_PYTHON_JOB_DEFINITION,
    PIXETL_JOB_DEFINITION,
    TILE_CACHE_JOB_DEFINITION,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
    AWS_REGION,
)


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
            client = (boto3.client(service, region_name=AWS_REGION),)
            self.mocked_services[service] = {
                "client": client[0],
                "mock": mocked_service,
            }

    def stop_services(self):
        for service in self.mocked_services.keys():
            self.mocked_services[service]["mock"].stop()

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
            },
        )


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # pragma: allowlist secret
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(autouse=True)
def batch_client():
    services = ["ec2", "ecs", "logs", "iam", "batch"]
    aws_mock = AWSMock(*services)

    original_run = ContainerCollection.run

    def patch_run(self, *k, **kwargs):
        kwargs["network"] = "host"
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

    yield aws_mock.mocked_services["batch"]["client"]

    resp = aws_mock.mocked_services["logs"]["client"].describe_log_streams(
        logGroupName="/aws/batch/job"
    )
    ls_name = resp["logStreams"][0]["logStreamName"]

    resp = aws_mock.mocked_services["logs"]["client"].get_log_events(
        logGroupName="/aws/batch/job", logStreamName=ls_name
    )

    for event in resp["events"]:
        print(event["message"])

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
