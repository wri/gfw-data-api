from moto import mock_batch, mock_iam, mock_ecs, mock_ec2, mock_logs
import boto3
import pytest
import os

from app.models.pydantic.job import Job
import app.tasks.batch as batch

DEFAULT_REGION = "us-east-1"


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


@pytest.fixture(autouse=True)
def batch_env():
    access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    security_token = os.environ.get('AWS_SECURITY_TOKEN', '')
    session_token = os.environ.get('AWS_SESSION_TOKEN', '')

    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

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

    ec2_client, iam_client, ecs_client, logs_client, batch_client = _get_clients()
    vpc_id, subnet_id, sg_id, iam_arn = _setup(ec2_client, iam_client)

    compute_name = "test_compute_env"
    resp = batch_client.create_compute_environment(
        computeEnvironmentName=compute_name,
        type="MANAGED",
        state="ENABLED",
        computeResources={
            "type": "EC2",
            "minvCpus": 5,
            "maxvCpus": 10,
            "desiredvCpus": 5,
            "instanceTypes": ["t2.small", "t2.medium"],
            "imageId": "some_image_id",
            "subnets": [subnet_id],
            "securityGroupIds": [sg_id],
            "ec2KeyPair": "string",
            "instanceRole": iam_arn.replace("role", "instance-profile"),
            "tags": {"string": "string"},
            "bidPercentage": 123,
            "spotIamFleetRole": "string",
        },
        serviceRole=iam_arn,
    )

    arn = resp["computeEnvironmentArn"]

    resp = batch_client.create_job_queue(
        jobQueueName="test_job_queue",
        state="ENABLED",
        priority=123,
        computeEnvironmentOrder=[{"order": 123, "computeEnvironment": arn}],
    )
    queue_arn = resp["jobQueueArn"]

    resp = batch_client.register_job_definition(
        jobDefinitionName="sayhellotomylittlefriend",
        type="container",
        containerProperties={
            "image": "geolambda:latest",
            "vcpus": 1,
            "memory": 128,
            "command": ["echo", "hello"],
        },
    )
    job_def_arn = resp["jobDefinitionArn"]
    yield batch_client, queue_arn, job_def_arn

    mockec2.stop()
    mockecs.stop()
    mocklogs.stop()
    mockiam.stop()
    mockbatch.stop()

    os.environ['AWS_ACCESS_KEY_ID'] = access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
    os.environ['AWS_SECURITY_TOKEN'] = security_token
    os.environ['AWS_SESSION_TOKEN'] = session_token


def test_batch_scheduler(batch_env):
    batch_client, queue_arn, job_def_arn = batch_env
    job1 = Job(job_name="job1", job_queue=queue_arn, job_definition=job_def_arn, command=["/bin/bash"])
    job2 = Job(job_name="job2", job_queue=queue_arn, job_definition=job_def_arn, command=["/bin/bash"])
    job3 = Job(job_name="job3", job_queue=queue_arn, job_definition=job_def_arn, command=["/bin/bash"], parents=[job1.job_name, job2.job_name])

    success = batch.execute([job1, job2, job3], lambda x: x)
    assert(success)

