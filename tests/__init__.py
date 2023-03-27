import contextlib
import io
import json
import os
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler
from typing import List, Optional, Tuple

import boto3
from moto import mock_batch, mock_ec2, mock_ecs, mock_iam, mock_lambda, mock_logs
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.settings.globals import (
    AWS_REGION,
    DATA_LAKE_BUCKET,
    TILE_CACHE_BUCKET,
    WRITER_DBNAME,
    WRITER_HOST,
    WRITER_PASSWORD,
    WRITER_PORT,
    WRITER_USERNAME,
)

REQUESTS_THUS_FAR: List = list()
LOG_GROUP = "/aws/batch/job"
ROOT = os.environ["ROOT"]

CSV_NAME = "test.csv"
CSV_PATH = os.path.join(os.path.dirname(__file__), "fixtures", CSV_NAME)

CSV2_NAME = "test2.csv"
CSV2_PATH = os.path.join(os.path.dirname(__file__), "fixtures", CSV_NAME)

TSV_NAME = "test.tsv"
TSV_PATH = os.path.join(os.path.dirname(__file__), "fixtures", TSV_NAME)

APPEND_TSV_NAME = "append_test.tsv"
APPEND_TSV_PATH = os.path.join(os.path.dirname(__file__), "fixtures", APPEND_TSV_NAME)

GEOJSON_NAME = "test.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "fixtures", GEOJSON_NAME)

GEOJSON_NAME2 = "test2.geojson"
GEOJSON_PATH2 = os.path.join(os.path.dirname(__file__), "fixtures", GEOJSON_NAME2)

SHP_NAME = "test.shp.zip"
SHP_PATH = os.path.join(os.path.dirname(__file__), "fixtures", SHP_NAME)

BUCKET = "test-bucket"
PORT = 9000

SessionLocal: Optional[Session] = None


class MemoryServer(BaseHTTPRequestHandler):
    @property
    def requests_thus_far(self):
        return REQUESTS_THUS_FAR

    @requests_thus_far.setter
    def requests_thus_far(self, value):
        global REQUESTS_THUS_FAR
        REQUESTS_THUS_FAR = value

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps({"requests": self.requests_thus_far}).encode("utf-8")
        )

    def do_DELETE(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"foo": "bar"}).encode("utf-8"))
        self.requests_thus_far = []

    def do_PUT(self):
        """Forward PUT request to Test Client and buffer requests in
        request_thus_far property."""

        content_length = int(self.headers["Content-Length"])
        put_data = self.rfile.read(content_length)
        request = json.loads(str(put_data.decode("utf-8")))
        self.requests_thus_far.append(
            {"method": "PUT", "path": str(self.path), "body": request}
        )

        # response = self.client.put(self.path, data=json.dumps(request))
        response = {
            "data": {"response": "This is a mocked response"},
            "status": "success",
        }

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(str(response).encode("utf-8"))

    def do_PATCH(self):
        content_length = int(self.headers["Content-Length"])
        put_data = self.rfile.read(content_length)
        request = json.loads(str(put_data.decode("utf-8")))
        self.requests_thus_far.append(
            {"method": "PATCH", "path": str(self.path), "body": request}
        )

        # response = self.client.patch(self.path, json=request)
        response = {
            "data": {"response": "This is a mocked response"},
            "status": "success",
        }

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(str(response).encode("utf-8"))

    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        put_data = self.rfile.read(content_length)
        request = json.loads(str(put_data.decode("utf-8")))
        self.requests_thus_far.append(
            {"method": "POST", "path": str(self.path), "body": request}
        )

        # response = self.client.post(self.path, data=json.dumps(request))
        response = {
            "data": {"response": "This is a mocked response"},
            "status": "success",
        }

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(str(response).encode("utf-8"))


class AWSMock(object):
    mocks = {
        "batch": mock_batch,
        "iam": mock_iam,
        "ecs": mock_ecs,
        "ec2": mock_ec2,
        "logs": mock_logs,
        "lambda": mock_lambda,
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

    def add_job_definition(self, job_definition_name, docker_image, mount_tmp=False):
        container_properties = {
            "image": f"{docker_image}:latest",
            "vcpus": 1,
            "memory": 128,
            "environment": [
                {"name": "AWS_ACCESS_KEY_ID", "value": "testing"},
                {"name": "AWS_SECRET_ACCESS_KEY", "value": "testing"},
                {"name": "ENDPOINT_URL", "value": "http://motoserver-s3:5000"},
                {"name": "DEBUG", "value": "1"},
                {"name": "TILE_CACHE", "value": TILE_CACHE_BUCKET},
                {"name": "DATA_LAKE", "value": DATA_LAKE_BUCKET},
                {"name": "AWS_HTTPS", "value": "NO"},
                {"name": "AWS_S3_ENDPOINT", "value": "motoserver-s3:5000"},
                {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
                {"name": "GDAL_DISABLE_READDIR_ON_OPEN", "value": "YES"},
                {
                    "name": "AWS_SECRETSMANAGER_URL",
                    "value": "http://motoserver-secretsmanager:5001",
                },
            ],
            "volumes": [
                {"host": {"sourcePath": f"{ROOT}/tests/fixtures/aws"}, "name": "aws"},
            ],
            "mountPoints": [
                {
                    "sourceVolume": "aws",
                    "containerPath": "/root/.aws",
                    "readOnly": True,
                },
            ],
        }

        if mount_tmp:
            container_properties["volumes"].append(
                {"host": {"sourcePath": f"{ROOT}/tests/fixtures/tmp"}, "name": "tmp"}
            )
            container_properties["mountPoints"].append(
                {"sourceVolume": "tmp", "containerPath": "/tmp", "readOnly": False}
            )

        return self.mocked_services["batch"]["client"].register_job_definition(
            jobDefinitionName=job_definition_name,
            type="container",
            containerProperties=container_properties,
        )

    def create_lambda_function(
        self, func_str, lambda_name, handler_name, runtime, role
    ):
        zip_output = io.BytesIO()
        zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
        zip_file.writestr("lambda_function.py", func_str)
        zip_file.close()
        zip_output.seek(0)

        return self.mocked_services["lambda"]["client"].create_function(
            Code={"ZipFile": zip_output.read()},
            FunctionName=lambda_name,
            Handler=handler_name,
            Runtime=runtime,
            Role=role,
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


@contextlib.contextmanager
def session():
    global SessionLocal

    if SessionLocal is None:
        db_conn = f"postgresql://{WRITER_USERNAME}:{WRITER_PASSWORD}@{WRITER_HOST}:{WRITER_PORT}/{WRITER_DBNAME}"  # pragma: allowlist secret
        engine = create_engine(db_conn, pool_size=1, max_overflow=0, echo=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db: Optional[Session] = None
    try:
        db = SessionLocal()
        yield db
    finally:
        if db is not None:
            db.close()


async def is_admin_mocked():
    return True


async def is_service_account_mocked():
    return True


async def get_api_key_mocked() -> Tuple[Optional[str], Optional[str]]:
    return str(uuid.uuid4()), "localhost"


def setup_clients(ec2_client, iam_client):
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
