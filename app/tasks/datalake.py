import os

import boto3
from typing.io import BinaryIO

from app.settings.globals import BUCKET

S3 = boto3.client("s3")


def inject_file(file_obj: BinaryIO, path: str):
    """
    Upload a file-like object to S3 data lake
    """
    S3.upload_fileobj(file_obj, BUCKET, path)
