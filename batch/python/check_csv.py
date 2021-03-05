import csv
import sys
from typing import Type
from urllib.parse import urlparse

import boto3
from logger import get_logger

LOGGER = get_logger(__name__)

s3_uri = sys.argv[1]
s3 = boto3.client("s3", region_name="us-east-1")
o = urlparse(s3_uri, allow_fragments=False)
bucket = o.netloc
key = o.path.lstrip("/")

bytes_range = "bytes=0-4096"
response = s3.get_object(Bucket=bucket, Key=key, Range=bytes_range)
data = response["Body"].read().decode("utf-8")

try:
    dialect: Type[csv.Dialect] = csv.Sniffer().sniff(data)
    # TODO: verify if dialect is correct (delimiter etc)
except csv.Error:
    raise TypeError("Not a valid CSV file")

LOGGER.debug(dialect.delimiter)
