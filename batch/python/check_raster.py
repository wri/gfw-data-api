import sys

import boto3
import rasterio
from logger import get_logger

LOGGER = get_logger(__name__)

s3_uri = sys.argv[1]
zipped = sys.argv[2]
s3 = boto3.client("s3", region_name="us-east-1")

if zipped:
    s3_uri = f"zip+{s3_uri}"

with rasterio.open(s3_uri) as src:
    driver = src.driver
    LOGGER.debug(driver)
