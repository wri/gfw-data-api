import sys

import boto3
import fiona

s3_uri = sys.argv[1]
zipped = sys.argv[2]
s3 = boto3.client("s3", region_name="us-east-1")

if zipped:
    s3_uri = f"zip+{s3_uri}"

with fiona.open(s3_uri) as src:
    driver = src.driver
    print(driver)

