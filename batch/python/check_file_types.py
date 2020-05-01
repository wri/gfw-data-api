import csv
from typing import Type
from urllib.parse import urlparse

import boto3
import fiona
import rasterio


S3 = boto3.client("s3", region_name="us-east-1")


async def get_csv_dialect(s3_uri) -> csv.Dialect:

    o = urlparse(s3_uri, allow_fragments=False)
    bucket = o.netloc
    key = o.path.lstrip("/")

    Bytes_range = "bytes=0-4096"
    response = S3.get_object(Bucket=bucket, Key=key, Range=Bytes_range)
    data = response["Body"].read()

    try:
        dialect: Type[csv.Dialect] = csv.Sniffer().sniff(data)
        # TODO: verify if dialect is correct (delimiter etc)
    except csv.Error:
        raise TypeError("Not a valid CSV file")
    else:
        return dialect()


async def get_vector_source_driver(s3_uri, zipped=True) -> str:
    if zipped:
        s3_uri = f"zip+{s3_uri}"

    try:
        with fiona.open(s3_uri) as src:
            driver = src.driver
    except Exception:
        # TODO: catch correct exception if fiona can't read it and handle it
        raise
    else:
        return driver


async def get_raster_source_driver(s3_uri) -> str:
    try:
        with rasterio.open(s3_uri) as src:
            driver = src.driver
    except Exception:
        # TODO: catch correct exception if rasterio can't read it and handle it
        raise
    else:
        return driver
