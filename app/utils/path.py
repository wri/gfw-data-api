import os
from typing import Tuple
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from app.utils.aws import get_s3_client


def split_s3_path(s3_path: str) -> Tuple[str, str]:
    o = urlparse(s3_path, allow_fragments=False)
    return o.netloc, o.path.lstrip("/")


def is_zipped(s3_uri: str) -> bool:
    """Get basename of source file.

    If Zipfile, add VSIZIP prefix for GDAL
    """
    bucket, key = split_s3_path(s3_uri)
    client = get_s3_client()
    _, ext = os.path.splitext(s3_uri)

    try:
        header = client.head_object(Bucket=bucket, Key=key)
        # TODO: moto does not return the correct ContenType so have to go for the ext
        if header["ContentType"] == "application/x-zip-compressed" or ext == ".zip":
            return True
    except (KeyError, ClientError):
        raise FileNotFoundError(f"Cannot access source file {s3_uri}")

    return False


def get_layer_name(uri):
    name, ext = os.path.splitext(os.path.basename(uri))
    if ext == "":
        return name
    else:
        return get_layer_name(name)
