from typing import Tuple
from urllib.parse import urlparse


def split_s3_path(s3_path: str) -> Tuple[str, str]:
    o = urlparse(s3_path, allow_fragments=False)
    return o.netloc, o.path.lstrip("/")


def gdal_path(s3_uri: str, zipped: bool) -> str:
    """
    Rename source using gdal Virtual file system notation.
    """
    bucket, path = split_s3_path(s3_uri)
    if zipped:
        vsizip = "/vsizip"
    else:
        vsizip = ""

    return f"{vsizip}/vsis3/{bucket}/{path}"
