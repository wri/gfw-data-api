from datetime import datetime
from typing import Callable, Awaitable, Dict, Any

from typing.io import BinaryIO

import boto3

S3 = boto3.client("s3")

from app.settings.globals import BUCKET


async def inject_file(
    file_obj: BinaryIO, path: str, callback: Callable[[Dict[str, Any]], Awaitable[None]]
):
    """
    Upload a file-like object to S3 data lake
    """
    try:
        S3.upload_fileobj(file_obj, BUCKET, path)
        status = "success"
        message = f"Injected file {path} into data lake"
        detail = None
    except Exception as e:
        status = "failed"
        message = f"Failed to injected file {path} into data lake"
        detail = str(e)

    await callback(
        {
            "datetime": datetime.now(),
            "status": status,
            "message": message,
            "detail": detail,
        }
    )

    # create default asset for version (in database)
    # Version status = pending
    #
    # Schedule batch job queues depending on source type
    # -> Vector
    # -> Tabular
    # -> Raster
    # Batch job would log to asset history

    # Monitor job queue to make sure all job terminate and once done, set version status to saved and register newly created asset with version
    # if job failed, set version status to failed with message "Default asset failed"
