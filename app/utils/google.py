import os
from typing import List, Optional, Sequence

from fastapi.logger import logger
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from retrying import retry

from ..settings.globals import AWS_GCS_KEY_SECRET_ARN, GOOGLE_APPLICATION_CREDENTIALS
from .aws import get_secret_client


def set_google_application_credentials(exception: Exception) -> bool:
    # Only continue + retry if we can't find the GCS credentials file
    if not isinstance(exception, (DefaultCredentialsError, FileNotFoundError)):
        logger.error(f"Some other exception happened!: {exception}")
        return False
    # We will not reach out to AWS Secret Manager if no secret is set...
    elif not AWS_GCS_KEY_SECRET_ARN:
        logger.error(
            "No AWS_GCS_KEY_SECRET_ARN set. "
            "Cannot write Google Application Credential file."
        )
        return False
    # ...or if we don't know where to write the credential file.
    elif not GOOGLE_APPLICATION_CREDENTIALS:
        logger.error(
            "No GOOGLE_APPLICATION_CREDENTIALS set. "
            "Cannot write Google Application Credential file"
        )
        return False
    # But if all those conditions are met, write the GCS credentials file
    # and return True to retry
    else:
        logger.info("GCS key file is missing. Fetching key from secret manager")
        client = get_secret_client()
        response = client.get_secret_value(SecretId=AWS_GCS_KEY_SECRET_ARN)

        os.makedirs(
            os.path.dirname(GOOGLE_APPLICATION_CREDENTIALS),
            exist_ok=True,
        )

        logger.info("Writing GCS key to file")
        with open(GOOGLE_APPLICATION_CREDENTIALS, "w") as f:
            f.write(response["SecretString"])

    # make sure that global ENV VAR is set
    logger.info("Setting environment's GOOGLE_APPLICATION_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

    return True


@retry(
    retry_on_exception=set_google_application_credentials,
    stop_max_attempt_number=2,
)
def get_gs_files(
    bucket: str, prefix: str, limit: Optional[int], extensions: Sequence[str] = tuple()
) -> List[str]:
    """Get all matching files in GCS."""

    storage_client = storage.Client.from_service_account_json(
        GOOGLE_APPLICATION_CREDENTIALS
    )

    blobs = storage_client.list_blobs(bucket, prefix=prefix, max_results=limit)
    files = [
        f"/vsigs/{bucket}/{blob.name}"
        for blob in blobs
        if not extensions or any(blob.name.endswith(ext) for ext in extensions)
    ]
    return files
