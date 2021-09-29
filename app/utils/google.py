import os
from typing import List, Sequence

from fastapi.logger import logger
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from retrying import retry

from ..settings.globals import AWS_GCS_KEY_SECRET_ARN, GOOGLE_APPLICATION_CREDENTIALS
from .aws import get_secret_client


def set_google_application_credentials(exception: Exception) -> bool:
    # We will not reach out to AWS Secret Manager if no secret is set.
    if not isinstance(exception, DefaultCredentialsError):
        return False
    elif not AWS_GCS_KEY_SECRET_ARN:
        logger.error(
            "No AWS_GCS_KEY_SECRET_ARN set. Cannot write Google Application Credential file."
        )
        return False
    # ...Or if we don't know where to write the credential file.
    elif not GOOGLE_APPLICATION_CREDENTIALS:
        logger.error(
            "No GOOGLE_APPLICATION_CREDENTIALS set. Cannot write Google Application Credential file"
        )
        return False
    else:
        logger.info("GCS key file is missing. Trying to fetch key from secret manager")
        client = get_secret_client()
        response = client.get_secret_value(SecretId=AWS_GCS_KEY_SECRET_ARN)

        os.makedirs(
            os.path.dirname(GOOGLE_APPLICATION_CREDENTIALS),
            exist_ok=True,
        )

        logger.info("Writing GCS key to file")
        with open(GOOGLE_APPLICATION_CREDENTIALS, "w") as f:
            f.write(response["SecretString"])

        with open(GOOGLE_APPLICATION_CREDENTIALS, "r") as f:
            logger.debug(f"This is what we wrote: {f.read()}")

    # make sure that global ENV VAR is set
    # FIXME: Better to set the default in Docker compose file?
    logger.info("Setting environment's GOOGLE_APPLICATION_CREDENTIALS to ")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

    logger.debug(f"os.environ: {os.environ}")

    return True


@retry(
    retry_on_exception=set_google_application_credentials,
    stop_max_attempt_number=2,
)
def get_gs_files(
    bucket: str,
    prefix: str,
    extensions: Sequence[str] = tuple()
) -> List[str]:
    """Get all matching files in GCS."""

    storage_client = storage.Client.from_service_account_json(
        GOOGLE_APPLICATION_CREDENTIALS
    )

    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    files = [
        f"/vsigs/{bucket}/{blob.name}"
        for blob in blobs
        if not extensions or any(blob.name.endswith(ext) for ext in extensions)
    ]
    return files
