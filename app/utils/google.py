import json
import os
from typing import List, Optional, Sequence

import aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds
from fastapi.logger import logger
from retrying import retry

from ..settings.globals import AWS_GCS_KEY_SECRET_ARN, GOOGLE_APPLICATION_CREDENTIALS
from .aws import get_secret_client


def set_google_application_credentials(exception: Exception) -> bool:
    # Only continue + retry if we can't find the GCS credentials file
    if not (
        isinstance(exception, RuntimeError) and
        str(exception).endswith("GOOGLE_APPLICATION_CREDENTIALS is invalid.")
    ):
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
async def get_gs_files_async(
    bucket: str,
    prefix: str,
    limit: Optional[int] = None,  # Ignored for this function! :(
    exit_after_max: Optional[int] = None,
    extensions: Sequence[str] = tuple(),
) -> List[str]:
    """Get all matching files in GCS."""

    matches: List[str] = list()
    num_matches: int = 0

    sam = aiogoogle.auth.ServiceAccountManager()
    await sam.detect_default_creds_source()
    creds = sam.creds
    creds["scopes"] = [
        "https://www.googleapis.com/auth/devstorage.read_only",
        "https://www.googleapis.com/auth/cloud-platform.read-only",
    ]

    async with aiogoogle.Aiogoogle(service_account_creds=creds) as aiogoogle_api:
        storage = await aiogoogle_api.discover('storage', 'v1')
        full_res = await aiogoogle_api.as_service_account(
            # NOTE: maxResults limits the number of results per page, but not
            # the total results returned. So I'm afraid the limit arg to this
            # function is a lie :(
            storage.objects.list(bucket=bucket, prefix=prefix),
            full_res=True
        )
    async for page in full_res:
        for blob in page["items"]:
            if not extensions or any(blob.name.endswith(ext) for ext in extensions):
                matches.append(f"/vsigs/{bucket}/{blob['name']}")
                num_matches += 1
                if exit_after_max and num_matches >= exit_after_max:
                    return matches

    return matches
