import os
from typing import List, Optional, Sequence

import aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds
from fastapi.logger import logger
from the_retry import retry

from ..settings.globals import AWS_GCS_KEY_SECRET_ARN, GOOGLE_APPLICATION_CREDENTIALS
from .aws import get_secret_client


async def set_google_application_credentials() -> bool:
    # We will not reach out to AWS Secret Manager if no secret is set...
    if not AWS_GCS_KEY_SECRET_ARN:
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
    attempts=2,
    expected_exception=(RuntimeError,),
    on_exception=set_google_application_credentials,
)
async def list_gs_objects(bucket: str, prefix: str) -> aiogoogle.models.Response:
    sam = aiogoogle.auth.ServiceAccountManager()

    await sam.detect_default_creds_source()

    creds = sam.creds
    creds["scopes"] = [
        "https://www.googleapis.com/auth/devstorage.read_only",
        "https://www.googleapis.com/auth/cloud-platform.read-only",
    ]

    async with aiogoogle.Aiogoogle(service_account_creds=creds) as aiogoogle_api:
        storage = await aiogoogle_api.discover('storage', 'v1')
        results = await aiogoogle_api.as_service_account(
            storage.objects.list(bucket=bucket, prefix=prefix),
            full_res=True
        )
    return results


async def get_gs_files_async(
    bucket: str,
    prefix: str,
    limit: Optional[int] = None,  # Ignored for this function! :(
    exit_after_max: Optional[int] = None,
    extensions: Sequence[str] = tuple(),
) -> List[str]:
    """Get all matching files in GCS."""

    # NOTE: We can limit the number of results per page in list_gs_objects
    # but not the total results returned from GCS. So I'm afraid the limit
    # arg to this function is a lie :(

    matches: List[str] = list()
    num_matches: int = 0

    list_resp: aiogoogle.models.Response = await list_gs_objects(bucket, prefix)
    async for page in list_resp:
        for blob in page["items"]:
            if not extensions or any(blob.name.endswith(ext) for ext in extensions):
                matches.append(f"/vsigs/{bucket}/{blob['name']}")
                num_matches += 1
                if exit_after_max and num_matches >= exit_after_max:
                    return matches

    return matches
