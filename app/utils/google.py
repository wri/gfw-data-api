from typing import List, Sequence

# from google.auth.exceptions import DefaultCredentialsError
# from google.cloud import storage


def get_gs_files(
    bucket: str, prefix: str, extensions: Sequence[str] = (".tif",)
) -> List[str]:
    """Get all matching files in GCS."""

    # try:
    #     storage_client = storage.Client()
    # except DefaultCredentialsError:
    #     raise MissingGCSKeyError()
    #
    # blobs = storage_client.list_blobs(bucket, prefix=prefix)
    # files = [
    #     f"/vsigs/{bucket}/{blob.name}"
    #     for blob in blobs
    #     if any(blob.name.endswith(ext) for ext in extensions)
    # ]
    # return files

    return []
