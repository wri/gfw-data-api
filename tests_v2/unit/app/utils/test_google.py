import random
from collections import defaultdict
from typing import DefaultDict, List, Optional

import pytest
from _pytest.monkeypatch import MonkeyPatch
from google.cloud import storage

from app.utils.google import get_gs_files


class MockBlob:
    name: str

    def __init__(self, name):
        self.name = name


class MockStorageClient:
    blob_store: DefaultDict[str, List[MockBlob]] = defaultdict(list)

    def from_service_account_json(self, _: str):
        return self

    def add_blobs(self, bucket: str, blob_names: List[str]):
        self.blob_store[bucket] += [MockBlob(blob_name) for blob_name in blob_names]

    def list_blobs(self, bucket: str, prefix: str, max_results: Optional[int] = None):
        blobs = [
            blob for blob in self.blob_store[bucket] if blob.name.startswith(prefix)
        ]

        if max_results is not None:
            return random.choices(blobs, k=max_results)
        return blobs


@pytest.mark.asyncio
async def test_get_gs_files(monkeypatch: MonkeyPatch):
    good_bucket = "good_bucket"
    good_prefix = "good_prefix"

    blob_store_client = MockStorageClient()

    monkeypatch.setattr(storage, "Client", blob_store_client)

    blob_store_client.add_blobs(good_bucket, [f"{good_prefix}/world.tif"])

    keys = get_gs_files(good_bucket, good_prefix)
    assert len(keys) == 1
    assert keys[0] == f"/vsigs/{good_bucket}/{good_prefix}/world.tif"

    keys = get_gs_files(good_bucket, good_prefix, extensions=[".pdf"])
    assert len(keys) == 0

    keys = get_gs_files(good_bucket, "bad_prefix")
    assert len(keys) == 0

    keys = get_gs_files("bad_bucket", "doesnt_matter")
    assert len(keys) == 0

    blob_store_client.add_blobs(good_bucket, [f"{good_prefix}/another_world.csv"])

    keys = get_gs_files(good_bucket, good_prefix)
    assert len(keys) == 2
    assert f"/vsigs/{good_bucket}/{good_prefix}/another_world.csv" in keys
    assert f"/vsigs/{good_bucket}/{good_prefix}/world.tif" in keys

    keys = get_gs_files(good_bucket, good_prefix, extensions=[".csv"])
    assert len(keys) == 1
    assert keys[0] == f"/vsigs/{good_bucket}/{good_prefix}/another_world.csv"

    keys = get_gs_files(good_bucket, good_prefix, limit=1)
    assert len(keys) == 1
    assert (
        f"/vsigs/{good_bucket}/{good_prefix}/another_world.csv" in keys
        or f"/vsigs/{good_bucket}/{good_prefix}/world.tif" in keys
    )

    blob_store_client.add_blobs(good_bucket, [f"{good_prefix}/coverage_layer.tif"])
    keys = get_gs_files(good_bucket, good_prefix)
    assert len(keys) == 3
    assert f"/vsigs/{good_bucket}/{good_prefix}/another_world.csv" in keys
    assert f"/vsigs/{good_bucket}/{good_prefix}/coverage_layer.tif" in keys
    assert f"/vsigs/{good_bucket}/{good_prefix}/world.tif" in keys

    keys = get_gs_files(good_bucket, good_prefix, exit_after_max=1, extensions=[".tif"])
    assert len(keys) == 1
    assert (
        f"/vsigs/{good_bucket}/{good_prefix}/coverage_layer.tif" in keys
        or f"/vsigs/{good_bucket}/{good_prefix}/world.tif" in keys
    )
