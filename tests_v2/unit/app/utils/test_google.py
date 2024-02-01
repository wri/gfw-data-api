from typing import List
from unittest.mock import Mock

from _pytest.monkeypatch import MonkeyPatch

from app.utils import google
from app.utils.google import get_gs_files


good_bucket: str = "good_bucket"
good_prefix: str = "good_prefix"
all_the_files: List[str] = [
    f"{good_prefix}/irrelevant.mp3",
    f"{good_prefix}/something.csv",
    f"{good_prefix}/world.tif"
]

all_the_files_gdal_notation: List[str] = [
    f"/vsigs/{good_bucket}/{x}" for x in all_the_files
]


def test_get_matching_gs_files_no_filtering(monkeypatch: MonkeyPatch):
    mock_get_prefix_objects = Mock(return_value=all_the_files)
    monkeypatch.setattr(google, "get_prefix_objects", mock_get_prefix_objects)

    keys = get_gs_files(good_bucket, good_prefix)
    assert len(keys) == 3
    assert set(keys) == set(all_the_files_gdal_notation)


def test_get_matching_gs_files_match_extensions(monkeypatch: MonkeyPatch):
    mock_get_prefix_objects = Mock(return_value=all_the_files)
    monkeypatch.setattr(google, "get_prefix_objects", mock_get_prefix_objects)

    keys = get_gs_files(good_bucket, good_prefix, extensions=[".tif"])
    assert keys == [f"/vsigs/{good_bucket}/{good_prefix}/world.tif"]


def test_get_matching_gs_files_no_matches(monkeypatch: MonkeyPatch):
    mock_get_prefix_objects = Mock(return_value=all_the_files)
    monkeypatch.setattr(google, "get_prefix_objects", mock_get_prefix_objects)

    keys = get_gs_files(good_bucket, good_prefix, extensions=[".pdf"])
    assert keys == []


def test_get_matching_gs_files_early_exit(monkeypatch: MonkeyPatch):
    mock_get_prefix_objects = Mock(return_value=all_the_files)
    monkeypatch.setattr(google, "get_prefix_objects", mock_get_prefix_objects)

    keys = get_gs_files(good_bucket, good_prefix, exit_after_max=1)
    assert len(keys) == 1
    assert keys[0] in all_the_files_gdal_notation
