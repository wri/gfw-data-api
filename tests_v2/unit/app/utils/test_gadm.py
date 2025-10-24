import pytest

from app.utils.gadm import (
    GADM_41_IDS_MISSING_REVISION,
    extract_level_id,
    fix_id_pattern,
)


@pytest.mark.asyncio
async def test_extract_level_gid() -> None:
    # Normal gid values
    match1 = "USA.5.10_1"
    assert extract_level_id(0, match1) == "USA"
    assert extract_level_id(1, match1) == "5"
    assert extract_level_id(2, match1) == "10"

    # Ghana values with bad formatting (missing dot after GHA in gadm 4.1)
    match2 = "GHA7.1_2"
    assert extract_level_id(0, match2) == "GHA"
    assert extract_level_id(1, match2) == "7"
    assert extract_level_id(2, match2) == "1"

    # Indonesia values with bad formatting (missing suffix _1 in gadm 4.1)
    match3 = "IDN.35.4"
    assert extract_level_id(0, match3) == "IDN"
    assert extract_level_id(1, match3) == "35"
    assert extract_level_id(2, match3) == "4"


@pytest.mark.asyncio
async def test_GADM_ids_are_unchanged_if_they_are_NOT_4_1() -> None:
    orig_pattern = r"USA.5.10\__"
    assert fix_id_pattern(2, orig_pattern, "gadm", "3.9") == orig_pattern


@pytest.mark.asyncio
async def test_GADM_ids_are_unchanged_if_they_are_NOT_problematic() -> None:
    orig_id = "USA.5.10"
    assert orig_id not in GADM_41_IDS_MISSING_REVISION

    orig_pattern = orig_id + r"\__"
    assert fix_id_pattern(2, orig_pattern, "gadm", "4.1") == orig_pattern


@pytest.mark.asyncio
async def test_strip_revision_sql_for_those_missing_revision() -> None:
    for problem_gid in GADM_41_IDS_MISSING_REVISION:
        pattern = problem_gid + r"\__"
        assert fix_id_pattern(2, pattern, "gadm", "4.1") == problem_gid


# In the official GADM 4.1 files some Ghana GIDs were missing a "." between "GHA"
# and the region IDs (so what should have been "GHA.2.1_1" was "GHA2.1_1". We had
# instituted a workaround in the Data API for this, but later decided to fix the
# IDs by pre-processing the GADM file. These next three tests are to verify that
# the new, fixed, IDs are dealt with correctly.
@pytest.mark.asyncio
async def test_GHA_GID_0_is_NOT_changed_now_that_we_have_mitigated_at_source() -> None:
    orig_pattern = "GHA"
    assert fix_id_pattern(0, orig_pattern, "gadm", "4.1") == orig_pattern


@pytest.mark.asyncio
async def test_GHA_GID_1s_are_NOT_changed_now_that_we_have_mitigated_at_source() -> (
    None
):
    orig_pattern = r"GHA.4\__"
    assert fix_id_pattern(1, orig_pattern, "gadm", "4.1") == orig_pattern


@pytest.mark.asyncio
async def test_GHA_GID_2s_are_NOT_changed_now_that_we_have_mitigated_at_source() -> (
    None
):
    orig_pattern = r"GHA.4.1\__"
    assert fix_id_pattern(2, orig_pattern, "gadm", "4.1") == orig_pattern
