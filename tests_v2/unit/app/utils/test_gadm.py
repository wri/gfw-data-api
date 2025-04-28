import pytest

from app.utils.gadm import extract_level_id, fix_id_pattern


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
async def test_fix_id_pattern_unknown_gadms_pass_through() -> None:
    orig_pattern = r"USA.5.10\__"
    assert fix_id_pattern(2, orig_pattern, "gadm", "3.9") == orig_pattern


@pytest.mark.asyncio
async def test_fix_id_pattern_non_problematic_features_pass_through() -> None:
    orig_pattern = r"USA.4.1\__"
    assert fix_id_pattern(2, orig_pattern, "gadm", "4.1") == orig_pattern


@pytest.mark.asyncio
async def test_fix_id_pattern_strip_revision_sql_for_those_missing_it() -> None:
    orig_pattern = r"IDN.35.4\__"
    assert fix_id_pattern(2, orig_pattern, "gadm", "4.1") == "IDN.35.4"


@pytest.mark.asyncio
async def test_fix_id_pattern_pass_through_ghana_level_0() -> None:
    orig_pattern = r"GHA"
    assert fix_id_pattern(0, orig_pattern, "gadm", "4.1") == orig_pattern


@pytest.mark.asyncio
async def test_fix_id_pattern_remove_period_in_ghana_level_1() -> None:
    orig_pattern = r"GHA.4\__"
    assert fix_id_pattern(1, orig_pattern, "gadm", "4.1") == r"GHA4\__"


@pytest.mark.asyncio
async def test_fix_id_pattern_remove_period_in_ghana_level_2() -> None:
    orig_pattern = r"GHA.4.1\__"
    assert fix_id_pattern(1, orig_pattern, "gadm", "4.1") == r"GHA4.1\__"
