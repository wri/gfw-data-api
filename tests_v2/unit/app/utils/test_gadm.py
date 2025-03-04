import pytest

from app.utils.gadm import extract_level_id


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
