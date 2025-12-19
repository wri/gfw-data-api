import pytest

from app.routes.datasets.utils.query_helpers import normalize_sql

@pytest.mark.asyncio
async def test_it_does_something():
    result = await normalize_sql(
        "test_dataset", None, "select * from public.my_table;", "v2025" )
    assert result == "SELECT * FROM test_dataset.v2025"