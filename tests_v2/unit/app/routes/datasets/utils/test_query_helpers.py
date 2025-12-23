import pytest
from fastapi import HTTPException

from app.models.pydantic.geostore import Geometry
from app.routes.datasets.utils.query_helpers import scrutinize_sql

test_dataset: str = "test_dataset"
test_version: str = "v2025"

# FIXME: The errors should really be in JSEND format...


@pytest.mark.asyncio
async def test_scrutinize_sql_passes_through_benign_queries():
    sql: str = "SELECT * FROM test_dataset.v2025"

    result = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert result == sql


@pytest.mark.asyncio
async def test_scrutinize_sql_only_one_statement_allowed():
    sql: str = "SELECT * FROM test_dataset.v2025; select * from something_else"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Must use exactly one SQL statement."


@pytest.mark.asyncio
async def test_scrutinize_sql_only_select_statements_allowed():
    sql: str = "DELETE FROM bar;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Must use SELECT statements only."


@pytest.mark.asyncio
async def test_scrutinize_sql_must_not_have_a_with_clause():
    sql: str = "WITH t as (select 1) SELECT * FROM version;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Must not have WITH clause."


@pytest.mark.asyncio
async def test_scrutinize_sql_no_sql_value_functions():
    sql: str = "select current_catalog from mytable;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Use of sql value functions is not allowed."


@pytest.mark.asyncio
async def test_scrutinize_sql_only_one_table_allowed():
    sql: str = "SELECT * FROM version, version2;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Must list exactly one table in FROM clause."


@pytest.mark.asyncio
async def test_scrutinize_sql_no_sub_queries_allowed():
    sql: str = "SELECT * FROM (select * from a) as b;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Must not use sub queries."


@pytest.mark.asyncio
async def test_scrutinize_sql_no_postgis_functions_allowed():
    sql: str = "SELECT PostGIS_Full_Version() FROM data;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert (
        exc_info.value.detail
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio
async def test_scrutinize_sql_no_admin_functions_allowed():
    sql: str = "SELECT pg_create_restore_point() FROM data;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert (
        exc_info.value.detail
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio
async def test_scrutinize_sql_no_sys_functions_allowed():
    sql: str = "SELECT txid_current() from mytable;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert (
        exc_info.value.detail
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio
async def test_scrutinize_sql_forbidden_not_allowed():
    sql: str = "SELECT current_setting() FROM mytable;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert (
        exc_info.value.detail
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio
async def test_scrutinize_sql_with_geom():
    geometry = Geometry(type="Point", coordinates=[0, 0])
    sql_in: str = "SELECT * FROM mytable WHERE id = 1"
    sql_expected: str = (
        """SELECT * FROM test_dataset.v2025 WHERE id = 1 AND st_intersects(geom, st_setsrid(st_geomfromgeojson('{"type": "Point", "coordinates": [0, 0]}'), 4326))"""
    )

    result = await scrutinize_sql(test_dataset, geometry, sql_in, test_version)
    assert result == sql_expected


@pytest.mark.asyncio
async def test_scrutinize_sql_with_geom_no_where():
    geometry = Geometry(type="Point", coordinates=[0, 0])
    sql_in: str = "SELECT * FROM mytable;"
    sql_expected: str = (
        """SELECT * FROM test_dataset.v2025 WHERE st_intersects(geom, st_setsrid(st_geomfromgeojson('{"type": "Point", "coordinates": [0, 0]}'), 4326))"""
    )
    result = await scrutinize_sql(test_dataset, geometry, sql_in, test_version)
    assert result == sql_expected


@pytest.mark.asyncio
async def test_scrutinize_sql_gibberish():
    sql: str = "foo;"

    with pytest.raises(HTTPException) as exc_info:
        _ = await scrutinize_sql(test_dataset, None, sql, test_version)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == 'syntax error at or near "foo", at index 0'
