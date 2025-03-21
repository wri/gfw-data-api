from unittest.mock import patch

import pytest
from httpx import AsyncClient

from app.crud.geostore import get_gadm_geostore, get_gadm_geostore_id
from app.errors import RecordNotFoundError


@pytest.mark.asyncio
async def test_get_gadm_geostore_generates_correct_sql_for_country_lookup():
    provider = "gadm"
    version = "4.1"
    adm_level = 0
    simplify = None
    country_id = "MEX"

    with patch("app.crud.geostore.get_first_row") as mock_get_first_row:
        mock_get_first_row.return_value = None
        try:
            _ = await get_gadm_geostore(
                provider, version, adm_level, simplify, country_id
            )
        except RecordNotFoundError:
            pass

    expected_sql = (
        "SELECT adm_level, gfw_area__ha, gfw_bbox, gfw_geostore_id, "
        "gid_0 AS level_id, country AS name, ST_AsGeoJSON(geom) AS geojson "
        '\nFROM gadm_administrative_boundaries."v4.1.64" \n'
        "WHERE adm_level='0' AND gid_0='MEX'"
    )

    actual_sql = str(
        mock_get_first_row.call_args.args[0].compile(
            compile_kwargs={"literal_binds": True}
        )
    )

    assert mock_get_first_row.called is True
    assert actual_sql == expected_sql


@pytest.mark.asyncio
async def test_get_gadm_geostore_generates_correct_sql_for_region_lookup():
    provider = "gadm"
    version = "4.1"
    adm_level = 1
    simplify = None
    country = "MEX"
    region = "5"

    with patch("app.crud.geostore.get_first_row") as mock_get_first_row:
        mock_get_first_row.return_value = None
        try:
            _ = await get_gadm_geostore(
                provider, version, adm_level, simplify, country, region
            )
        except RecordNotFoundError:
            pass

    expected_sql = (
        "SELECT adm_level, gfw_area__ha, gfw_bbox, gfw_geostore_id, "
        "gid_1 AS level_id, name_1 AS name, ST_AsGeoJSON(geom) AS geojson "
        '\nFROM gadm_administrative_boundaries."v4.1.64" \n'
        r"WHERE adm_level='1' AND gid_1 LIKE 'MEX.5\__'"
    )

    actual_sql = str(
        mock_get_first_row.call_args.args[0].compile(
            compile_kwargs={"literal_binds": True}
        )
    )

    assert mock_get_first_row.called is True
    assert actual_sql == expected_sql


@pytest.mark.asyncio
async def test_get_gadm_geostore_generates_correct_sql_for_subregion_lookup():
    provider = "gadm"
    version = "4.1"
    adm_level = 2
    simplify = None
    country = "MEX"
    region = "5"
    subregion = "2"

    with patch("app.crud.geostore.get_first_row") as mock_get_first_row:
        mock_get_first_row.return_value = None
        try:
            _ = await get_gadm_geostore(
                provider, version, adm_level, simplify, country, region, subregion
            )
        except RecordNotFoundError:
            pass

    expected_sql = (
        "SELECT adm_level, gfw_area__ha, gfw_bbox, gfw_geostore_id, "
        "gid_2 AS level_id, name_2 AS name, ST_AsGeoJSON(geom) AS geojson "
        '\nFROM gadm_administrative_boundaries."v4.1.64" \n'
        r"WHERE adm_level='2' AND gid_2 LIKE 'MEX.5.2\__'"
    )

    actual_sql = str(
        mock_get_first_row.call_args.args[0].compile(
            compile_kwargs={"literal_binds": True}
        )
    )

    assert mock_get_first_row.called is True
    assert actual_sql == expected_sql


class TestGadmGeostoreIDLookup:
    @pytest.mark.asyncio
    async def test_get_gadm_geostore_id_generates_correct_sql_for_country_lookup(self):
        provider = "gadm"
        version = "4.1"
        adm_level = 0
        country_id = "MEX"

        with patch("app.crud.geostore.get_first_row") as mock_get_first_row:
            mock_get_first_row.return_value = None
            try:
                _ = await get_gadm_geostore_id(
                    provider, version, adm_level, country_id, None, None
                )
            except RecordNotFoundError:
                pass

        expected_sql = (
            "SELECT gfw_geostore_id "
            '\nFROM gadm_administrative_boundaries."v4.1.64" \n'
            "WHERE adm_level='0' AND gid_0='MEX'"
        )

        actual_sql = str(
            mock_get_first_row.call_args.args[0].compile(
                compile_kwargs={"literal_binds": True}
            )
        )

        assert mock_get_first_row.called is True
        assert actual_sql == expected_sql

    @pytest.mark.asyncio
    async def test_get_gadm_geostore_id_generates_correct_sql_for_region_lookup(self):
        provider = "gadm"
        version = "4.1"
        adm_level = 1
        country = "MEX"
        region = "5"

        with patch("app.crud.geostore.get_first_row") as mock_get_first_row:
            mock_get_first_row.return_value = None
            try:
                _ = await get_gadm_geostore_id(
                    provider, version, adm_level, country, region, None
                )
            except RecordNotFoundError:
                pass

        expected_sql = (
            "SELECT gfw_geostore_id "
            '\nFROM gadm_administrative_boundaries."v4.1.64" \n'
            r"WHERE adm_level='1' AND gid_1 LIKE 'MEX.5\__'"
        )

        actual_sql = str(
            mock_get_first_row.call_args.args[0].compile(
                compile_kwargs={"literal_binds": True}
            )
        )

        assert mock_get_first_row.called is True
        assert actual_sql == expected_sql

    @pytest.mark.asyncio
    async def test_get_gadm_geostore_id_generates_correct_sql_for_subregion_lookup(self):
        provider = "gadm"
        version = "4.1"
        adm_level = 2
        country = "MEX"
        region = "5"
        subregion = "2"

        with patch("app.crud.geostore.get_first_row") as mock_get_first_row:
            mock_get_first_row.return_value = None
            try:
                _ = await get_gadm_geostore_id(
                    provider, version, adm_level, country, region, subregion
                )
            except RecordNotFoundError:
                pass

        expected_sql = (
            "SELECT gfw_geostore_id "
            '\nFROM gadm_administrative_boundaries."v4.1.64" \n'
            r"WHERE adm_level='2' AND gid_2 LIKE 'MEX.5.2\__'"
        )

        actual_sql = str(
            mock_get_first_row.call_args.args[0].compile(
                compile_kwargs={"literal_binds": True}
            )
        )

        assert mock_get_first_row.called is True
        assert actual_sql == expected_sql
