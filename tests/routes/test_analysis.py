import pytest


@pytest.mark.skip("Moto not up to the task... Need localstack to test")
@pytest.mark.asyncio
async def test_raster_analysis__success(async_client, lambda_client):
    """Basic test to check if lambda is successfully called and returns
    response correctly."""
    lambda_client(FUNC_STR)

    response = await async_client.get(
        f"/analysis/zonal/{SAMPLE_GEOSTORE_ID}?geostore_origin=rw&group_by=umd_tree_cover_loss__year&filters=is__umd_regional_primary_forest_2001&filters=umd_tree_cover_density_2000__30&sum=area__ha&start_date=2001"
    )

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["data"] == SAMPLE_RESPONSE


@pytest.mark.skip("Moto not up to the task... Need localstack to test")
@pytest.mark.asyncio
async def test_raster_analysis__bad_params(async_client, lambda_client):
    """Basic test to check if empty data api response as expected."""
    lambda_client(FUNC_STR)

    response = await async_client.get(
        f"/analysis/zonal/{SAMPLE_GEOSTORE_ID}?geostore_origin=rw&group_by=not_umd_tree_cover_loss__year"
    )
    assert response.status_code == 422

    response = await async_client.get(
        f"/analysis/zonal/{SAMPLE_GEOSTORE_ID}?geostore_origin=rw&filters=umd_tree_cover_density_2000__100"
    )
    assert response.status_code == 422

    response = await async_client.get(
        f"/analysis/zonal/{SAMPLE_GEOSTORE_ID}?geostore_origin=rw&group_by=umd_tree_cover_loss__year&sum=area__ha&start_date=01-01-2020"
    )
    assert response.status_code == 422


@pytest.mark.skip("Moto not up to the task... Need localstack to test")
@pytest.mark.asyncio
async def test_raster_analysis__lambda_error(async_client, lambda_client):
    """Basic test to check if empty data api response as expected."""
    lambda_client(FAIL_FUNC_STR)

    response = await async_client.get(
        f"/analysis/zonal/{SAMPLE_GEOSTORE_ID}?geostore_origin=rw&group_by=umd_tree_cover_loss__year&filters=is__umd_regional_primary_forest_2001&filters=umd_tree_cover_density_2000__30&sum=area__ha&start_date=2001"
    )
    assert response.status_code == 500


SAMPLE_GEOSTORE_ID = "02ca2fbafa2d818aa3d1b974a581fbd0"
SAMPLE_GEOM = {
    "type": "Polygon",
    "coordinates": [
        [
            [21.2213665467773, 8.71702013483262],
            [22.6046727661946, 9.76677821562538],
            [23.5247019026918, 8.39439974501934],
            [22.2914713580293, 7.39261623963793],
            [21.2213665467773, 8.71702013483262],
        ]
    ],
}

SAMPLE_RESPONSE = [
    {
        "umd_tree_cover_loss__year": 2001,
        "whrc_aboveground_co2_emissions__Mg": 4725839.568149567,
        "area__ha": 9062.295372572193,
    },
    {
        "umd_tree_cover_loss__year": 2002,
        "whrc_aboveground_co2_emissions__Mg": 10236027.031405449,
        "area__ha": 19467.922227259143,
    },
    {
        "umd_tree_cover_loss__year": 2003,
        "whrc_aboveground_co2_emissions__Mg": 664927.4396429062,
        "area__ha": 1198.7822644872308,
    },
    {
        "umd_tree_cover_loss__year": 2004,
        "whrc_aboveground_co2_emissions__Mg": 24786885.65533161,
        "area__ha": 50160.609432575555,
    },
    {
        "umd_tree_cover_loss__year": 2005,
        "whrc_aboveground_co2_emissions__Mg": 16741551.406709671,
        "area__ha": 34389.349423456726,
    },
    {
        "umd_tree_cover_loss__year": 2006,
        "whrc_aboveground_co2_emissions__Mg": 1323112.0096797943,
        "area__ha": 2876.0619227617744,
    },
    {
        "umd_tree_cover_loss__year": 2007,
        "whrc_aboveground_co2_emissions__Mg": 11364359.208642006,
        "area__ha": 22211.82034506309,
    },
    {
        "umd_tree_cover_loss__year": 2008,
        "whrc_aboveground_co2_emissions__Mg": 12732765.328830719,
        "area__ha": 23651.389989792646,
    },
    {
        "umd_tree_cover_loss__year": 2009,
        "whrc_aboveground_co2_emissions__Mg": 8506505.681572914,
        "area__ha": 15368.460867847505,
    },
    {
        "umd_tree_cover_loss__year": 2010,
        "whrc_aboveground_co2_emissions__Mg": 25909511.252091408,
        "area__ha": 51296.00273744643,
    },
    {
        "umd_tree_cover_loss__year": 2011,
        "whrc_aboveground_co2_emissions__Mg": 29460326.683029175,
        "area__ha": 59814.872639220586,
    },
    {
        "umd_tree_cover_loss__year": 2012,
        "whrc_aboveground_co2_emissions__Mg": 22206639.708688736,
        "area__ha": 42497.41962443359,
    },
    {
        "umd_tree_cover_loss__year": 2013,
        "whrc_aboveground_co2_emissions__Mg": 13796477.497389793,
        "area__ha": 26515.835781746788,
    },
    {
        "umd_tree_cover_loss__year": 2014,
        "whrc_aboveground_co2_emissions__Mg": 12810609.365278244,
        "area__ha": 26426.905888879537,
    },
    {
        "umd_tree_cover_loss__year": 2015,
        "whrc_aboveground_co2_emissions__Mg": 2347976.97966671,
        "area__ha": 4392.868116629246,
    },
    {
        "umd_tree_cover_loss__year": 2016,
        "whrc_aboveground_co2_emissions__Mg": 5947469.693613052,
        "area__ha": 11076.600443766252,
    },
    {
        "umd_tree_cover_loss__year": 2017,
        "whrc_aboveground_co2_emissions__Mg": 1670474.577199936,
        "area__ha": 3318.6336389434746,
    },
    {
        "umd_tree_cover_loss__year": 2018,
        "whrc_aboveground_co2_emissions__Mg": 1307955.4714803696,
        "area__ha": 2606.1186336164346,
    },
    {
        "umd_tree_cover_loss__year": 2019,
        "whrc_aboveground_co2_emissions__Mg": 2112176.0595617294,
        "area__ha": 4314.016010218192,
    },
]

FUNC_STR = f"""
def lambda_handler(event, context):
    assert event["geometry"] == {SAMPLE_GEOM}
    assert event["group_by"] == ["umd_tree_cover_loss__year"]
    assert set(event["filters"]) == set(["is__umd_regional_primary_forest_2001", "umd_tree_cover_density_2000__30"])
    assert event["sum"] == ["area__ha"]

    return {{
        "statusCode": 200,
        "body": {{
            "status": "success",
            "data": {SAMPLE_RESPONSE}
        }}
    }}
"""

FAIL_FUNC_STR = """
def lambda_handler(event, context):
    return {
        "statusCode": 500,
        "body": {
            "status": "failed",
            "message": "Failure"
        }
    }
"""
