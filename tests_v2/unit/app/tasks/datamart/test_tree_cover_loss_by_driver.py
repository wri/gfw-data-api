import pytest
from pytest_unordered import unordered
from unittest.mock import AsyncMock, patch
from uuid import UUID

from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import AnalysisStatus
from app.tasks.datamart.land import compute_tree_cover_loss_by_driver

@pytest.mark.asyncio
@patch("app.tasks.datamart.land.datamart_crud.update_result")
@patch("app.tasks.datamart.land._query_dataset_json")
@patch("app.tasks.datamart.land.get_geostore")
async def test_compute_tsc_tree_cover_loss_by_driver_happy_path(
        mock_get_geostore: AsyncMock,
        mock_query_dataset: AsyncMock,
        mock_update_result: AsyncMock,
):
    # Arrange
    test_resource_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    test_geostore_id = UUID("123e4567-e89b-12d3-a456-426614174001")
    test_canopy_cover = 30
    test_dataset_version = {
        "umd_tree_cover_loss": "v1.11",
        "tsc_tree_cover_loss_drivers": "v2023",
        "umd_tree_cover_density_2000": "v1.8",
    }

    mock_get_geostore.return_value = AsyncMock(
        geostore_id=test_geostore_id, origin="rw"
    )
    mock_query_dataset.return_value = [
        {
            "umd_tree_cover_loss__year": 2020,
            "tsc_tree_cover_loss_drivers__driver": "Permanent agriculture",
            "area__ha": 150.5,
            "gfw_forest_carbon_gross_emissions__Mg_CO2e": 2500.0
        },
        {
            "umd_tree_cover_loss__year": 2021,
            "tsc_tree_cover_loss_drivers__driver": "Commodity driven deforestation",
            "area__ha": 75.2,
            "gfw_forest_carbon_gross_emissions__Mg_CO2e": 1800.0
        }
    ]

    # Act
    await compute_tree_cover_loss_by_driver(
        test_resource_id,
        test_geostore_id,
        test_canopy_cover,
        test_dataset_version,
    )

    # Assert
    mock_get_geostore.assert_awaited_once_with(
        test_geostore_id, GeostoreOrigin.rw
    )

    expected_query = (
        "SELECT SUM(area__ha), SUM(gfw_forest_carbon_gross_emissions__Mg_CO2e) "
        "FROM data WHERE umd_tree_cover_density_2000__threshold >= 30 "
        "GROUP BY umd_tree_cover_loss__year, tsc_tree_cover_loss_drivers__driver"
    )

    mock_query_dataset.assert_awaited_once_with(
        "umd_tree_cover_loss",
        "v1.11",
        expected_query,
        mock_get_geostore.return_value,
        test_dataset_version,  # Last parameter matches code order
    )

    # Verify result structure
    mock_update_result.assert_awaited_once()
    update_call_args = mock_update_result.call_args.args
    assert str(update_call_args[0]) == str(test_resource_id)
    assert update_call_args[1]["status"] == AnalysisStatus.saved
    assert update_call_args[1]["result"]["tree_cover_loss_by_driver"] == unordered([
        {
            'drivers_type': 'Permanent agriculture',
            'loss_area_ha': 150.5, 'gross_carbon_emissions_Mg': 2500.0
        },
        {
            'drivers_type': 'Commodity driven deforestation',
            'loss_area_ha': 75.2, 'gross_carbon_emissions_Mg': 1800.0
        }
    ])
    assert update_call_args[1]["result"]["yearly_tree_cover_loss_by_driver"] == unordered([
        {
            'drivers_type': 'Permanent agriculture',
            'loss_year': 2020, 'loss_area_ha': 150.5,
            'gross_carbon_emissions_Mg': 2500.0
        },
        {
            'drivers_type': 'Commodity driven deforestation',
            'loss_year': 2021,
            'loss_area_ha': 75.2,
            'gross_carbon_emissions_Mg': 1800.0
        }
    ])


@pytest.mark.asyncio
@patch("app.tasks.datamart.land.datamart_crud.update_result")
@patch("app.tasks.datamart.land._query_dataset_json")
@patch("app.tasks.datamart.land.get_geostore")
async def test_compute_wri_google_tree_cover_loss_by_driver_happy_path(
        mock_get_geostore: AsyncMock,
        mock_query_dataset: AsyncMock,
        mock_update_result: AsyncMock,
):
    # Arrange
    test_resource_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    test_geostore_id = UUID("123e4567-e89b-12d3-a456-426614174001")
    test_canopy_cover = 30
    test_dataset_version = {
        "umd_tree_cover_loss": "v1.11",
        "wri_google_tree_cover_loss_drivers": "v2023",
        "umd_tree_cover_density_2000": "v1.8",
    }

    mock_get_geostore.return_value = AsyncMock(
        geostore_id=test_geostore_id, origin="rw"
    )
    mock_query_dataset.return_value = [
        {
            "umd_tree_cover_loss__year": 2020,
            "wri_google_tree_cover_loss_drivers__category": "Permanent agriculture",
            "area__ha": 150.5,
            "gfw_forest_carbon_gross_emissions__Mg_CO2e": 2500.0
        },
        {
            "umd_tree_cover_loss__year": 2021,
            "wri_google_tree_cover_loss_drivers__category": "Hard commodities",
            "area__ha": 75.2,
            "gfw_forest_carbon_gross_emissions__Mg_CO2e": 1800.0
        }
    ]

    # Act
    await compute_tree_cover_loss_by_driver(
        test_resource_id,
        test_geostore_id,
        test_canopy_cover,
        test_dataset_version,
    )

    # Assert
    mock_get_geostore.assert_awaited_once_with(
        test_geostore_id, GeostoreOrigin.rw
    )

    expected_query = (
        "SELECT SUM(area__ha), SUM(gfw_forest_carbon_gross_emissions__Mg_CO2e) "
        "FROM data WHERE umd_tree_cover_density_2000__threshold >= 30 "
        "GROUP BY umd_tree_cover_loss__year, wri_google_tree_cover_loss_drivers__category"
    )

    mock_query_dataset.assert_awaited_once_with(
        "umd_tree_cover_loss",
        "v1.11",
        expected_query,
        mock_get_geostore.return_value,
        test_dataset_version,  # Last parameter matches code order
    )

    # Verify result structure
    mock_update_result.assert_awaited_once()
    update_call_args = mock_update_result.call_args.args
    assert str(update_call_args[0]) == str(test_resource_id)
    assert update_call_args[1]["status"] == AnalysisStatus.saved
    assert update_call_args[1]["result"]["tree_cover_loss_by_driver"] == unordered([
        {
            'drivers_type': 'Permanent agriculture',
            'loss_area_ha': 150.5, 'gross_carbon_emissions_Mg': 2500.0
        },
        {
            'drivers_type': 'Hard commodities',
            'loss_area_ha': 75.2, 'gross_carbon_emissions_Mg': 1800.0
        }
    ])
    assert update_call_args[1]["result"]["yearly_tree_cover_loss_by_driver"] == unordered([
        {
            'drivers_type': 'Permanent agriculture',
            'loss_year': 2020, 'loss_area_ha': 150.5,
            'gross_carbon_emissions_Mg': 2500.0
        },
        {
            'drivers_type': 'Hard commodities',
            'loss_year': 2021,
            'loss_area_ha': 75.2,
            'gross_carbon_emissions_Mg': 1800.0
        }
    ])
