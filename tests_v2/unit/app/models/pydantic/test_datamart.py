from app.models.pydantic.datamart import TreeCoverLossByDriverResult
from tests_v2.unit.app.routes.datamart.test_land import (
    MOCK_RESOURCE,
    MOCK_RESULT_OLD_DRIVERS,
)


class TestTreeCoverLossByDriverResult:

    def test_from_rows_existing_behavior(self):
        foo = TreeCoverLossByDriverResult.from_rows(MOCK_RESULT_OLD_DRIVERS)
        assert foo == MOCK_RESOURCE["result"]

    def test_from_rows_override_with_old_driver_value_map(self):
        driver_value_map = {
            "Unknown": 0,
            "Permanent agriculture": 1,
            "Commodity driven deforestation": 2,
            "Shifting agriculture": 3,
            "Forestry": 4,
            "Wildfire": 5,
            "Urbanization": 6,
            "Other natural disturbances": 7,
        }

        foo = TreeCoverLossByDriverResult.from_rows(
            MOCK_RESULT_OLD_DRIVERS,
            "tsc_tree_cover_loss_drivers__driver",
            driver_value_map,
        )
        assert foo == MOCK_RESOURCE["result"]

    def test_from_rows_override_with_new_driver_value_map(self):
        driver_value_map = {
            "Unknown": 0,
            "Permanent agriculture": 1,
            "Hard commodities": 2,  # This changed
            "Shifting cultivation": 3,  # This changed
            "Logging": 4,  # This changed
            "Wildfire": 5,
            "Settlements & Infrastructure": 6,  # This changed
            "Other natural disturbances": 7,
        }

        foo = TreeCoverLossByDriverResult.from_rows(
            MOCK_ROWS_NEW_DRIVERS, "tree_cover_loss_driver", driver_value_map
        )
        assert foo == MOCK_RESOURCE_NEW_DRIVERS["result"]


MOCK_ROWS_NEW_DRIVERS = [
    {
        "umd_tree_cover_loss__year": 2001,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.12564,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 28.11417,
    },
    {
        "umd_tree_cover_loss__year": 2001,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.06282,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 13.86187,
    },
    {
        "umd_tree_cover_loss__year": 2003,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.06282,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 12.16466,
    },
    {
        "umd_tree_cover_loss__year": 2003,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.43973,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 77.28171,
    },
    {
        "umd_tree_cover_loss__year": 2005,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.06282,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 14.34482,
    },
    {
        "umd_tree_cover_loss__year": 2005,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.06282,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 14.34482,
    },
    {
        "umd_tree_cover_loss__year": 2006,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.25127,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 57.71025,
    },
    {
        "umd_tree_cover_loss__year": 2006,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.18846,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 48.81624,
    },
    {
        "umd_tree_cover_loss__year": 2007,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.25127,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 58.39589,
    },
    {
        "umd_tree_cover_loss__year": 2007,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.18846,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 45.15989,
    },
    {
        "umd_tree_cover_loss__year": 2008,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.06282,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 11.98014,
    },
    {
        "umd_tree_cover_loss__year": 2010,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 7.85233,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 1889.3324,
    },
    {
        "umd_tree_cover_loss__year": 2010,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.06282,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 16.12296,
    },
    {
        "umd_tree_cover_loss__year": 2011,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.56537,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 150.05441,
    },
    {
        "umd_tree_cover_loss__year": 2012,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.31409,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 83.36714,
    },
    {
        "umd_tree_cover_loss__year": 2012,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.87946,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 230.97061,
    },
    {
        "umd_tree_cover_loss__year": 2013,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 2.7012,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 664.09112,
    },
    {
        "umd_tree_cover_loss__year": 2013,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 2.38711,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 671.10284,
    },
    {
        "umd_tree_cover_loss__year": 2014,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 9.92534,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 2522.98435,
    },
    {
        "umd_tree_cover_loss__year": 2014,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 15.2021,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 4051.21655,
    },
    {
        "umd_tree_cover_loss__year": 2015,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.31409,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 61.73333,
    },
    {
        "umd_tree_cover_loss__year": 2015,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.18846,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 47.61591,
    },
    {
        "umd_tree_cover_loss__year": 2016,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 0.06282,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 16.57533,
    },
    {
        "umd_tree_cover_loss__year": 2016,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.56537,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 154.72899,
    },
    {
        "umd_tree_cover_loss__year": 2017,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 7.66387,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 2393.61433,
    },
    {
        "umd_tree_cover_loss__year": 2017,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 1.0051,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 273.83728,
    },
    {
        "umd_tree_cover_loss__year": 2018,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 2.57556,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 837.21451,
    },
    {
        "umd_tree_cover_loss__year": 2018,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.12564,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 28.54922,
    },
    {
        "umd_tree_cover_loss__year": 2019,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 4.58576,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 1443.30266,
    },
    {
        "umd_tree_cover_loss__year": 2019,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 0.37691,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 116.02771,
    },
    {
        "umd_tree_cover_loss__year": 2020,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 38.38217,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 12920.90405,
    },
    {
        "umd_tree_cover_loss__year": 2020,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 5.96777,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 1596.47661,
    },
    {
        "umd_tree_cover_loss__year": 2021,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 16.58411,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 5812.126,
    },
    {
        "umd_tree_cover_loss__year": 2022,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 21.9237,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 8243.00054,
    },
    {
        "umd_tree_cover_loss__year": 2023,
        "tree_cover_loss_driver": "Logging",
        "area__ha": 47.99342,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 17686.31639,
    },
    {
        "umd_tree_cover_loss__year": 2023,
        "tree_cover_loss_driver": "Settlements & Infrastructure",
        "area__ha": 2.38711,
        "gfw_forest_carbon_gross_emissions__Mg_CO2e": 884.85835,
    },
]

MOCK_RESOURCE_NEW_DRIVERS = {
    "status": "saved",
    "message": None,
    "result": {
        "tree_cover_loss_by_driver": [
            {
                "drivers_type": "Logging",
                "loss_area_ha": 162.19765,
                "gross_carbon_emissions_Mg": 54895.34635,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_area_ha": 30.15296,
                "gross_carbon_emissions_Mg": 8282.9517,
            },
        ],
        "yearly_tree_cover_loss_by_driver": [
            {
                "drivers_type": "Logging",
                "loss_year": 2001,
                "loss_area_ha": 0.12564,
                "gross_carbon_emissions_Mg": 28.11417,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2001,
                "loss_area_ha": 0.06282,
                "gross_carbon_emissions_Mg": 13.86187,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2003,
                "loss_area_ha": 0.06282,
                "gross_carbon_emissions_Mg": 12.16466,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2003,
                "loss_area_ha": 0.43973,
                "gross_carbon_emissions_Mg": 77.28171,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2005,
                "loss_area_ha": 0.06282,
                "gross_carbon_emissions_Mg": 14.34482,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2005,
                "loss_area_ha": 0.06282,
                "gross_carbon_emissions_Mg": 14.34482,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2006,
                "loss_area_ha": 0.25127,
                "gross_carbon_emissions_Mg": 57.71025,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2006,
                "loss_area_ha": 0.18846,
                "gross_carbon_emissions_Mg": 48.81624,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2007,
                "loss_area_ha": 0.25127,
                "gross_carbon_emissions_Mg": 58.39589,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2007,
                "loss_area_ha": 0.18846,
                "gross_carbon_emissions_Mg": 45.15989,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2008,
                "loss_area_ha": 0.06282,
                "gross_carbon_emissions_Mg": 11.98014,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2010,
                "loss_area_ha": 7.85233,
                "gross_carbon_emissions_Mg": 1889.3324,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2010,
                "loss_area_ha": 0.06282,
                "gross_carbon_emissions_Mg": 16.12296,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2011,
                "loss_area_ha": 0.56537,
                "gross_carbon_emissions_Mg": 150.05441,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2012,
                "loss_area_ha": 0.31409,
                "gross_carbon_emissions_Mg": 83.36714,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2012,
                "loss_area_ha": 0.87946,
                "gross_carbon_emissions_Mg": 230.97061,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2013,
                "loss_area_ha": 2.7012,
                "gross_carbon_emissions_Mg": 664.09112,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2013,
                "loss_area_ha": 2.38711,
                "gross_carbon_emissions_Mg": 671.10284,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2014,
                "loss_area_ha": 9.92534,
                "gross_carbon_emissions_Mg": 2522.98435,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2014,
                "loss_area_ha": 15.2021,
                "gross_carbon_emissions_Mg": 4051.21655,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2015,
                "loss_area_ha": 0.31409,
                "gross_carbon_emissions_Mg": 61.73333,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2015,
                "loss_area_ha": 0.18846,
                "gross_carbon_emissions_Mg": 47.61591,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2016,
                "loss_area_ha": 0.06282,
                "gross_carbon_emissions_Mg": 16.57533,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2016,
                "loss_area_ha": 0.56537,
                "gross_carbon_emissions_Mg": 154.72899,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2017,
                "loss_area_ha": 7.66387,
                "gross_carbon_emissions_Mg": 2393.61433,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2017,
                "loss_area_ha": 1.0051,
                "gross_carbon_emissions_Mg": 273.83728,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2018,
                "loss_area_ha": 2.57556,
                "gross_carbon_emissions_Mg": 837.21451,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2018,
                "loss_area_ha": 0.12564,
                "gross_carbon_emissions_Mg": 28.54922,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2019,
                "loss_area_ha": 4.58576,
                "gross_carbon_emissions_Mg": 1443.30266,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2019,
                "loss_area_ha": 0.37691,
                "gross_carbon_emissions_Mg": 116.02771,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2020,
                "loss_area_ha": 38.38217,
                "gross_carbon_emissions_Mg": 12920.90405,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2020,
                "loss_area_ha": 5.96777,
                "gross_carbon_emissions_Mg": 1596.47661,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2021,
                "loss_area_ha": 16.58411,
                "gross_carbon_emissions_Mg": 5812.126,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2022,
                "loss_area_ha": 21.9237,
                "gross_carbon_emissions_Mg": 8243.00054,
            },
            {
                "drivers_type": "Logging",
                "loss_year": 2023,
                "loss_area_ha": 47.99342,
                "gross_carbon_emissions_Mg": 17686.31639,
            },
            {
                "drivers_type": "Settlements & Infrastructure",
                "loss_year": 2023,
                "loss_area_ha": 2.38711,
                "gross_carbon_emissions_Mg": 884.85835,
            },
        ],
    },
    "metadata": {
        "aoi": {"type": "geostore", "geostore_id": ""},
        "canopy_cover": 30,
        "sources": [
            {"dataset": "umd_tree_cover_loss", "version": "v1.11"},
            {"dataset": "wri_google_tree_cover_loss__category", "version": "v2024"},
            {"dataset": "umd_tree_cover_density_2000", "version": "v1.8"},
        ],
    },
}
