from aenum import Enum


class RasterLayer(str, Enum):
    area__ha = "area__ha"
    alert__count = "alert__count"
    whrc_aboveground_co2_emissions__Mg = "whrc_aboveground_co2_emissions__Mg"

    # TODO once we have code for _latest page, use that to dynamically generate enum
    umd_tree_cover_loss__year = "umd_tree_cover_loss__year"
    is__umd_regional_primary_forest_2001 = "is__umd_regional_primary_forest_2001"
    is__umd_tree_cover_gain = "is__umd_tree_cover_gain"
    whrc_aboveground_biomass_stock_2000__Mg = "whrc_aboveground_biomass_stock_2000__Mg"
    tsc_tree_cover_loss_drivers__type = "tsc_tree_cover_loss_drivers__type"
    gfw_plantations__type = "gfw_plantations__type"
    wdpa_protected_areas__class = "wdpa_protected_areas__class"
    esa_land_cover_2015__class = "esa_land_cover_2015__class"
    umd_glad_alerts__isoweek = "umd_glad_alerts__isoweek"
    umd_glad_alerts__date = "umd_glad_alerts__date"

    # expand out tree cover density
    umd_tree_cover_density_2000__10 = "umd_tree_cover_density_2000__10"
    umd_tree_cover_density_2000__15 = "umd_tree_cover_density_2000__15"
    umd_tree_cover_density_2000__20 = "umd_tree_cover_density_2000__20"
    umd_tree_cover_density_2000__25 = "umd_tree_cover_density_2000__25"
    umd_tree_cover_density_2000__30 = "umd_tree_cover_density_2000__30"
    umd_tree_cover_density_2000__50 = "umd_tree_cover_density_2000__50"
    umd_tree_cover_density_2000__75 = "umd_tree_cover_density_2000__75"
    umd_tree_cover_density_2010__10 = "umd_tree_cover_density_2010__10"
    umd_tree_cover_density_2010__15 = "umd_tree_cover_density_2010__15"
    umd_tree_cover_density_2010__20 = "umd_tree_cover_density_2010__20"
    umd_tree_cover_density_2010__25 = "umd_tree_cover_density_2010__25"
    umd_tree_cover_density_2010__30 = "umd_tree_cover_density_2010__30"
    umd_tree_cover_density_2010__50 = "umd_tree_cover_density_2010__50"
    umd_tree_cover_density_2010__75 = "umd_tree_cover_density_2010__75"


