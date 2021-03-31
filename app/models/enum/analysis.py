from enum import Enum


class RasterLayer(str, Enum):
    area__ha = "area__ha"
    alert__count = "alert__count"
    whrc_aboveground_co2_emissions__Mg = "whrc_aboveground_co2_emissions__Mg"

    # TODO once we have code for _latest page, use that to dynamically generate enum
    umd_tree_cover_loss__year = "umd_tree_cover_loss__year"
    is__umd_regional_primary_forest_2001 = "is__umd_regional_primary_forest_2001"
    is__umd_tree_cover_gain = "is__umd_tree_cover_gain"
    whrc_aboveground_biomass_stock_2000__Mg_ha_1 = (
        "whrc_aboveground_biomass_stock_2000__Mg_ha-1"
    )
    tsc_tree_cover_loss_drivers__type = "tsc_tree_cover_loss_drivers__type"
    gfw_plantations__type = "gfw_plantations__type"
    wdpa_protected_areas__iucn_cat = "wdpa_protected_areas__iucn_cat"
    esa_land_cover_2015__class = "esa_land_cover_2015__class"
    umd_glad_alerts__isoweek = "umd_glad_alerts__isoweek"
    umd_glad_alerts__date = "umd_glad_alerts__date"
    is__birdlife_alliance_for_zero_extinction_sites = (
        "is__birdlife_alliance_for_zero_extinction_sites"
    )
    is__gmw_mangroves_1996 = "is__gmw_mangroves_1996"
    is__gmw_mangroves_2016 = "is__gmw_mangroves_2016"
    ifl_intact_forest_landscapes__year = "ifl_intact_forest_landscapes__year"
    is__gfw_tiger_landscapes = "is__gfw_tiger_landscapes"
    is__landmark_land_rights = "is__landmark_land_rights"
    is__gfw_land_rights = "gfw_land_rights"
    is__birdlife_key_biodiversity_areas = "is__birdlife_key_biodiversity_areas"
    is__gfw_mining = "is__gfw_mining"
    is__gfw_peatlands = "is__gfw_peatlands"
    is__gfw_oil_palm = "is__gfw_oil_palm"
    is__gfw_wood_fiber = "is__gfw_wood_fiber"
    is__gfw_resource_rights = "is__gfw_resource_rights"
    is__gfw_managed_forests = "is__gfw_managed_forests"
    rspo_oil_palm__certification_status = "rspo_oil_palm__certification_status"
    idn_forest_area__type = "idn_forest_area__type"
    per_forest_concessions__type = "per_forest_concessions__type"
    bra_biomes__name = "bra_biomes__name"

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


# TODO this is temporarily hardcoded, should be dynamically generated (see GTC-1159)
RASTER_FIELDS = [
    {
        "field_name_": "area__ha",
        "field_alias": None,
        "field_description": None,
        "field_type": "numeric",
    },
    {
        "field_name_": "alert__count",
        "field_alias": None,
        "field_description": None,
        "field_type": "integer",
    },
    {
        "field_name_": "latitude",
        "field_alias": None,
        "field_description": None,
        "field_type": "numeric",
    },
    {
        "field_name_": "longitude",
        "field_alias": None,
        "field_description": None,
        "field_type": "numeric",
    },
    {
        "field_name_": "umd_tree_cover_loss__year",
        "field_alias": None,
        "field_description": None,
        "field_type": "integer",
    },
    {
        "field_name_": "umd_glad_alerts__date",
        "field_alias": None,
        "field_description": None,
        "field_type": "date",
    },
    {
        "field_name_": "umd_glad_alerts__isoweek",
        "field_alias": None,
        "field_description": None,
        "field_type": "integer",
    },
    {
        "field_name_": "umd_glad_landsat_alerts__date",
        "field_alias": None,
        "field_description": None,
        "field_type": "date",
    },
    {
        "field_name_": "umd_glad_landsat_alerts__isoweek",
        "field_alias": None,
        "field_description": None,
        "field_type": "integer",
    },
    {
        "field_name_": "umd_glad_landsat_alerts__confidence",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "is__umd_regional_primary_forest_2001",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "umd_tree_cover_density_2000__threshold",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "umd_tree_cover_density_2010__threshold",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "is__umd_tree_cover_gain",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "whrc_aboveground_biomass_stock_2000__Mg_ha-1",
        "field_alias": None,
        "field_description": None,
        "field_type": "numeric",
    },
    {
        "field_name_": "whrc_aboveground_co2_emissions__Mg",
        "field_alias": None,
        "field_description": None,
        "field_type": "numeric",
    },
    {
        "field_name_": "tsc_tree_cover_loss_drivers__type",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "gfw_plantations__type",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "wdpa_protected_areas__iucn_cat",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "esa_land_cover_2015__class",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "is__birdlife_alliance_for_zero_extinction_sites",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gmw_mangroves_1996",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gmw_mangroves_2016",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "ifl_intact_forest_landscapes__year",
        "field_alias": None,
        "field_description": None,
        "field_type": "integer",
    },
    {
        "field_name_": "is__gfw_tiger_landscapes",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__landmark_land_rights",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gfw_land_rights",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__birdlife_key_biodiversity_areas",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gfw_mining",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gfw_peatlands",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gfw_oil_palm",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gfw_wood_fiber",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gfw_resource_rights",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "is__gfw_managed_forests",
        "field_alias": None,
        "field_description": None,
        "field_type": "boolean",
    },
    {
        "field_name_": "rspo_oil_palm__certification_status",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "idn_forest_area__type",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "per_forest_concession__type",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
    {
        "field_name_": "bra_biome__name",
        "field_alias": None,
        "field_description": None,
        "field_type": "text",
    },
]
