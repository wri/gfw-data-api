from unittest.mock import patch

import pytest

from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.tasks.raster_tile_cache_assets.symbology import colormap_symbology

wm_tile_set_co = {
    "pixel_meaning": "is_default",
    "data_type": "uint16",
    "resampling": "nearest",
    "grid": "zoom_12",
    "source_type": "raster",
    "source_driver": "GeoTIFF",
    "source_uri": [
        "s3://gfw-data-lake-dev/umd_regional_primary_forest_2001/v201901.2/raster/epsg-4326/10/40000/is/gdal-geotiff/tiles.geojson"
    ],
    "symbology": {
        "type": "discrete",
        "colormap": {"1": {"red": 102, "green": 134, "blue": 54, "alpha": 255}},
    },
}


@pytest.mark.asyncio
@patch(
    "app.tasks.raster_tile_cache_assets.symbology._create_colormapped_asset",
    return_value=([], "colormapped_uri"),
)
@patch(
    "app.tasks.raster_tile_cache_assets.symbology._create_intensity_asset",
    return_value=([], "intensity_uri"),
)
@patch(
    "app.tasks.raster_tile_cache_assets.symbology._merge_assets",
    return_value=([], "merged_uri"),
)
async def test_colormap_symbology_no_intensity(
    mock_merge_assets,
    mock_create_intensity_asset,
    mock_create_colormapped_asset,
):
    _ = await colormap_symbology(
        "umd_regional_primary_forest_2001",
        "v201901.2",
        "pixel_meaning",
        RasterTileSetSourceCreationOptions(**wm_tile_set_co),
        12,
        12,
        {12: {"source_reprojection_job": "some_job"}},
    )
    assert mock_merge_assets.called is False
    assert mock_create_intensity_asset.called is False
    assert mock_create_colormapped_asset.called is True


@pytest.mark.asyncio
@patch(
    "app.tasks.raster_tile_cache_assets.symbology._create_colormapped_asset",
    return_value=([], "colormapped_uri"),
)
@patch(
    "app.tasks.raster_tile_cache_assets.symbology._create_intensity_asset",
    return_value=([], "intensity_uri"),
)
@patch(
    "app.tasks.raster_tile_cache_assets.symbology._merge_assets",
    return_value=([], "merged_uri"),
)
async def test_colormap_symbology_with_intensity(
    mock_merge_assets,
    mock_create_intensity_asset,
    mock_create_colormapped_asset,
):
    intensity_symbology = {
        "type": "discrete_intensity",
        "colormap": {"1": {"red": 102, "green": 134, "blue": 54}},
    }

    with patch.dict(wm_tile_set_co, {"symbology": intensity_symbology}, clear=False):
        _ = await colormap_symbology(
            "umd_regional_primary_forest_2001",
            "v201901.2",
            "pixel_meaning",
            RasterTileSetSourceCreationOptions(**wm_tile_set_co),
            12,
            12,
            {12: {"source_reprojection_job": "some_job"}},
        )
        assert mock_merge_assets.called is True
        assert mock_create_intensity_asset.called is True
        assert mock_create_colormapped_asset.called is True
