import pytest

from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.statistics import BandStats, RasterStats
from app.tasks.raster_tile_cache_assets.utils import convert_float_to_int

input_cm_symbology = {
    "type": "gradient",
    "colormap": {
        0: {"red": 255, "green": 128, "blue": 0},
        1: {"red": 0, "green": 128, "blue": 255},
    },
}

expected_cm_symbology = {
    "type": "gradient",
    "colormap": {
        1: {"red": 255, "green": 128, "blue": 0},
        65535: {"red": 0, "green": 128, "blue": 255},
    },
}

input_cm_symbology_with_alpha = {
    "type": "gradient",
    "colormap": {
        0: {"red": 255, "green": 128, "blue": 0, "alpha": 255},
        1: {"red": 0, "green": 128, "blue": 255, "alpha": 255},
    },
}

expected_cm_symbology_with_alpha = {
    "type": "gradient",
    "colormap": {
        1: {"red": 255, "green": 128, "blue": 0, "alpha": 255},
        65535: {"red": 0, "green": 128, "blue": 255, "alpha": 255},
    },
}


@pytest.mark.parametrize(
    "input_symbology, expected",
    [
        (input_cm_symbology, expected_cm_symbology),
        (input_cm_symbology_with_alpha, expected_cm_symbology_with_alpha),
    ],
)
def test_convert_float_to_int(input_symbology, expected):
    old_co = RasterTileSetSourceCreationOptions(
        **{
            "source_type": "raster",
            "source_uri": ["s3://some_bucket/some_prefix/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "float64",
            "no_data": "nan",
            "pixel_meaning": "some_pixel_meaning",
            "grid": "90/27008",
            "symbology": input_symbology,
        }
    )

    stats = RasterStats(
        **{"bands": [BandStats(**{"min": 0, "max": 1, "mean": 0.5})]}
    ).dict()

    new_co, calc_str = convert_float_to_int(stats, old_co)

    expected_calc_str = "(A != np.nan) * (1 + (A - 0.0) * 65534.0).astype(np.uint16)"
    assert calc_str == expected_calc_str

    assert new_co.symbology.dict() == expected
