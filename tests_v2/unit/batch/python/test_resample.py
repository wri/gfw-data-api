import logging
from typing import Any, List, Tuple

import boto3
import pytest

from app.settings.globals import AWS_GCS_KEY_SECRET_ARN, AWS_REGION, AWS_SECRETSMANAGER_URL
from pyproj import CRS


MODULE_PATH_UNDER_TEST = "batch.python.resample"


@pytest.fixture(scope="module")
def gcs_secret():
    secret_client = boto3.client(
        "secretsmanager", region_name=AWS_REGION, endpoint_url=AWS_SECRETSMANAGER_URL
    )
    secret_client.create_secret(
        Name=AWS_GCS_KEY_SECRET_ARN,
        SecretString="foosecret",  # pragma: allowlist secret
    )

    from batch.python.resample import Bounds, intersecting_tiles

    yield Bounds, intersecting_tiles

    secret_client.delete_secret(SecretId=AWS_GCS_KEY_SECRET_ARN)


def test_intersecting_tiles_wm_same_crs_no_scaling_within_1_tile(gcs_secret):
    """Make sure that when we start with a (region slightly smaller than a) wm
    tile and look for intersecting tiles in the same zoom level we get just
    that tile."""
    Bounds, intersecting_tiles = gcs_secret

    source_crs = CRS.from_epsg(3857)
    src_tiles_info: List[Tuple[str, Any]] = [
        (
            "some_tile_id",
            {
                "type": "Polygon",
                # An area slightly inside tile 003R_003C of zoom level 12
                "coordinates": [
                    [
                        [-12520000.0, 10020000.0],  # Bottom-left
                        [-12520000.0, 12520000.0],  # Top-left
                        [-10020000.0, 12520000.0],  # Top-right
                        [-10020000.0, 10020000.0],  # Bottom-right
                        [-12520000.0, 10020000.0],  # Bottom-left
                    ]
                ],
            },
        )
    ]
    target_grid_name: str = "zoom_12"
    logger = logging.getLogger()

    result: List[Tuple[str, Bounds]] = intersecting_tiles(
        source_crs, src_tiles_info, target_grid_name, logger
    )

    assert result == [
        (
            "003R_003C",
            (
                -12523442.714243278,
                10018754.17151941,
                -10018754.171394622,
                12523442.714399263,
            ),
        )
    ]


def test_intersecting_tiles_wm_same_crs_no_scaling_straddling_4_tiles(gcs_secret):
    """Make sure that when we start with a region straddling the corner of 4 wm
    tiles and look for intersecting tiles in the same zoom level we get all
    4."""
    Bounds, intersecting_tiles = gcs_secret

    source_crs = CRS.from_epsg(3857)
    src_tiles_info: List[Tuple[str, Any]] = [
        (
            "some_tile_id",
            {
                "type": "Polygon",
                # An area straddling the top-left corner of 003R_003C of
                # zoom level 12
                "coordinates": [
                    [
                        [-12550000.0, 12500000.0],  # Bottom-left
                        [-12550000.0, 12600000.0],  # Top-left
                        [-11500000.0, 12600000.0],  # Top-right
                        [-11500000.0, 12500000.0],  # Bottom-right
                        [-12520000.0, 12500000.0],  # Bottom-left
                    ]
                ],
            },
        )
    ]
    target_grid_name: str = "zoom_12"
    logger = logging.getLogger()

    result: List[Tuple[str, Bounds]] = intersecting_tiles(
        source_crs, src_tiles_info, target_grid_name, logger
    )

    expected_tile_ids = {"003R_003C", "002R_002C", "002R_003C", "003R_002C"}
    assert expected_tile_ids == set([tile_info[0] for tile_info in result])


def test_intersecting_tiles_wm_same_crs_zoom_out(gcs_secret):
    """Also an area straddling the corners of 4 wm tiles, but this time target
    zoom level is one level out, turning those 4 into one tile."""
    Bounds, intersecting_tiles = gcs_secret

    source_crs = CRS.from_epsg(3857)
    src_tiles_info: List[Tuple[str, Any]] = [
        (
            "some_tile_id",
            {
                "type": "Polygon",
                # An area straddling the top-left corner of 003R_003C of
                # zoom level 12
                "coordinates": [
                    [
                        [-12550000.0, 12500000.0],  # Bottom-left
                        [-12550000.0, 12600000.0],  # Top-left
                        [-11500000.0, 12600000.0],  # Top-right
                        [-11500000.0, 12500000.0],  # Bottom-right
                        [-12520000.0, 12500000.0],  # Bottom-left
                    ]
                ],
            },
        )
    ]
    target_grid_name: str = "zoom_11"
    logger = logging.getLogger()

    result: List[Tuple[str, Bounds]] = intersecting_tiles(
        source_crs, src_tiles_info, target_grid_name, logger
    )

    expected_tile_ids = {"001R_001C"}
    assert expected_tile_ids == set([tile_info[0] for tile_info in result])


def test_intersecting_tiles_epsg_4326_to_wm(gcs_secret):
    """Go from a small epsg:4326 source tile to zoom level 10 tiles."""
    Bounds, intersecting_tiles = gcs_secret

    source_crs = CRS.from_epsg(4326)
    src_tiles_info: List[Tuple[str, Any]] = [
        (
            "some_tile_id",
            {
                "type": "Polygon",
                # Remember these are in degrees due to source CRS
                "coordinates": [
                    [
                        [0.0, -1.0],  # Bottom-left
                        [0.0, 0.0],  # Top-left
                        [1.0, 0.0],  # Top-right
                        [1.0, -1.0],  # Bottom-right
                        [0.0, -1.0],  # Bottom-left
                    ]
                ],
            },
        )
    ]
    target_grid_name: str = "zoom_10"
    logger = logging.getLogger()

    result: List[Tuple[str, Bounds]] = intersecting_tiles(
        source_crs, src_tiles_info, target_grid_name, logger
    )

    expected_tile_ids = {"002R_002C"}
    assert expected_tile_ids == set([tile_info[0] for tile_info in result])
