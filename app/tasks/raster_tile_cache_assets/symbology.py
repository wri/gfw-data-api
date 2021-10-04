from collections import defaultdict
from typing import (
    Any,
    Callable,
    Coroutine,
    DefaultDict,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
)

from fastapi.logger import logger

from app.crud.assets import create_asset
from app.models.enum.assets import AssetType
from app.models.enum.creation_options import ColorMapType, RasterDrivers
from app.models.enum.pixetl import DataType, Grid, PhotometricType, ResamplingMethod
from app.models.enum.sources import RasterSourceType
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import Job
from app.models.pydantic.metadata import RasterTileSetMetadata
from app.settings.globals import PIXETL_DEFAULT_RESAMPLING
from app.tasks import callback_constructor
from app.tasks.raster_tile_cache_assets.utils import (
    get_zoom_source_uri,
    reproject_to_web_mercator,
    scale_batch_job,
    tile_uri_to_tiles_geojson,
)
from app.tasks.raster_tile_set_assets.utils import create_gdaldem_job, create_pixetl_job
from app.tasks.utils import sanitize_batch_job_name
from app.utils.path import get_asset_uri

MAX_8_BIT_INTENSITY = 55
MAX_16_BIT_INTENSITY = 31

SymbologyFuncType = Callable[
    [str, str, str, RasterTileSetSourceCreationOptions, int, int, Dict[Any, Any]],
    Coroutine[Any, Any, Tuple[List[Job], str]],
]


class SymbologyInfo(NamedTuple):
    bit_depth: Literal[8, 16]
    req_input_bands: Optional[int]
    function: SymbologyFuncType


def generate_date_conf_calc_string() -> str:
    """Create the calc string for classic GLAD/RADD alerts."""
    day = "(A - ((A >= 30000) * 10000) - ((A >= 20000) * 20000))"
    confidence = "(1 * (A >= 30000))"  # 0 for low confidence, 1 for high

    red = f"({day} / 255)"
    green = f"({day} % 255)"
    blue = f"(({confidence} + 1) * 100 + B)"

    return f"np.ma.array([{red}, {green}, {blue}])"


def generate_8_bit_integrated_calc_string() -> str:
    """Create the calc string needed to encode/merge the 8-bit integrated alerts
    :return:
    """
    # <LONG EXPLANATION WITH EXAMPLE CODE>

    _first_alert = """
    np.ma.array(
        np.ma.array(
            np.minimum(
                A.filled(65535),
                np.minimum(
                    B.filled(65535),
                    C.filled(65535)
                )
            ),
            mask=(A.mask & B.mask & C.mask)
        ).filled(0),
        mask=(A.mask & B.mask & C.mask)
    )
    """
    first_alert = "".join(_first_alert.split())

    first_day = f"({first_alert} >> 1)"

    # At this point confidence is encoded as 0 for high, 1 for low.
    # Reverse that here for use in the Blue channel
    first_confidence = f"(({first_day} > 0) * " f"(({first_alert} & 1) == 0) * 1)"

    # Use the maximum intensity of the three alert systems
    _max_intensity = """
        np.maximum(
            D.filled(0),
            np.maximum(
                E.filled(0),
                F.filled(0)
            )
        )
    """
    max_intensity = "".join(_max_intensity.split())

    red = f"({first_day} / 255).astype(np.uint8)"
    green = f"({first_day} % 255).astype(np.uint8)"
    blue = (
        f"(({first_day} > 0) * "
        f"(({first_confidence} + 1) * 100 + {max_intensity}))"
        ".astype(np.uint8)"
    )

    gladl_conf = (
        "((A.filled(0) >> 1) > 0) * "
        "(((A.filled(0) & 1) == 0) * 2 + (A.filled(0) & 1))"
    )
    glads2_conf = (
        "((B.filled(0) >> 1) > 0) * "
        "(((B.filled(0) & 1) == 0) * 2 + (B.filled(0) & 1))"
    )
    radd_conf = (
        "((C.filled(0) >> 1) > 0) * "
        "(((C.filled(0) & 1) == 0) * 2 + (C.filled(0) & 1))"
    )

    alpha = f"({gladl_conf} << 6) | " f"({glads2_conf} << 4) | " f"({radd_conf} << 2)"

    return f"np.ma.array([{red}, {green}, {blue}, {alpha}])"


async def no_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Skip symbology step."""
    if source_asset_co.source_uri:
        wm_source_uri: str = tile_uri_to_tiles_geojson(
            get_asset_uri(
                dataset,
                version,
                AssetType.raster_tile_set,
                source_asset_co.copy(
                    deep=True, update={"grid": f"zoom_{zoom_level}"}
                ).dict(by_alias=True),
                "epsg:3857",
            )
        )
        return list(), wm_source_uri
    else:
        raise RuntimeError("No source URI set.")


async def colormap_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Create an RGBA raster with gradient or discrete breakpoint symbology."""

    assert source_asset_co.symbology is not None  # make mypy happy
    add_intensity_as_alpha: bool = source_asset_co.symbology.type in (
        ColorMapType.discrete_intensity,
        ColorMapType.gradient_intensity,
    )

    colormap_jobs, colormapped_asset_uri = await _create_colormapped_asset(
        dataset,
        version,
        pixel_meaning,
        source_asset_co,
        zoom_level,
        jobs_dict,
        not add_intensity_as_alpha,
    )

    # Optionally add intensity as alpha band
    intensity_jobs: Sequence[Job] = tuple()
    merge_jobs: Sequence[Job] = tuple()

    if add_intensity_as_alpha:
        intensity_co = source_asset_co.copy(
            deep=True,
            update={
                "calc": None,
                "data_type": DataType.uint8,
            },
        )

        intensity_max_zoom_calc_string = "np.ma.array((~A.mask) * 255)"

        # But wait! Apply intensity scaling for each zoom level to mirror what
        # is done by the front end for TCL
        # Adapted from gfw-tile-cache/lambdas/raster_tiler/lambda_function.py#L65-L79
        # def scale_intensity(z_l: int) -> str:
        #     """Simplified implementing of d3.scalePow() Assuming that both
        #     domain and range always start with 0."""
        #     exp = 0.3 + ((z_l - 3) / 20) if z_l < 11 else 1
        #     domain = (0, 255)
        #     scale_range = (0, 255)
        #     m = scale_range[1] / domain[1] ** exp
        #     b = scale_range[0]
        #
        #     return f"np.ma.array({m} * A ** {exp} + {b})"
        #
        # intensity_co.calc = scale_intensity(zoom_level)

        intensity_jobs, intensity_uri = await _create_intensity_asset(
            dataset,
            version,
            pixel_meaning,
            intensity_co,
            zoom_level,
            max_zoom,
            jobs_dict,
            intensity_max_zoom_calc_string,
            ResamplingMethod.average,
        )

        merge_jobs, final_asset_uri = await _merge_assets(
            dataset,
            version,
            pixel_meaning,
            tile_uri_to_tiles_geojson(colormapped_asset_uri),
            tile_uri_to_tiles_geojson(intensity_uri),
            zoom_level,
            [*colormap_jobs, *intensity_jobs],
        )
    else:
        final_asset_uri = colormapped_asset_uri
    return [*colormap_jobs, *intensity_jobs, *merge_jobs], final_asset_uri


async def date_conf_intensity_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Create a Raster Tile Set asset which is the combination of a date_conf
    asset and a new derived intensity asset.

    At native resolution (max_zoom) it creates an "intensity" asset
    which contains the value 55 everywhere there is data in the source
    (date_conf) raster. For lower zoom levels it resamples the higher
    zoom level intensity tiles using the "bilinear" resampling method.
    Finally the merge function combines the date_conf and intensity
    assets into a three band RGB-encoded asset suitable for converting
    to PNGs with gdal2tiles in the final stage of
    raster_tile_cache_asset
    """
    intensity_co = source_asset_co.copy(
        deep=True, update={"calc": None, "band_count": 1, "data_type": DataType.uint8}
    )
    intensity_max_calc_string = f"(A > 0) * {MAX_8_BIT_INTENSITY}"

    intensity_jobs, intensity_uri = await _create_intensity_asset(
        dataset,
        version,
        pixel_meaning,
        intensity_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        intensity_max_calc_string,
        ResamplingMethod.bilinear,
    )

    wm_date_conf_uri: str = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        source_asset_co.copy(deep=True, update={"grid": f"zoom_{zoom_level}"}).dict(
            by_alias=True
        ),
        "epsg:3857",
    )

    merge_calc_string: str = generate_date_conf_calc_string()

    merge_jobs, final_asset_uri = await _merge_assets(
        dataset,
        version,
        pixel_meaning,
        tile_uri_to_tiles_geojson(wm_date_conf_uri),
        tile_uri_to_tiles_geojson(intensity_uri),
        zoom_level,
        intensity_jobs,
        merge_calc_string,
        3,
    )

    return [*intensity_jobs, *merge_jobs], final_asset_uri


async def date_conf_intensity_multi_8_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Create a Raster Tile Set asset which combines the earliest detected
    alerts of three date_conf bands/alert systems (new encoding) with a new
    derived intensity asset, and the confidences of each of the original
    alerts.

    At native resolution (max_zoom) it creates a three band "intensity"
    asset (one band per original band) which contains the value 55
    everywhere there is data in the source (date_conf) band. For lower
    zoom levels it resamples the previous zoom level intensity asset
    using the bilinear resampling method, causing isolated pixels to
    "fade". Finally the merge function takes the alert with the minimum
    date of the three bands and encodes its date, confidence, and the
    maximum of the three intensities into three 8-bit bands according to
    the formula the front end expects, and also adds a fourth band which
    encodes the confidences of all three original alert systems.
    """

    # What we want is a value of 55 (max intensity for this scenario)
    # anywhere there is an alert in any system. We can't just do
    # "((A > 0) | (B > 0) | (C > 0)) * 55" because "A | B" includes only
    # those values unmasked in both A and B. In fact we don't want masked
    # values at all! So first replace masked values with 0
    intensity_calc_string = (
        "np.ma.array(["
        f"((A.filled(0) >> 1) > 0) * {MAX_8_BIT_INTENSITY},"  # GLAD-L
        f"((B.filled(0) >> 1) > 0) * {MAX_8_BIT_INTENSITY},"  # GLAD-S2
        f"((C.filled(0) >> 1) > 0) * {MAX_8_BIT_INTENSITY}"  # RADD
        "])"
    )

    intensity_co = source_asset_co.copy(
        deep=True, update={"calc": None, "band_count": 3, "data_type": DataType.uint8}
    )

    intensity_jobs, intensity_uri = await _create_intensity_asset(
        dataset,
        version,
        pixel_meaning,
        intensity_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        intensity_calc_string,
        ResamplingMethod.bilinear,
    )

    merge_calc_string: str = generate_8_bit_integrated_calc_string()

    wm_date_conf_uri: str = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        source_asset_co.copy(deep=True, update={"grid": f"zoom_{zoom_level}"}).dict(
            by_alias=True
        ),
        "epsg:3857",
    )

    merge_jobs, final_asset_uri = await _merge_assets(
        dataset,
        version,
        pixel_meaning,
        tile_uri_to_tiles_geojson(wm_date_conf_uri),
        tile_uri_to_tiles_geojson(intensity_uri),
        zoom_level,
        [*intensity_jobs],
        merge_calc_string,
    )
    return [*intensity_jobs, *merge_jobs], final_asset_uri


async def year_intensity_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Create Raster Tile Set asset which combines year raster and intensity
    raster into one.

    At native resolution (max_zoom) it will create intensity raster
    based on given source. For lower zoom levels it will resample higher
    zoom level tiles using average resampling method. Once intensity
    raster tile set is created it will combine it with source (year)
    raster into an RGB-encoded raster.
    """

    intensity_calc_string = "(A > 0) * 255"

    intensity_jobs, intensity_uri = await _create_intensity_asset(
        dataset,
        version,
        pixel_meaning,
        source_asset_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        intensity_calc_string,
        ResamplingMethod.average,
    )

    merge_calc_string = (
        "np.ma.array("
        "[A, np.ma.zeros(A.shape, dtype='uint8'), B], "
        "fill_value=0"
        ").astype('uint8')"
    )

    wm_source_uri: str = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        source_asset_co.copy(deep=True, update={"grid": f"zoom_{zoom_level}"}).dict(
            by_alias=True
        ),
        "epsg:3857",
    )

    merge_jobs, final_asset_uri = await _merge_assets(
        dataset,
        version,
        pixel_meaning,
        tile_uri_to_tiles_geojson(wm_source_uri),
        tile_uri_to_tiles_geojson(intensity_uri),
        zoom_level,
        [*intensity_jobs],
        merge_calc_string,
        3,
    )
    return [*intensity_jobs, *merge_jobs], final_asset_uri


async def _create_colormapped_asset(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    jobs_dict: Dict,
    with_alpha: bool,
) -> Tuple[List[Job], str]:
    wm_source_co = source_asset_co.copy(
        deep=True, update={"grid": f"zoom_{zoom_level}"}
    )

    wm_source_uri: str = tile_uri_to_tiles_geojson(
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            wm_source_co.dict(by_alias=True),
            "epsg:3857",
        )
    )

    colormap_co = wm_source_co.copy(
        deep=True,
        update={
            "source_uri": [wm_source_uri],
            "calc": None,
            "resampling": PIXETL_DEFAULT_RESAMPLING,
            "pixel_meaning": f"colormap_{pixel_meaning}",
        },
    )

    colormap_asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        colormap_co.dict(by_alias=True),
        "epsg:3857",
    )

    # Create an asset record
    colormap_asset_model = AssetCreateIn(
        asset_type=AssetType.raster_tile_set,
        asset_uri=colormap_asset_uri,
        is_managed=True,
        creation_options=colormap_co,
    ).dict(by_alias=True)
    colormap_asset_record = await create_asset(dataset, version, **colormap_asset_model)

    logger.debug(
        f"Created asset record for {colormap_asset_uri} "
        f"with creation options: {colormap_co}"
    )

    parents = [jobs_dict[zoom_level]["source_reprojection_job"]]
    job_name = sanitize_batch_job_name(
        f"{dataset}_{version}_{pixel_meaning}_{zoom_level}"
    )

    # Apply the colormap
    gdaldem_job = await create_gdaldem_job(
        dataset,
        version,
        colormap_co,
        with_alpha,
        job_name,
        callback_constructor(colormap_asset_record.asset_id),
        parents=parents,
    )
    gdaldem_job = scale_batch_job(gdaldem_job, zoom_level)

    return [gdaldem_job], colormap_asset_uri


async def _create_intensity_asset(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
    max_zoom_calc: str,
    resampling: ResamplingMethod,
) -> Tuple[List[Job], str]:
    """Create intensity Raster Tile Set asset based on source asset.

    Create Intensity value layer(s) using provided calc function,
    resample intensity based on provided resampling method.
    """

    source_uri: Optional[List[str]] = get_zoom_source_uri(
        dataset, version, source_asset_co, zoom_level, max_zoom
    )
    intensity_source_co: RasterTileSetSourceCreationOptions = source_asset_co.copy(
        deep=True,
        update={
            "source_uri": source_uri,
            "no_data": None,
            "pixel_meaning": f"intensity_{pixel_meaning}",
            "resampling": resampling,
        },
    )
    source_job = jobs_dict[zoom_level]["source_reprojection_job"]

    if zoom_level != max_zoom:
        previous_level_intensity_reprojection_job = [
            jobs_dict[zoom_level + 1]["intensity_reprojection_job"]
        ]
    else:
        previous_level_intensity_reprojection_job = [source_job]

    intensity_job, intensity_uri = await reproject_to_web_mercator(
        dataset,
        version,
        intensity_source_co,
        zoom_level,
        max_zoom,
        previous_level_intensity_reprojection_job,
        max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
        max_zoom_calc=max_zoom_calc,
    )
    jobs_dict[zoom_level]["intensity_reprojection_job"] = intensity_job

    return [intensity_job], intensity_uri


async def _merge_assets(
    dataset: str,
    version: str,
    pixel_meaning: str,
    asset1_uri: str,
    asset2_uri: str,
    zoom_level: int,
    parents: List[Job],
    calc_str: str = "np.ma.array([A, B, C, D])",
    band_count: int = 4,
) -> Tuple[List[Job], str]:
    """Create RGBA-encoded raster tile set from two source assets, potentially
    using a custom merge function (the default works for 3+1 band sources, such
    as RGB + Intensity as Alpha)"""

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint8,  # FIXME: Revisit for 16-bit assets
        band_count=band_count,
        no_data=[0 for i in range(band_count)],
        resampling=ResamplingMethod.nearest,
        grid=Grid(f"zoom_{zoom_level}"),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        source_uri=[asset1_uri, asset2_uri],
        calc=calc_str,
        photometric=PhotometricType.rgb,
    )

    asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        encoded_co.dict(by_alias=True),
        "epsg:3857",
    )

    logger.debug(
        f"ATTEMPTING TO CREATE MERGED ASSET WITH THESE CREATION OPTIONS: {encoded_co}"
    )

    # Create an asset record
    asset_options = AssetCreateIn(
        asset_type=AssetType.raster_tile_set,
        asset_uri=asset_uri,
        is_managed=True,
        creation_options=encoded_co,
        metadata=RasterTileSetMetadata(),
    ).dict(by_alias=True)

    asset = await create_asset(dataset, version, **asset_options)
    logger.debug(
        f"ZOOM LEVEL {zoom_level} MERGED ASSET CREATED WITH ASSET_ID {asset.asset_id}"
    )

    callback = callback_constructor(asset.asset_id)
    pixetl_job = await create_pixetl_job(
        dataset,
        version,
        encoded_co,
        job_name=f"merge_assets_zoom_{zoom_level}",
        callback=callback,
        parents=parents,
    )

    pixetl_job = scale_batch_job(pixetl_job, zoom_level)

    return (
        [pixetl_job],
        tile_uri_to_tiles_geojson(asset_uri),
    )


_symbology_constructor: Dict[str, SymbologyInfo] = {
    ColorMapType.date_conf_intensity: SymbologyInfo(
        8, 1, date_conf_intensity_symbology
    ),
    ColorMapType.date_conf_intensity_multi_8: SymbologyInfo(
        8, 3, date_conf_intensity_multi_8_symbology
    ),
    ColorMapType.year_intensity: SymbologyInfo(8, 1, year_intensity_symbology),
    ColorMapType.gradient: SymbologyInfo(8, 1, colormap_symbology),
    ColorMapType.gradient_intensity: SymbologyInfo(8, 1, colormap_symbology),
    ColorMapType.discrete: SymbologyInfo(8, 1, colormap_symbology),
    ColorMapType.discrete_intensity: SymbologyInfo(8, 1, colormap_symbology),
}

symbology_constructor: DefaultDict[str, SymbologyInfo] = defaultdict(
    lambda: SymbologyInfo(8, None, no_symbology)
)
symbology_constructor.update(**_symbology_constructor)
