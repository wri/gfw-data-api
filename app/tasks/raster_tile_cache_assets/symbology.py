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

SymbologyFuncType = Callable[
    [str, str, str, RasterTileSetSourceCreationOptions, int, int, Dict[Any, Any]],
    Coroutine[Any, Any, Tuple[List[Job], str]],
]


class SymbologyInfo(NamedTuple):
    bit_depth: Literal[8, 16]
    req_input_bands: Optional[List[int]]
    function: SymbologyFuncType


def date_conf_rgb_calc(
    date_conf_band: str = "A", intensity_band: str = "B"
) -> Tuple[str, str, str]:
    """Create the calc strings to merge a dateconf and intensity band into Red,
    Green, and Blue channels."""
    # The date-conf format goes like this:
    # Take 20000 for a low confidence alert, 30000 for a high confidence
    # alert, (or 40000 for an alert seen by multiple systems in the case
    # of integrated alerts,) and add the number of days since December 31 2014.
    # 0 is the no-data value
    #
    # So, some example values in the date_conf encoding:
    # 20001 is a low confidence alert on January 1st, 2015
    # 30055 is a high confidence alert on February 24, 2015
    # 21847 is a low confidence alert on January 21, 2020
    # 18030 and 50389 are bogus values

    day = f"(({date_conf_band}.data >= 20000) * ({date_conf_band}.data % 10000))"
    confidence = (
        f"({date_conf_band}.data // 30000)"  # 0 for low confidence, 1 for high/highest
    )

    # The format the front end is expecting, meanwhile, goes like this:
    # Red = floor(day / 255)
    # Green = day % 255
    # Blue = ((confidence + 1) * 100) + intensity

    red = f"({day} // 255)"
    green = f"({day} % 255)"
    blue = f"({date_conf_band}.data >= 20000) * (({confidence} + 1) * 100 + {intensity_band}.data)"

    return f"{red}", f"{green}", f"{blue}"


def date_conf_merge_calc() -> str:
    """Create the calc string for classic GLAD/RADD alerts."""
    red, green, blue = date_conf_rgb_calc()
    return f"np.ma.array([{red}, {green}, {blue}], mask=False)"


def integrated_alerts_merge_calc() -> str:
    """Create the calc string needed to encode/merge the 8-bit integrated alerts
    :return:
    """
    # The RGB channels are identical in encoding to the date-conf encoding
    # used for GLADL, GLADS2, and RADD

    red, green, blue = date_conf_rgb_calc()

    # The front end is ALSO expecting the confidences of all the alerts in the
    # original alert systems packed into an 8-bit value as the Alpha channel
    # according to the following scheme:
    # Alpha = (gladl_conf << 6) | (glads2_conf << 4) | (radd_conf << 2)
    #
    # Where gladl_conf, glads2_conf, and radd_conf are 2 for high confidence,
    # 1 for low confidence, and 0 for not detected.
    # Thus, the last 2 bits are currently unused.
    #
    # But it doesn't really NEED all that, all it REALLY needs to know is if
    # the combined alert is low, high, or highest confidence. So we slimmed-
    # down and saved just that fact by encoding it as a 40k+ alert value.
    # However rather than change the encoding as presented to the front end
    # we create a fake combined confidence value. We accomplish that by
    # setting RADD's confidence to 1 for low conf, 2 for high conf, and
    # doing a bitwise OR with 4 (indicating a low confidence alert in GLAD-S2)
    # for highest confidence.
    # In the future suggest a new way for the front end to obtain this info
    # that's more elegant (such as by more efficiently packing into the Blue
    # channel).
    alpha = "((1 * (A.data >= 20000) + 1 * (A.data >= 30000)) | (4 * (A.data >= 40000))) << 2"

    return f"np.ma.array([{red}, {green}, {blue}, {alpha}], mask=False)"


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
    """Create an RGB(A) raster with gradient or discrete breakpoint
    symbology."""

    assert source_asset_co.symbology is not None  # make mypy happy

    if source_asset_co.symbology.type in (
        ColorMapType.discrete_intensity,
        ColorMapType.gradient_intensity,
    ):
        add_intensity_as_alpha: bool = True
        colormap_asset_pixel_meaning: str = f"colormap_{pixel_meaning}"
    else:
        add_intensity_as_alpha = False
        colormap_asset_pixel_meaning = pixel_meaning

    colormap_jobs, colormapped_asset_uri = await _create_colormapped_asset(
        dataset,
        version,
        colormap_asset_pixel_meaning,
        source_asset_co,
        zoom_level,
        jobs_dict,
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

        # We also need to depend on the original source reprojection job
        source_job = jobs_dict[zoom_level]["source_reprojection_job"]

        merge_jobs, final_asset_uri = await _merge_assets(
            dataset,
            version,
            pixel_meaning,
            tile_uri_to_tiles_geojson(colormapped_asset_uri),
            tile_uri_to_tiles_geojson(intensity_uri),
            zoom_level,
            [*colormap_jobs, *intensity_jobs, source_job],
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

    merge_calc_string: str = date_conf_merge_calc()

    # We also need to depend on the original source reprojection job
    source_job = jobs_dict[zoom_level]["source_reprojection_job"]

    merge_jobs, final_asset_uri = await _merge_assets(
        dataset,
        version,
        pixel_meaning,
        tile_uri_to_tiles_geojson(wm_date_conf_uri),
        tile_uri_to_tiles_geojson(intensity_uri),
        zoom_level,
        [*intensity_jobs, source_job],
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

    At native resolution (max_zoom) it an "intensity" asset which
    contains the value 55 everywhere there is data in any of the source
    bands. For lower zoom levels it resamples the previous zoom level
    intensity asset using the bilinear resampling method, causing
    isolated pixels to "fade". Finally the merge function takes the
    alert with the minimum date of the three bands and encodes its date,
    confidence, and the intensities into three 8-bit bands according to
    the formula the front end expects, and also adds a fourth band which
    encodes the confidences of all three original alert systems.
    """

    # What we want is a value of 55 (max intensity for this scenario)
    # anywhere there is an alert in any system.
    intensity_max_calc_string = (
        f"np.ma.array((A.data > 0) * {MAX_8_BIT_INTENSITY}, mask=False)"
    )

    intensity_co = source_asset_co.copy(
        deep=True,
        update={
            "calc": None,
            "band_count": 1,
            "data_type": DataType.uint8,
        },
    )

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

    merge_calc_string: str = integrated_alerts_merge_calc()

    # We also need to depend on the original source reprojection job
    source_job = jobs_dict[zoom_level]["source_reprojection_job"]

    merge_jobs, final_asset_uri = await _merge_assets(
        dataset,
        version,
        pixel_meaning,
        tile_uri_to_tiles_geojson(wm_date_conf_uri),
        tile_uri_to_tiles_geojson(intensity_uri),
        zoom_level,
        [*intensity_jobs, source_job],
        merge_calc_string,
    )
    return [*intensity_jobs, *merge_jobs], final_asset_uri


async def value_intensity_symbology(
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
    raster into an RGB-encoded raster. This symbology is used for the
    Tree Cover Loss dataset.
    """

    if source_asset_co.band_count == 1:
        intensity_calc_string = "(A > 0) * 255"
    elif source_asset_co.band_count == 2:
        intensity_calc_string = "((A > 0) & (B > 0)) * 255"
    else:
        raise RuntimeError(
            f"Too many bands in source asset ({source_asset_co.band_count}), max 2 bands supported."
        )

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

    # The resulting raster channels are as follows:
    # 1. Intensity
    # 2. Values in second band of sources asset, or all zeros if only one band
    # 3. Values in first band of source assets (B for backwards compatibility)
    # 4. Alpha (which is set to 255 everywhere intensity is >0)
    if source_asset_co.band_count == 1:
        merge_calc_string = "np.ma.array([B, np.ma.zeros(A.shape, dtype='uint8'), A, (B > 0) * 255], fill_value=0).astype('uint8')"
    elif source_asset_co.band_count == 2:
        merge_calc_string = (
            "np.ma.array([C, A, B, (B > 0) * 255], fill_value=0).astype('uint8')"
        )
    else:
        raise RuntimeError(
            f"Too many bands in source asset ({source_asset_co.band_count}), max 2 bands supported."
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

    # We also need to depend on the original source reprojection job
    source_job = jobs_dict[zoom_level]["source_reprojection_job"]

    merge_jobs, final_asset_uri = await _merge_assets(
        dataset,
        version,
        pixel_meaning,
        tile_uri_to_tiles_geojson(wm_source_uri),
        tile_uri_to_tiles_geojson(intensity_uri),
        zoom_level,
        [*intensity_jobs, source_job],
        merge_calc_string,
        4,
    )
    return [*intensity_jobs, *merge_jobs], final_asset_uri


async def _create_colormapped_asset(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    jobs_dict: Dict,
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
            "pixel_meaning": pixel_meaning,
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
            "band_count": 1,
        },
    )

    if zoom_level == max_zoom:
        parent_jobs: List[Job] = [jobs_dict[zoom_level]["source_reprojection_job"]]
    else:
        parent_jobs = [jobs_dict[zoom_level + 1]["intensity_reprojection_job"]]

    intensity_job, intensity_uri = await reproject_to_web_mercator(
        dataset,
        version,
        intensity_source_co,
        zoom_level,
        max_zoom,
        parent_jobs,
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
        no_data=None,
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
        8, [1], date_conf_intensity_symbology
    ),
    ColorMapType.date_conf_intensity_multi_8: SymbologyInfo(
        8, [1], date_conf_intensity_multi_8_symbology
    ),
    ColorMapType.year_intensity: SymbologyInfo(8, [1], value_intensity_symbology),
    ColorMapType.value_intensity: SymbologyInfo(8, [1, 2], value_intensity_symbology),
    ColorMapType.gradient: SymbologyInfo(8, [1], colormap_symbology),
    ColorMapType.gradient_intensity: SymbologyInfo(8, [1], colormap_symbology),
    ColorMapType.discrete: SymbologyInfo(8, [1], colormap_symbology),
    ColorMapType.discrete_intensity: SymbologyInfo(8, [1], colormap_symbology),
}

symbology_constructor: DefaultDict[str, SymbologyInfo] = defaultdict(
    lambda: SymbologyInfo(8, None, no_symbology)
)
symbology_constructor.update(**_symbology_constructor)
