import os
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
from app.models.pydantic.jobs import BuildRGBJob, Job, PixETLJob
from app.models.pydantic.metadata import RasterTileSetMetadata
from app.models.pydantic.symbology import Symbology
from app.settings.globals import PIXETL_DEFAULT_RESAMPLING
from app.tasks import callback_constructor
from app.tasks.raster_tile_cache_assets.utils import (
    get_zoom_source_uri,
    reproject_to_web_mercator,
    scale_batch_job,
    tile_uri_to_tiles_geojson,
)
from app.tasks.raster_tile_set_assets.utils import (
    JOB_ENV,
    create_gdaldem_job,
    create_pixetl_job,
)
from app.tasks.utils import sanitize_batch_job_name
from app.utils.path import get_asset_uri, split_s3_path

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
        return list(), source_asset_co.source_uri[0]
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
    """Create an RGBA raster with gradient or discrete symbology."""

    assert source_asset_co.symbology  # make mypy happy
    source_uri = (
        [tile_uri_to_tiles_geojson(uri) for uri in source_asset_co.source_uri]
        if source_asset_co.source_uri
        else None
    )

    creation_options = source_asset_co.copy(
        deep=True,
        update={
            "source_uri": source_uri,
            "calc": None,
            "resampling": PIXETL_DEFAULT_RESAMPLING,
            "grid": f"zoom_{zoom_level}",
            "pixel_meaning": f"colormap_{pixel_meaning}",
        },
    )

    new_asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        creation_options.dict(by_alias=True),
        "epsg:3857",
    )

    # Create an asset record
    asset_options = AssetCreateIn(
        asset_type=AssetType.raster_tile_set,
        asset_uri=new_asset_uri,
        is_managed=True,
        creation_options=creation_options,
        metadata=RasterTileSetMetadata(),
    ).dict(by_alias=True)
    symbology_asset_record = await create_asset(dataset, version, **asset_options)

    logger.debug(f"Created asset record for {new_asset_uri}")

    parents = [jobs_dict[zoom_level]["source_reprojection_job"]]
    job_name = sanitize_batch_job_name(
        f"{dataset}_{version}_{pixel_meaning}_{zoom_level}"
    )
    gdaldem_job = await create_gdaldem_job(
        dataset,
        version,
        creation_options,
        job_name,
        callback_constructor(symbology_asset_record.asset_id),
        parents=parents,
    )

    gdaldem_job = scale_batch_job(gdaldem_job, zoom_level)

    intensity_jobs: Tuple[List[Job], str] = tuple()
    merge_jobs: Tuple[List[Job], str] = tuple()
    if source_asset_co.symbology.type in (
        ColorMapType.discrete_intensity,
        ColorMapType.gradient_intensity
    ):
        max_zoom_calc_string = "(A > 0) * 255"
        intensity_co = source_asset_co.copy(
            deep=True,
            update={"data_type": DataType.uint8}
        )

        intensity_jobs, intensity_uri = await _create_intensity_asset(
            dataset,
            version,
            pixel_meaning,
            intensity_co,
            zoom_level,
            max_zoom,
            jobs_dict,
            max_zoom_calc_string,
            ResamplingMethod.bilinear
        )

        merge_jobs, final_asset_uri = await _merge_colormap_and_intensity(
            dataset,
            version,
            pixel_meaning,
            tile_uri_to_tiles_geojson(new_asset_uri),
            tile_uri_to_tiles_geojson(intensity_uri),
            zoom_level,
            [gdaldem_job, *intensity_jobs]
        )
    else:
        final_asset_uri = new_asset_uri
    return [gdaldem_job, *intensity_jobs, *merge_jobs], final_asset_uri


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
    intensity_co = source_asset_co.copy(deep=True, update={"data_type": DataType.uint8})

    # What we want is a value of 55 (max intensity for the 8-bit scenario)
    # anywhere there is an alert in any system. We can't just do
    # "((A > 0) | (B > 0) | (C > 0)) * 55" because "A | B" includes only
    # those values unmasked in both A and B. In fact we don't want masked
    # values at all! So first replace masked values with 0
    max_zoom_calc_string = (
        "np.ma.array(["
        f"((A.filled(0) >> 1) > 0) * {MAX_8_BIT_INTENSITY},"  # GLAD-L
        f"((B.filled(0) >> 1) > 0) * {MAX_8_BIT_INTENSITY},"  # GLAD-S2
        f"((C.filled(0) >> 1) > 0) * {MAX_8_BIT_INTENSITY}"   # RADD
        "])"
    )
    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        intensity_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        max_zoom_calc_string,
        ResamplingMethod.bilinear,
        _merge_intensity_and_date_conf_multi_8,
    )


async def date_conf_intensity_multi_16_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Create a Raster Tile Set asset which combines a three band/alert system
    date_conf asset (new encoding) with a new derived intensity asset.

    At native resolution (max_zoom) it creates a three band "intensity"
    asset (one band per original band) which contains the value 31
    everywhere there is data in the source (date_conf) asset. For lower
    zoom levels it resamples the previous zoom level intensity asset
    using the bilinear resampling method, causing isolated pixels to
    "fade". Finally the merge function combines the date_conf and
    intensity assets into a four band 16-bit-per-band asset suitable for
    converting to 16-bit PNGs with a modified gdal2tiles in the final
    stage of raster_tile_cache_asset. The final merged asset is saved in
    the legacy GLAD-L/RADD date_conf format.
    """
    intensity_co = source_asset_co.copy(deep=True, update={"data_type": DataType.uint8})

    # What we want is a value of 31 anywhere there is an alert, band-by-band.
    # Why is 31 the maximum instead of 55 like in the 8-bit symbology?
    # Because in the end we have to pack the intensities into one 16-bit
    # band. That means (unless we want to get really complicated) we have
    # 5 bits for each intensity value with 1 bit left over. 2^5 = 32, so the
    # largest value we can have for intensity in the 16-bit symbology is 31.
    max_zoom_calc_string = (
        "np.ma.array(["
        f"((A.filled(0) >> 1) > 0) * {MAX_16_BIT_INTENSITY},"  # GLAD-L
        f"((B.filled(0) >> 1) > 0) * {MAX_16_BIT_INTENSITY},"  # GLAD-S2
        f"((C.filled(0) >> 1) > 0) * {MAX_16_BIT_INTENSITY}"  # RADD
        "])"
    )

    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        intensity_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        max_zoom_calc_string,
        ResamplingMethod.bilinear,
        _merge_intensity_and_date_conf_multi_16,
    )


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
    zoom level intensity tiles using the bilinear resampling method,
    causing isolated pixels to "fade". Finally the merge function
    combines the date_conf and intensity assets into a three band RGB-
    encoded asset suitable for converting to PNGs with gdal2tiles in the
    final stage of raster_tile_cache_asset
    """
    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        source_asset_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        f"(A > 0) * {MAX_8_BIT_INTENSITY}",
        ResamplingMethod.bilinear,
        _merge_intensity_and_date_conf,
    )


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
    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        source_asset_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        "(A > 0) * 255",
        ResamplingMethod.average,
        _merge_intensity_and_year,
    )


async def _date_intensity_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
    max_zoom_calc: str,
    resampling: ResamplingMethod,
    merge_function: Callable,
) -> Tuple[List[Job], str]:
    """Create Raster Tile Set asset which combines source asset and intensity
    raster into one.

    Create Intensity value layer using provided calc function, resample
    intensity based on provided resampling method. Merge intensity value
    with source asset using provided merge function.
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
    date_job = jobs_dict[zoom_level]["source_reprojection_job"]

    if zoom_level != max_zoom:
        previous_level_intensity_reprojection_job = [
            jobs_dict[zoom_level + 1]["intensity_reprojection_job"]
        ]
    else:
        previous_level_intensity_reprojection_job = [date_job]

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

    assert source_asset_co.source_uri, "No source URI set"
    date_uri = source_asset_co.source_uri[0]

    merge_jobs, dst_uri = await merge_function(
        dataset,
        version,
        pixel_meaning,
        tile_uri_to_tiles_geojson(date_uri),
        tile_uri_to_tiles_geojson(intensity_uri),
        zoom_level,
        [date_job, intensity_job],
    )
    jobs_dict[zoom_level]["merge_intensity_jobs"] = merge_jobs

    return [intensity_job, *merge_jobs], dst_uri


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
    """Create intensity Raster Tile Set asset which based on source asset

    Create Intensity value layer(s) using provided calc function, resample
    intensity based on provided resampling method.
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


async def _merge_colormap_and_intensity(
    dataset: str,
    version: str,
    pixel_meaning: str,
    colormap_asset_uri: str,
    intensity_asset_uri: str,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[List[Job], str]:
    """Create RGBA-encoded raster tile set based on colormap and intensity
    raster tile sets."""

    # TODO: Make flexible enough to replace other merge functions

    calc_str = f"np.ma.array([A, B, C, D])"

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint8,
        band_count=4,
        no_data=[0, 0, 0, 0],
        resampling=ResamplingMethod.nearest,
        overwrite=False,
        grid=Grid(f"zoom_{zoom_level}"),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        source_uri=[colormap_asset_uri, intensity_asset_uri],
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
    asset_prefix = asset_uri.rsplit("/", 1)[0]

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
        job_name=f"merge_colormap_and_intensity_zoom_{zoom_level}",
        callback=callback,
        parents=parents,
    )

    pixetl_job = scale_batch_job(pixetl_job, zoom_level)

    return (
        [pixetl_job],
        os.path.join(asset_prefix, "tiles.geojson"),
    )


async def _merge_intensity_and_date_conf_multi_8(
    dataset: str,
    version: str,
    pixel_meaning: str,
    date_conf_uri: str,
    intensity_uri: str,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[List[Job], str]:
    """Create RGB-encoded raster tile set based on date_conf and intensity
    raster tile sets."""
    # import numpy as np

    # Plausible raw values, bands as seen when creating the raster tile set
    # A = np.ma.array([30080, 20060, 21040, 0], dtype=np.uint16)
    # B = np.ma.array([1, 1, 2, 0], dtype=np.uint8)
    # C = np.ma.array([140, 200, 1000, 0], dtype=np.uint16)
    # D = np.ma.array([31040, 21040, 20040, 0], dtype=np.uint16)

    # The new encoding can be summarized as
    # value = 2 * DAY + (1 if low confidence else 0)
    # So if matrix X is in the new encoding,
    # DAY = X >> 1 and
    # CONFIDENCE = X & 1 where a CONFIDENCE value of 1 = low, and 0 -= high

    # Anyway, this the calc function I specify when creating the raster
    # tile set, resulting in the A, B, and C seen coming into this function,
    # which are in the new encoding.
    # A, B, C = np.ma.array([
    #     (A>=20000) * (2 * (A - (20000 + (A>=30000) * 10000)) + (A<30000) * 1),
    #     (B>0) * (2 * (C + 1461) + (B<2) * 1),
    #     (D>=20000) * (2 * (D - (20000 + (D>=30000) * 10000)) + (D<30000) * 1)
    # ], dtype=np.uint16)

    # Also at this point we have the new intensity bands
    # D = np.ma.array([50, 21, 0, 16], dtype=np.uint8)
    # E = np.ma.array([55, 34, 16, 20], dtype=np.uint8)
    # F = np.ma.array([0, 15, 0, 45], dtype=np.uint8)

    # This is how we want the final channels encoded
    # RED = "(DAY / 255)"
    # GREEN = "(DAY % 255)"
    # BLUE = "(((CONFIDENCE + 1) * 100) + INTENSITY)"
    # ALPHA = First 2 bits GLAD-L confidence, then 2 bits for GLAD-S2 confidence,
    #         then 2 bits for RADD confidence, then 2 unused bits.
    #         Confidences should be a value of 2 for high, 1 for low, 0
    #         for not detected.

    # But we've got three systems, and we want the minimum (first detection)
    # in any. np.minimum doesn't exclude masked values, so we have to replace
    # them with something bigger than our data... and then re-mask the
    # previously masked values afterwards. So unless I'm mistaken, just to
    # get the minimum of the three alert systems requires something like this:

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

    calc_str = f"np.ma.array([{red}, {green}, {blue}, {alpha}])"

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint8,
        band_count=4,
        no_data=[0, 0, 0, 0],
        resampling=ResamplingMethod.nearest,
        overwrite=False,
        grid=Grid(f"zoom_{zoom_level}"),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        source_uri=[date_conf_uri, intensity_uri],
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
    asset_prefix = asset_uri.rsplit("/", 1)[0]

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
        job_name=f"merge_intensity_and_date_conf_multi_8_zoom_{zoom_level}",
        callback=callback,
        parents=parents,
    )

    pixetl_job = scale_batch_job(pixetl_job, zoom_level)

    return (
        [pixetl_job],
        os.path.join(asset_prefix, "tiles.geojson"),
    )


async def _merge_intensity_and_date_conf_multi_16(
    dataset: str,
    version: str,
    pixel_meaning: str,
    date_conf_uri: str,
    intensity_uri: str,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[List[Job], str]:
    """Create RGB-encoded raster tile set based on date_conf and intensity
    raster tile sets."""

    # Change back to the encoding the frontend is expecting
    # As a reminder, that is as follows:
    # The leading integer of the decimal representation is 2 for a low-confidence
    # alert and 3 for a high-confidence alert, followed by the number of days
    # since December 31 2014.
    # 0 is the no-data value

    # GLAD-L date and confidence
    red = (
        "((A.filled(0) >> 1) > 0) * "
        "((A.filled(0) >> 1) + 20000 + (10000 * (A.filled(0) & 1 == 0)))"
    )
    # GLAD-S2 date and confidence
    green = (
        "((B.filled(0) >> 1) > 0) * "
        "((B.filled(0) >> 1) + 20000 + (10000 * (B.filled(0) & 1 == 0)))"
    )
    # RADD date and confidence
    blue = (
        "((C.filled(0) >> 1)> 0) * "
        "((C.filled(0) >> 1) + 20000 + (10000 * (C.filled(0) & 1 == 0)))"
    )
    # GLAD-L, GLAD-S2, and RADD intensities
    alpha = (
        "(D.astype(np.uint16).data << 11) | "
        "(E.astype(np.uint16).data << 6) | "
        "(F.astype(np.uint16).data << 1)"
    )

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint16,
        band_count=4,
        no_data=[0, 0, 0, 0],
        resampling=ResamplingMethod.nearest,
        overwrite=False,
        grid=Grid(f"zoom_{zoom_level}"),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        source_uri=[date_conf_uri, intensity_uri],
        calc=f"np.ma.array([{red}, {green}, {blue}, {alpha}])",
        photometric=PhotometricType.rgb,
    )

    asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        encoded_co.dict(by_alias=True),
        "epsg:3857",
    )
    asset_prefix = asset_uri.rsplit("/", 1)[0]

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
        job_name=f"merge_intensity_and_date_conf_multi_16_zoom_{zoom_level}",
        callback=callback,
        parents=parents,
    )

    pixetl_job = scale_batch_job(pixetl_job, zoom_level)

    return (
        [pixetl_job],
        os.path.join(asset_prefix, "tiles.geojson"),
    )


async def _merge_intensity_and_date_conf(
    dataset: str,
    version: str,
    pixel_meaning: str,
    date_conf_uri: str,
    intensity_uri: str,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[List[Job], str]:
    """Create RGB-encoded raster tile set based on date_conf and intensity
    raster tile sets."""

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint8,
        resampling=PIXETL_DEFAULT_RESAMPLING,
        overwrite=False,
        grid=Grid(f"zoom_{zoom_level}"),
        symbology=Symbology(type=ColorMapType.date_conf_intensity),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        source_uri=[date_conf_uri, intensity_uri],
        # TODO: GTC-1091
        #  Something similar to this should work with PixETL, we might need to make the an masked array
        #  [((A -((A>=30000) * 10000) - ((A>=20000) * 20000)) * (A>=20000)/255).astype('uint8'), ((A -((A>=30000) * 10000) - ((A>=20000) * 20000)) * (A>=20000) % 255).astype('uint8'), (((A>=30000) + 1)*100 + B).astype('uint8')]
        calc="This is a placeholder",  # this will not be used in PixETL but pydantic requires some input value
    )

    asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        encoded_co.dict(by_alias=True),
        "epsg:3857",
    )
    asset_prefix = asset_uri.rsplit("/", 1)[0]

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

    cmd = [
        "merge_intensity.sh",
        date_conf_uri,
        intensity_uri,
        asset_prefix,
    ]

    callback = callback_constructor(asset.asset_id)

    rgb_encoding_job = BuildRGBJob(
        dataset=dataset,
        job_name=f"merge_intensity_zoom_{zoom_level}",
        command=cmd,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents],
    )

    rgb_encoding_job = scale_batch_job(rgb_encoding_job, zoom_level)

    _prefix = split_s3_path(asset_prefix)[1].split("/")
    prefix = "/".join(_prefix[2:-1]) + "/"
    cmd = [
        "run_pixetl_prep.sh",
        "-s",
        asset_prefix,
        "-d",
        dataset,
        "-v",
        version,
        "--prefix",
        prefix,
    ]
    tiles_geojson_job = PixETLJob(
        dataset=dataset,
        job_name=f"generate_tiles_geojson_{zoom_level}",
        command=cmd,
        environment=JOB_ENV,
        callback=callback,
        parents=[rgb_encoding_job.job_name],
        vcpus=1,
        memory=2500,
    )

    return (
        [rgb_encoding_job, tiles_geojson_job],
        os.path.join(asset_prefix, "tiles.geojson"),
    )


async def _merge_intensity_and_year(
    dataset: str,
    version: str,
    pixel_meaning: str,
    year_uri: str,
    intensity_uri: str,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[List[Job], str]:
    """Create RGB encoded raster tile set based on date_conf and intensity
    raster tile sets."""

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint8,
        resampling=PIXETL_DEFAULT_RESAMPLING,
        overwrite=False,
        grid=Grid(f"zoom_{zoom_level}"),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        source_uri=[intensity_uri, year_uri],
        band_count=3,
        no_data=0,  # FIXME: Shouldn't this be [0, 0, 0]?
        photometric=PhotometricType.rgb,
        calc="np.ma.array([A, np.ma.zeros(A.shape, dtype='uint8'), B], fill_value=0).astype('uint8')",
    )

    asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        encoded_co.dict(by_alias=True),
        "epsg:3857",
    )
    asset_prefix = asset_uri.rsplit("/", 1)[0]

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
        job_name=f"merge_year_intensity_zoom_{zoom_level}",
        callback=callback,
        parents=parents,
    )

    pixetl_job = scale_batch_job(pixetl_job, zoom_level)

    return (
        [pixetl_job],
        os.path.join(asset_prefix, "tiles.geojson"),
    )


_symbology_constructor: Dict[str, SymbologyInfo] = {
    ColorMapType.date_conf_intensity: SymbologyInfo(
        8, 1, date_conf_intensity_symbology
    ),
    ColorMapType.date_conf_intensity_multi_8: SymbologyInfo(
        8, 3, date_conf_intensity_multi_8_symbology
    ),
    ColorMapType.date_conf_intensity_multi_16: SymbologyInfo(
        16, 3, date_conf_intensity_multi_16_symbology
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
