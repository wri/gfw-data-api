import os
from collections import defaultdict
from typing import Any, Callable, Coroutine, DefaultDict, Dict, List, Optional, Tuple

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

SymbologyFuncType = Callable[
    [str, str, str, RasterTileSetSourceCreationOptions, int, int, Dict[Any, Any]],
    Coroutine[Any, Any, Tuple[List[Job], str]],
]


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
            "pixel_meaning": pixel_meaning,
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
    job = await create_gdaldem_job(
        dataset,
        version,
        creation_options,
        job_name,
        callback_constructor(symbology_asset_record.asset_id),
        parents=parents,
    )

    job = scale_batch_job(job, zoom_level)

    return [job], new_asset_uri


async def date_conf_intensity_multi_8_symbology(
    dataset: str,
    version: str,
    pixel_meaning: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Create Raster Tile Set asset which combines date_conf raster and
    intensity raster into one.

    At native resolution (max_zoom) it will create intensity raster
    based on given source. For lower zoom levels it will resample higher
    zoom level tiles using bilinear resampling method. Once intensity
    raster tile set is created it will combine it with source
    (date_conf) raster into RGB-encoded raster.
    """
    intensity_co = source_asset_co.copy(
        deep=True, update={"calc": None, "data_type": DataType.uint8, "band_count": 1}
    )
    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        intensity_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        # What we want is a value of 55 (max intensity) anywhere there is
        # an alert in any system. We can't just do
        # "((A > 0) | (B > 0) | (C > 0)) * 55" because "A | B" includes only
        # those values unmasked in both A and B. So first replace masked
        # values with 0 and then re-mask them later
        "np.ma.array(((A.filled(0) | B.filled(0) | C.filled(0)) > 0) * 55, mask=(A.mask & B.mask & C.mask))",
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
    """Create Raster Tile Set asset which combines date_conf raster and
    intensity raster into one.

    At native resolution (max_zoom) it will create intensity raster
    based on given source. For lower zoom levels it will resample higher
    zoom level tiles using bilinear resampling method. Once intensity
    raster tile set is created it will combine it with source
    (date_conf) raster into RGB-encoded raster.
    """
    intensity_co = source_asset_co.copy(deep=True, update={"data_type": DataType.uint8})
    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        intensity_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        "np.ma.array([(A>0)*31, (B>0)*31, (C>0)*31])",
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
    """Create Raster Tile Set asset which combines date_conf raster and
    intensity raster into one.

    At native resolution (max_zoom) it will create intensity raster
    based on given source. For lower zoom levels it will resample higher
    zoom level tiles using bilinear resampling method. Once intensity
    raster tile set is created it will combine it with source
    (date_conf) raster into RGB-encoded raster.
    """
    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        source_asset_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        "(A>0)*55",
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
    raster into RGB-encoded raster.
    """
    return await _date_intensity_symbology(
        dataset,
        version,
        pixel_meaning,
        source_asset_co,
        zoom_level,
        max_zoom,
        jobs_dict,
        "(A>0)*255",
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
    # A = B = C = D = np.ma.MaskedArray(np.zeros((3, 3)))
    # blah = [((A - ((A >= 30000) * 10000) - ((A >= 20000) * 20000)) * (A >= 20000)/255).astype('uint8'), ((A - ((A >= 30000) * 10000) - ((A >= 20000) * 20000)) * (A >= 20000) % 255).astype('uint8'), (((A >= 30000) + 1)*100 + B).astype('uint8')]
    # blah = [((np.minimum(A, np.minimum(B, C)) - ((np.minimum(A, np.minimum(B, C)) >= 30000) * 10000) - ((np.minimum(A, np.minimum(B, C)) >= 20000) * 20000)) * (np.minimum(A, np.minimum(B, C)) >= 20000)/255).astype('uint8'), ((np.minimum(A, np.minimum(B, C)) - ((np.minimum(A, np.minimum(B, C)) >= 30000) * 10000) - ((np.minimum(A, np.minimum(B, C)) >= 20000) * 20000)) * (np.minimum(A, np.minimum(B, C)) >= 20000) % 255).astype('uint8'), (((np.minimum(A, np.minimum(B, C)) >= 30000) + 1)*100 + D).astype('uint8'), ((((A>=30000)*2 + (A>0)*1) << 6) | (((B>=30000)*2 + (B>0)*1) << 4) | (((C>=30000)*2 + (C>0)*1) << 2))]
    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint8,
        band_count=4,
        no_data=[0, 0, 0, 0],
        resampling=PIXETL_DEFAULT_RESAMPLING,
        overwrite=False,
        grid=Grid(f"zoom_{zoom_level}"),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        symbology=Symbology(type=ColorMapType.date_conf_intensity_multi_8),
        source_uri=[date_conf_uri, intensity_uri],
        # We want the minimum of the arrays (alert systems) excluding
        # masked values (i.e. 0, which should be masked). np.minimum
        # doesn't exclude masked values, so replace them with something
        # bigger than our data... and then re-mask the previously masked
        # values afterwards. So unless I'm mistaken, just to get the minimum
        # of the three alert systems requires
        # np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask))
        # Oye. What a monster...
        calc="np.ma.array([((np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) - ((np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) >= 30000) * 10000) - ((np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) >= 20000) * 20000)) * (np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) >= 20000)/255).astype('uint8'), ((np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) - ((np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) >= 30000) * 10000) - ((np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) >= 20000) * 20000)) * np.ma.array((np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))) >= 20000), mask=(A.mask & B.mask & C.mask)) % 255).astype('uint8'), (((np.ma.array(np.minimum(A.filled(65535), np.minimum(B.filled(65535), C.filled(65535))), mask=(A.mask & B.mask & C.mask)) >= 30000) + 1)*100 + D).astype('uint8'), ((((A >= 30000)*2 + (A > 0)*1) << 6).filled(0) | (((B >= 30000)*2 + (B > 0)*1) << 4).filled(0) | (((C >= 30000)*2 + (C > 0)*1) << 2).filled(0)).astype('uint8')])",
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

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint16,
        band_count=4,
        no_data=[0, 0, 0, 0],
        resampling=PIXETL_DEFAULT_RESAMPLING,
        overwrite=False,
        grid=Grid(f"zoom_{zoom_level}"),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        symbology=Symbology(type=ColorMapType.date_conf_intensity_multi_16),
        source_uri=[date_conf_uri, intensity_uri],
        calc="np.ma.array([A, B, C, (D << 11) | (E << 6) | (F << 1)])",
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


_symbology_constructor: Dict[str, SymbologyFuncType] = {
    ColorMapType.date_conf_intensity: date_conf_intensity_symbology,
    ColorMapType.date_conf_intensity_multi_8: date_conf_intensity_multi_8_symbology,
    ColorMapType.date_conf_intensity_multi_16: date_conf_intensity_multi_16_symbology,
    ColorMapType.year_intensity: year_intensity_symbology,
    ColorMapType.gradient: colormap_symbology,
    ColorMapType.discrete: colormap_symbology,
}

symbology_constructor: DefaultDict[str, SymbologyFuncType] = defaultdict(
    lambda: no_symbology
)
symbology_constructor.update(**_symbology_constructor)
