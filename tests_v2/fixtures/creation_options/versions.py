# Vector source creation options
bucket = "my_bucket"
shp_name = "my_shape.zip"
tif_name = "tile.tif"

VECTOR_SOURCE_CREATION_OPTIONS = {
    "source_driver": "ESRI Shapefile",
    "source_type": "vector",
    "source_uri": [f"s3://{bucket}/{shp_name}"],
    "indices": [
        {"column_names": ["geom"], "index_type": "gist"},
        {"column_names": ["geom_wm"], "index_type": "gist"},
        {"column_names": ["gfw_geostore_id"], "index_type": "hash"},
    ],
    "create_dynamic_vector_tile_cache": True,
    "add_to_geostore": True,
}

RASTER_CREATION_OPTIONS = {
    "source_driver": "GeoTIFF",
    "source_type": "raster",
    "source_uri": [f"s3://{bucket}/{tif_name}"],
    "pixel_meaning": "year",
    "data_type": "uint16",
    "grid": "10/40000",
    "compute_stats": False,
}
