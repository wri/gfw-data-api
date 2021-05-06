# Vector source creation options
bucket = "my_bucket"
shp_name = "my_shape.zip"

VECTOR_SOURCE_CREATION_OPTIONS = {
    "source_driver": "ESRI Shapefile",
    "source_type": "vector",
    "source_uri": [f"s3://{bucket}/{shp_name}"],
    "layers": None,
    "indices": [
        {"column_names": ["geom"], "index_type": "gist"},
        {"column_names": ["geom_wm"], "index_type": "gist"},
        {"column_names": ["gfw_geostore_id"], "index_type": "hash"},
    ],
    "create_dynamic_vector_tile_cache": True,
    "add_to_geostore": True,
}
