environment = [
    {
        "name": "my_first_dataset__date_conf",
        "no_data": 0,
        "raster_table": None,
        "decode_expression": "",
        "encode_expression": "",
        "source_uri": "s3://gfw-data-lake-test/my_first_dataset/v1/raster/epsg-4326/10/40000/date_conf/geotiff/{tile_id}.tif",
        "grid": "10/40000",
        "tile_scheme": "nw",
    },
    {
        "name": "my_first_dataset__date",
        "no_data": 0,
        "raster_table": None,
        "decode_expression": "(A + 16435).astype('datetime64[D]').astype(str)",
        "encode_expression": "(datetime64(A) - 16435).astype(uint16)",
        "source_layer": "my_first_dataset__date_conf",
        "calc": "A % 10000",
    },
    {
        "name": "my_first_dataset__confidence",
        "no_data": 0,
        "raster_table": {
            "rows": [
                {"value": 2, "meaning": "nominal"},
                {"value": 3, "meaning": "high"},
                {"value": 4, "meaning": "highest"},
            ],
            "default_meaning": "not_detected",
        },
        "decode_expression": "",
        "encode_expression": "",
        "source_layer": "my_first_dataset__date_conf",
        "calc": "floor(A / 10000).astype(uint8)",
    },
]

sql = "select sum(area__ha) from data where is__umd_regional_primary_forest_2001 != 'false' and umd_tree_cover_density_2000__threshold >= 30 and umd_tree_cover_loss__year >= 2001 group by umd_tree_cover_loss__year"
