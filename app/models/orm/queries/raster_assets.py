data_environment_raster_tile_sets = """
    SELECT
      assets.asset_id,
      assets.dataset,
      assets.version,
      creation_options,
      asset_uri,
      rb.values_table
    FROM
      assets
      LEFT JOIN asset_metadata am
        ON am.asset_id = assets.asset_id
      JOIN versions
        ON versions.dataset = assets.dataset
        AND versions.version = assets.version
      LEFT JOIN raster_band_metadata rb
        ON rb.asset_metadata_id = am.id
      WHERE assets.asset_type = 'Raster tile set'
      AND assets.creation_options->>'pixel_meaning' NOT LIKE '%tcd%'
      AND assets.creation_options->>'grid' = :grid
    """
