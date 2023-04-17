from ....application import db

_dataset_sql = """
SELECT
  datasets.*,
  version_array AS versions,
  coalesce(metadata, '{}') as metadata
FROM
  datasets
  LEFT JOIN
    (
      SELECT
        dataset,
        ARRAY_AGG(version) AS version_array
      FROM
        versions
      GROUP BY
        dataset
    )
    t USING (dataset)
  LEFT JOIN
    (
      SELECT dataset, ROW_TO_JSON(dataset_metadata.*) as metadata
      FROM
        dataset_metadata
    )
    m USING (dataset)
    ORDER BY dataset
    LIMIT(:limit)
    OFFSET(:offset);"""

all_datasets = db.text(_dataset_sql)
