from ....application import db

_dataset_sql = """
SELECT
  datasets.*,
  version_array AS versions
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
    t USING (dataset);"""

all_datasets = db.text(_dataset_sql)
