from ....application import db

_fields_sql = """
SELECT
    column_name as name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END as data_type
  FROM information_schema.columns
  WHERE
    table_schema = :dataset AND table_name = :version;"""

fields = db.text(_fields_sql)
