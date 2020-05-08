from ....application import db

_fields_sql = """
SELECT
    column_name as field_name, udt_name as field_type
  FROM information_schema.columns
  WHERE
    table_name = :dataset AND table_schema = :version;"""

fields = db.text(_fields_sql)
