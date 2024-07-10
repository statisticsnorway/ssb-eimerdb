APPLICATION_JSON = "application/json"

# query keys
TABLE_NAME_KEY = "table_name"
COLUMNS_KEY = "columns"
OPERATION_KEY = "operation"
SELECT_CLAUSE_KEY = "select_clause"
WHERE_CLAUSE_KEY = "where_clause"
SET_CLAUSE_KEY = "set_clause"

# table config keys
CREATED_BY_KEY = "created_by"
BUCKET_KEY = "bucket"
EDITABLE_KEY = "editable"
TABLE_PATH_KEY = "table_path"
PARTITION_COLUMNS_KEY = "partition_columns"
SCHEMA_KEY = "schema"

DUCKDB_DEFAULT_CONFIG = {"preserve_insertion_order": False}

ROW_ID_DEF = {"name": "row_id", "type": "string", "label": "Unique row ID"}
PANDAS_OUTPUT_FORMAT = "pandas"
ARROW_OUTPUT_FORMAT = "arrow"

CHANGES_ALL = "all"
CHANGES_RECENT = "recent"

SELECT_STAR_QUERY = "SELECT * FROM"

DEFAULT_COMPRESSION = "snappy"
DEFAULT_MIN_ROWS_PER_GROUP = 500_000
