"""A collection of useful functions.

The template and this example uses Google style docstrings as described at:
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Any
from typing import Optional

import pyarrow as pa
from dapla import AuthClient
from google.cloud import storage

from eimerdb.eimerdb_constants import APPLICATION_JSON
from eimerdb.eimerdb_constants import BUCKET_KEY
from eimerdb.eimerdb_constants import COLUMNS_KEY
from eimerdb.eimerdb_constants import CREATED_BY_KEY
from eimerdb.eimerdb_constants import OPERATION_KEY
from eimerdb.eimerdb_constants import SELECT_CLAUSE_KEY
from eimerdb.eimerdb_constants import SET_CLAUSE_KEY
from eimerdb.eimerdb_constants import TABLE_NAME_KEY
from eimerdb.eimerdb_constants import WHERE_CLAUSE_KEY

logger = logging.getLogger(__name__)


def filter_partition_select_on_table(
    table_name: str, partition_select: Optional[dict[str, Any]]
) -> Optional[dict[str, Any]]:
    """A function that gets the partition select for a table.

    Supports both:
        {
            "table1": {
                "col1": ["value1", "value2"]
            }
        }
        and:
        {
            "col1": ["value1", "value2"]
        }

    Args:
        table_name (str): The name of the table.
        partition_select (dict): The partition select.

    Returns:
        dict: The partition select for the table, or None.
    """
    if partition_select is None:
        return partition_select

    # if partition_select is a dictionary with table names as keys
    if all(isinstance(v, dict) for v in partition_select.values()):
        return partition_select.get(table_name)

    # if partition_select is a dictionary with partition column names as keys
    return partition_select


def get_datetime() -> str:
    """A function that returns a datetime string.

    Returns:
        datetime string.

    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def get_initials() -> str:
    """A function that returns user initials.

    Returns:
        The user's initials or "user" if the user is None.
    """
    user: str | None = AuthClient.fetch_email_from_credentials()
    if user is None:
        user = os.getenv("DAPLA_USER")
    if user is None:
        return "user"
    user_split: str = user.split("@")[0]
    return user_split


def get_json(bucket_name: str, blob_path: str) -> dict[str, Any]:
    """A function that retrieves a JSON file from Google Cloud Storage.

    Args:
        bucket_name (str): Name of the bucket.
        blob_path (str): Path to the blob.

    Returns:
        str: The JSON content.
    """
    token = AuthClient.fetch_google_credentials()
    client = storage.Client(credentials=token)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)

    json_content = blob.download_as_text()

    data: dict[str, Any] = json.loads(json_content)
    return data


def _get_timestamp_data_type(data_type: str) -> pa.TimestampType:
    """A function that returns the timestamp data type.

    Args:
        data_type (str): The timestamp data type.

    Returns:
        pa.TimestampType: The PyArrow timestamp data type.
    """
    unit_start = data_type.find("(") + 1
    unit_end = data_type.find(")")
    unit = data_type[unit_start:unit_end]
    return pa.timestamp(unit)


def arrow_schema_from_json(json_schema: list[dict[str, Any]]) -> pa.Schema:
    """A function that converts a JSON schema to an Arrow schema.

    Args:
        json_schema (list[dict]): A JSON schema with name, type, and label.

    Returns:
        pa.Schema: The PyArrow schema.
    """
    fields = []
    for field_dict in json_schema:
        name = field_dict["name"]
        data_type = field_dict["type"]
        label = field_dict["label"]

        if "timestamp" in data_type:
            field_type = _get_timestamp_data_type(data_type)
        elif "dictionary" in data_type:
            field_type = getattr(pa, data_type)(
                field_dict["indices"], field_dict["values"]
            )
        else:
            field_type = getattr(pa, data_type)()

        metadata = {"label": label}
        field = pa.field(name, field_type, metadata=metadata)
        fields.append(field)

    return pa.schema(fields)


select_pattern = re.compile(r"\bSELECT\b")
from_pattern = re.compile(r"\bFROM\s+(\w+)")
join_pattern = re.compile(r"JOIN\s+(\w+)\s+ON")
where_pattern = re.compile(
    r"WHERE\s+((?!GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|FETCH|UNION|INT"
    r"ERSECT|EXCEPT|INTO|TABLESAMPLE).*?)\s*(?:GROUP\s+BY|HAVING|ORDER\s+BY|"
    r"LIMIT|OFFSET|FETCH|UNION|INTERSECT|EXCEPT|INTO|TABLESAMPLE|$)",
)

update_pattern = re.compile(
    r"^UPDATE\s+(\w+)\s+SET\s+(.*?)\s+(?:WHERE\s+(.*))?$",
    re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE,
)

delete_pattern = re.compile(
    r"^DELETE\s+FROM\s+(\w+)\s+(?:WHERE\s+(.*))?$", re.IGNORECASE | re.DOTALL
)


def parse_sql_query(sql_query: str) -> dict[str, Any]:
    """A function that parses the provided SQL query.

    Args:
        sql_query (str): An SQL query.

    Returns:
        dict: A dictionary with keys: Operation, columns, table_name, and sql_filter.

    Raises:
        ValueError: If there is a syntax error or if the query is not supported.
    """
    stripped_query = " ".join(sql_query.split())
    select_match = select_pattern.search(sql_query)
    update_match = re.match(update_pattern, stripped_query)
    delete_match = re.match(delete_pattern, stripped_query)

    if delete_match:
        table_name, where_clause = delete_match.groups()

        return {
            OPERATION_KEY: "DELETE",
            TABLE_NAME_KEY: table_name,
            WHERE_CLAUSE_KEY: where_clause,
        }

    if update_match:
        table_name, set_clause, where_clause = update_match.groups()

        return {
            OPERATION_KEY: "UPDATE",
            TABLE_NAME_KEY: table_name,
            SET_CLAUSE_KEY: set_clause,
            WHERE_CLAUSE_KEY: where_clause,
        }

    if select_match:
        join_tables: list[str] = join_pattern.findall(sql_query)
        from_match: list[str] = from_pattern.findall(sql_query)
        where_match = where_pattern.search(sql_query)

        return {
            OPERATION_KEY: "SELECT",
            COLUMNS_KEY: ["*"],
            SELECT_CLAUSE_KEY: "",
            TABLE_NAME_KEY: sorted(list(set(from_match + join_tables))),
            WHERE_CLAUSE_KEY: where_match.group(1) if where_match else None,
        }

    raise ValueError("Error parsing sql-query. Syntax error or query not supported.")


def create_eimerdb(bucket_name: str, db_name: str) -> None:
    """Creates an EimerDB instance.

    Args:
        bucket_name: A GCP bucket.
        db_name: Name of the instance.
    """
    storage_client = storage.Client(credentials=AuthClient.fetch_google_credentials())
    bucket = storage_client.bucket(bucket_name)

    creator = get_initials()
    full_path = f"eimerdb/{db_name}"

    json_about = {
        "eimerdb_name": f"{db_name}",
        "path": f"gs://{bucket_name}/{full_path}",
        BUCKET_KEY: bucket_name,
        "eimer_path": full_path,
        CREATED_BY_KEY: creator,
        "time_created": get_datetime(),
    }

    about_blob = bucket.blob(f"{full_path}/config/about.json")
    about_blob.upload_from_string(
        data=json.dumps(json_about), content_type=APPLICATION_JSON
    )

    user_roles = {creator: "admin"}
    user_roles_blob = bucket.blob(f"{full_path}/config/users.json")
    user_roles_blob.upload_from_string(
        data=json.dumps(user_roles), content_type=APPLICATION_JSON
    )

    role_groups = {
        "role_groups": {
            "admin": {
                "operations": "all",
                "functions": "all",
                "tables": "all",
            },
            "editor": {
                "operations": [
                    "insert",
                    "delete",
                    "update",
                    "reset",
                ],
                "functions": "",
                "tables": "",
            },
        }
    }

    role_groups_blob = bucket.blob(f"{full_path}/config/role_groups.json")
    role_groups_blob.upload_from_string(
        data=json.dumps(role_groups),
        content_type=APPLICATION_JSON,
    )

    tables_blob = bucket.blob(f"{full_path}/config/tables.json")
    tables_blob.upload_from_string(data=json.dumps({}), content_type=APPLICATION_JSON)

    logger.info("EimerDB instance %s created.", db_name)
