"""A collection of useful functions.

The template and this example uses Google style docstrings as described at:
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
"""

import json
import logging
import re
from datetime import datetime
from typing import Any

import pyarrow as pa
from dapla import AuthClient
from google.cloud import storage

from eimerdb.eimerdb_constants import APPLICATION_JSON
from eimerdb.eimerdb_constants import BUCKET_KEY
from eimerdb.eimerdb_constants import COLUMNS_KEY
from eimerdb.eimerdb_constants import CREATED_BY_KEY
from eimerdb.eimerdb_constants import OPERATION_KEY
from eimerdb.eimerdb_constants import SELECT_CLAUSE_KEY
from eimerdb.eimerdb_constants import TABLE_NAME_KEY
from eimerdb.eimerdb_constants import WHERE_CLAUSE_KEY

logger = logging.getLogger(__name__)


def get_datetime() -> str:
    """A function that returns a datetime string.

    Returns:
        datetime string.

    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def get_initials() -> str:
    """A function that returns user initials.

    Returns:
        The users initials.

    """
    try:
        user = AuthClient.fetch_local_user_from_jupyter()["username"]
        user_split: str = user.split("@")[0]
        return user_split
    except KeyError:
        return "user"


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
            unit_start = data_type.find("(") + 1
            unit_end = data_type.find(")")
            unit = data_type[unit_start:unit_end]
            field_type = pa.timestamp(unit)
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


def parse_sql_query(sql_query: str) -> dict[str, Any]:
    """A function that parses the provided SQL query.

    Args:
        sql_query (str): An SQL query.

    Returns:
        dict: A dictionary with keys: Operation, columns, table_name, and sql_filter.

    Raises:
        ValueError: If there is a syntax error or if the query is not supported.
    """
    select_pattern = re.compile(r"\bSELECT\b")
    from_pattern = re.compile(r"\bFROM\s+(\w+)")
    join_pattern = re.compile(r"JOIN\s+(\w+)\s+ON")
    where_pattern = re.compile(
        r"WHERE\s+((?!(?:GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|FETCH|UNION|"
        r"INTERSECT|EXCEPT|INTO|TABLESAMPLE)).*?)\s*(?:GROUP\s+BY|HAVING|ORDER\s+BY|"
        r"LIMIT|OFFSET|FETCH|UNION|INTERSECT|EXCEPT|INTO|TABLESAMPLE|$)",
    )

    update_pattern = re.compile(
        r"^UPDATE\s+(\w+)\s+SET\s+(.*?)\s+(?:WHERE\s+(.*))?$",
        re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE,
    )

    update_match = re.match(update_pattern, sql_query)

    delete_pattern = re.compile(
        r"^DELETE\s+FROM\s+(\w+)\s+(?:WHERE\s+(.*))?$", re.IGNORECASE | re.DOTALL
    )

    delete_match = re.match(delete_pattern, sql_query)

    select_clause = ""
    where_clause = ""

    select_match = select_pattern.search(sql_query)

    from_match: list[Any] = from_pattern.findall(sql_query)
    join_tables: list[Any] = join_pattern.findall(sql_query)

    tables = from_match + join_tables

    where_match = where_pattern.search(sql_query)

    if where_match:
        where_clause = where_match.group(1).strip()

    if delete_match:
        table_name, where_clause = delete_match.groups()

        return {
            OPERATION_KEY: "DELETE",
            TABLE_NAME_KEY: table_name,
            WHERE_CLAUSE_KEY: where_clause.strip() if where_clause else None,
        }

    if update_match:
        groups = update_match.groups()
        table_name, set_clause, where_clause = groups

        return {
            OPERATION_KEY: "UPDATE",
            TABLE_NAME_KEY: table_name,
            "set_clause": set_clause,
            WHERE_CLAUSE_KEY: where_clause.strip() if where_clause else None,
        }

    elif select_match:
        result = {
            OPERATION_KEY: "SELECT",
            COLUMNS_KEY: ["*"],
            SELECT_CLAUSE_KEY: select_clause,
            TABLE_NAME_KEY: tables,
            WHERE_CLAUSE_KEY: where_clause,
        }

        return result

    else:
        raise ValueError(
            "Error parsing sql-query. Syntax error or query not supported."
        )


def create_eimerdb(bucket_name: str, db_name: str) -> None:
    """Creates an EimerDB instance.

    Args:
        bucket_name: A GCP bucket.
        db_name: Name of the instance.
    """
    creator = get_initials()
    token = AuthClient.fetch_google_credentials()
    client = storage.Client(credentials=token)
    bucket = client.bucket(bucket_name)
    full_path = f"eimerdb/{db_name}"
    parts = db_name.split("/")
    name = parts[-1]
    json_about = {
        "eimerdb_name": f"{name}",
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
