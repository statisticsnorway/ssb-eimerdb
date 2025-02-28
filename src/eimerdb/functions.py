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
from eimerdb.eimerdb_constants import BASE_PATH_KEY
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


def get_json(file_path: str) -> dict[str, Any]:
    """
    A function that retrieves a JSON file from a file path.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict[str, Any]: The JSON content as a dictionary.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

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


def create_eimerdb(base_path: str, db_name: str) -> None:
    """
    Creates an EimerDB instance in the local filesystem.

    Args:
        base_path (str): The root directory where the database will be created.
        db_name (str): The name of the instance.
    """
    full_path = os.path.join(base_path, "eimerdb", db_name)
    os.makedirs(os.path.join(full_path, "config"), exist_ok=True)

    creator = get_initials()

    json_about: Dict[str, str] = {
        "eimerdb_name": db_name,
        "path": os.path.abspath(full_path),
        BASE_PATH_KEY: base_path,
        "eimer_path": full_path,
        CREATED_BY_KEY: creator,
        "time_created": get_datetime(),
    }

    with open(os.path.join(full_path, "config", "about.json"), "w", encoding="utf-8") as f:
        json.dump(json_about, f, indent=4)

    user_roles = {creator: "admin"}
    with open(os.path.join(full_path, "config", "users.json"), "w", encoding="utf-8") as f:
        json.dump(user_roles, f, indent=4)

    role_groups = {
        "role_groups": {
            "admin": {
                "operations": "all",
                "functions": "all",
                "tables": "all",
            },
            "editor": {
                "operations": ["insert", "delete", "update", "reset"],
                "functions": "",
                "tables": "",
            },
        }
    }

    with open(os.path.join(full_path, "config", "role_groups.json"), "w", encoding="utf-8") as f:
        json.dump(role_groups, f, indent=4)

    with open(os.path.join(full_path, "config", "tables.json"), "w", encoding="utf-8") as f:
        json.dump({}, f, indent=4)

    logger.info("EimerDB instance %s created at %s.", db_name, full_path)
