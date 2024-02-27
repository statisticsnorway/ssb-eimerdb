"""
A collection of useful functions.

The template and this example uses Google style docstrings as described at:
https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
"""

import json
import os
import re
from datetime import datetime
import pyarrow as pa
from dapla import AuthClient
from google.cloud import storage
import logging

logger = logging.getLogger(__name__)


def get_datetime() -> str:
    """A function that returns a datetime string.

    Returns:
        datetime string.

    """
    datetime_now = datetime.now()
    datetime_str = datetime_now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return datetime_str


JUPYTERHUB_USER_ENV = "JUPYTERHUB_USER"


def get_initials() -> str:
    """A function that returns user initials.

    Returns:
        The users initials.

    """
    user = AuthClient.fetch_local_user_from_jupyter()["username"]
    return user.split("@")[0]


def get_json(bucket_name: str,
             blob_path: str) -> dict:
    """A function that gets a json file from Google Cloud Storage.

    Args:
        bucket_name: Name of bucket
        blob_path: Path to blob

    Returns:
        The users initials.

    """
    token = AuthClient.fetch_google_credentials()
    client = storage.Client(credentials=token)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)

    json_content = blob.download_as_text()

    data = json.loads(json_content)
    return data

def arrow_schema_from_json(json_schema: list) -> pa.Schema:
    """A function converts a json file to an arrow schema.

     Args:
        json_schema: A json schema with name, type and label

    Returns:
        Pyarrow schema.

    """
    fields = []
    for field_dict in json_schema:
        name = field_dict["name"]
        data_type = field_dict["type"]
        label = field_dict["label"]
        
        if 'timestamp' in data_type:
            unit_start = data_type.find("(") + 1
            unit_end = data_type.find(")")
            unit = data_type[unit_start:unit_end]
            field_type = getattr(pa, 'timestamp')(unit)
        else:
            field_type = getattr(pa, data_type)()
        
        metadata = {"label": label}
        field = pa.field(name, field_type, metadata=metadata)
        fields.append(field)

    return pa.schema(fields)

def parse_sql_query(sql_query: str) -> dict:
    """A function that parses the given sql query.

     Args:
        sql_query: An sql query.

    Returns:
        Dictionary with keys: Operation, columns, table_name and sql_filter.

    """

    select_pattern = re.compile(r"^SELECT\s+(.*?)\s+FROM", re.IGNORECASE)
    from_pattern = re.compile(r"FROM\s+(\w+)", re.IGNORECASE)
    join_pattern = re.compile(r"JOIN\s+(\w+)\s+ON", re.IGNORECASE)
    where_pattern = re.compile(
        r"WHERE\s+((?!(?:GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|FETCH|UNION|"
        r"INTERSECT|EXCEPT|INTO|TABLESAMPLE)).*?)\s*(?:GROUP\s+BY|HAVING|ORDER\s+BY|"
        r"LIMIT|OFFSET|FETCH|UNION|INTERSECT|EXCEPT|INTO|TABLESAMPLE|$)",
        re.IGNORECASE,
    )

    update_pattern = re.compile(
        r"""
        ^UPDATE\s+(\w+)\s+SET\s+(.*?)\s+(?:WHERE\s+(.*))?$
    """,
        re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE,
    )

    update_match = re.match(update_pattern, sql_query)

    select_clause = ""
    from_table = ""
    join_tables = []
    where_clause = ""

    select_match = select_pattern.search(sql_query)
    if select_match:
        select_clause = select_match.group(1).strip()

    from_match = from_pattern.search(sql_query)
    if from_match:
        from_table = from_match.group(1).strip()

    join_tables = join_pattern.findall(sql_query)

    where_match = where_pattern.search(sql_query)
    if where_match:
        where_clause = where_match.group(1).strip()

    if select_match:
        result = {
            "operation": "SELECT",
            "columns": ["*"],
            "select_clause": select_clause,
            "table_name": from_table,
            "join_tables": join_tables,
            "where_clause": where_clause,
        }

        return result

    elif update_match:
        groups = update_match.groups()
        table_name, set_clause, where_clause = groups

        return {
            "operation": "UPDATE",
            "table_name": table_name,
            "set_clause": set_clause,
            "where_clause": where_clause.strip() if where_clause else None,
        }
    else:
        raise ValueError("Error parsing sql-query. Syntax error or query not supported.")



def create_eimerdb(bucket_name: str, db_name: str) -> None:
    """Creates an EimerDB instance.

     Args:
        bucket_name: A GCP bucket.
        db_name: Name of the instance.

    Returns:
        success or failure

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
        "bucket": bucket_name,
        "eimer_path": full_path,
        "created_by": creator,
        "time_created": get_datetime(),
    }

    about_blob = bucket.blob(f"{full_path}/config/about.json")
    about_blob.upload_from_string(
        data=json.dumps(json_about), content_type="application/json"
    )

    user_roles = {creator: "admin"}
    user_roles_blob = bucket.blob(f"{full_path}/config/users.json")
    user_roles_blob.upload_from_string(
        data=json.dumps(user_roles), content_type="application/json"
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
        content_type="application/json",
    )

    tables = {}

    tables_blob = bucket.blob(f"{full_path}/config/tables.json")
    tables_blob.upload_from_string(
        data=json.dumps(tables), content_type="application/json"
    )
    logger.info("EimerDB instance %s created.", db_name)


def example_function(number1: int, number2: int) -> str:
    """Example function comparing two integers.

    This function can be deleted. It is used to show and test generating
    documentation from code, type hinting, testing, and testing examples
    in the code.


    Args:
        number1: The first number.
        number2: The second number, which will be compared to number1.

    Returns:
        A string describing which number is the greatest.

    Examples:
        Examples should be written in doctest format, and should illustrate how
        to use the function.

        >>> example_function(1, 2)
        1 is less than 2

    """
    if number1 < number2:
        return f"{number1} is less than {number2}"
    else:
        return f"{number1} is greater than or equal to {number2}"

