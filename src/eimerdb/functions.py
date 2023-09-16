"""A collection of useful functions.

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


def get_datetime():
    """A function that returns a datetime string.

    Returns:
        datetime string.

    """
    datetime_now = datetime.now()
    datetime_str = datetime_now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return datetime_str


def get_initials():
    """A function that returns a datetime string.

    Returns:
        The users initials.

    """
    user = os.environ.get("JUPYTERHUB_USER")
    initials = user.split("@")[0][:3]
    return initials


def get_json(bucket_name, blob_path):
    """A function that gets a json file from google cloud storage.

    Args:
        bucket_name: Name of bucket

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


def arrow_schema_from_json(json_schema: list):
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
        field_type = getattr(pa, data_type)()
        metadata = {"label": label}
        field = pa.field(name, field_type, metadata=metadata)
        fields.append(field)
    return pa.schema(fields)


def parse_sql_query(sql_query: str):
    """A function that parses the given sql query.

     Args:
        sql_query: An sql query.

    Returns:
        Dictionary with keys: Operation, columns, table_name and sql_filter.

    """
    select_pattern = r"^SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+(.*))?$"
    update_pattern = r"^UPDATE\s+(\w+)\s+SET\s+(.*?)\s+(?:WHERE\s+(.*))?$"

    select_match = re.match(select_pattern, sql_query, re.IGNORECASE)
    update_match = re.match(update_pattern, sql_query, re.IGNORECASE)

    if select_match:
        groups = select_match.groups()
        columns_str, table_name, rest_of_query = groups
        columns = [
            re.sub(
                r"^COUNT\((.*?)\)$",
                r"\1",
                col.strip().split(" AS ")[0],
            )
            for col in columns_str.split(",")
        ]

        return {
            "operation": "SELECT",
            "columns": columns,
            "table_name": table_name,
            "sql_filter": rest_of_query.strip() if rest_of_query else None,
        }
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
        raise ValueError(
            "Unsupported SQL operation. Only SELECT and UPDATE statements are allowed."
        )


def create_eimerdb(bucket_name: str, db_name: str):
    """Creates an EimerDB instance.

     Args:
        bucket_name: A google cloud storage bucket.
        db_name: Name of the instance.

    Returns:
        success or failure

    """
    creator = get_initials()
    token = AuthClient.fetch_google_credentials()
    client = storage.Client(credentials=token)
    bucket = client.bucket(bucket_name)
    full_path = f"eimerdb/{db_name}"
    about_blob = bucket.blob(f"{full_path}/config/about.json")
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
    print("EimerDB instance created.")


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
