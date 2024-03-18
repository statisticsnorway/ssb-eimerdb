"""EimerDB Instance Module.

This module contains the EimerDBInstance class, which represents an instance
of the EimerDB database. It provides methods to interact with EimerDB,
including managing users, creating tables, inserting data,
and querying data.

Author: Stian Elisenberg
Date: September 16, 2023
"""

import json
import logging
from typing import Any
from typing import Optional
from typing import Union
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from dapla import AuthClient
from dapla import FileClient
from google.cloud import storage
from pyarrow import Table

from .abstract_db_instance import AbstractDbInstance
from .eimerdb_constants import APPLICATION_JSON
from .eimerdb_constants import ARROW_OUTPUT_FORMAT
from .eimerdb_constants import BUCKET_KEY
from .eimerdb_constants import CHANGES_ALL
from .eimerdb_constants import CHANGES_RECENT
from .eimerdb_constants import CREATED_BY_KEY
from .eimerdb_constants import EDITABLE_KEY
from .eimerdb_constants import OPERATION_KEY
from .eimerdb_constants import PANDAS_OUTPUT_FORMAT
from .eimerdb_constants import PARTITION_COLUMNS_KEY
from .eimerdb_constants import ROW_ID_DEF
from .eimerdb_constants import SCHEMA_KEY
from .eimerdb_constants import TABLE_PATH_KEY
from .functions import arrow_schema_from_json
from .functions import get_datetime
from .functions import get_initials
from .functions import get_json
from .functions import parse_sql_query
from .instance_query_worker import QueryWorker

logger = logging.getLogger(__name__)


class DbOperation(str):
    """Represents a database operation."""

    SELECT_QUERY_OPERATION = "SELECT"
    UPDATE_QUERY_OPERATION = "UPDATE"
    DELETE_QUERY_OPERATION = "DELETE"


class EimerDBInstance(AbstractDbInstance):
    """Represents an instance of the EimerDB database.

    This class provides methods to interact with EimerDB, including
    managing users, creating tables, inserting data, and querying data.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket
            where the EimerDB database is hosted.
        eimer_name (str): The name of the EimerDB instance.

    Attributes:
        bucket_name (str): The name of the Google Cloud Storage bucket.
        eimerdb_name (str): The name of the EimerDB instance.
        path (str): The path to the EimerDB configuration.
        eimer_path (str): The path to the EimerDB instance.
        created_by (str): The name of the user who created the EimerDB instance.
        time_created (str): The timestamp when the EimerDB instance
            was created.
        users (dict): A dictionary of EimerDB users and their roles (admin, user).
        role_groups (dict): A dictionary of role groups and their members.
        is_admin (bool): Indicates whether the current user is an admin.

    Methods:
        add_user(username, role): Add a user to EimerDB with a specified role.
        remove_user(username): Remove a user from EimerDB.
        create_table(table_name, schema, partition_columns=None,
            editable=True): Create a new table in EimerDB.
        main_table_insert(table_name, df): Insert data into the main table
            of a specified schema.
        query(sql_query, partition_select=None, unedited=False): Execute an SQL
            query on a table in EimerDB.
    """

    def __init__(self, bucket_name: str, eimer_name: str) -> None:
        """Initialize EimerDBInstance.

        Args:
            bucket_name (str): Name of GCS bucket.
            eimer_name (str): Name of EimerDB instance.

        Attributes:
            bucket_name (str): GCS bucket name.
            eimer_name (str): EimerDB instance name.
        """
        about_path = f"eimerdb/{eimer_name}/config/about.json"
        json_about = get_json(bucket_name, about_path)

        eimer_path = json_about["eimer_path"]
        users: dict[str, Any] = get_json(bucket_name, f"{eimer_path}/config/users.json")

        initials = get_initials()
        if initials in users and users[initials] == "admin":
            role_groups: Optional[dict[str, Any]] = get_json(
                bucket_name, f"{eimer_path}/config/role_groups.json"
            )
            is_admin: bool = True
        else:
            users = {initials: ""}
            role_groups = None
            is_admin = False

        super().__init__(
            bucket_name=bucket_name,
            eimerdb_name=json_about["eimerdb_name"],
            path=json_about["path"],
            eimer_path=eimer_path,
            created_by=json_about[CREATED_BY_KEY],
            time_created=json_about["time_created"],
            tables=get_json(bucket_name, f"{eimer_path}/config/tables.json"),
            users=users,
            role_groups=role_groups,
            is_admin=is_admin,
        )

        self.query_worker = QueryWorker(self)

    def add_user(self, username: str, role: Any) -> None:
        """Add a user with a specified role.

        Args:
            username (str): Name of the user to add.
            role (Any): Role to assign (admin or user).

        Raises:
            PermissionError: If the user is not an admin.
            ValueError: If the user already exists.
        """
        if self.is_admin is not True:
            raise PermissionError("Cannot add user. You are not an admin!")

        if username in self.users:
            raise ValueError(f"User {username} already exists!")

        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self.bucket_name)

        self.users.update({username: role})
        user_roles_blob = bucket.blob(f"{self.eimer_path}/config/users.json")

        user_roles_blob.upload_from_string(
            data=json.dumps(self.users), content_type=APPLICATION_JSON
        )
        print(f"User {username} added with the role {role}!")

    def remove_user(self, username: str) -> None:
        """Remove a users access to the database.

        Args:
            username (str): Name of the user to remove.

        Raises:
            PermissionError: If the user is not an admin.
            ValueError: If the user does not exist.
        """
        if self.is_admin is not True:
            raise PermissionError("Cannot remove user. You are not an admin!")

        if username not in self.users:
            raise ValueError(f"User {username} does not exist.")

        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self.bucket_name)

        del self.users[username]
        user_roles_blob = bucket.blob(f"{self.eimer_path}/config/users.json")
        user_roles_blob.upload_from_string(
            data=json.dumps(self.users), content_type=APPLICATION_JSON
        )
        print(f"User {username} successfully removed!")

    def create_table(
        self,
        table_name: str,
        schema: list[dict[str, Any]],
        partition_columns: Optional[list[str]] = None,
        editable: Optional[bool] = True,
    ) -> None:
        """Create a new table in EimerDB.

        Args:
            table_name (str): Name of the new table.
            schema (str): JSON schema for the table.
            partition_columns (list, optional): List of partition columns.
            editable (bool, optional): Indicates if the table is editable.

        Raises:
            PermissionError: If the current user is not an admin.
        """
        if self.is_admin is not True:
            raise PermissionError("Cannot create table. You are not an admin!")

        schema.insert(0, ROW_ID_DEF)

        new_table = {
            table_name: {
                CREATED_BY_KEY: get_initials(),
                TABLE_PATH_KEY: f"{self.eimer_path}/{table_name}",
                BUCKET_KEY: self.bucket_name,
                EDITABLE_KEY: editable,
                SCHEMA_KEY: schema,
                PARTITION_COLUMNS_KEY: partition_columns,
            }
        }
        self.tables.update(new_table)

        token = AuthClient.fetch_google_credentials()
        bucket = storage.Client(credentials=token).bucket(self.bucket_name)

        tables_blob = bucket.blob(f"{self.eimer_path}/config/tables.json")
        tables_blob.upload_from_string(
            data=json.dumps(self.tables), content_type=APPLICATION_JSON
        )

    def insert(self, table_name: str, df: pd.DataFrame) -> None:
        """Insert unedited data into a main table.

        Args:
            table_name (str): Name of the table to insert data into.
            df (pandas.DataFrame): DataFrame containing the data to insert

        Raises:
            PermissionError: If the current user is not an admin.
        """
        if self.is_admin is not True:
            raise PermissionError(
                "Cannot insert into main table. You are not an admin!"
            )

        df["row_id"] = df.apply(lambda row: str(uuid4()), axis=1)

        arrow_schema = self.get_arrow_schema(table_name, False)
        table = pa.Table.from_pandas(df, schema=arrow_schema)

        df_raw = df.copy()
        df_raw["user"] = get_initials()
        df_raw["datetime"] = get_datetime()
        df_raw[OPERATION_KEY] = "insert"

        table_raw = pa.Table.from_pandas(
            df_raw, schema=self.get_arrow_schema(table_name, True)
        )
        timestamp_column = table_raw["datetime"].cast(pa.timestamp("ns"))

        table_raw = table_raw.drop(["datetime"])

        table_raw = table_raw.add_column(
            len(table_raw.column_names),
            pa.field("datetime", pa.timestamp("ns")),
            timestamp_column,
        )

        insert_id = uuid4()
        json_data = self.tables[table_name]
        table_path = json_data[TABLE_PATH_KEY]
        partitions = json_data[PARTITION_COLUMNS_KEY]
        filename = f"insert_{insert_id}_{{i}}.parquet"

        fs = FileClient.get_gcs_file_system()

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=table,
            root_path=f"gs://{self.bucket_name}/{table_path}",
            partition_cols=partitions,
            basename_template=filename,
            filesystem=fs,
            schema=arrow_schema,
        )
        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=table_raw,
            root_path=f"gs://{self.bucket_name}/{table_path}_raw",
            partition_cols=partitions,
            basename_template=filename,
            filesystem=fs,
        )
        print("Data successfully inserted!")

    def get_changes(self, table_name: str) -> Table:
        """Retrieve changes for a given table.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.

        Returns:
            Table: A pyarrow table containing the changes for the specified table.
        """
        path = self.tables[table_name][TABLE_PATH_KEY]

        fs = FileClient.get_gcs_file_system()
        # noinspection PyTypeChecker
        dataset = ds.dataset(
            f"{self.bucket_name}/{path}_changes/",
            format="parquet",
            partitioning="hive",
            schema=self.get_arrow_schema(table_name, True),
            filesystem=fs,
        )

        df_changes: Table = dataset.to_table()
        return df_changes

    def get_inserts(self, table_name: str, raw: bool) -> pa.Table:
        """Retrieve changes for a given table.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.
            raw (bool): Indicates whether to retrieve the raw schema.

        Returns:
            DataFrame: A pandas DataFrame containing the changes for the specified table.
        """
        if raw is True:
            suffix = "_raw"
        else:
            suffix = ""

        path = self.tables[table_name][TABLE_PATH_KEY] + suffix

        fs = FileClient.get_gcs_file_system()
        # noinspection PyTypeChecker
        dataset = ds.dataset(
            f"{self.bucket_name}/{path}/",
            format="parquet",
            partitioning="hive",
            schema=self.get_arrow_schema(table_name, raw),
            filesystem=fs,
        )

        df_inserts: pa.Table = dataset.to_table()
        return df_inserts

    def combine_changes(self, table_name: str) -> None:
        """Combines the files containing the changes of the table into one file.

        Args:
            table_name (str): The name of the table for which changes are to be merged.
        """
        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self.bucket_name)

        partitions = self.tables[table_name][PARTITION_COLUMNS_KEY]
        source_folder = self.tables[table_name][TABLE_PATH_KEY] + "_changes"
        blobs_to_delete = list(bucket.list_blobs(prefix=source_folder))

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=self.get_changes(table_name),
            root_path=f"gs://{self.bucket_name}/{source_folder}",
            partition_cols=partitions,
            basename_template=f"merged_commit_{uuid4()}_{{i}}.parquet",
            schema=self.get_arrow_schema(table_name, True),
            filesystem=FileClient.get_gcs_file_system(),
        )
        for blob in blobs_to_delete:
            blob.delete()

        print("The changes were successfully merged into one file per partition!")

    def combine_inserts(self, table_name: str, raw: bool) -> None:
        """Combines the files containing the inserts of the table into one file.

        Args:
            table_name (str): The name of the table.
            raw (bool): Indicates whether to retrieve the raw schema.
        """
        if raw is True:
            suffix = "/_raw"
        else:
            suffix = ""

        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self.bucket_name)

        partitions = self.tables[table_name][PARTITION_COLUMNS_KEY]
        source_folder = self.tables[table_name][TABLE_PATH_KEY] + suffix
        blobs_to_delete = list(bucket.list_blobs(prefix=source_folder))

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=self.get_inserts(table_name, raw),
            root_path=f"gs://{self.bucket_name}/{source_folder}",
            partition_cols=partitions,
            basename_template=f"merged_commit_{uuid4()}_{{i}}.parquet",
            schema=self.get_arrow_schema(table_name, raw),
            filesystem=FileClient.get_gcs_file_system(),
        )
        for blob in blobs_to_delete:
            blob.delete()

        print("The inserts were successfully merged into one file per partition!")

    def get_arrow_schema(
        self,
        table_name: str,
        raw: bool,
    ) -> pa.Schema:
        """Get the arrow schema for a specified table.

        Args:
            table_name (str): The name of the table.
            raw (bool): Indicates whether to retrieve the raw schema.

        Returns:
            pa.Schema: The arrow schema for the specified table.
        """
        json_data = self.tables[table_name]
        arrow_schema = arrow_schema_from_json(json_data[SCHEMA_KEY])

        if raw is True:
            arrow_schema = arrow_schema.append(pa.field("user", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("datetime", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("operation", pa.string()))

        return arrow_schema

    def query(
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
    ) -> Union[pd.DataFrame, pa.Table, str]:
        """Execute an SQL query on an EimerDB table.

        Args:
            sql_query (str): SQL query to execute.
            partition_select (dict, optional): Dictionary specifying partition filters.
            unedited (bool): Indicates whether to include unedited data.
            output_format (str): Desired output format ('pandas' or 'arrow').

        Returns:
            pandas.DataFrame, pyarrow.Table, str: The result of the SQL query.

        Raises:
            ValueError: If the output format is invalid, table is not editable, or invalid query.
        """
        if output_format not in [PANDAS_OUTPUT_FORMAT, ARROW_OUTPUT_FORMAT]:
            raise ValueError(
                f"Invalid output format: {output_format}. Supported formats: pandas, arrow."
            )

        parsed_query: dict[str, Any] = parse_sql_query(sql_query)
        query_operation = parsed_query[OPERATION_KEY]
        fs = FileClient.get_gcs_file_system()

        match query_operation:
            case DbOperation.SELECT_QUERY_OPERATION:
                return self.query_worker.query_select(
                    parsed_query=parsed_query,
                    sql_query=sql_query,
                    partition_select=partition_select,
                    unedited=unedited,
                    output_format=output_format,
                    fs=fs,
                )
            case DbOperation.UPDATE_QUERY_OPERATION:
                return self.query_worker.query_update_or_delete(
                    parsed_query=parsed_query,
                    update_sql_query=sql_query,
                    partition_select=partition_select,
                    fs=fs,
                )
            case DbOperation.DELETE_QUERY_OPERATION:
                return self.query_worker.query_update_or_delete(
                    parsed_query=parsed_query,
                    update_sql_query=None,
                    partition_select=partition_select,
                    fs=fs,
                )
            case _:
                raise ValueError(f"Unsupported SQL operation: {query_operation}.")

    def query_changes(
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
        changes_output: str = CHANGES_ALL,
    ) -> Optional[Union[pd.DataFrame, pa.Table]]:
        """Query changes made in the database table.

        Args:
            sql_query (str): The SQL query to execute.
            partition_select (Dict, optional):
                Dictionary containing partition selection criteria. Defaults to None.
            unedited (bool):
                Flag indicating whether to retrieve unedited changes. Defaults to False.
            output_format (str):
                The desired output format ('pandas' or 'arrow'). Defaults to 'pandas'.
            changes_output (str):
                The changes that are to be retrieved ('recent' or 'all'). Defaults to 'all'.

        Returns:
            Optional[pd.DataFrame, pa.Table]:
                Returns a pandas DataFrame if 'pandas' output format is specified,
                an arrow Table if 'arrow' output format is specified,
                or None if operation is different from SELECT.

        Raises:
            ValueError: If the output format is invalid.
        """
        if output_format not in (PANDAS_OUTPUT_FORMAT, ARROW_OUTPUT_FORMAT):
            raise ValueError(f"Invalid output format: {output_format}")

        # Validate changes_output
        if changes_output not in (CHANGES_ALL, CHANGES_RECENT):
            raise ValueError(f"Invalid changes output: {changes_output}")

        parsed_query = parse_sql_query(sql_query)
        # Check if the operation is SELECT
        if parsed_query[OPERATION_KEY] != "SELECT":
            raise ValueError(
                f"Operation {parsed_query[OPERATION_KEY]} is not supported."
            )

        return self.query_worker.query_changes(
            sql_query=sql_query,
            partition_select=partition_select,
            unedited=unedited,
            output_format=output_format,
            changes_output=changes_output,
        )
