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

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from dapla import AuthClient
from dapla import FileClient
from gcsfs import GCSFileSystem
from google.cloud import storage
from pyarrow import Table

from .eimerdb_constants import APPLICATION_JSON
from .eimerdb_constants import BUCKET_KEY
from .eimerdb_constants import COLUMNS_KEY
from .eimerdb_constants import CREATED_BY_KEY
from .eimerdb_constants import EDITABLE_KEY
from .eimerdb_constants import OPERATION_KEY
from .eimerdb_constants import PARTITION_COLUMNS_KEY
from .eimerdb_constants import SCHEMA_KEY
from .eimerdb_constants import TABLE_NAME_KEY
from .eimerdb_constants import TABLE_PATH_KEY
from .eimerdb_constants import WHERE_CLAUSE_KEY
from .functions import arrow_schema_from_json
from .functions import get_datetime
from .functions import get_initials
from .functions import get_json
from .functions import parse_sql_query
from .query import filter_partitions
from .query import get_partitioned_files
from .query import update_pyarrow_table

logger = logging.getLogger(__name__)

ROW_ID_DEF = {"name": "row_id", "type": "string", "label": "Unique row ID"}
PANDAS_OUTPUT_FORMAT = "pandas"
ARROW_OUTPUT_FORMAT = "arrow"

CHANGES_ALL = "all"
CHANGES_RECENT = "recent"

SELECT_STAR_QUERY = "SELECT * FROM"


class DbOperation(str):
    """Represents a database operation."""

    SELECT_QUERY_OPERATION = "SELECT"
    UPDATE_QUERY_OPERATION = "UPDATE"
    DELETE_QUERY_OPERATION = "DELETE"


class EimerDBInstance:
    """Represents an instance of the EimerDB database.

    This class provides methods to interact with EimerDB, including
    managing users, creating tables, inserting data, and querying data.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket
            where the EimerDB database is hosted.
        eimer_name (str): The name of the EimerDB instance.

    Attributes:
        bucket (str): The name of the Google Cloud Storage bucket.
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
        self.bucket = bucket_name

        about_path = f"eimerdb/{eimer_name}/config/about.json"
        json_about = get_json(bucket_name, about_path)

        self.eimerdb_name = json_about["eimerdb_name"]
        self.path = json_about["path"]
        self.eimer_path = json_about["eimer_path"]
        self.created_by = json_about[CREATED_BY_KEY]
        self.time_created = json_about["time_created"]

        initials = get_initials()

        users_path = f"{self.eimer_path}/config/users.json"
        users = get_json(bucket_name, users_path)

        role_groups_path = f"{self.eimer_path}/config/role_groups.json"
        role_groups = get_json(bucket_name, role_groups_path)

        tables_path = f"{self.eimer_path}/config/tables.json"
        tables = get_json(bucket_name, tables_path)

        self.tables = tables

        self.users: dict[str, Any] = {initials: ""}
        self.role_groups: Optional[dict[str, Any]] = None
        self.is_admin: bool = False

        if initials in users and users[initials] == "admin":
            self.users = users
            self.role_groups = role_groups
            self.is_admin = True

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
        bucket = client.bucket(self.bucket)

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
        bucket = client.bucket(self.bucket)

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
                BUCKET_KEY: self.bucket,
                EDITABLE_KEY: editable,
                SCHEMA_KEY: schema,
                PARTITION_COLUMNS_KEY: partition_columns,
            }
        }
        self.tables.update(new_table)

        token = AuthClient.fetch_google_credentials()
        bucket = storage.Client(credentials=token).bucket(self.bucket)

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

        arrow_schema = self._get_arrow_schema(table_name, False)
        table = pa.Table.from_pandas(df, schema=arrow_schema)

        df_raw = df.copy()
        df_raw["user"] = get_initials()
        df_raw["datetime"] = get_datetime()
        df_raw[OPERATION_KEY] = "insert"

        table_raw = pa.Table.from_pandas(
            df_raw, schema=self._get_arrow_schema(table_name, True)
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
            root_path=f"gs://{self.bucket}/{table_path}",
            partition_cols=partitions,
            basename_template=filename,
            filesystem=fs,
            schema=arrow_schema,
        )
        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=table_raw,
            root_path=f"gs://{self.bucket}/{table_path}_raw",
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
            f"{self.bucket}/{path}_changes/",
            format="parquet",
            partitioning="hive",
            schema=self._get_arrow_schema(table_name, True),
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
            f"{self.bucket}/{path}/",
            format="parquet",
            partitioning="hive",
            schema=self._get_arrow_schema(table_name, raw),
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
        bucket = client.bucket(self.bucket)

        partitions = self.tables[table_name][PARTITION_COLUMNS_KEY]
        source_folder = self.tables[table_name][TABLE_PATH_KEY] + "_changes"
        blobs_to_delete = list(bucket.list_blobs(prefix=source_folder))

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=self.get_changes(table_name),
            root_path=f"gs://{self.bucket}/{source_folder}",
            partition_cols=partitions,
            basename_template=f"merged_commit_{uuid4()}_{{i}}.parquet",
            schema=self._get_arrow_schema(table_name, True),
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
        bucket = client.bucket(self.bucket)

        partitions = self.tables[table_name][PARTITION_COLUMNS_KEY]
        source_folder = self.tables[table_name][TABLE_PATH_KEY] + suffix
        blobs_to_delete = list(bucket.list_blobs(prefix=source_folder))

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=self.get_inserts(table_name, raw),
            root_path=f"gs://{self.bucket}/{source_folder}",
            partition_cols=partitions,
            basename_template=f"merged_commit_{uuid4()}_{{i}}.parquet",
            schema=self._get_arrow_schema(table_name, raw),
            filesystem=FileClient.get_gcs_file_system(),
        )
        for blob in blobs_to_delete:
            blob.delete()

        print("The inserts were successfully merged into one file per partition!")

    def _get_arrow_schema(
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

    def _query_select(
        self,
        fs: GCSFileSystem,
        parsed_query: dict[str, Any],
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
    ) -> Union[pd.DataFrame, pa.Table]:
        con = duckdb.connect()
        tables = parsed_query[TABLE_NAME_KEY]

        for table_name in tables:
            table_config = self.tables[table_name]

            table_files = get_partitioned_files(
                table_name=table_name,
                instance_name=self.eimerdb_name,
                table_config=table_config,
                suffix="_raw",
                fs=fs,
                partition_select=partition_select,
                unedited=unedited,
            )

            # noinspection PyTypeChecker
            df = pq.read_table(table_files, filesystem=fs)
            df_changes = None
            editable = table_config[EDITABLE_KEY]

            if editable is True and unedited is False:
                select_query = SELECT_STAR_QUERY
                df_changes = self.query_changes(
                    f"{select_query} {table_name}",
                    partition_select,
                    output_format=ARROW_OUTPUT_FORMAT,
                    changes_output=CHANGES_RECENT,
                )

            if editable is True and unedited is False and df_changes is not None:
                if df_changes is not None and df_changes.num_rows != 0:
                    df = update_pyarrow_table(df, df_changes)

            con.register(table_name, df)
            del df

        if output_format == PANDAS_OUTPUT_FORMAT:
            return con.execute(sql_query).df()
        else:
            return con.execute(sql_query).arrow()

    def _query_update(
        self,
        fs: GCSFileSystem,
        parsed_query: dict[str, Any],
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
    ) -> str:
        table_name = parsed_query[TABLE_NAME_KEY]
        where_clause = parsed_query[WHERE_CLAUSE_KEY]

        table_config = self.tables[table_name]
        partitions = table_config[PARTITION_COLUMNS_KEY]

        if table_config[EDITABLE_KEY] is not True:
            raise ValueError(f"The table {table_name} is not editable!")

        df_update_results: pd.DataFrame = self.query(
            f"{SELECT_STAR_QUERY} {table_name} WHERE {where_clause}",
            partition_select,
        )

        df_update_results["user"] = get_initials()
        df_update_results["datetime"] = get_datetime()
        df_update_results["operation"] = "update"

        arrow_schema = self._get_arrow_schema(table_name, True)

        dataset = pa.Table.from_pandas(df_update_results, schema=arrow_schema)
        con = duckdb.connect()
        con.register("dataset", dataset)
        con.execute(f"CREATE TABLE updates AS FROM dataset WHERE {where_clause}")

        sql_query = sql_query.replace(f"UPDATE {table_name}", "UPDATE updates")
        con.execute(sql_query)
        df_updates_commits = con.table("updates").df()

        table_path = self.tables[table_name][TABLE_PATH_KEY] + "_changes"
        update_table = pa.Table.from_pandas(df_updates_commits, schema=arrow_schema)

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=update_table,
            root_path=f"gs://{self.bucket}/{table_path}",
            partition_cols=partitions,
            basename_template=f"commit_{uuid4()}_{{i}}.parquet",
            schema=self._get_arrow_schema(table_name, True),
            filesystem=fs,
        )
        return f"{df_updates_commits.shape[0]} rows updated by {get_initials()}"

    def _query_delete(
        self,
        fs: GCSFileSystem,
        parsed_query: dict[str, Any],
        partition_select: Optional[dict[str, Any]] = None,
    ) -> str:
        table_name = parsed_query[TABLE_NAME_KEY]
        where_clause = parsed_query[WHERE_CLAUSE_KEY]

        table_config = self.tables[table_name]

        if table_config[EDITABLE_KEY] is not True:
            raise ValueError(f"The table {table_name} is not editable!")

        partitions = table_config[PARTITION_COLUMNS_KEY]

        df_delete_results: pd.DataFrame = self.query(
            sql_query=f"{SELECT_STAR_QUERY} {table_name} WHERE {where_clause}",
            partition_select=partition_select,
        )

        df_delete_results["user"] = get_initials()
        df_delete_results["datetime"] = get_datetime()
        df_delete_results["operation"] = "delete"

        arrow_schema = self._get_arrow_schema(table_name, True)

        dataset = pa.Table.from_pandas(df_delete_results, schema=arrow_schema)
        con = duckdb.connect()
        con.register("dataset", dataset)
        con.execute(f"CREATE TABLE deletes AS FROM dataset WHERE {where_clause}")

        df_deletions = con.table("deletes").df()
        table_path = table_config[TABLE_PATH_KEY] + "_changes"
        deletion_table = pa.Table.from_pandas(df_deletions, schema=arrow_schema)

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=deletion_table,
            root_path=f"gs://{self.bucket}/{table_path}",
            partition_cols=partitions,
            basename_template=f"commit_{uuid4()}_{{i}}.parquet",
            filesystem=fs,
        )
        return f"{df_deletions.shape[0]} rows deleted by {get_initials()}"

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
                return self._query_select(
                    fs=fs,
                    parsed_query=parsed_query,
                    sql_query=sql_query,
                    partition_select=partition_select,
                    unedited=unedited,
                    output_format=output_format,
                )
            case DbOperation.UPDATE_QUERY_OPERATION:
                return self._query_update(
                    fs=fs,
                    parsed_query=parsed_query,
                    sql_query=sql_query,
                    partition_select=partition_select,
                )
            case DbOperation.DELETE_QUERY_OPERATION:
                return self._query_delete(
                    fs=fs,
                    parsed_query=parsed_query,
                    partition_select=partition_select,
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

        table_name = parsed_query[TABLE_NAME_KEY][0]
        table_config = self.tables[table_name]

        def get_partition_levels() -> str:
            partitions = table_config[PARTITION_COLUMNS_KEY]
            partitions_len = len(partitions) if partitions is not None else 0
            return "**/" * partitions_len + "*"

        def get_columns(_local_changes_output: str) -> Optional[list[str]]:
            if _local_changes_output == CHANGES_RECENT:
                return None

            columns = parsed_query.get(COLUMNS_KEY)
            if columns == ["*"]:
                return None

            return columns

        def get_duckdb_query(_local_changes_output: str) -> str:
            modified_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")

            if _local_changes_output == CHANGES_RECENT:
                return modified_query

            if (
                get_columns(_local_changes_output) is not None
                and table_config[EDITABLE_KEY] is True
                and unedited is not True
            ):
                # add row_id to the select clause
                return modified_query.replace(" FROM", ", row_id FROM")

            return modified_query

        fs = FileClient.get_gcs_file_system()

        def get_change_dataset(local_changes_output: str) -> Optional[Table]:
            changes_suffix = (
                "changes_all" if local_changes_output == CHANGES_ALL else "changes"
            )

            changes_files = fs.glob(
                f"gs://{table_config[BUCKET_KEY]}/eimerdb/{self.eimerdb_name}/{table_name}_{changes_suffix}/"
                f"{get_partition_levels()}"
            )

            try:
                max_depth = max(obj.count("/") for obj in changes_files)
            except ValueError:
                return None

            changes_files_max_depth = [
                obj for obj in changes_files if obj.count("/") == max_depth
            ]

            if partition_select is not None:
                changes_files_max_depth = filter_partitions(
                    table_files=changes_files_max_depth,
                    partition_select=partition_select,
                )

            # noinspection PyTypeChecker
            dataset = pq.read_table(
                source=changes_files_max_depth,
                schema=self._get_arrow_schema(table_name, True),
                filesystem=fs,
                columns=get_columns(local_changes_output),
            )

            return dataset if dataset.num_rows > 0 else None

        def get_changes_query_result(
            local_changes_output: str,
        ) -> Optional[Union[pd.DataFrame, pa.Table]]:
            dataset = get_change_dataset(local_changes_output)
            if dataset is None:
                return None

            query_result = duckdb.query(get_duckdb_query(local_changes_output))
            if output_format == PANDAS_OUTPUT_FORMAT:
                return query_result.df()
            else:
                column_order = [
                    field.name for field in self._get_arrow_schema(table_name, True)
                ]
                return query_result.arrow().select(column_order)

        def cast_if_arrow(
            table: Optional[Union[pd.DataFrame, pa.Table]]
        ) -> Optional[Union[pd.DataFrame, pa.Table]]:
            if table is None or output_format == PANDAS_OUTPUT_FORMAT:
                return table

            return table.cast(self._get_arrow_schema(table_name, True))

        def concat_changes(
            first: Optional[Union[pd.DataFrame, pa.Table]],
            second: Optional[Union[pd.DataFrame, pa.Table]],
        ) -> Optional[Union[pd.DataFrame, pa.Table]]:
            if first is None and second is None:
                return None

            if first is None:
                return cast_if_arrow(second)

            if second is None:
                return cast_if_arrow(first)

            if output_format == PANDAS_OUTPUT_FORMAT:
                return pd.concat([first, second])
            else:
                table = pa.concat_tables([first, second])
                return cast_if_arrow(table)

        # method body
        if changes_output == CHANGES_ALL:
            return concat_changes(
                first=get_changes_query_result(CHANGES_ALL),
                second=get_changes_query_result(CHANGES_RECENT),
            )
        else:
            return cast_if_arrow(get_changes_query_result(changes_output))
