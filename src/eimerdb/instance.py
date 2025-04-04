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
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from dapla import AuthClient
from dapla import FileClient
from google.cloud import storage
from google.cloud.storage import Blob

from .abstract_db_instance import AbstractDbInstance
from .eimerdb_constants import APPLICATION_JSON
from .eimerdb_constants import ARROW_OUTPUT_FORMAT
from .eimerdb_constants import BUCKET_KEY
from .eimerdb_constants import CHANGES_ALL
from .eimerdb_constants import CHANGES_RECENT
from .eimerdb_constants import CREATED_BY_KEY
from .eimerdb_constants import DEFAULT_COMPRESSION
from .eimerdb_constants import DEFAULT_MIN_ROWS_PER_GROUP
from .eimerdb_constants import EDITABLE_KEY
from .eimerdb_constants import OPERATION_KEY
from .eimerdb_constants import PANDAS_OUTPUT_FORMAT
from .eimerdb_constants import PARTITION_COLUMNS_KEY
from .eimerdb_constants import POLARS_OUTPUT_FORMAT
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
        """Add a user to the EimerDB with a role.

        Args:
            username (str): The username to add.
            role (Any): The role to assign to the user.

        Raises:
            PermissionError: If the current user is not authorized to add users.
            ValueError: If the given role is invalid.
        """
        if self._is_admin is not True:
            raise PermissionError("Cannot add user. You are not an admin!")

        if username in self._users:
            raise ValueError(f"User {username} already exists!")

        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self._bucket_name)

        self._users.update({username: role})
        user_roles_blob = bucket.blob(f"{self._eimer_path}/config/users.json")

        user_roles_blob.upload_from_string(
            data=json.dumps(self._users), content_type=APPLICATION_JSON
        )
        logger.info("User %s added with the role %s!", username, role)

    def remove_user(self, username: str) -> None:
        """Remove a user.

        Args:
            username (str): The username to remove.

        Raises:
            PermissionError: If the user is not authorized to remove users.
            ValueError: If the user does not exist.
        """
        if self._is_admin is not True:
            raise PermissionError("Cannot remove user. You are not an admin!")

        if username not in self._users:
            raise ValueError(f"User {username} does not exist.")

        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self._bucket_name)

        del self._users[username]
        user_roles_blob = bucket.blob(f"{self._eimer_path}/config/users.json")
        user_roles_blob.upload_from_string(
            data=json.dumps(self._users), content_type=APPLICATION_JSON
        )
        logger.info("User %s successfully removed!", username)

    def create_table(
        self,
        table_name: str,
        schema: list[dict[str, Any]],
        partition_columns: Optional[list[str]] = None,
        editable: Optional[bool] = True,
    ) -> None:
        """Create a new table with the given schema and store it in the table config.

        Args:
            table_name (str): The name of the table to create.
            schema (list[dict[str, Any]]): The schema definition as a list of field dicts.
            partition_columns (list[str] | None): Optional list of column names used for partitioning.
            editable (bool | None): Whether the table is editable. Defaults to True.

        Raises:
            PermissionError: If the user is not an admin.
        """
        if self._is_admin is not True:
            raise PermissionError("Cannot create table. You are not an admin!")

        schema.insert(0, ROW_ID_DEF)

        new_table = {
            table_name: {
                CREATED_BY_KEY: get_initials(),
                TABLE_PATH_KEY: f"{self._eimer_path}/{table_name}",
                BUCKET_KEY: self._bucket_name,
                EDITABLE_KEY: editable,
                SCHEMA_KEY: schema,
                PARTITION_COLUMNS_KEY: partition_columns,
            }
        }
        self._tables.update(new_table)

        token = AuthClient.fetch_google_credentials()
        bucket = storage.Client(credentials=token).bucket(self._bucket_name)

        tables_blob = bucket.blob(f"{self._eimer_path}/config/tables.json")
        tables_blob.upload_from_string(
            data=json.dumps(self._tables), content_type=APPLICATION_JSON
        )

    def insert(
        self,
        table_name: str,
        df: pd.DataFrame,
        custom_user: Optional[str] = None,
    ) -> list[str]:
        """Insert a DataFrame into an EimerDB table.

        Args:
            table_name (str): The name of the table to insert into.
            df (pd.DataFrame): The DataFrame to insert.
            custom_user (str | None): Overrides the current user.

        Returns:
            list[str]: A list containing the row IDs of the inserted rows.

        Raises:
            PermissionError: If the user does not have permission to insert into the table.
        """
        if self._is_admin is not True:
            raise PermissionError(
                f"Cannot insert into {table_name}. You are not an admin!"
            )

        uuid_list = [str(uuid4()) for _ in range(len(df))]
        df["row_id"] = uuid_list

        arrow_schema = self.get_arrow_schema(table_name, False)
        table = pa.Table.from_pandas(df, schema=arrow_schema)

        df_raw = df.copy()
        df_raw["user"] = custom_user or get_initials()
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
        json_data = self._tables[table_name]
        table_path = json_data[TABLE_PATH_KEY]
        partitions = json_data[PARTITION_COLUMNS_KEY]
        filename = f"insert_{insert_id}_{{i}}.parquet"

        fs = FileClient.get_gcs_file_system()

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=table,
            root_path=f"gs://{self._bucket_name}/{table_path}",
            partition_cols=partitions,
            basename_template=filename,
            compression=DEFAULT_COMPRESSION,
            min_rows_per_group=DEFAULT_MIN_ROWS_PER_GROUP,
            filesystem=fs,
            schema=arrow_schema,
        )
        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=table_raw,
            root_path=f"gs://{self._bucket_name}/{table_path}_raw",
            partition_cols=partitions,
            basename_template=filename,
            compression=DEFAULT_COMPRESSION,
            min_rows_per_group=DEFAULT_MIN_ROWS_PER_GROUP,
            filesystem=fs,
        )

        return uuid_list

    def _get_inserts_or_changes(
        self, table_name: str, source_folder: str, filtered_blobs: list[Blob], raw: bool
    ) -> Optional[pa.Table]:
        """Retrieve inserts or changes for a given table. Returns None if file not found.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.
            source_folder (str): The folder where the inserts/changes are stored.
            filtered_blobs (list): List of filtered blobs.
            raw (bool): Indicates whether to retrieve the raw schema. Only in use when
                retrieving inserts.

        Returns:
            DataFrame: A pandas DataFrame containing inserts/changes
            for the specified table, or None if file not found.
        """
        file_path_list = [
            f"gs://{blob.bucket.name}/{blob.name}"
            for blob in filtered_blobs
            if blob.name.endswith(".parquet")
        ]

        try:
            # noinspection PyTypeChecker
            dataset = ds.dataset(
                file_path_list,
                format="parquet",
                partitioning="hive",
                schema=self.get_arrow_schema(table_name, raw),
                filesystem=FileClient.get_gcs_file_system(),
            )
        except FileNotFoundError:
            return None

        return dataset.to_table()

    def _get_blobs(
        self,
        table_name: str,
        source_folder: str,
        partition_select: Optional[dict[str, Any]],
    ) -> list:
        """Retrieve a list of the parquet files for the given partition.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.
            source_folder (str): The folder where the inserts/changes are stored.
            partition_select (dict): A dictionary with the selected partitions

        Returns:
            list: A list of parquet files.
        """
        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self._bucket_name)

        prefix = source_folder.rstrip("/") + "/"
        all_blobs = list(bucket.list_blobs(prefix=prefix))

        def match_blob(blob_name: str) -> bool:
            if not partition_select:
                return True
            for key, values in partition_select.items():
                if not any(f"{key}={v}/" in blob_name for v in values):
                    return False
            return True

        filtered_blobs = [blob for blob in all_blobs if match_blob(blob.name)]
        return filtered_blobs

    def _write_to_table_and_delete_blobs(
        self,
        table_name: str,
        table: pa.Table,
        source_folder: str,
        raw: bool,
    ) -> None:
        """Write a table to a Hive-partitioned Parquet dataset, and delete matched blobs.

        Args:
            table_name (str): Name of the table.
            table (pa.Table): PyArrow table to write.
            source_folder (str): Folder (GCS path) to write to and clean up from.
            raw (bool): Whether to include additional metadata fields.
        """
        partitions = self.tables[table_name][PARTITION_COLUMNS_KEY]

        pq.write_to_dataset(
            table=table,
            root_path=f"gs://{self._bucket_name}/{source_folder}",
            partition_cols=partitions,
            basename_template=f"merged_commit_{uuid4()}_{{i}}.parquet",
            compression=DEFAULT_COMPRESSION,
            min_rows_per_group=DEFAULT_MIN_ROWS_PER_GROUP,
            schema=self.get_arrow_schema(table_name, raw),
            filesystem=FileClient.get_gcs_file_system(),
        )

    def combine_changes(
        self,
        table_name: str,
        partition_select: Optional[dict[str, list[Any]]] = None,
    ) -> None:
        """Combines a set of files to one single files for the given partitions.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.
            partition_select (Dict, optional): A dictionary with the selected partitions

        Returns:
            None
        """
        source_folder = self._tables[table_name][TABLE_PATH_KEY] + "_changes"

        filtered_blobs = self._get_blobs(
            table_name=table_name,
            source_folder=source_folder,
            partition_select=partition_select,
        )

        changes_table = self._get_inserts_or_changes(
            table_name=table_name,
            source_folder=source_folder,
            filtered_blobs=filtered_blobs,
            raw=True,
        )

        if changes_table is None:
            logger.info("No changes found for table %s", table_name)
            return

        self._write_to_table_and_delete_blobs(
            table_name=table_name,
            table=changes_table,
            source_folder=source_folder,
            raw=True,
        )

        for blob in filtered_blobs:
            blob.delete()

        logger.info("The changes were successfully merged into one file per partition!")

    def combine_inserts(
        self,
        table_name: str,
        raw: bool,
        partition_select: Optional[dict[str, Any]],
    ) -> None:
        """Combines a set of files to one single files for the given partitions.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.
            partition_select (Dict, optional): A dictionary with the selected partitions
            raw (bool): Whether to include additional metadata fields.

        Returns:
            None
        """
        suffix = "_raw" if raw else ""
        source_folder = self._tables[table_name][TABLE_PATH_KEY] + suffix

        filtered_blobs = self._get_blobs(
            table_name=table_name,
            source_folder=source_folder,
            partition_select=partition_select,
        )

        inserts_table = self._get_inserts_or_changes(
            table_name=table_name,
            source_folder=source_folder,
            filtered_blobs=filtered_blobs,
            raw=raw,
        )

        if inserts_table is None:
            logger.info("No inserts found for table %s", table_name)
            return

        self._write_to_table_and_delete_blobs(
            table_name=table_name,
            table=inserts_table,
            source_folder=source_folder,
            raw=raw,
        )

        for blob in filtered_blobs:
            blob.delete()

        logger.info("The inserts were successfully merged into one file per partition!")

    def get_arrow_schema(
        self,
        table_name: str,
        raw: bool,
    ) -> pa.Schema:
        """Retrieve the Arrow schema of a table.

        Args:
            table_name (str): The name of the table.
            raw (bool): Whether to include additional metadata fields.

        Returns:
            Schema: A pyarrow schema.
        """
        json_data = self._tables[table_name]
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
        timetravel: Optional[str] = None,
        custom_user: Optional[str] = None,
    ) -> Union[pd.DataFrame, pl.DataFrame, pa.Table, str]:
        """SQL query execution.

        Supports SELECT, UPDATE, and DELETE operations with optional partition filtering,
        time travel, and output format selection.

        Args:
            sql_query (str): The SQL query to execute.
            partition_select (dict[str, Any] | None): Optional partition filters to apply.
            unedited (bool): If True, the output will only contain unedited, raw data from the inserts.
            output_format (str): The format of the output. Must be 'pandas', 'arrow' or polars.
            timetravel (str | None): Optional timestamp for time travel queries.
            custom_user (str | None): Overrides the current user.

        Returns:
            Union[pd.DataFrame, pl.DataFrame, pa.Table, str]: The result of the query, depending on
            the operation and selected output format.

        Raises:
            ValueError: If the SQL operation or output format is unsupported.
        """
        if output_format not in [
            PANDAS_OUTPUT_FORMAT,
            POLARS_OUTPUT_FORMAT,
            ARROW_OUTPUT_FORMAT,
        ]:
            raise ValueError(
                f"Invalid output format: {output_format}. Supported formats: pandas, polars, arrow."
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
                    timetravel=timetravel,
                    fs=fs,
                )
            case DbOperation.UPDATE_QUERY_OPERATION:
                return self.query_worker.query_update_or_delete(
                    parsed_query=parsed_query,
                    update_sql_query=sql_query,
                    partition_select=partition_select,
                    fs=fs,
                    custom_user=custom_user,
                )
            case DbOperation.DELETE_QUERY_OPERATION:
                return self.query_worker.query_update_or_delete(
                    parsed_query=parsed_query,
                    update_sql_query=None,
                    partition_select=partition_select,
                    fs=fs,
                    custom_user=custom_user,
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
    ) -> Optional[Union[pd.DataFrame, pl.DataFrame, pa.Table]]:
        """Execute a SQL SELECT query against edited data.

        Supports partition filtering, output format selection, and the ability to include
        only unedited changes or all available changes.

        Args:
            sql_query (str): The SQL SELECT query to execute.
            partition_select (dict[str, Any] | None): Optional partition filters to apply.
            unedited (bool): If True, only unedited (raw) changes will be included in the result.
            output_format (str): The format of the output. Must be 'pandas', 'arrow', or 'polars'.
            changes_output (str): Whether to include only non-merged changes or all. Must be
                one of CHANGES_ALL or CHANGES_RECENT. May be deprecated in the future.

        Returns:
            pd.DataFrame | pl.DataFrame | pa.Table | None: The result of the query, depending on
            the selected output format.

        Raises:
            ValueError: If the output format, changes_output value, or SQL operation is invalid.
        """
        if output_format not in (
            PANDAS_OUTPUT_FORMAT,
            ARROW_OUTPUT_FORMAT,
            POLARS_OUTPUT_FORMAT,
        ):
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
