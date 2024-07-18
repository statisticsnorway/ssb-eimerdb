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

    def add_user(self, username: str, role: Any) -> None:  # noqa: D102
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

    def remove_user(self, username: str) -> None:  # noqa: D102
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

    def create_table(  # noqa: D102
        self,
        table_name: str,
        schema: list[dict[str, Any]],
        partition_columns: Optional[list[str]] = None,
        editable: Optional[bool] = True,
    ) -> None:
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

    def insert(self, table_name: str, df: pd.DataFrame) -> list[str]:  # noqa: D102
        if self._is_admin is not True:
            raise PermissionError(
                "Cannot insert into main table. You are not an admin!"
            )

        uuid_list = [str(uuid4()) for _ in range(len(df))]
        df["row_id"] = uuid_list

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
        self, table_name: str, source_folder: str, raw: bool
    ) -> Optional[pa.Table]:
        """Retrieve inserts or changes for a given table. Returns None if file not found.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.
            source_folder (str): The folder where the inserts/changes are stored.
            raw (bool): Indicates whether to retrieve the raw schema. Only in use when
                retrieving inserts.

        Returns:
            DataFrame: A pandas DataFrame containing inserts/changes
            for the specified table, or None if file not found.
        """
        try:
            # noinspection PyTypeChecker
            dataset = ds.dataset(
                f"{self.bucket_name}/{source_folder}/",
                format="parquet",
                partitioning="hive",
                schema=self.get_arrow_schema(table_name, raw),
                filesystem=FileClient.get_gcs_file_system(),
            )
        except FileNotFoundError:
            return None

        return dataset.to_table()

    def _write_to_table_and_delete_blobs(
        self, table_name: str, table: pa.Table, source_folder: str, raw: bool
    ) -> None:
        client = storage.Client(credentials=AuthClient.fetch_google_credentials())
        bucket = client.bucket(self._bucket_name)

        partitions = self._tables[table_name][PARTITION_COLUMNS_KEY]
        blobs_to_delete = list(bucket.list_blobs(prefix=source_folder + "/"))

        # noinspection PyTypeChecker
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
        for blob in blobs_to_delete:
            blob.delete()

    def combine_changes(self, table_name: str) -> None:  # noqa: D102
        source_folder = self._tables[table_name][TABLE_PATH_KEY] + "_changes"

        changes_table = self._get_inserts_or_changes(
            table_name=table_name, source_folder=source_folder, raw=True
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

        logger.info("The changes were successfully merged into one file per partition!")

    def combine_inserts(self, table_name: str, raw: bool) -> None:  # noqa: D102
        suffix = "_raw" if raw else ""
        source_folder = self._tables[table_name][TABLE_PATH_KEY] + suffix

        inserts_table = self._get_inserts_or_changes(
            table_name=table_name, source_folder=source_folder, raw=raw
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

        logger.info("The inserts were successfully merged into one file per partition!")

    def get_arrow_schema(  # noqa: D102
        self,
        table_name: str,
        raw: bool,
    ) -> pa.Schema:
        json_data = self._tables[table_name]
        arrow_schema = arrow_schema_from_json(json_data[SCHEMA_KEY])

        if raw is True:
            arrow_schema = arrow_schema.append(pa.field("user", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("datetime", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("operation", pa.string()))

        return arrow_schema

    def query(  # noqa: D102
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
        timetravel: Optional[str] = None,
    ) -> Union[pd.DataFrame, pa.Table, str]:
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
                    timetravel=timetravel,
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

    def query_changes(  # noqa: D102
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
        changes_output: str = CHANGES_ALL,
    ) -> Optional[Union[pd.DataFrame, pa.Table]]:
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
