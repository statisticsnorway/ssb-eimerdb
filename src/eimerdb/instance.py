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
from google.cloud import storage  # type: ignore
from pandas import DataFrame

from .functions import arrow_schema_from_json
from .functions import get_datetime
from .functions import get_initials
from .functions import get_json
from .functions import parse_sql_query
from .query import filter_partitions
from .query import get_partitioned_files
from .query import update_pyarrow_table

logger = logging.getLogger(__name__)


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
            bucket (str): GCS bucket name.
            eimerdb_name (str): EimerDB instance name.
            path (str): Configuration path.
            eimer_path (str): Instance path.
            created_by (str): Creator's name.
            time_created (str): Creation timestamp.
            users (dict): EimerDB users and roles.
            role_groups (dict): Role groups.
            is_admin (bool): Admin status.

        """
        self.bucket = bucket_name

        about_path = f"eimerdb/{eimer_name}/config/about.json"
        json_about = get_json(bucket_name, about_path)

        self.eimerdb_name = json_about["eimerdb_name"]
        self.path = json_about["path"]
        self.eimer_path = json_about["eimer_path"]
        self.created_by = json_about["created_by"]
        self.time_created = json_about["time_created"]

        initials = get_initials()

        users_path = f"{self.eimer_path}/config/users.json"
        users = get_json(bucket_name, users_path)

        role_groups_path = f"{self.eimer_path}/config/role_groups.json"
        role_groups = get_json(bucket_name, role_groups_path)

        tables_path = f"{self.eimer_path}/config/tables.json"
        tables = get_json(bucket_name, tables_path)

        self.tables = tables

        if initials in users and users[initials] == "admin":
            self.users = users
            self.role_groups = role_groups
            self.is_admin = True
        else:
            self.users = None  # type: ignore
            self.role_groups = None  # type: ignore
            self.is_admin = False

    def add_user(self, username: str, role: Any) -> None:
        """Add a user with a specified role.

        Args:
            username (str): Name of the user to add.
            role (Any): Role to assign (admin or user).

        Raises:
            Exception: If the user is not an admin or the user already exists.

        Returns:
            None

        """
        if self.is_admin is True:
            token = AuthClient.fetch_google_credentials()
            client = storage.Client(credentials=token)
            bucket = client.bucket(self.bucket)
            users = self.users
            new_user = {username: role}
            if username not in users:
                users.update(new_user)
                user_roles_blob = bucket.blob(f"{self.eimer_path}/config/users.json")
                user_roles_blob.upload_from_string(
                    data=json.dumps(users), content_type="application/json"
                )
                print(f"User {username} added with the role {role}!")
            else:
                raise Exception(f"User {username} already exists!")
        else:
            raise Exception("Cannot add user. You are not an admin!")
        return None

    def remove_user(self, username: str) -> None:
        """Remove a users access to the database.

        Args:
            username (str): Name of the user to remove.

        Raises:
            Exception: If the user is not an admin or the user does not exist.

        """
        if self.is_admin is True:
            token = AuthClient.fetch_google_credentials()
            client = storage.Client(credentials=token)
            bucket = client.bucket(self.bucket)
            users = self.users
            if username in users:
                del users[username]
                user_roles_blob = bucket.blob(f"{self.eimer_path}/config/users.json")
                user_roles_blob.upload_from_string(
                    data=json.dumps(users), content_type="application/json"
                )
                print(f"User {username} successfully removed!")
            else:
                print(f"The user {username} does not exist.")
        else:
            raise Exception("Cannot remove user. You are not an admin!")

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
            Exception: If the current user is not an admin.

        """
        if self.is_admin is True:
            token = AuthClient.fetch_google_credentials()
            client = storage.Client(credentials=token)
            row_id_def = {"name": "row_id", "type": "string", "label": "Unique row ID"}
            schema.insert(0, row_id_def)
            arrow_schema = arrow_schema_from_json(schema)  # type: ignore
            tables = self.tables
            bucket = client.bucket(self.bucket)
            initials = get_initials()
            new_table = {
                table_name: {
                    "created_by": initials,
                    "table_path": f"{self.eimer_path}/{table_name}",
                    "bucket": self.bucket,
                    "editable": editable,
                    "schema": arrow_schema,
                }
            }
            if partition_columns:
                new_table[table_name]["partition_columns"] = partition_columns  # type: ignore
            else:
                new_table[table_name]["partition_columns"] = None
            tables.update(new_table)
            tables_blob = bucket.blob(f"{self.eimer_path}/config/tables.json")
            tables_blob.upload_from_string(
                data=json.dumps(tables), content_type="application/json"
            )
        else:
            raise Exception("Cannot create table. You are not an admin!")

    def insert(self, table_name: str, df: pd.DataFrame) -> None:
        """Insert unedited data into a main table.

        Args:
            table_name (str): Name of the table to insert data into.
            df (pandas.DataFrame): DataFrame containing the data to insert.

        Raises:
            Exception: If the current user is not an admin.

        """
        if self.is_admin is True:
            json_data = self.tables[table_name]
            arrow_schema = arrow_schema_from_json(json_data["schema"])
            df["row_id"] = df.apply(lambda row: uuid4(), axis=1)
            df["row_id"] = df["row_id"].astype(str)
            table = pa.Table.from_pandas(df, schema=arrow_schema)

            df_raw = df.copy()
            df_raw["user"] = get_initials()
            df_raw["datetime"] = get_datetime()
            df_raw["operation"] = "insert"

            arrow_schema_raw = arrow_schema
            arrow_schema_raw = arrow_schema_raw.append(pa.field("user", pa.string()))
            arrow_schema_raw = arrow_schema_raw.append(
                pa.field("datetime", pa.string())
            )
            arrow_schema_raw = arrow_schema_raw.append(
                pa.field("operation", pa.string())
            )

            table_raw = pa.Table.from_pandas(df_raw, schema=arrow_schema_raw)
            timestamp_column = table_raw["datetime"].cast(pa.timestamp("ns"))

            table_raw = table_raw.drop(["datetime"])  # type: ignore

            table_raw = table_raw.add_column(
                len(table_raw.column_names),
                pa.field("datetime", pa.timestamp("ns")),
                timestamp_column,
            )

            insert_id = uuid4()
            table_path = json_data["table_path"]
            partitions = json_data["partition_columns"]
            filename = f"insert_{insert_id}_{{i}}.parquet"

            fs = FileClient.get_gcs_file_system()

            pq.write_to_dataset(
                table,
                root_path=f"gs://{self.bucket}/{table_path}",
                partition_cols=partitions,
                basename_template=filename,
                filesystem=fs,
                schema=arrow_schema,
            )
            pq.write_to_dataset(
                table_raw,
                root_path=f"gs://{self.bucket}/{table_path}_raw",
                partition_cols=partitions,
                basename_template=filename,
                filesystem=fs,
            )
            print("Data successfully inserted!")
        else:
            raise Exception("Cannot insert into main table. You are not an admin!")

    def query(
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: Optional[bool] = False,
        output_format: Optional[str] = None,
    ) -> Union[pd.DataFrame, pa.Table, str, None]:
        """Execute an SQL query on an EimerDB table.

        Args:
            sql_query (str): SQL query to execute.
            partition_select (dict): Dictionary specifying partition filters.
            unedited (bool): Indicates whether to include unedited data.
            output_format (str, optional): Desired output format ('pandas' or 'arrow').

        Returns:
            pandas.DataFrame: Resulting DataFrame from the query.

        Raises:
            Exception: If the table is not editable (for UPDATE queries).

        """
        instance_name = self.eimerdb_name
        if output_format is None:
            output_format = "pandas"

        parsed_query = parse_sql_query(sql_query)
        try:
            where_clause = parsed_query["where_clause"]
        except KeyError:
            where_clause = ""

        if parsed_query["operation"] == "SELECT":
            con = duckdb.connect()
            tables = parsed_query["table_name"]
            for table_name in tables:
                json_data = self.tables[table_name]
                table_config = self.tables[table_name]
                editable = table_config["editable"]
                fs = FileClient.get_gcs_file_system()

                table_files = get_partitioned_files(
                    table_name,
                    instance_name,
                    table_config,
                    "_raw",
                    fs,
                    partition_select,
                    unedited,
                )
                if partition_select is not None:
                    table_files = filter_partitions(table_files, partition_select)

                df = pq.read_table(table_files, filesystem=fs)  # type: ignore

                if editable is True and unedited is False:
                    slct_qry = "SELECT * FROM"
                    df_changes = self.query_changes(
                        f"{slct_qry} {table_name}",
                        partition_select,
                        output_format="arrow",
                        changes_output="recent",
                    )

                if editable is True and unedited is False and df_changes is not None:
                    if df_changes.num_rows != 0:
                        df = update_pyarrow_table(df, df_changes)  # type: ignore
                con.register(table_name, df)
                del df

            if output_format == "pandas":
                output: pd.DataFrame = con.execute(sql_query).df()
            elif output_format == "arrow":
                output: pa.Table = con.execute(sql_query).arrow()  # type: ignore

            return output

        elif parsed_query["operation"] == "UPDATE":
            table_name = parsed_query["table_name"]
            table_config = self.tables[table_name]
            editable = table_config["editable"]
            if editable is False:
                raise Exception(f"The table {table_name} is not editable!")
            try:
                columns = parsed_query["columns"]
            except Exception:
                columns = None
            if columns == ["*"]:
                columns = None
            json_data = self.tables[table_name]
            table_name = parsed_query["table_name"]
            arrow_schema = arrow_schema_from_json(json_data["schema"])
            where_clause = parsed_query["where_clause"]
            table_config = self.tables[table_name]
            partitions = table_config["partition_columns"]

            arrow_schema = arrow_schema.append(pa.field("user", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("datetime", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("operation", pa.string()))

            slct_qry = "SELECT * FROM"

            df_update_results: pd.DataFrame = self.query(  # type: ignore
                f"{slct_qry} {table_name} WHERE {where_clause}", partition_select
            )

            df_update_results["user"] = get_initials()
            df_update_results["datetime"] = get_datetime()
            df_update_results["operation"] = "update"
            dataset = pa.Table.from_pandas(df_update_results, schema=arrow_schema)
            con = duckdb.connect()
            con.register("dataset", dataset)
            con.execute(f"CREATE TABLE updates AS FROM dataset WHERE {where_clause}")
            sql_query = sql_query.replace(f"UPDATE {table_name}", "UPDATE updates")
            con.execute(sql_query)
            df_updates_commits = con.table("updates").df()

            df_updates_len = len(df_updates_commits)

            table_path = self.tables[table_name]["table_path"] + "_changes"
            fs = FileClient.get_gcs_file_system()

            update_table = pa.Table.from_pandas(df_updates_commits, schema=arrow_schema)

            unique_file_id = uuid4()
            filename = f"commit_{unique_file_id}_{{i}}.parquet"

            pq.write_to_dataset(
                update_table,
                root_path=f"gs://{self.bucket}/{table_path}",
                partition_cols=partitions,
                basename_template=filename,
                filesystem=fs,
            )
            return print(f"{df_updates_len} rows updated by {get_initials()}")

        elif parsed_query["operation"] == "DELETE":
            table_name = parsed_query["table_name"]
            table_config = self.tables[table_name]
            editable = table_config["editable"]
            if editable is False:
                raise Exception(f"The table {table_name} is not editable!")
            try:
                columns = parsed_query["columns"]
            except Exception:
                columns = None
            if columns == ["*"]:
                columns = None
            json_data = self.tables[table_name]
            table_name = parsed_query["table_name"]
            arrow_schema = arrow_schema_from_json(json_data["schema"])
            where_clause = parsed_query["where_clause"]
            instance_name = self.eimerdb_name
            table_config = self.tables[table_name]
            partitions = table_config["partition_columns"]

            arrow_schema = arrow_schema.append(pa.field("user", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("datetime", pa.string()))
            arrow_schema = arrow_schema.append(pa.field("operation", pa.string()))

            slct_qry = "SELECT * FROM"

            df_delete_results: pd.DataFrame = self.query(  # type: ignore
                f"{slct_qry} {table_name} WHERE {where_clause}", partition_select
            )

            df_delete_results["user"] = get_initials()
            df_delete_results["datetime"] = get_datetime()
            df_delete_results["operation"] = "delete"
            dataset = pa.Table.from_pandas(df_delete_results, schema=arrow_schema)
            con = duckdb.connect()
            con.register("dataset", dataset)
            con.execute(f"CREATE TABLE deletes AS FROM dataset WHERE {where_clause}")

            df_deletions = con.table("deletes").df()

            df_deletions_len = len(df_deletions)

            table_path = self.tables[table_name]["table_path"] + "_changes"
            fs = FileClient.get_gcs_file_system()

            deletion_Table = pa.Table.from_pandas(df_deletions, schema=arrow_schema)

            unique_file_id = uuid4()
            filename = f"commit_{unique_file_id}_{{i}}.parquet"

            pq.write_to_dataset(
                deletion_Table,
                root_path=f"gs://{self.bucket}/{table_path}",
                partition_cols=partitions,
                basename_template=filename,
                filesystem=fs,
            )
            return print(f"{df_deletions_len} rows deleted by {get_initials()}")
        else:
            return None

    def query_changes(
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: Optional[bool] = False,
        output_format: Optional[str] = None,
        changes_output: Optional[str] = "all",
    ) -> Union[pd.DataFrame, pa.Table, None]:
        """Query changes made in the database table.

        Args:
            sql_query (str): The SQL query to execute.
            partition_select (Dict, optional):
                Dictionary containing partition selection criteria. Defaults to None.
            unedited (bool, optional):
                Flag indicating whether to retrieve unedited changes. Defaults to False.
            output_format (str, optional):
                The desired output format ('pandas' or 'arrow'). Defaults to 'pandas'.
            changes_output (str, optional):
                The changes that are to be retrieved ('recent' or 'all'). Defaults to 'all'.

        Returns:
            Union[pd.DataFrame, pa.Table, str]:
                Returns a pandas DataFrame if 'pandas' output format is specified,
                an arrow Table if 'arrow' output format is specified,
        """
        if output_format is None:
            output_format = "pandas"

        parsed_query = parse_sql_query(sql_query)
        table_name = parsed_query["table_name"][0]
        table_config = self.tables[table_name]
        editable = table_config["editable"]
        instance_name = self.eimerdb_name
        table_schema = arrow_schema_from_json(self.tables[table_name]["schema"])
        if parsed_query["operation"] == "SELECT":
            try:
                columns = parsed_query["columns"]
            except Exception:
                columns = None
            if columns == ["*"]:
                columns = None
            partitions = table_config["partition_columns"]
            bucket_name = table_config["bucket"]
            partitions_len = len(partitions)
            partition_levels = "**/" * partitions_len + "*"
            fs = FileClient.get_gcs_file_system()
            table_name_changes = table_name + "_changes"
            table_files_changes = fs.glob(
                f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name_changes}/{partition_levels}"
            )
            try:
                max_depth = max(obj.count("/") for obj in table_files_changes)
                no_changes = False
            except ValueError:
                no_changes = True
                if output_format == "pandas":
                    df_changes: pd.DataFrame = pd.DataFrame()
                elif output_format == "arrow":
                    df_changes: pa.Table = pa.table([])  # type: ignore

            if no_changes is not True:
                table_files_changes = [
                    obj for obj in table_files_changes if obj.count("/") == max_depth
                ]
                if partition_select is not None:
                    table_files_changes = filter_partitions(
                        table_files_changes,
                        partition_select,
                    )

                dataset: pa.Table = pq.read_table(table_files_changes, filesystem=fs)
                sql_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")
                if dataset.num_rows != 0:
                    con = duckdb.connect()
                    if output_format == "pandas":
                        df_changes: pd.DataFrame = con.execute(sql_query).df()  # type: ignore
                    elif output_format == "arrow":
                        df_changes: pa.Table = con.execute(sql_query).arrow()  # type: ignore

                elif dataset.num_rows == 0:
                    if output_format == "pandas":
                        df_changes: pd.DataFrame = pd.DataFrame()  # type: ignore
                    elif output_format == "arrow":
                        df_changes: pa.Table = pa.table([])  # type: ignore

            table_name_changes_all = table_name + "_changes_all"
            table_files_changes_all = fs.glob(
                f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name_changes_all}/{partition_levels}"
            )
            try:
                max_depth = max(obj.count("/") for obj in table_files_changes_all)
                no_changes_all = False
            except ValueError:
                no_changes_all = True
            if no_changes_all is not True:
                table_files_changes_all = [
                    obj
                    for obj in table_files_changes_all
                    if obj.count("/") == max_depth
                ]
                if partition_select is not None:
                    table_files_changes_all = filter_partitions(
                        table_files_changes_all, partition_select
                    )

                dataset = pq.read_table(
                    table_files_changes_all, filesystem=fs, columns=columns
                )
                sql_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")
                if columns is not None and editable is True and unedited is False:
                    sql_query = sql_query.replace(" FROM", ", row_id FROM")
                if dataset.num_rows != 0:
                    con = duckdb.connect()
                    if output_format == "pandas":
                        df_changes_all: pd.DataFrame = con.execute(sql_query).df()
                        df: pd.DataFrame = pd.concat([df_changes_all, df_changes])
                    elif output_format == "arrow":
                        df_changes_all: pa.Table = con.execute(sql_query).arrow()  # type: ignore
                        df: pa.Table = pa.concat_tables([df_changes_all, df_changes])  # type: ignore
                        df: pa.Table = df.cast(table_schema)  # type: ignore

            if changes_output == "all":
                if no_changes_all is not True:
                    return df
                elif no_changes_all is True:
                    return df_changes
            elif changes_output == "recent":
                return df_changes
            else:
                return None
        else:
            return None

    def get_changes(self, table_name: str) -> DataFrame:
        """Retrieve changes for a given table.

        Args:
            table_name (str): The name of the table for which changes are to be retrieved.

        Returns:
            DataFrame: A pandas DataFrame containing the changes for the specified table.
        """
        fs = FileClient.get_gcs_file_system()
        bucket = self.bucket
        path = self.tables[table_name]["table_path"]
        dataset = ds.dataset(
            f"{bucket}/{path}_changes/",
            format="parquet",
            partitioning="hive",
            filesystem=fs,
        )
        df_changes: DataFrame = dataset.to_table().to_pandas()
        return df_changes

    def combine_changes(self, table_name: str) -> None:
        """Combines the files containing the changes of the table into one file.

        Args:
            table_name (str): The name of the table for which changes are to be merged.

        Returns:
            None
        """
        fs = FileClient.get_gcs_file_system()
        df_changes = self.get_changes(table_name)

        token = AuthClient.fetch_google_credentials()
        client = storage.Client(credentials=token)
        bucket = client.bucket(self.bucket)
        path = self.tables[table_name]["table_path"]
        partitions = self.tables[table_name]["partition_columns"]
        source_folder = path + "_changes"
        blobs_to_delete = list(bucket.list_blobs(prefix=source_folder))
        filename = f"merged_commit_{uuid4()}_{{i}}.parquet"
        table = pa.Table.from_pandas(df_changes)
        pq.write_to_dataset(
            table,
            root_path=f"gs://{self.bucket}/{source_folder}",
            partition_cols=partitions,
            basename_template=filename,
            filesystem=fs,
        )
        for blob in blobs_to_delete:
            blob.delete()

        logging.info(
            "The changes were successfully combined into one file per partition!"
        )

        print("The changes were successfully combined into one file per partition!")
        return None
