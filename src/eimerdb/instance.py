"""EimerDB Instance Module.

This module contains the EimerDBInstance class, which represents an instance
of the EimerDB database. It provides methods to interact with EimerDB,
including managing users, creating tables, inserting data,
and querying data.

Author: Stian Elisenberg
Date: September 16, 2023
"""

import json
from uuid import uuid4

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from dapla import AuthClient
from dapla import FileClient
from .functions import arrow_schema_from_json
from .functions import get_datetime
from .functions import get_initials
from .functions import get_json
from .functions import parse_sql_query
from google.cloud import storage

class EimerDBInstance:
    """Represents an instance of the EimerDB database.

    This class provides methods to interact with EimerDB, including
    managing users, creating tables, inserting data, and querying data.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket
            where EimerDB is hosted.
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
    def __init__(self, bucket_name, eimer_name):
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
            self.users = None
            self.role_groups = None
            self.is_admin = False

    def add_user(self, username, role):
        """Add a user with a specified role.

        Args:
            username (str): Name of the user to add.
            role (str): Role to assign (admin or user).

        Raises:
            Exception: If the user is not an admin or the user already exists.

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

    def remove_user(self, username):
        """Remove a user by their username.

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

    def create_table(self, table_name, schema, partition_columns=None, editable=True):
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
            arrow_schema_from_json(schema)
            tables = self.tables
            bucket = client.bucket(self.bucket)
            initials = get_initials()
            new_table = {
                table_name: {
                    "created_by": initials,
                    "table_path": f"{self.eimer_path}/{table_name}",
                    "bucket": self.bucket,
                    "editable": editable,
                    "schema": schema,
                }
            }
            if partition_columns:
                new_table[table_name]["partition_columns"] = partition_columns
            else:
                new_table[table_name]["partition_columns"] = None
            tables.update(new_table)
            tables_blob = bucket.blob(f"{self.eimer_path}/config/tables.json")
            tables_blob.upload_from_string(
                data=json.dumps(tables), content_type="application/json"
            )
        else:
            raise Exception("Cannot create table. You are not an admin!")

    def main_table_insert(self, table_name, df, raw=True):
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

            table = pa.Table.from_pandas(df, schema=arrow_schema)
            fs = FileClient.get_gcs_file_system()

            table_path = json_data["table_path"]
            partitions = json_data["partition_columns"]
            filename = f"{table_name}_data_{{i}}.parquet"
            pq.write_to_dataset(
                table,
                root_path=f"gs://{self.bucket}/{table_path}",
                partition_cols=partitions,
                basename_template=filename,
                filesystem=fs,
            )
            if raw is True:
                pq.write_to_dataset(
                    table,
                    root_path=f"gs://{self.bucket}/{table_path}_raw",
                    partition_cols=partitions,
                    basename_template=filename,
                    filesystem=fs,
                )
            print("Data successfully inserted!")
        else:
            Exception("Cannot insert into main table. You are not an admin!")

    def query(self, sql_query, partition_select=None, unedited=False):
        """Execute an SQL query on an EimerDB table.

        Args:
            sql_query (str): SQL query to execute.
            partition_select (dict): Dictionary specifying partition filters.
            unedited (bool): Indicates whether to include unedited data.

        Returns:
            pandas.DataFrame: Resulting DataFrame from the query.

        Raises:
            Exception: If the table is not editable (for UPDATE queries).

        """
        parsed_query = parse_sql_query(sql_query)
        table_name = parsed_query["table_name"]
        table_config = self.tables[table_name]
        editable = table_config["editable"]
        instance_name = self.eimerdb_name
        if parsed_query["operation"] == "SELECT":
            try:
                columns = parsed_query["columns"]
            except Exception:
                columns = None
            if columns == ["*"]:
                columns = None
            if editable is True and unedited is False and columns is not None:
                if "uuid" not in columns:
                    columns.append("uuid")
            partitions = table_config["partition_columns"]
            bucket_name = table_config["bucket"]
            partitions_len = len(partitions)
            partition_levels = "**/" * partitions_len + "*"
            fs = FileClient.get_gcs_file_system()
            if unedited is True:
                table_name_parts = table_name + "_raw"
            else:
                table_name_parts = table_name
            table_files = fs.glob(
                f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name_parts}/{partition_levels}"
            )
            max_depth = max(obj.count("/") for obj in table_files)
            table_files = [obj for obj in table_files if obj.count("/") == max_depth]

            if partition_select is not None:
                filtered_files = []
                for file in table_files:
                    parts = file.split("/")

                    all_matches = True

                    for key, values in partition_select.items():
                        match_found = any(f"{key}={value}" in parts for value in values)

                        if not match_found:
                            all_matches = False
                            break
                    if all_matches:
                        filtered_files.append(file)
                    table_files = filtered_files
            dataset = pq.read_table(table_files, filesystem=fs, columns=columns)
            sql_query_raw = sql_query
            sql_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")
            if columns is not None and editable is True and unedited is False:
                sql_query = sql_query.replace(" FROM", ", uuid FROM")

            con = duckdb.connect()
            df = con.execute(sql_query).df()
            if editable is True and unedited is False:
                table_name_changes = table_name + "_changes"
                table_files_changes = fs.glob(
                    f"gs://{bucket_name}/eimerdb/{instance_name}/"
                    f"{table_name_changes}/{partition_levels}"
                )
            if editable is True and unedited is False and len(table_files_changes) > 0:
                df_changes = self.query_changes(sql_query_raw, partition_select)
                if len(df_changes) == 0 or df_changes is None:
                    pass
                else:
                    df_changes["datetime"] = pd.to_datetime(
                        df_changes["datetime"]
                    ).apply(lambda x: x.timestamp())
                    df_changes.sort_values("datetime", ascending=False, inplace=True)
                    df_changes.drop_duplicates(
                        subset="uuid", keep="first", inplace=True
                    )
                    df_cols = [col for col in df.columns if col != "uuid"]

                    insert_rows = df_changes[df_changes["operation"] == "insert"]
                    delete_rows = df_changes[df_changes["operation"] == "delete"]
                    reset_rows = df_changes[df_changes["operation"] == "reset"]
                    
                    # Fungerer foreløpig ikke!
                    if not insert_rows.empty:
                        for _, row in insert_rows.iterrows():
                            if pd.isnull(row["uuid_x"]):
                                new_uuid = str(uuid4())
                                new_row = {col: row[col + "_y"] for col in df_cols}
                                new_row["uuid"] = new_uuid
                                df = df.append(new_row, ignore_index=True)
                    if not delete_rows.empty:
                        for _, row in delete.iterrows():
                            df = df[df["uuid"] != row["uuid"]]

                    common_cols = df.columns.intersection(df_changes.columns)
                    update_df = df_changes[common_cols]
                    df.update(update_df)
            for p in partitions:
                try:
                    df[p] = df[p].astype(str)
                except KeyError:
                    pass
            return df
        elif parsed_query["operation"] == "UPDATE":
            if editable is False:
                raise Exception(f"The table {table_name} is not editable!")
            try:
                columns = parsed_query["columns"]
            except Exception:
                columns = None
            if columns == ["*"]:
                columns = None
            table_name = parsed_query["table_name"]
            where_clause = parsed_query["where_clause"]
            instance_name = self.eimerdb_name
            table_config = self.tables[table_name]
            partitions = table_config["partition_columns"]
            bucket_name = table_config["bucket"]
            partitions_len = len(partitions)
            partition_levels = "**/" * partitions_len + "*"
            
            df = self.query(f"SELECT * FROM {table_name} WHERE {where_clause}", partition_select)
            df["user"] = get_initials()
            df["datetime"] = get_datetime()
            df["operation"] = "update"
            dataset = pa.Table.from_pandas(df)
            con = duckdb.connect()
            con.execute(f"CREATE TABLE updates AS FROM dataset WHERE {where_clause}")
            sql_query = sql_query.replace(f"UPDATE {table_name}", "UPDATE updates")
            con.execute(sql_query)
            df_updates = con.table("updates").df()
            
            df_updates_len = len(df_updates)

            table_path = self.tables[table_name]["table_path"] + "_changes"
            fs = FileClient.get_gcs_file_system()
            update_table = pa.Table.from_pandas(df_updates)

            uuid = uuid4()
            filename = f"commit_{uuid}_{{i}}.parquet"

            pq.write_to_dataset(
                update_table,
                root_path=f"gs://{self.bucket}/{table_path}",
                partition_cols=partitions,
                basename_template=filename,
                filesystem=fs,
            )
            return print(f"{df_updates_len} rows updated by {get_initials()}")
    def query_changes(self, sql_query, partition_select=None, unedited=False):
        parsed_query = parse_sql_query(sql_query)
        table_name = parsed_query["table_name"]
        table_config = self.tables[table_name]
        editable = table_config["editable"]
        instance_name = self.eimerdb_name
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
                df_changes = pd.DataFrame()
            if no_changes is not True:
                table_files_changes = [obj for obj in table_files_changes if obj.count("/") == max_depth]
                if partition_select is not None:
                    filtered_files = []
                    for file in table_files_changes:
                        parts = file.split("/")

                        all_matches = True

                        for key, values in partition_select.items():
                            match_found = any(f"{key}={value}" in parts for value in values)

                            if not match_found:
                                all_matches = False
                                break
                        if all_matches:
                            filtered_files.append(file)
                        table_files_changes = filtered_files
                dataset = pq.read_table(table_files_changes, filesystem=fs, columns=columns)
                sql_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")
                if columns is not None and editable is True and unedited is False:
                    sql_query = sql_query.replace(" FROM", ", uuid FROM")

                con = duckdb.connect()
                df_changes = con.execute(sql_query).df()

            table_name_changes_all = table_name + "_changes_all"
            table_files_changes_all = fs.glob(
                f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name_changes_all}/{partition_levels}"
            )
            try:
                max_depth = max(obj.count("/") for obj in table_files_changes_all)
                no_changes_all = False
            except ValueError:
                no_changes_all = True
                df_changes_all = pd.DataFrame()
            if no_changes_all is not True:
                table_files_changes_all = [obj for obj in table_files_changes_all if obj.count("/") == max_depth]
                if partition_select is not None:
                    filtered_files = []
                    for file in table_files_changes_all:
                        parts = file.split("/")

                        all_matches = True

                        for key, values in partition_select.items():
                            match_found = any(f"{key}={value}" in parts for value in values)

                            if not match_found:
                                all_matches = False
                                break
                        if all_matches:
                            filtered_files.append(file)
                        table_files_changes_all = filtered_files
                dataset = pq.read_table(table_files_changes_all, filesystem=fs, columns=columns)
                sql_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")
                if columns is not None and editable is True and unedited is False:
                    sql_query = sql_query.replace(" FROM", ", uuid FROM")

                con = duckdb.connect()
                df_changes_all = con.execute(sql_query).df()

            df = pd.concat([df_changes_all, df_changes])
            return df

    def get_changes(self, table_name):
        fs = FileClient.get_gcs_file_system()
        table_changes = table_name + "_changes"
        bucket = self.bucket
        path = self.tables[table_name]["table_path"]
        dataset = ds.dataset(
            f"{bucket}/{path}_changes/",
            format="parquet",
            partitioning="hive",
            filesystem=fs,
        )
        df_changes = dataset.to_table().to_pandas()
        return df_changes


    def merge_changes(self, table_name):
        fs = FileClient.get_gcs_file_system()
        df_changes = self.get_changes(table_name)

        token = AuthClient.fetch_google_credentials()
        client = storage.Client(credentials=token)
        bucket = client.bucket(self.bucket)
        path = self.tables[table_name]["table_path"]
        partitions = self.tables[table_name]["partition_columns"]
        source_folder = self.tables[table_name]["table_path"] + "_changes"
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
        print("The changes were successfully merged into one file per partition!")


    def merge_changes_into_main(self, table_name):
        if self.is_admin is True:
            merged = self.query(
                f"SELECT * FROM {table_name}", partition_select=None, unedited=False
            )
            self.main_table_insert(table_name, merged, raw=False)
            partitions = self.tables[table_name]["partition_columns"]
            partition_levels = "**/" * len(partitions) + "*"
            fs = FileClient.get_gcs_file_system()
            table_files = fs.glob(
                f"gs://ssb-prod-mva-melding-data-produkt/eimerdb/mvabasen/hovedtabell_changes/{partition_levels}"
            )
            max_depth = max(obj.count("/") for obj in table_files)
            files_to_move = [obj for obj in table_files if obj.count("/") == max_depth]

            for file in files_to_move:
                moved_file = file.replace("hovedtabell_changes", "hovedtabell_changes_all")
                fs.mv(f"gs://{file}", f"gs://{moved_file}")
            print("Changes merged into main successfully!")

