from functions import (
    get_datetime,
    get_initials,
    get_json,
    arrow_schema_from_json,
    parse_sql_query,
    create_eimerdb,
)
from dapla import FileClient, AuthClient
from datetime import datetime
import os
from google.cloud import storage
import json
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


class EimerDBInstance:
    def __init__(self, bucket_name, eimer_name):
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
        if self.is_admin == True:
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
        if self.is_admin == True:
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
                print(f"The user '{username}' does not exist.")
        else:
            raise Exception("Cannot remove user. You are not an admin!")

    def create_table(self, table_name, schema, partition_columns=None, editable=True):
        if self.is_admin == True:
            token = AuthClient.fetch_google_credentials()
            client = storage.Client(credentials=token)
            bucket = client.bucket(self.bucket)
            arrow_schema_from_json(schema)
            schema = json.loads(schema)
            tables = self.tables
            initials = get_initials()
            new_table = {
                table_name: {
                    "created_by": initials,
                    "table_path": f"{self.eimer_path}/{table_name}",
                    "editable": editable,
                    "schema": schema,
                }
            }
            if partition_columns:
                new_table[table_name]["partition_columns"] = partition_columns
            else:
                new_table[table_name]["partition_columns"] = None
            tables.update(new_table)
            tables_blob_blob = bucket.blob(f"{self.eimer_path}/config/tables.json")
            tables_blob_blob.upload_from_string(
                data=json.dumps(tables), content_type="application/json"
            )
        else:
            raise Exception("Cannot create table. You are not an admin!")

    def main_table_insert(self, table_name, df):
        if self.is_admin == True:
            token = AuthClient.fetch_google_credentials()
            client = storage.Client(credentials=token)
            bucket = client.bucket(self.bucket)
            json_data = self.tables[table_name]
            json_schema = json.dumps(json_data["schema"])
            arrow_schema = arrow_schema_from_json(json.dumps(json_data["schema"]))

            table = pa.Table.from_pandas(df, schema=arrow_schema)
            fs = FileClient.get_gcs_file_system()

            table_path = json_data["table_path"]
            partitions = json_data["partition_columns"]
            filename = f"{table_name}_data_{{i}}"
            pq.write_to_dataset(
                table,
                root_path=f"gs://{self.bucket}/{table_path}",
                partition_cols=partitions,
                basename_template=filename,
                filesystem=fs,
            )
            print("Data successfully inserted!")
        else:
            Exception("Cannot insert into main table. You are not an admin!")

    def read_table(self, sql_query, partition_select=None):
        parsed_query = parse_sql_query(sql_query)
        try:
            columns = parsed_query["columns"]
        except:
            columns = None
        if columns == ["*"]:
            columns = None
        table_name = parsed_query["table_name"]
        sql_filter = parsed_query["sql_filter"]

        instance_name = self.eimerdb_name
        table_config = self.tables[table_name]
        partitions = table_config["partition_columns"]
        bucket_name = table_config["bucket"]
        partitions_len = len(partitions)
        partition_levels = "**/" * partitions_len + "*"
        fs = FileClient.get_gcs_file_system()
        table_files = fs.glob(
            f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name}/{partition_levels}"
        )
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
        fs = FileClient.get_gcs_file_system()
        dataset = pq.read_table(table_files, filesystem=fs, columns=columns)
        if sql_query and dataset:
            sql_query = sql_query.replace(table_name, "dataset")

            con = duckdb.connect()
            df = con.execute(sql_query).df()
            return df