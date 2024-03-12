import unittest
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from google.cloud.storage import Blob
from parameterized import parameterized  # type: ignore

from eimerdb.instance import EimerDBInstance


class TestEimerDBInstanceAdminUser(unittest.TestCase):
    @patch("eimerdb.instance.get_json")
    @patch("eimerdb.instance.get_initials", return_value="admin_user")
    def setUp(self, _: Mock, mock_get_json: Mock) -> None:
        mock_get_json.side_effect = [
            {
                "eimerdb_name": "test_eimerdb",
                "path": "path/to/config",
                "eimer_path": "path/to/eimer",
                "created_by": "admin_user",
                "time_created": "2024-03-09T12:00:00Z",
            },
            {"admin_user": "admin"},
            {"admin_user": {"admin_group": ["admin_user"]}},
            {
                "table1": {
                    "bucket": "test_bucket",
                    "created_by": "admin_user",
                    "editable": True,
                    "partition_columns": None,
                    "schema": [
                        {"label": "Unique row ID", "name": "row_id", "type": "string"},
                        {"label": "Field 1", "name": "field1", "type": "int8"},
                    ],
                    "table_path": "path/to/eimer/table1",
                },
                "table2": {
                    "bucket": "test_bucket",
                    "created_by": "admin_user",
                    "editable": False,
                    "partition_columns": None,
                    "schema": [
                        {"label": "Unique row ID", "name": "row_id", "type": "string"},
                        {"label": "Field 1", "name": "field1", "type": "int8"},
                    ],
                    "table_path": "path/to/eimer/table2",
                },
            },
        ]

        self.instance = EimerDBInstance("test_bucket", "test_eimer")

    def test_init_admin(self) -> None:
        self.assertEqual(self.instance.bucket, "test_bucket")
        self.assertEqual(self.instance.eimerdb_name, "test_eimerdb")
        self.assertEqual(self.instance.path, "path/to/config")
        self.assertEqual(self.instance.eimer_path, "path/to/eimer")
        self.assertEqual(self.instance.created_by, "admin_user")
        self.assertEqual(self.instance.time_created, "2024-03-09T12:00:00Z")
        self.assertEqual(self.instance.users, {"admin_user": "admin"})
        self.assertEqual(
            self.instance.role_groups, {"admin_user": {"admin_group": ["admin_user"]}}
        )
        self.assertEqual(True, self.instance.is_admin)

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_add_user_success(
        self, mock_storage_client: Mock, mock_fetch_credentials: Mock
    ) -> None:
        # Setup
        mock_credentials = Mock()
        mock_fetch_credentials.return_value = mock_credentials
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        # Test
        self.instance.add_user("new_user", "user")

        # Assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("path/to/eimer/config/users.json")
        mock_blob.upload_from_string.assert_called_once()

    def test_add_user_user_exists(self) -> None:
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.add_user("admin_user", "admin")
        self.assertEqual("User admin_user already exists!", str(context.exception))

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_remove_user_success(
        self, mock_storage_client: Mock, mock_fetch_credentials: Mock
    ) -> None:
        # Setup
        mock_credentials = Mock()
        mock_fetch_credentials.return_value = mock_credentials
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        # Test
        self.instance.remove_user("admin_user")

        # Assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("path/to/eimer/config/users.json")
        mock_blob.upload_from_string.assert_called_once()

    def test_remove_user_user_not_exists(self) -> None:
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.remove_user("new_user")
        self.assertEqual("User new_user does not exist.", str(context.exception))

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_create_table_admin(
        self, mock_storage_client: Mock, mock_fetch_credentials: Mock
    ) -> None:
        # Setup
        mock_credentials = Mock()
        mock_fetch_credentials.return_value = mock_credentials

        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        table_name = "table2"
        schema = [{"name": "field1", "type": "int8", "label": "Field 1"}]

        # Test
        self.instance.create_table(table_name, schema)

        # Assertions
        self.assertEqual(
            self.instance.tables,
            {
                "table1": {
                    "bucket": "test_bucket",
                    "created_by": "admin_user",
                    "editable": True,
                    "partition_columns": None,
                    "schema": [
                        {"label": "Unique row ID", "name": "row_id", "type": "string"},
                        {"label": "Field 1", "name": "field1", "type": "int8"},
                    ],
                    "table_path": "path/to/eimer/table1",
                },
                table_name: {
                    "bucket": "test_bucket",
                    "created_by": "user",
                    "editable": True,
                    "partition_columns": None,
                    "schema": [
                        {"label": "Unique row ID", "name": "row_id", "type": "string"},
                        {"label": "Field 1", "name": "field1", "type": "int8"},
                    ],
                    "table_path": "path/to/eimer/table2",
                },
            },
        )

        # Mock assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("path/to/eimer/config/tables.json")
        mock_blob.upload_from_string.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    def test_main_table_insert(self, mock_get_gcs_file_system: Mock) -> None:
        mock_fs = mock_get_gcs_file_system.return_value

        # Sample DataFrame for testing
        df = pd.DataFrame({"row_id": ["1", "2", "2"], "field1": [1, 2, 3]})

        expected_calls = [
            call(
                table=ANY,
                root_path="gs://test_bucket/path/to/eimer/table1",
                partition_cols=None,
                basename_template=ANY,
                filesystem=mock_fs,
                schema=ANY,
            ),
            call(
                table=ANY,
                root_path="gs://test_bucket/path/to/eimer/table1_raw",
                partition_cols=None,
                basename_template=ANY,
                filesystem=mock_fs,
            ),
        ]
        with patch("eimerdb.instance.pq.write_to_dataset") as mock_write_to_dataset:
            # Call the method under test
            self.instance.insert(table_name="table1", df=df)
            mock_write_to_dataset.assert_has_calls(expected_calls)

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.ds.dataset")
    def test_get_changes(self, mock_dataset: Mock, _: Mock) -> None:
        schema_fields = [
            pa.field("row_id", pa.string(), metadata={"label": "Unique row ID"}),
            pa.field("field1", pa.int8(), metadata={"label": "Field1"}),
        ]

        schema_fields.extend(
            [
                pa.field("user", pa.string()),
                pa.field("datetime", pa.string()),
                pa.field("operation", pa.string()),
            ]
        )

        schema = pa.schema(schema_fields)

        expected_table = pa.Table.from_pydict(
            {
                "row_id": ["1"],
                "field1": [1],
                "user": ["user42"],
                "datetime": ["2024-03-12T12:00:00"],
                "operation": ["insert"],
            },
            schema=schema,
        )

        dataset = Mock(spec=ds.Dataset)
        dataset.to_table.return_value = expected_table
        mock_dataset.return_value = dataset

        # Call the get_changes method
        changes_df = self.instance.get_changes("table1")

        # Assert that ds.dataset is called with the correct arguments
        mock_dataset.assert_called_once_with(
            "test_bucket/path/to/eimer/table1_changes/",
            format="parquet",
            partitioning="hive",
            schema=self.instance._get_arrow_schema("table1", True),
            filesystem=ANY,
        )

        # Assert that the returned DataFrame is the same as the mock DataFrame
        self.assertIs(expected_table, changes_df)

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.ds.dataset")
    @parameterized.expand(  # type: ignore
        [
            (True,),
            (False,),
        ]
    )
    def test_get_inserts(self, mock_dataset: Mock, raw: bool) -> None:
        schema_fields = [
            pa.field("row_id", pa.string(), metadata={"label": "Unique row ID"}),
            pa.field("field1", pa.int8(), metadata={"label": "Field1"}),
        ]

        if not raw:
            schema_fields.extend(
                [
                    pa.field("user", pa.string()),
                    pa.field("datetime", pa.string()),
                    pa.field("operation", pa.string()),
                ]
            )

        schema = pa.schema(schema_fields)

        expected_data = {
            "row_id": ["1"],
            "field1": [1],
        }

        if not raw:
            expected_data.update(
                {
                    "user": ["user42"],
                    "datetime": ["2024-03-12T12:00:00"],
                    "operation": ["insert"],
                }
            )

        expected_table = pa.Table.from_pydict(expected_data, schema=schema)

        dataset = Mock(spec=ds.Dataset)
        dataset.to_table.return_value = expected_table
        mock_dataset.return_value = dataset

        inserts_table = self.instance.get_inserts("table1", raw=raw)

        suffix = "_raw" if raw else ""

        mock_dataset.assert_called_once_with(
            f"test_bucket/path/to/eimer/table1{suffix}/",
            format="parquet",
            partitioning="hive",
            schema=self.instance._get_arrow_schema("table1", raw),
            filesystem=ANY,
        )

        self.assertIs(expected_table, inserts_table)

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.uuid4")
    @patch("eimerdb.instance.pq.write_to_dataset")
    @patch("eimerdb.instance.EimerDBInstance.get_changes")
    def test_combine_changes(
        self,
        mock_get_changes: Mock,
        mock_write_to_dataset: Mock,
        mock_uuid4: Mock,
        _: Mock,
        mock_client: Mock,
        mock_fetch_credentials: Mock,
    ) -> None:
        # Mock the return value of get_changes
        schema_fields = [
            pa.field("row_id", pa.string(), metadata={"label": "Unique row ID"}),
            pa.field("field1", pa.int8(), metadata={"label": "Field1"}),
        ]

        schema_fields.extend(
            [
                pa.field("user", pa.string()),
                pa.field("datetime", pa.string()),
                pa.field("operation", pa.string()),
            ]
        )

        schema = pa.schema(schema_fields)

        expected_table = pa.Table.from_pydict(
            {
                "row_id": ["1"],
                "field1": [1],
                "user": ["user42"],
                "datetime": ["2024-03-12T12:00:00"],
                "operation": ["insert"],
            },
            schema=schema,
        )

        mock_get_changes.return_value = expected_table

        # Mock the return values of other dependencies
        mock_uuid4.return_value = "mocked_uuid"

        blob_1 = Mock(spec=Blob)
        blob_2 = Mock(spec=Blob)

        mock_fetch_credentials.return_value = "token"
        mock_client.return_value.bucket.return_value.list_blobs.return_value = [
            blob_1,
            blob_2,
        ]

        # Call the merge_changes method
        self.instance.combine_changes("table1")

        # Assert that the dependencies are called with the expected arguments
        mock_write_to_dataset.assert_called_once_with(
            table=mock_get_changes.return_value,
            root_path="gs://test_bucket/path/to/eimer/table1_changes",
            partition_cols=None,
            basename_template="merged_commit_mocked_uuid_{i}.parquet",
            schema=self.instance._get_arrow_schema("table1", True),
            filesystem=ANY,
        )
        blob_1.delete.assert_called_once()
        blob_2.delete.assert_called_once()

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.uuid4")
    @patch("eimerdb.instance.pq.write_to_dataset")
    @patch("eimerdb.instance.EimerDBInstance.get_changes")
    @parameterized.expand(  # type: ignore
        [
            (True,),
            (False,),
        ]
    )
    def test_combine_inserts(
        self,
        mock_get_inserts: Mock,
        mock_write_to_dataset: Mock,
        mock_uuid4: Mock,
        _: Mock,
        mock_client: Mock,
        mock_fetch_credentials: Mock,
        raw: bool,
    ) -> None:
        # Mock the return value of get_changes
        schema_fields = [
            pa.field("row_id", pa.string(), metadata={"label": "Unique row ID"}),
            pa.field("field1", pa.int64(), metadata={"label": "Field1"}),
        ]

        if not raw:
            schema_fields.extend(
                [
                    pa.field("user", pa.string()),
                    pa.field("datetime", pa.string()),
                    pa.field("operation", pa.string()),
                ]
            )

        schema = pa.schema(schema_fields)

        expected_data = {
            "row_id": ["1"],
            "field1": [1],
        }

        if not raw:
            expected_data.update(
                {
                    "user": ["user42"],
                    "datetime": ["2024-03-12T12:00:00"],
                    "operation": ["insert"],
                }
            )

        expected_table = pa.Table.from_pydict(expected_data, schema=schema)

        mock_get_inserts.return_value = expected_table

        # Mock the return values of other dependencies
        mock_uuid4.return_value = "mocked_uuid"

        blob_1 = Mock(spec=Blob)
        blob_2 = Mock(spec=Blob)

        mock_fetch_credentials.return_value = "token"
        mock_client.return_value.bucket.return_value.list_blobs.return_value = [
            blob_1,
            blob_2,
        ]

        suffix = "_raw" if raw else ""

        self.instance.combine_inserts("table1", raw)

        mock_write_to_dataset.assert_called_once_with(
            table=mock_get_inserts.return_value,
            root_path=f"gs://test_bucket/path/to/eimer/table1{suffix}",
            partition_cols=None,
            basename_template="merged_insert_mocked_uuid_{i}.parquet",
            schema=self.instance._get_arrow_schema("table1", raw),
            filesystem=ANY,
        )
        blob_1.delete.assert_called_once()
        blob_2.delete.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.pq.read_table")
    @patch("eimerdb.instance.pa.concat_tables")
    @patch("eimerdb.instance.pd.concat")
    @patch("eimerdb.instance.pd.DataFrame")
    def test_query_changes(
        self,
        mock_pd_dataframe: Mock,
        mock_pd_concat: Mock,
        mock_pa_concat_tables: Mock,
        mock_pq_read_table: Mock,
        mock_fileclient_gcs: Mock,
    ) -> None:
        # Set up mocks
        mock_pd_dataframe.return_value = Mock()
        mock_pd_concat.return_value = Mock()
        mock_pa_concat_tables.return_value = Mock()

        mock_pq_read_table.return_value = Mock()

        mock_fileclient_gcs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/example_table_suffix/date=2022-01-01/file1.parquet",
            "gs://example_bucket/eimerdb/example_instance/example_table_suffix/date=2022-01-01/file2.parquet",
        ]

        # Test SELECT
        result = self.instance.query_changes("SELECT * FROM table1 WHERE condition")
        self.assertIsInstance(result, Mock)

        # Test UPDATE
        result = self.instance.query_changes(
            "UPDATE table1 SET row_id = 1 WHERE row_id = 2"
        )
        self.assertIsNone(result)

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.get_partitioned_files")
    @patch("eimerdb.instance.pq.read_table")
    def test__query_select(
        self,
        mock_pq_read_table: Mock,
        mock_get_partitioned_files: Mock,
        mock_gcs_filesystem: Mock,
    ) -> None:
        # Mock input parameters
        parsed_query = {"table_name": ["table1"]}
        sql_query = "SELECT * FROM table1"
        fs_mock = mock_gcs_filesystem.return_value
        partition_select = None
        unedited = False
        output_format = "pandas"

        # Mock return values
        mock_pq_read_table.return_value = pd.DataFrame({"row_id": [1, 2, 3]})

        # Call the method
        result = self.instance._query_select(
            fs=fs_mock,
            parsed_query=parsed_query,
            sql_query=sql_query,
            partition_select=partition_select,
            unedited=unedited,
            output_format=output_format,
        )

        assert result.equals(pd.DataFrame({"row_id": [1, 2, 3]}))

        mock_get_partitioned_files.assert_called_once_with(
            table_name="table1",
            instance_name="test_eimerdb",
            table_config=self.instance.tables["table1"],
            suffix="_raw",
            fs=fs_mock,
            partition_select=partition_select,
            unedited=unedited,
        )
        mock_pq_read_table.assert_called_once()

    #
    # START _query_update
    #

    @unittest.skip("FIX ME")
    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.EimerDBInstance.query")
    def test__query_update_success(self, mock_query: Mock, _: Mock) -> None:
        with patch("eimerdb.instance.duckdb.connect"):
            # Set up test data
            # self.instance._get_arrow_schema = MagicMock(return_value=None)

            # Mock the query method
            mock_query.return_value = pd.DataFrame(
                {"field1": [1, 2, 3], "row_id": ["1", "2", "3"]}
            )

            parsed_query = {
                "operation": "UPDATE",
                "set_clause": "field1='1'",
                "table_name": "table1",
                "where_clause": "row_id=1",
            }

            # Call the method
            result = self.instance._query_update(
                MagicMock(), parsed_query, "UPDATE table1 SET field1=4 WHERE row_id='1'"
            )

            # Assertions
            self.assertIn("rows updated by user", result)

    def test__query_update_non_editable_table(self) -> None:
        parsed_query = {
            "operation": "UPDATE",
            "set_clause": "field1='1'",
            "table_name": "table2",
            "where_clause": "row_id=1",
        }

        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance._query_update(
                MagicMock(), parsed_query, "UPDATE table2 SET field1='1' WHERE row_id=1"
            )
        self.assertEqual(
            "The table table2 is not editable!",
            str(context.exception),
        )

    #
    # START query
    #

    def test_query_invalid_output_format_expect_exception(self) -> None:
        with self.assertRaises(ValueError) as context:
            self.instance.query("SELECT * FROM table1", output_format="invalid")
        self.assertEqual(
            "Invalid output format: invalid. Supported formats: pandas, arrow.",
            str(context.exception),
        )

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.parse_sql_query")
    def test_query_invalid_sql_operation_expect_exception(
        self, mock_parse_sql_query: Mock, _: Mock
    ) -> None:
        mock_parse_sql_query.return_value = {
            "operation": "INVALID OPERATION",
            "table_name": "table1",
        }

        with self.assertRaises(ValueError) as context:
            self.instance.query("INVALID OPERATION")
        self.assertEqual(
            "Unsupported SQL operation: INVALID OPERATION.",
            str(context.exception),
        )

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.EimerDBInstance._query_select")
    def test_query_select(self, mock_query_select: Mock, _: Mock) -> None:
        mock_query_select.return_value = pd.DataFrame({"row_id": [1, 2, 3]})

        self.instance.query("SELECT * FROM table1")

        mock_query_select.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.EimerDBInstance._query_update")
    def test_query_update(self, mock_query_update: Mock, _: Mock) -> None:
        mock_query_update.return_value = "1 rows updated by user"

        result = self.instance.query(
            "UPDATE table1 SET col1='value' WHERE col2='value'"
        )

        assert result == "1 rows updated by user"
        mock_query_update.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.EimerDBInstance._query_delete")
    def test_query_delete(self, mock_query_delete: Mock, _: Mock) -> None:
        mock_query_delete.return_value = "1 rows deleted by user"

        result = self.instance.query("DELETE FROM table1 WHERE col='value'")

        assert result == "1 rows deleted by user"
        mock_query_delete.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    def test_query_drop_table_expect_exception(self, _: Mock) -> None:
        with self.assertRaises(ValueError) as context:
            self.instance.query("DROP TABLE table1")

        self.assertEqual(
            "Error parsing sql-query. Syntax error or query not supported.",
            str(context.exception),
        )
