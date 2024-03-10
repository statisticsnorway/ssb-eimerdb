import unittest
from unittest.mock import ANY
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from google.cloud.storage import Blob

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
    @patch("eimerdb.instance.pq.write_to_dataset")
    def test_main_table_insert_raw_false(
        self, mock_write_to_dataset: Mock, mock_get_gcs_file_system: Mock
    ) -> None:
        # mock_from_pandas.return_value = Mock()
        mock_fs = mock_get_gcs_file_system.return_value
        # Mock pq.write_to_dataset
        mock_write_to_dataset.side_effect = [
            Mock(),  # For main table
            Mock(),  # For raw table
        ]

        # Sample DataFrame for testing
        df = pd.DataFrame({"row_id": ["1", "2", "2"], "field1": [1, 2, 3]})

        # Call the method under test
        self.instance.main_table_insert(table_name="table1", df=df, raw=False)

        # Assertions
        mock_write_to_dataset.assert_called_with(
            table=ANY,
            root_path="gs://test_bucket/path/to/eimer/table1",
            partition_cols=None,
            basename_template="table1_data_{i}.parquet",
            filesystem=mock_fs,
        )

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    def test_main_table_insert_raw_true(self, mock_get_gcs_file_system: Mock) -> None:
        mock_fs = mock_get_gcs_file_system.return_value

        # Sample DataFrame for testing
        df = pd.DataFrame({"row_id": ["1", "2", "2"], "field1": [1, 2, 3]})

        expected_calls = [
            call(
                table=ANY,
                root_path="gs://test_bucket/path/to/eimer/table1",
                partition_cols=None,
                basename_template="table1_data_{i}.parquet",
                filesystem=mock_fs,
            ),
            call(
                table=ANY,
                root_path="gs://test_bucket/path/to/eimer/table1_raw",
                partition_cols=None,
                basename_template="table1_data_{i}.parquet",
                filesystem=mock_fs,
            ),
        ]
        with patch("eimerdb.instance.pq.write_to_dataset") as mock_write_to_dataset:
            # Call the method under test
            self.instance.main_table_insert(table_name="table1", df=df, raw=True)
            mock_write_to_dataset.assert_has_calls(expected_calls)

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.ds.dataset")
    def test_get_changes(self, mock_dataset: Mock, _: Mock) -> None:
        expected_df = pd.DataFrame([{"row_id": "1", "field1": 1}])

        dataset = Mock(spec=ds.Dataset)
        dataset.to_table.return_value.to_pandas.return_value = expected_df
        mock_dataset.return_value = dataset

        # Call the get_changes method
        changes_df = self.instance.get_changes("table1")

        # Assert that ds.dataset is called with the correct arguments
        mock_dataset.assert_called_once_with(
            "test_bucket/path/to/eimer/table1_changes/",
            format="parquet",
            partitioning="hive",
            filesystem=ANY,
        )

        # Assert that the returned DataFrame is the same as the mock DataFrame
        self.assertIs(expected_df, changes_df)

    @patch("eimerdb.instance.storage.Client")
    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.uuid4")
    @patch("eimerdb.instance.pq.write_to_dataset")
    @patch("eimerdb.instance.EimerDBInstance.get_changes")
    def test_merge_changes(
        self,
        mock_get_changes: Mock,
        mock_write_to_dataset: Mock,
        mock_uuid4: Mock,
        _: Mock,
        mock_client: Mock,
    ) -> None:
        # Mock the return value of get_changes
        mock_get_changes.return_value = pd.DataFrame([{"row_id": "1", "field1": 1}])

        # Mock the return values of other dependencies
        mock_uuid4.return_value = "mocked_uuid"

        blob_1 = Mock(spec=Blob)
        blob_2 = Mock(spec=Blob)

        mock_client.return_value.bucket.return_value.list_blobs.return_value = [
            blob_1,
            blob_2,
        ]

        # Call the merge_changes method
        self.instance.merge_changes("table1")

        # Assert that the dependencies are called with the expected arguments
        mock_write_to_dataset.assert_called_once_with(
            table=pa.Table.from_pandas(mock_get_changes.return_value),
            root_path="gs://test_bucket/path/to/eimer/table1_changes",
            partition_cols=None,
            basename_template="merged_commit_mocked_uuid_{i}.parquet",
            filesystem=ANY,
        )
        blob_1.delete.assert_called_once()
        blob_2.delete.assert_called_once()
