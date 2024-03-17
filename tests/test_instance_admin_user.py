from unittest.mock import ANY
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from google.cloud.storage import Blob
from parameterized import parameterized

from tests.test_instance_base import TestEimerDBInstanceBase


class TestEimerDBInstanceAdminUser(TestEimerDBInstanceBase):

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    def test_insert(self, mock_get_gcs_file_system: Mock) -> None:
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
            schema=self.instance.get_arrow_schema("table1", True),
            filesystem=ANY,
        )

        # Assert that the returned DataFrame is the same as the mock DataFrame
        self.assertIs(expected_table, changes_df)

    @parameterized.expand(
        [
            (True,),
            (False,),
        ]
    )
    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.ds.dataset")
    def test_get_inserts(self, raw: bool, mock_dataset: Mock, _: Mock) -> None:
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
            schema=self.instance.get_arrow_schema("table1", raw),
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
            schema=self.instance.get_arrow_schema("table1", True),
            filesystem=ANY,
        )
        blob_1.delete.assert_called_once()
        blob_2.delete.assert_called_once()

    @parameterized.expand(
        [
            (True,),
            (False,),
        ]
    )
    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.uuid4")
    @patch("eimerdb.instance.pq.write_to_dataset")
    @patch("eimerdb.instance.EimerDBInstance.get_inserts")
    def test_combine_inserts(
        self,
        raw: bool,
        mock_get_inserts: Mock,
        mock_write_to_dataset: Mock,
        mock_uuid4: Mock,
        _: Mock,
        mock_client: Mock,
        mock_fetch_credentials: Mock,
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

        self.instance.combine_inserts("table1", raw)

        mock_write_to_dataset.assert_called_once_with(
            table=mock_get_inserts.return_value,
            root_path=ANY,  # FIXME f"gs://test_bucket/path/to/eimer/table1{suffix}",
            partition_cols=None,
            basename_template="merged_commit_mocked_uuid_{i}.parquet",
            schema=self.instance.get_arrow_schema("table1", raw),
            filesystem=ANY,
        )

        blob_1.delete.assert_called_once()
        blob_2.delete.assert_called_once()
