from unittest.mock import ANY
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from google.cloud.storage import Blob
from parameterized import parameterized

from tests.test_instance_base import TestEimerDBInstanceBase


@pytest.fixture(autouse=True)
def patch_get_gcs_file_system():
    with patch("eimerdb.instance.FileClient.get_gcs_file_system"):
        yield


@pytest.fixture(autouse=True)
def patch_fetch_google_credentials():
    with patch(
        "eimerdb.instance.AuthClient.fetch_google_credentials", return_value="token"
    ):
        yield


@pytest.fixture(autouse=True)
def patch_uuid4():
    with patch("eimerdb.instance.uuid4", return_value="mocked_uuid"):
        yield


class TestEimerDBInstanceAdminUser(TestEimerDBInstanceBase):

    def test_insert_given_valid_data_expect_list_of_row_ids(self) -> None:
        # Sample DataFrame for testing
        df = pd.DataFrame({"field1": [1, 2]})

        with patch("eimerdb.instance.pq.write_to_dataset") as mock_write_to_dataset:
            expected_calls = [
                call(
                    table=ANY,
                    root_path="gs://test_bucket/path/to/eimer/table1",
                    partition_cols=None,
                    basename_template=ANY,
                    filesystem=ANY,
                    schema=ANY,
                ),
                call(
                    table=ANY,
                    root_path="gs://test_bucket/path/to/eimer/table1_raw",
                    partition_cols=None,
                    basename_template=ANY,
                    filesystem=ANY,
                ),
            ]

            # Call the method under test
            row_ids = self.instance.insert(table_name="table1", df=df)

            # Assert the return value
            self.assertEqual(["mocked_uuid", "mocked_uuid"], row_ids)

            # Assert that the dependencies are called with the expected arguments
            mock_write_to_dataset.assert_has_calls(expected_calls)

    @staticmethod
    def _get_expected_table(raw: bool) -> pa.Table:
        schema_fields = [
            pa.field("row_id", pa.string(), metadata={"label": "Unique row ID"}),
            pa.field("field1", pa.int8(), metadata={"label": "Field1"}),
        ]
        expected_data = {
            "row_id": ["1"],
            "field1": [1],
        }

        if not raw:
            schema_fields.extend(
                [
                    pa.field("user", pa.string()),
                    pa.field("datetime", pa.string()),
                    pa.field("operation", pa.string()),
                ]
            )
            expected_data.update(
                {
                    "user": ["user42"],
                    "datetime": ["2024-03-12T12:00:00"],
                    "operation": ["insert"],
                }
            )

        return pa.Table.from_pydict(expected_data, schema=pa.schema(schema_fields))

    @parameterized.expand(
        [
            (True,),
            (False,),
        ]
    )
    def test_get_inserts_or_changes(self, raise_file_not_found_error: bool) -> None:
        expected_source_folder = "path/to/eimer/table1_changes"
        expected_table = self._get_expected_table(False)

        with patch("eimerdb.instance.ds.dataset") as mock_dataset:
            if raise_file_not_found_error:
                mock_dataset.side_effect = FileNotFoundError
            else:
                dataset = Mock(spec=ds.Dataset)
                dataset.to_table.return_value = expected_table
                mock_dataset.return_value = dataset

            # Call the method under test
            inserts_table = self.instance._get_inserts_or_changes(
                table_name="table1", source_folder=expected_source_folder, raw=False
            )

            # Assert that the dependencies are called with the expected arguments

            mock_dataset.assert_called_once_with(
                "test_bucket/path/to/eimer/table1_changes/",
                format="parquet",
                partitioning="hive",
                schema=self.instance.get_arrow_schema("table1", False),
                filesystem=ANY,
            )

            if raise_file_not_found_error:
                self.assertIsNone(inserts_table)
            else:
                self.assertIs(expected_table, inserts_table)

    def test_write_to_table_and_delete_blobs(self) -> None:
        with patch("eimerdb.instance.storage.Client") as mock_client, patch(
            "eimerdb.instance.pq.write_to_dataset"
        ) as mock_write_to_dataset:
            # Setup mocks
            expected_table = self._get_expected_table(False)
            mock_write_to_dataset.return_value = expected_table

            blob_1 = Mock(spec=Blob)
            blob_2 = Mock(spec=Blob)

            mock_client.return_value.bucket.return_value.list_blobs.return_value = [
                blob_1,
                blob_2,
            ]

            # Call the method under test
            self.instance._write_to_table_and_delete_blobs(
                table_name="table1",
                table=expected_table,
                source_folder="path/to/eimer/table1",
                raw=False,
            )

            # Assert that the dependencies are called with the expected arguments

            mock_write_to_dataset.assert_called_once_with(
                table=expected_table,
                root_path="gs://test_bucket/path/to/eimer/table1",
                partition_cols=None,
                basename_template="merged_commit_mocked_uuid_{i}.parquet",
                schema=self.instance.get_arrow_schema("table1", False),
                filesystem=ANY,
            )

            blob_1.delete.assert_called_once()
            blob_2.delete.assert_called_once()

    @parameterized.expand(
        [
            (False,),
            (True,),
        ]
    )
    def test_combine_changes(self, expect_table: bool) -> None:
        expected_source_folder = "path/to/eimer/table1_changes"

        with patch(
            "eimerdb.instance.EimerDBInstance._get_inserts_or_changes"
        ) as mock_get_inserts_or_changes, patch(
            "eimerdb.instance.EimerDBInstance._write_to_table_and_delete_blobs",
            return_value=None,
        ) as mock_write_to_table_and_delete_blobs:
            # Setup mocks
            mock_get_inserts_or_changes.return_value = (
                self._get_expected_table(False) if expect_table else None
            )

            # Call the merge_changes method
            self.instance.combine_changes("table1")

            # Mock asserts

            mock_get_inserts_or_changes.assert_called_once_with(
                table_name="table1", source_folder=expected_source_folder, raw=True
            )

            if not expect_table:
                mock_write_to_table_and_delete_blobs.assert_not_called()
                return

            # Assert that the dependencies are called with the expected arguments
            mock_write_to_table_and_delete_blobs.assert_called_once_with(
                table_name="table1",
                table=mock_get_inserts_or_changes.return_value,
                source_folder="path/to/eimer/table1_changes",
                raw=True,
            )

    @parameterized.expand(
        [
            (True, False),
            (False, False),
            (True, True),
            (False, True),
        ]
    )
    def test_combine_inserts(self, raw: bool, expect_table: bool) -> None:
        suffix = "_raw" if raw else ""
        expected_source_folder = "path/to/eimer/table1" + suffix

        with patch(
            "eimerdb.instance.EimerDBInstance._get_inserts_or_changes"
        ) as mock_get_inserts_or_changes, patch(
            "eimerdb.instance.EimerDBInstance._write_to_table_and_delete_blobs",
            return_value=None,
        ) as mock_write_to_table_and_delete_blobs:
            # Setup mocks
            mock_get_inserts_or_changes.return_value = (
                self._get_expected_table(raw) if expect_table else None
            )

            # Call the method under test
            self.instance.combine_inserts("table1", raw)

            # Mock asserts

            mock_get_inserts_or_changes.assert_called_once_with(
                table_name="table1", source_folder=expected_source_folder, raw=raw
            )

            if not expect_table:
                mock_write_to_table_and_delete_blobs.assert_not_called()
                return

            mock_write_to_table_and_delete_blobs.assert_called_once_with(
                table_name="table1",
                table=mock_get_inserts_or_changes.return_value,
                source_folder=expected_source_folder,
                raw=raw,
            )
