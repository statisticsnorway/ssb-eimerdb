import unittest
from unittest.mock import Mock

import pyarrow as pa

from eimerdb.query import filter_partitions
from eimerdb.query import get_partitioned_files
from eimerdb.query import update_pyarrow_table


class TestGetPartitionedFiles(unittest.TestCase):

    def setUp(self) -> None:
        self.table_name = "table1"
        self.instance_name = "example_instance"
        self.table_config = {
            "partition_columns": ["date"],
            "bucket": "example_bucket",
        }
        self.suffix = "_suffix"
        self.fs = Mock()
        self.partition_select = {"partition_col1": ["value1"]}

    #
    # START get_partitioned_files
    #

    def test_get_partitioned_files_no_partition_select(self) -> None:
        # Setup mocks
        self.fs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2",
        ]

        # Call the function under test
        files = get_partitioned_files(
            table_name=self.table_name,
            instance_name=self.instance_name,
            table_config=self.table_config,
            suffix=self.suffix,
            fs=self.fs,
        )

        # Assert result
        self.assertEqual(
            [
                "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2"
            ],
            files,
        )

    def test_get_partitioned_files_with_partition_select(self) -> None:
        # Setup mocks
        self.fs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value1",
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2",
        ]

        # Call the function under test
        files = get_partitioned_files(
            table_name=self.table_name,
            instance_name=self.instance_name,
            table_config=self.table_config,
            suffix=self.suffix,
            fs=self.fs,
            partition_select=self.partition_select,
        )

        # Assert result
        self.assertEqual(
            [
                "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value1"
            ],
            files,
        )

    def test_get_partitioned_files_with_partition_select_and_unedited(self) -> None:
        # Setup mocks
        self.fs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/table1_suffix/partition_col1=value1",
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2",
        ]

        # Call the function under test
        files = get_partitioned_files(
            table_name=self.table_name,
            instance_name=self.instance_name,
            table_config=self.table_config,
            suffix=self.suffix,
            fs=self.fs,
            partition_select=self.partition_select,
            unedited=True,
        )

        # Assert result
        self.assertEqual(
            [
                "gs://example_bucket/eimerdb/example_instance/table1_suffix/partition_col1=value1"
            ],
            files,
        )

        # Assert mocks
        self.fs.glob.assert_called_with(
            "gs://example_bucket/eimerdb/example_instance/table1_suffix/**/*"
        )

    #
    # START filter_partitions
    #

    def test_filter_partitions(self) -> None:
        table_files = [
            "file1/partition_col1=value1/partition_col2=value2",
            "file3/partition_col1=value2/partition_col2=value3",
        ]

        # Call the function under test
        filtered_files = filter_partitions(
            table_files=table_files, partition_select=self.partition_select
        )

        # Assert result
        self.assertEqual(
            [
                "file1/partition_col1=value1/partition_col2=value2",
            ],
            filtered_files,
        )

    # noinspection PyArgumentList
    def test_update_pyarrow_table(self):
        # Create the original PyArrow table
        original_schema = pa.schema(
            [pa.field("row_id", pa.string()), pa.field("value", pa.int32())]
        )

        original_data = [
            pa.array(["id1", "id2", "id3"]),
            pa.array([1, 2, 3]),
        ]
        original_table = pa.Table.from_arrays(original_data, schema=original_schema)

        # Create the changes PyArrow table
        changes_schema = pa.schema(
            [
                pa.field("row_id", pa.string()),
                pa.field("value", pa.int32()),
                pa.field("datetime", pa.timestamp("ns")),
                pa.field("operation", pa.string()),
                pa.field("user", pa.string()),
            ]
        )
        changes_data = [
            pa.array(["id2", "id3", "id4"]),
            pa.array([1, 2, 3]),
            pa.array(
                [1640995200000000000, 1640995200000000000, 1640995200000000000]
            ),  # Timestamps in nanoseconds
            pa.array(["update", "delete", "update"]),
            pa.array(["user2", "user3", "user4"]),
        ]
        changes_table = pa.Table.from_arrays(changes_data, schema=changes_schema)

        # Call the function under test
        updated_table = update_pyarrow_table(
            df=original_table,
            df_changes=changes_table,
            timetravel=None,
        )

        # Assert the expected output

        expected_schema = pa.schema(
            [
                pa.field("row_id", pa.string()),
                pa.field("value", pa.int32()),
            ]
        )

        expected_data = [
            pa.array(["id1", "id2", "id4"]),
            pa.array([1, 1, 3]),  # Value of 'id3' deleted
        ]
        expected_output = pa.Table.from_arrays(expected_data, schema=expected_schema)

        # Assert the table contents are equal
        self.assertTrue(updated_table.equals(expected_output))
