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
        self.fs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2",
        ]

        files = get_partitioned_files(
            self.table_name, self.instance_name, self.table_config, self.suffix, self.fs
        )

        self.assertEqual(
            [
                "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2"
            ],
            files,
        )

    def test_get_partitioned_files_with_partition_select(self) -> None:
        self.fs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value1",
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2",
        ]

        files = get_partitioned_files(
            self.table_name,
            self.instance_name,
            self.table_config,
            self.suffix,
            self.fs,
            self.partition_select,
        )

        self.assertEqual(
            [
                "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value1"
            ],
            files,
        )

    def test_get_partitioned_files_with_partition_select_and_unedited(self) -> None:
        self.fs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/table1_suffix/partition_col1=value1",
            "gs://example_bucket/eimerdb/example_instance/table1/partition_col1=value2",
        ]

        files = get_partitioned_files(
            self.table_name,
            self.instance_name,
            self.table_config,
            self.suffix,
            self.fs,
            self.partition_select,
            True,
        )

        self.assertEqual(
            [
                "gs://example_bucket/eimerdb/example_instance/table1_suffix/partition_col1=value1"
            ],
            files,
        )

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

        filtered_files = filter_partitions(table_files, self.partition_select)

        self.assertEqual(
            filtered_files,
            [
                "file1/partition_col1=value1/partition_col2=value2",
            ],
        )

    # noinspection PyArgumentList
    def test_update_pyarrow_table(self):
        # Create the original PyArrow table
        original_schema = pa.schema(
            [
                pa.field("row_id", pa.string()),
                pa.field("value", pa.int32()),
                pa.field("datetime", pa.timestamp("ns")),
                pa.field("operation", pa.string()),
                pa.field("user", pa.string()),
            ]
        )

        original_data = [
            pa.array(["id1", "id2", "id3"]),
            pa.array([1, 2, 3]),
            pa.array(
                [1640995200000000000, 1640995200000000000, 1640995200000000000]
            ),  # Timestamps in nanoseconds
            pa.array(["update", "update", "delete"]),
            pa.array(["user1", "user2", "user3"]),
        ]
        original_table = pa.Table.from_arrays(original_data, schema=original_schema)

        # Create the changes PyArrow table
        changes_schema = pa.schema(
            [
                pa.field("row_id", pa.string()),
                pa.field("datetime", pa.timestamp("ns")),
                pa.field("operation", pa.string()),
                pa.field("user", pa.string()),
            ]
        )
        changes_data = [
            pa.array(["id2", "id3", "id4"]),
            pa.array(
                [1640995200000000000, 1640995200000000000, 1640995200000000000]
            ),  # Timestamps in nanoseconds
            pa.array(["update", "delete", "update"]),
            pa.array(["user2", "user3", "user4"]),
        ]
        changes_table = pa.Table.from_arrays(changes_data, schema=changes_schema)

        # Call the function under test
        update_pyarrow_table(original_table, changes_table)

        # Stian: Kommenter inn resten
        # updated_table = update_pyarrow_table(original_table, changes_table)

        # Assert the expected output

        # expected_schema = pa.schema([
        #     pa.field("row_id", pa.string()),
        #     pa.field("value", pa.int32()),
        #     pa.field("datetime", pa.timestamp("ns")),
        #     pa.field("operation", pa.string()),
        #     pa.field("user", pa.string())
        # ])
        #
        # expected_data = [
        #     pa.array(["id1", "id2", "id4"]),
        #     pa.array([1, 2, None]),  # Value of 'id3' deleted
        #     pa.array([1640995200000000000, 1640995200000000000, 1640995200000000000]),  # Updated timestamps
        #     pa.array(["update", "update", "update"]),  # Deleted 'delete' operation
        #     pa.array(["user1", "user2", "user4"])  # Updated user
        # ]
        # expected_output = pa.Table.from_arrays(expected_data, schema=expected_schema)
        #
        # # Assert the table contents are equal
        # self.assertTrue(updated_table.equals(expected_output))
