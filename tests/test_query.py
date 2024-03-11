import unittest
from unittest.mock import Mock

from eimerdb.query import filter_partitions
from eimerdb.query import get_partitioned_files


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
