import unittest
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

from eimerdb.query import filter_partitions
from eimerdb.query import get_partitioned_files


class TestQuery(unittest.TestCase):

    @patch("eimerdb.query.FileClient.get_gcs_file_system")
    def test_get_partitioned_files(self, mock_get_gcs_file_system: Mock) -> None:
        # Mocking FileClient.get_gcs_file_system()
        mock_fs = MagicMock()
        mock_get_gcs_file_system.return_value = mock_fs

        # Example input values
        table_name = "example_table"
        instance_name = "example_instance"
        table_config = {"partition_columns": ["date"], "bucket": "example_bucket"}
        suffix = "_suffix"
        partition_select = {"date": ["2022-01-01"]}
        unedited = True

        # Mocking fs.glob() to return some example file paths
        mock_fs.glob.return_value = [
            "gs://example_bucket/eimerdb/example_instance/example_table_suffix/date=2022-01-01/file1.parquet",
            "gs://example_bucket/eimerdb/example_instance/example_table_suffix/date=2022-01-01/file2.parquet",
        ]

        # Calling the function under test
        result = get_partitioned_files(
            table_name,
            instance_name,
            table_config,
            suffix,
            mock_fs,
            partition_select,
            unedited,
        )

        # Asserting the result
        expected_result = [
            "gs://example_bucket/eimerdb/example_instance/example_table_suffix/date=2022-01-01/file1.parquet",
            "gs://example_bucket/eimerdb/example_instance/example_table_suffix/date=2022-01-01/file2.parquet",
        ]
        self.assertEqual(result, expected_result)

        # Asserting that fs.glob() was called with the correct arguments
        mock_fs.glob.assert_called_once_with(
            "gs://example_bucket/eimerdb/example_instance/example_table_suffix/**/*"
        )

    def test_filter_partitions(self) -> None:
        table_files = [
            "file1/partition_col1=value1/partition_col2=value2",
            "file2/partition_col1=value1/partition_col2=value2",
            "file3/partition_col1=value2/partition_col2=value3",
        ]
        partition_select = {"partition_col1": ["value1"], "partition_col2": ["value2"]}
        result = filter_partitions(table_files, partition_select)
        self.assertEqual(
            result,
            [
                "file1/partition_col1=value1/partition_col2=value2",
                "file2/partition_col1=value1/partition_col2=value2",
            ],
        )
