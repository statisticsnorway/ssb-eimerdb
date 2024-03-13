from typing import Union
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
from parameterized import parameterized

from tests.test_instance_base import TestEimerDBInstanceBase


class TestEimerDBInstanceQueryChanges(TestEimerDBInstanceBase):

    VALID_QUERY = "SELECT * FROM table1 WHERE row_id='1'"

    def test_query_changes_invalid_output_format_expect_exception(self):
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.query_changes(
                sql_query=self.VALID_QUERY, output_format="invalid"
            )
        self.assertEqual("Invalid output format: invalid", str(context.exception))

    def test_query_changes_invalid_changes_output_expect_exception(self):
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.query_changes(
                sql_query=self.VALID_QUERY, changes_output="invalid"
            )
        self.assertEqual("Invalid changes output: invalid", str(context.exception))

    def test_query_changes_sql_with_update_expect_exception(self):
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.query_changes(
                sql_query="UPDATE table1 SET col1=2 WHERE row_id='1'",
            )
        self.assertEqual("Operation UPDATE is not supported.", str(context.exception))

    @parameterized.expand(
        [
            (True, "pandas", "all", 2),
            (False, "pandas", "all", 2),
            (False, "pandas", "recent", 1),
            (True, "arrow", "all", 2),
            (False, "arrow", "all", 2),
            (False, "arrow", "recent", 1),
        ]
    )
    def test_query_changes_pandas_all(
        self,
        unedited: bool,
        output_format: str,
        changes_output: str,
        expected_rows: int,
    ) -> None:
        # Mock the file system
        mock_fs = Mock()

        with patch(
            "eimerdb.instance.FileClient.get_gcs_file_system"
        ) as mock_get_gcs_file_system:
            mock_get_gcs_file_system.return_value = mock_fs

            mock_fs.glob.return_value = [
                "gs://bucket/eimerdb/eimerdb_name/table1_changes/part1"
                "gs://bucket/eimerdb/eimerdb_name/table1_changes_all/part1"
                "gs://bucket/eimerdb/eimerdb_name/table1_changes/part1",
                "gs://bucket/eimerdb/eimerdb_name/table1_changes/part2",
            ]

            # Mock the table data
            mock_table_data = Mock()
            mock_table_data.num_rows = 2  # Mocking non-empty table

            mock_table_data_df = Mock()
            mock_table_data_df.return_value = mock_table_data

            expected_df = pd.DataFrame.from_records(
                [
                    {
                        "row_id": "1",
                        "field1": 1,
                        "user": "user1",
                        "datetime": "2021-01-01T00:00:00Z",
                        "operation": "INSERT",
                    }
                ]
            )

            # Mock the duckdb query result
            mock_duckdb_query_result = Mock()
            mock_duckdb_query_result.df.return_value = expected_df
            mock_duckdb_query_result.arrow.return_value = pa.Table.from_pandas(
                expected_df
            )

            # Patching methods with mocks
            with patch(
                "eimerdb.instance.pq.read_table", return_value=mock_table_data
            ), patch(
                "eimerdb.instance.duckdb.query", return_value=mock_duckdb_query_result
            ):
                result: Union[pd.DataFrame, pa.Table] = self.instance.query_changes(
                    sql_query=self.VALID_QUERY,
                    unedited=unedited,
                    output_format=output_format,
                    changes_output=changes_output,
                )

            # Assertions
            self.assertIsNotNone(result)
            assert len(result) == expected_rows
            assert mock_fs.glob.call_count == expected_rows
