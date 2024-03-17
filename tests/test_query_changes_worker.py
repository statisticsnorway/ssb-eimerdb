from typing import Union
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
from parameterized import parameterized

from eimerdb.instance_query_changes_worker import QueryChangesWorker
from tests.test_instance_base import TestEimerDBInstanceBase


class TestQueryChangesWorker(TestEimerDBInstanceBase):

    worker_instance: QueryChangesWorker

    VALID_STAR_QUERY = "SELECT * FROM table1 WHERE row_id='1'"
    VALID_SELECT_QUERY = "SELECT field1 FROM table1 WHERE row_id='1'"

    def setUp(self) -> None:
        super().setUp()
        self.worker_instance = QueryChangesWorker(self.instance)

    def test_query_changes_sql_with_update_expect_exception(self):
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.worker_instance.query_changes(
                sql_query="UPDATE table1 SET col1=2 WHERE row_id='1'",
            )
        self.assertEqual("Operation UPDATE is not supported.", str(context.exception))

    @parameterized.expand(
        [
            (VALID_STAR_QUERY, 1),
        ]
    )
    def test_query_changes_pandas_all(
        self,
        sql_query: str,
        expected_rows: int,
    ) -> None:
        # Mock the file system
        mock_fs = Mock()

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
        mock_duckdb_query_result.arrow.return_value = pa.Table.from_pandas(expected_df)

        # Patching methods with mocks
        with patch(
            "eimerdb.instance_query_changes_worker.FileClient.get_gcs_file_system",
            return_value=mock_fs,
        ), patch(
            "eimerdb.instance_query_changes_worker.pq.read_table",
            return_value=mock_table_data,
        ), patch(
            "eimerdb.instance_query_changes_worker.duckdb.DuckDBPyConnection.query",
            return_value=mock_duckdb_query_result,
        ):
            result: Union[pd.DataFrame, pa.Table] = self.worker_instance.query_changes(
                sql_query=sql_query
            )

        # Assertions
        self.assertIsNotNone(result)
        assert len(result) == expected_rows
        assert mock_fs.glob.call_count == expected_rows
