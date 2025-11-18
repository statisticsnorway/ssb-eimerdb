import datetime
import os
from typing import Any
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pytest
from dapla_auth_client.const import DaplaRegion
from parameterized import parameterized

from eimerdb.eimerdb_constants import DEFAULT_COMPRESSION
from eimerdb.eimerdb_constants import DEFAULT_MIN_ROWS_PER_GROUP
from eimerdb.instance_query_worker import QueryWorker
from tests.test_instance_base import TestEimerDBInstanceBase

VALID_STAR_QUERY = "SELECT * FROM table1 WHERE row_id='1'"
PARTITION_SELECT = {"table1": {"field1": [1]}}


@pytest.fixture(autouse=True)
def patch_uuid4():
    with patch("eimerdb.instance_query_worker.uuid4", return_value="mocked_uuid"):
        yield


class TestQueryWorker(TestEimerDBInstanceBase):

    def setUp(self) -> None:
        super().setUp()
        self.worker_instance = QueryWorker(self.instance)

    #
    # query_select
    #

    @parameterized.expand(
        [
            ("table2", False, "pandas", None),
            ("table1", False, "pandas", None),
            ("table1", False, "pandas", 0),
            ("table1", False, "pandas", 1),
            ("table1", True, "arrow", None),
        ]
    )
    def test_query_select_success(
        self,
        table_name: str,
        unedited: bool,
        output_format,
        changes_count: int | None,
    ) -> None:
        # Mock input parameters
        parsed_query = {"table_name": [table_name]}
        sql_query = f"SELECT * FROM {table_name}"
        partition_select = None

        with patch(
            "eimerdb.instance_query_worker.get_partitioned_files"
        ) as mock_get_partitioned_files, patch(
            "eimerdb.instance_query_worker.pq.read_table"
        ) as mock_pq_read_table, patch(
            "eimerdb.instance_query_worker.QueryWorker.query_changes"
        ) as mock_query_changes, patch(
            "eimerdb.instance_query_worker.update_pyarrow_table"
        ) as mock_update_pyarrow_table:
            expected_df = pd.DataFrame({"row_id": [1, 2, 3]})
            expected_table = pa.Table.from_pandas(expected_df)

            # Mock query_changes
            if changes_count is None:
                mock_query_changes.return_value = None
            else:
                mock_table = MagicMock()
                mock_table.num_rows = changes_count
                mock_query_changes.return_value = mock_table

            # Mock update_pyarrow_table
            mock_update_pyarrow_table.return_value = expected_table

            # Mock pq_read_table
            mock_pq_read_table.return_value = expected_table

            # Call the method
            result = self.worker_instance.query_select(
                parsed_query=parsed_query,
                sql_query=sql_query,
                partition_select=partition_select,
                unedited=unedited,
                output_format=output_format,
                fs=MagicMock(),
            )

            if output_format == "pandas":
                self.assertTrue(result.equals(expected_df))
            else:
                self.assertTrue(result.equals(expected_table))

            mock_get_partitioned_files.assert_called_once_with(
                table_name=table_name,
                instance_name="test_eimerdb",
                table_config=self.instance.tables[table_name],
                suffix="_raw",
                fs=ANY,
                partition_select=partition_select,
                unedited=unedited,
                timetravel=None,
            )
            mock_pq_read_table.assert_called_once()

            if table_name == "table1" and unedited is False:
                mock_query_changes.assert_called_once()

    #
    # query_update
    #

    def test_query_update_non_editable_table_expect_exception(self) -> None:
        parsed_query = {
            "operation": "UPDATE",
            "set_clause": "field1=1",
            "table_name": "table2",
            "where_clause": "row_id='1'",
        }

        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.worker_instance.query_update_or_delete(
                parsed_query=parsed_query,
                update_sql_query="UPDATE table2 SET field1='1' WHERE row_id=1",
                partition_select=None,
                fs=MagicMock(),
            )
        self.assertEqual(
            "The table table2 is not editable!",
            str(context.exception),
        )

    def test_query_update_success(self) -> None:
        # Setup mocks
        with patch(
            "eimerdb.instance_query_worker.pq.write_to_dataset"
        ) as mock_write_to_dataset, patch(
            "eimerdb.instance_query_worker.QueryWorker.query_select"
        ) as mock_query_method:
            mock_query_method.return_value = pd.DataFrame(
                {"field1": [1, 2, 3], "row_id": ["1", "2", "3"]}
            )

            parsed_query = {
                "operation": "UPDATE",
                "set_clause": "field1=1",
                "table_name": "table1",
                "where_clause": "row_id='1'",
            }

            # Call the method
            result = self.worker_instance.query_update_or_delete(
                parsed_query=parsed_query,
                update_sql_query="UPDATE table1 SET field1=4 WHERE row_id='1'",
                partition_select=None,
                fs=MagicMock(),
            )

            # Assertions
            if os.getenv("DAPLA_REGION") == DaplaRegion.DAPLA_LAB.value:
                dapla_user = os.getenv("DAPLA_USER")
                if dapla_user is not None:
                    user_split = dapla_user.split("@")[0]
                    self.assertEqual(f"1 rows updated by {user_split}", result)
                else:
                    self.assertEqual("1 rows updated by user", result)

            mock_write_to_dataset.assert_called_with(
                table=ANY,
                root_path="gs://test_bucket/path/to/eimer/table1_changes",
                partition_cols=None,
                basename_template="commit_mocked_uuid_{i}.parquet",
                schema=self.instance.get_arrow_schema("table1", True),
                compression=DEFAULT_COMPRESSION,
                min_rows_per_group=DEFAULT_MIN_ROWS_PER_GROUP,
                filesystem=ANY,
            )

    #
    # START _query_delete
    #

    def test_query_delete_non_editable_table_expect_exception(self) -> None:
        parsed_query = {
            "operation": "DELETE",
            "table_name": "table2",
            "where_clause": "row_id='1'",
        }

        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.worker_instance.query_update_or_delete(
                parsed_query=parsed_query,
                update_sql_query=None,
                partition_select=None,
                fs=MagicMock(),
            )
        self.assertEqual(
            "The table table2 is not editable!",
            str(context.exception),
        )

    def test_query_delete_success(self) -> None:
        # Setup mocks
        with patch(
            "eimerdb.instance_query_worker.pq.write_to_dataset"
        ) as mock_write_to_dataset, patch(
            "eimerdb.instance_query_worker.QueryWorker.query_select"
        ) as mock_query_method:
            mock_query_method.return_value = pd.DataFrame(
                {"field1": [1, 2, 3], "row_id": ["1", "2", "3"]}
            )

            parsed_query = {
                "operation": "DELETE",
                "table_name": "table1",
                "where_clause": "row_id='1'",
            }

            # Call the method
            result = self.worker_instance.query_update_or_delete(
                parsed_query=parsed_query,
                update_sql_query=None,
                partition_select=None,
                fs=MagicMock(),
            )

            # Assertions
            if os.getenv("DAPLA_REGION") == DaplaRegion.DAPLA_LAB.value:
                dapla_user = os.getenv("DAPLA_USER")
                if dapla_user is not None:
                    user_split = dapla_user.split("@")[0]
                    self.assertEqual(f"1 rows deleted by {user_split}", result)
                else:
                    self.assertEqual("1 rows deleted by user", result)

            mock_write_to_dataset.assert_called_with(
                table=ANY,
                root_path="gs://test_bucket/path/to/eimer/table1_changes",
                partition_cols=None,
                basename_template="commit_mocked_uuid_{i}.parquet",
                filesystem=ANY,
                compression=DEFAULT_COMPRESSION,
                min_rows_per_group=DEFAULT_MIN_ROWS_PER_GROUP,
                schema=None,
            )

    @parameterized.expand(
        [
            ("table2", None, True, "pandas", "all", 2),
            ("table1", None, True, "pandas", "all", 2),
            ("table1", None, False, "pandas", "all", 2),
            ("table1", None, False, "pandas", "all", 0),
            ("table1", None, False, "pandas", "recent", 1),
            ("table1", None, False, "pandas", "all", 2),
            ("table1", PARTITION_SELECT, False, "pandas", "recent", 1),
            ("table1", None, False, "arrow", "recent", 1),
            ("table1", None, False, "arrow", "all", 2),
            ("table1", PARTITION_SELECT, False, "arrow", "recent", 1),
        ]
    )
    def test_query_changes(
        self,
        table_name: str,
        partition_select: dict[str, Any] | None,
        unedited: bool,
        output_format: str,
        changes_output: str,
        expected_rows: int,
    ) -> None:
        # Mock the file system
        mock_fs = Mock()

        glob_files = []
        if expected_rows == 1:
            glob_files = ["gs://bucket/eimerdb/eimerdb_name/table1_changes/part1"]
        elif expected_rows == 2:
            glob_files = [
                "gs://bucket/eimerdb/eimerdb_name/table1_changes_all/part1",
                "gs://bucket/eimerdb/eimerdb_name/table1_changes/part2",
            ]

        mock_fs.glob.return_value = glob_files

        # Mock the table data
        mock_table_data = Mock()
        mock_table_data.num_rows = expected_rows

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
        mock_duckdb_query_result.df.return_value = (
            expected_df if expected_rows > 0 else pd.DataFrame()
        )
        mock_duckdb_query_result.fetch_arrow_table.return_value = (
            pa.Table.from_pandas(expected_df)
            if expected_rows > 0
            else pa.Table.from_pandas(pd.DataFrame())
        )

        # Patching methods with mocks
        with patch(
            "eimerdb.instance_query_worker.FileClient.get_gcs_file_system",
            return_value=mock_fs,
        ), patch(
            "eimerdb.instance_query_worker.pq.read_table",
            return_value=mock_table_data,
        ), patch(
            "eimerdb.instance_query_worker.duckdb.DuckDBPyConnection.query",
            return_value=mock_duckdb_query_result,
        ):
            result = self.worker_instance.query_changes(
                sql_query=f"SELECT * FROM {table_name}",
                partition_select=partition_select,
                unedited=unedited,
                output_format=output_format,
                changes_output=changes_output,
            )

        # Assertions
        if expected_rows > 0:
            self.assertIsNotNone(result)
            self.assertTrue(
                isinstance(result, pa.Table) or isinstance(result, pd.DataFrame)
            )

            if isinstance(result, pa.Table) or isinstance(result, pd.DataFrame):
                self.assertEqual(expected_rows, len(result))
        else:
            self.assertIsNone(result)

    @parameterized.expand(
        [
            ("pandas", False, False),
            ("pandas", True, False),
            ("pandas", False, True),
            ("pandas", True, True),
            ("arrow", False, False),
            ("arrow", True, False),
            ("arrow", False, True),
            ("arrow", True, True),
        ]
    )
    def test_concat_changes(
        self,
        output_format: str,
        use_first: bool,
        use_second: bool,
    ) -> None:
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

        if output_format == "pandas":
            first = expected_df if use_first else None
            second = expected_df if use_second else None
        else:
            first = pa.Table.from_pandas(expected_df) if use_first else None
            second = pa.Table.from_pandas(expected_df) if use_second else None

        # Call the method
        result: pa.Table = self.worker_instance._concat_changes(
            first=first, second=second, table_name="table1", output_format=output_format
        )

        # Assertions
        if use_first and use_second:
            self.assertEqual(2, len(result))
        elif use_first or use_second:
            self.assertEqual(1, len(result))
        else:
            self.assertIsNone(result)

    @parameterized.expand(
        [
            ("no_timetravel", None, {"value": [10, 20, 30]}),
            ("timetravel_matches_some", "2024-04-16 12:00:00", {"value": [10, 20]}),
            ("timetravel_matches_none", "2024-04-14 12:00:00", {"value": []}),
            ("timetravel_matches_all", "2024-04-18 12:00:00", {"value": [10, 20, 30]}),
        ]
    )
    def test_timetravel_filter(self, _: str, timetravel: str, expected_data) -> None:
        target_table = pa.Table.from_pydict(
            {
                "user": ["user1", "user2", "user3"],
                "operation": ["create", "update", "delete"],
                "datetime": [
                    datetime.datetime(2024, 4, 15, 12, 0, 0),
                    datetime.datetime(2024, 4, 16, 12, 0, 0),
                    datetime.datetime(2024, 4, 17, 12, 0, 0),
                ],
                "value": [10, 20, 30],
            }
        )

        result_table = QueryWorker._timetravel_filter(
            target_table=target_table, timetravel=timetravel
        )

        expected_table = (
            target_table
            if timetravel is None
            else pa.Table.from_pydict(
                mapping=expected_data, schema=pa.schema([("value", pa.int64())])
            )
        )

        self.assertTrue(expected_table.equals(result_table))
