from typing import Optional
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
from parameterized import parameterized

from eimerdb.instance_query_worker import QueryWorker
from tests.test_instance_base import TestEimerDBInstanceBase


class TestQueryWorker(TestEimerDBInstanceBase):
    worker_instance: QueryWorker

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
        changes_count: Optional[int],
    ) -> None:
        # Mock input parameters
        parsed_query = {"table_name": [table_name]}
        sql_query = f"SELECT * FROM {table_name}"
        partition_select = None
        unedited = unedited
        output_format = output_format

        with patch(
            "eimerdb.instance.FileClient.get_gcs_file_system"
        ) as mock_gcs_filesystem, patch(
            "eimerdb.instance_query_worker.get_partitioned_files"
        ) as mock_get_partitioned_files, patch(
            "eimerdb.instance_query_worker.pq.read_table"
        ) as mock_pq_read_table, patch(
            "eimerdb.instance_query_worker.QueryChangesWorker.query_changes"
        ) as mock_query_changes, patch(
            "eimerdb.instance_query_worker.update_pyarrow_table"
        ) as mock_update_pyarrow_table:
            fs_mock = mock_gcs_filesystem.return_value

            if changes_count is None:
                mock_query_changes.return_value = None
            else:
                mock_table = MagicMock()
                mock_table.num_rows = changes_count
                mock_query_changes.return_value = mock_table

            expected_df = pd.DataFrame({"row_id": [1, 2, 3]})
            mock_update_pyarrow_table.return_value = expected_df

            # Mock return values
            mock_pq_read_table.return_value = pd.DataFrame({"row_id": [1, 2, 3]})

            # Call the method
            result = self.worker_instance.query_select(
                parsed_query=parsed_query,
                sql_query=sql_query,
                partition_select=partition_select,
                unedited=unedited,
                output_format=output_format,
                fs=fs_mock,
            )

            if output_format == "pandas":
                assert result.equals(expected_df)
            else:
                assert result.equals(pa.Table.from_pandas(expected_df))

            mock_get_partitioned_files.assert_called_once_with(
                table_name=table_name,
                instance_name="test_eimerdb",
                table_config=self.instance.tables[table_name],
                suffix="_raw",
                fs=fs_mock,
                partition_select=partition_select,
                unedited=unedited,
            )
            mock_pq_read_table.assert_called_once()

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

    @patch("eimerdb.instance_query_worker.QueryWorker.query_select")
    @patch("eimerdb.instance_query_worker.pq.write_to_dataset")
    @patch("eimerdb.instance_query_worker.uuid4")
    def test_query_update_success(
        self, mock_uuid4: Mock, mock_write_to_dataset: Mock, mock_query_method: Mock
    ) -> None:
        # Setup mocks
        mock_uuid4.return_value = "mocked_uuid"

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
        self.assertEqual("1 rows updated by user", result)

        mock_write_to_dataset.assert_called_with(
            table=ANY,
            root_path="gs://test_bucket/path/to/eimer/table1_changes",
            partition_cols=None,
            basename_template="commit_mocked_uuid_{i}.parquet",
            schema=self.instance.get_arrow_schema("table1", True),
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

    @patch("eimerdb.instance_query_worker.QueryWorker.query_select")
    @patch("eimerdb.instance_query_worker.pq.write_to_dataset")
    @patch("eimerdb.instance_query_worker.uuid4")
    def test_query_delete_success(
        self, mock_uuid4: Mock, mock_write_to_dataset: Mock, mock_query_method: Mock
    ) -> None:
        # Setup mocks
        mock_uuid4.return_value = "mocked_uuid"

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
        self.assertEqual("1 rows deleted by user", result)

        mock_write_to_dataset.assert_called_with(
            table=ANY,
            root_path="gs://test_bucket/path/to/eimer/table1_changes",
            partition_cols=None,
            basename_template="commit_mocked_uuid_{i}.parquet",
            filesystem=ANY,
            schema=None,
        )
