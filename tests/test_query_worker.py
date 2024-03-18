from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd

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

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance_query_worker.get_partitioned_files")
    @patch("eimerdb.instance_query_worker.pq.read_table")
    def test_query_select_success(
        self,
        mock_pq_read_table: Mock,
        mock_get_partitioned_files: Mock,
        mock_gcs_filesystem: Mock,
    ) -> None:
        # Mock input parameters
        parsed_query = {"table_name": ["table1"]}
        sql_query = "SELECT * FROM table1"
        fs_mock = mock_gcs_filesystem.return_value
        partition_select = None
        unedited = False
        output_format = "pandas"

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

        assert result.equals(pd.DataFrame({"row_id": [1, 2, 3]}))

        mock_get_partitioned_files.assert_called_once_with(
            table_name="table1",
            instance_name="test_eimerdb",
            table_config=self.instance.tables["table1"],
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
                sql_query="UPDATE table2 SET field1='1' WHERE row_id=1",
                partition_select=None,
                is_update=True,
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
            sql_query="UPDATE table1 SET field1=4 WHERE row_id='1'",
            partition_select=None,
            is_update=True,
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
                sql_query=None,
                partition_select=None,
                is_update=False,
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
            sql_query=None,
            partition_select=None,
            is_update=False,
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
