from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd

from tests.test_instance_base import TestEimerDBInstanceBase


class TestEimerDBInstanceQuery(TestEimerDBInstanceBase):

    #
    # START _query_select
    #

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.get_partitioned_files")
    @patch("eimerdb.instance.pq.read_table")
    def test__query_select(
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
        result = self.instance._query_select(
            fs=fs_mock,
            parsed_query=parsed_query,
            sql_query=sql_query,
            partition_select=partition_select,
            unedited=unedited,
            output_format=output_format,
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
    # START _query_update
    #

    def test__query_update_non_editable_table(self) -> None:
        parsed_query = {
            "operation": "UPDATE",
            "set_clause": "field1=1",
            "table_name": "table2",
            "where_clause": "row_id='1'",
        }

        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance._query_update(
                MagicMock(), parsed_query, "UPDATE table2 SET field1='1' WHERE row_id=1"
            )
        self.assertEqual(
            "The table table2 is not editable!",
            str(context.exception),
        )

    @patch("eimerdb.instance.EimerDBInstance.query")
    @patch("eimerdb.instance.pq.write_to_dataset")
    @patch("eimerdb.instance.uuid4")
    def test__query_update_success(
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
        result = self.instance._query_update(
            MagicMock(), parsed_query, "UPDATE table1 SET field1=4 WHERE row_id='1'"
        )

        # Assertions
        self.assertEqual("1 rows updated by user", result)

        mock_write_to_dataset.assert_called_with(
            table=ANY,
            root_path="gs://test_bucket/path/to/eimer/table1_changes",
            partition_cols=None,
            basename_template="commit_mocked_uuid_{i}.parquet",
            schema=self.instance._get_arrow_schema("table1", True),
            filesystem=ANY,
        )

    #
    # START _query_delete
    #

    def test__query_delete_non_editable_table(self) -> None:
        parsed_query = {
            "operation": "DELETE",
            "table_name": "table2",
            "where_clause": "row_id='1'",
        }

        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance._query_delete(fs=MagicMock(), parsed_query=parsed_query)
        self.assertEqual(
            "The table table2 is not editable!",
            str(context.exception),
        )

    @patch("eimerdb.instance.EimerDBInstance.query")
    @patch("eimerdb.instance.pq.write_to_dataset")
    @patch("eimerdb.instance.uuid4")
    def test__query_delete_success(
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
        result = self.instance._query_delete(fs=MagicMock(), parsed_query=parsed_query)

        # Assertions
        self.assertEqual("1 rows deleted by user", result)

        mock_write_to_dataset.assert_called_with(
            table=ANY,
            root_path="gs://test_bucket/path/to/eimer/table1_changes",
            partition_cols=None,
            basename_template="commit_mocked_uuid_{i}.parquet",
            filesystem=ANY,
        )

    #
    # START query
    #

    def test_query_invalid_output_format_expect_exception(self) -> None:
        with self.assertRaises(ValueError) as context:
            self.instance.query("SELECT * FROM table1", output_format="invalid")
        self.assertEqual(
            "Invalid output format: invalid. Supported formats: pandas, arrow.",
            str(context.exception),
        )

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.parse_sql_query")
    def test_query_invalid_sql_operation_expect_exception(
        self, mock_parse_sql_query: Mock, _: Mock
    ) -> None:
        mock_parse_sql_query.return_value = {
            "operation": "INVALID OPERATION",
            "table_name": "table1",
        }

        with self.assertRaises(ValueError) as context:
            self.instance.query("INVALID OPERATION")
        self.assertEqual(
            "Unsupported SQL operation: INVALID OPERATION.",
            str(context.exception),
        )

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.EimerDBInstance._query_select")
    def test_query_select(self, mock_query_select: Mock, _: Mock) -> None:
        mock_query_select.return_value = pd.DataFrame({"row_id": [1, 2, 3]})

        self.instance.query("SELECT * FROM table1")

        mock_query_select.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.EimerDBInstance._query_update")
    def test_query_update(self, mock_query_update: Mock, _: Mock) -> None:
        mock_query_update.return_value = "1 rows updated by user"

        result = self.instance.query(
            "UPDATE table1 SET col1='value' WHERE col2='value'"
        )

        assert result == "1 rows updated by user"
        mock_query_update.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance.EimerDBInstance._query_delete")
    def test_query_delete(self, mock_query_delete: Mock, _: Mock) -> None:
        mock_query_delete.return_value = "1 rows deleted by user"

        result = self.instance.query("DELETE FROM table1 WHERE col='value'")

        assert result == "1 rows deleted by user"
        mock_query_delete.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    def test_query_drop_table_expect_exception(self, _: Mock) -> None:
        with self.assertRaises(ValueError) as context:
            self.instance.query("DROP TABLE table1")

        self.assertEqual(
            "Error parsing sql-query. Syntax error or query not supported.",
            str(context.exception),
        )
