from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

from tests.test_instance_base import TestEimerDBInstanceBase


class TestEimerDBInstanceQuery(TestEimerDBInstanceBase):

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
    @patch("eimerdb.instance_query_worker.QueryWorker.query_select")
    def test_query_select_expect_result(self, mock_query_select: Mock, _: Mock) -> None:
        mock_query_select.return_value = pd.DataFrame({"row_id": [1, 2, 3]})

        self.instance.query("SELECT * FROM table1")

        mock_query_select.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance_query_worker.QueryWorker.query_update_or_delete")
    def test_query_update_expect_result(
        self, mock_query_update_or_delete: Mock, _: Mock
    ) -> None:
        mock_query_update_or_delete.return_value = "1 rows updated by user"

        result = self.instance.query(
            "UPDATE table1 SET col1='value' WHERE col2='value'"
        )

        assert result == "1 rows updated by user"
        mock_query_update_or_delete.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance_query_worker.QueryWorker.query_update_or_delete")
    def test_query_delete_expect_result(
        self, mock_query_update_or_delete: Mock, _: Mock
    ) -> None:
        mock_query_update_or_delete.return_value = "1 rows deleted by user"

        result = self.instance.query("DELETE FROM table1 WHERE col='value'")

        assert result == "1 rows deleted by user"
        mock_query_update_or_delete.assert_called_once()

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    def test_query_drop_table_expect_exception(self, _: Mock) -> None:
        with self.assertRaises(ValueError) as context:
            self.instance.query("DROP TABLE table1")

        self.assertEqual(
            "Error parsing sql-query. Syntax error or query not supported.",
            str(context.exception),
        )

    #
    # query_changes
    #

    def test_query_changes_with_update_expect_exception(self) -> None:
        with self.assertRaises(ValueError) as context:
            self.instance.query_changes(
                sql_query="UPDATE table1 SET col1='value' WHERE row_id='1'"
            )

        self.assertEqual(
            "Operation UPDATE is not supported.",
            str(context.exception),
        )

    @patch("eimerdb.instance.FileClient.get_gcs_file_system")
    @patch("eimerdb.instance_query_worker.QueryWorker.query_changes")
    def test_query_changes_expect_result(
        self, mock_query_changes: Mock, _: Mock
    ) -> None:
        mock_query_changes.return_value = pa.Table

        result = self.instance.query_changes(sql_query="SELECT * FROM table1")

        assert result is not None
