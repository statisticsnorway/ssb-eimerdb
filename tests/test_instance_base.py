import unittest
from unittest.mock import Mock
from unittest.mock import patch

from eimerdb.instance import EimerDBInstance


class TestEimerDBInstanceBase(unittest.TestCase):
    @patch("eimerdb.instance.get_initials", return_value="admin_user")
    @patch("eimerdb.instance.get_json")
    def setUp(self, mock_get_json: Mock, _: Mock) -> None:
        mock_get_json.side_effect = [
            {
                "eimerdb_name": "test_eimerdb",
                "path": "path/to/config",
                "eimer_path": "path/to/eimer",
                "created_by": "admin_user",
                "time_created": "2024-03-09T12:00:00Z",
            },
            {"admin_user": "admin"},
            {"admin_user": {"admin_group": ["admin_user"]}},
            {
                "table1": {
                    "bucket": "test_bucket",
                    "created_by": "admin_user",
                    "editable": True,
                    "partition_columns": None,
                    "schema": [
                        {"label": "Unique row ID", "name": "row_id", "type": "string"},
                        {"label": "Field 1", "name": "field1", "type": "int8"},
                    ],
                    "table_path": "path/to/eimer/table1",
                },
                "table2": {
                    "bucket": "test_bucket",
                    "created_by": "admin_user",
                    "editable": False,
                    "partition_columns": None,
                    "schema": [
                        {"label": "Unique row ID", "name": "row_id", "type": "string"},
                        {"label": "Field 1", "name": "field1", "type": "int8"},
                    ],
                    "table_path": "path/to/eimer/table2",
                },
            },
        ]

        self.instance = EimerDBInstance("test_bucket", "test_eimer")
