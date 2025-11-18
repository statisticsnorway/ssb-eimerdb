import unittest
from unittest.mock import patch

import pandas as pd

from eimerdb.instance import EimerDBInstance


class TestEimerDBInstanceNonAdminUser(unittest.TestCase):
    def setUp(self) -> None:
        with (
            patch("eimerdb.instance.get_initials", return_value="non_admin_user"),
            patch("eimerdb.instance.get_json") as mock_get_json,
        ):
            mock_get_json.side_effect = [
                {
                    "eimerdb_name": "test_eimerdb",
                    "path": "/path/to/config",
                    "eimer_path": "/path/to/eimer",
                    "created_by": "admin_user",
                    "time_created": "2024-03-09T12:00:00Z",
                },
                {"non_admin_user": "user"},
                {"admin_user": {"admin_group": ["admin_user"]}},
                {"table1": {"created_by": "admin_user"}},
            ]

            self.instance = EimerDBInstance("test_bucket", "test_eimer")

    def test_init_non_admin(self) -> None:
        self.assertEqual("admin_user", self.instance._created_by)
        self.assertEqual({"non_admin_user": ""}, self.instance._users)

        self.assertIsNone(self.instance._role_groups)
        self.assertFalse(self.instance._is_admin)

    def test_add_user_non_admin(self) -> None:
        # Test & Assertion
        with self.assertRaises(PermissionError) as context:
            self.instance.add_user("new_user", "user")
        self.assertEqual(
            "Cannot add user. You are not an admin!", str(context.exception)
        )

    def test_remove_user_non_admin(self) -> None:
        # Test & Assertion
        with self.assertRaises(PermissionError) as context:
            self.instance.remove_user("non_admin_user")
        self.assertEqual(
            "Cannot remove user. You are not an admin!", str(context.exception)
        )

    def test_create_table_non_admin(self) -> None:
        # Test & Assertion
        with self.assertRaises(PermissionError) as context:
            self.instance.create_table("table2", [{"name": "field1", "type": "int8"}])
        self.assertEqual(
            "Cannot create table. You are not an admin!", str(context.exception)
        )

    def test_main_table_insert_non_admin(self) -> None:
        # Test & Assertion
        with self.assertRaises(PermissionError) as context:
            self.instance.insert("table1", pd.DataFrame())
        self.assertEqual(
            "Cannot insert into main table. You are not an admin!",
            str(context.exception),
        )
