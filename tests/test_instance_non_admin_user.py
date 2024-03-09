import unittest
from unittest.mock import Mock
from unittest.mock import patch

from eimerdb.instance import EimerDBInstance


class TestEimerDBInstanceNonAdminUser(unittest.TestCase):
    @patch("eimerdb.instance.get_json")
    @patch("eimerdb.instance.get_initials", return_value="non_admin_user")
    def setUp(self, _: Mock, mock_get_json: Mock) -> None:
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

        self.bucket_name = "test_bucket"
        self.eimer_name = "test_eimer"
        self.instance = EimerDBInstance(self.bucket_name, self.eimer_name)

    def test_init_non_admin(self) -> None:
        self.assertEqual(self.instance.created_by, "admin_user")
        self.assertEqual(self.instance.users, {"non_admin_user": ""})
        self.assertIsNone(self.instance.role_groups)
        self.assertEqual(self.instance.is_admin, False)
