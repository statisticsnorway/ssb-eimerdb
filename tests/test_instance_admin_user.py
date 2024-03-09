import unittest
from unittest.mock import Mock
from unittest.mock import patch

from eimerdb.instance import EimerDBInstance


class TestEimerDBInstanceAdminUser(unittest.TestCase):
    @patch("eimerdb.instance.get_json")
    @patch("eimerdb.instance.get_initials", return_value="admin_user")
    def setUp(self, _: Mock, mock_get_json: Mock) -> None:
        mock_get_json.side_effect = [
            {
                "eimerdb_name": "test_eimerdb",
                "path": "/path/to/config",
                "eimer_path": "/path/to/eimer",
                "created_by": "admin_user",
                "time_created": "2024-03-09T12:00:00Z",
            },
            {"admin_user": "admin"},
            {"admin_user": {"admin_group": ["admin_user"]}},
            {"table1": {"created_by": "admin_user"}},
        ]

        self.bucket_name = "test_bucket"
        self.eimer_name = "test_eimer"
        self.instance = EimerDBInstance(self.bucket_name, self.eimer_name)

    def test_init_admin(self) -> None:
        self.assertEqual(self.instance.bucket, "test_bucket")
        self.assertEqual(self.instance.eimerdb_name, "test_eimerdb")
        self.assertEqual(self.instance.path, "/path/to/config")
        self.assertEqual(self.instance.eimer_path, "/path/to/eimer")
        self.assertEqual(self.instance.created_by, "admin_user")
        self.assertEqual(self.instance.time_created, "2024-03-09T12:00:00Z")
        self.assertEqual(self.instance.users, {"admin_user": "admin"})
        self.assertEqual(
            self.instance.role_groups, {"admin_user": {"admin_group": ["admin_user"]}}
        )
        self.assertEqual(self.instance.is_admin, True)

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_add_user_success(
        self, mock_storage_client: Mock, mock_fetch_credentials: Mock
    ) -> None:
        # Setup
        mock_credentials = Mock()
        mock_fetch_credentials.return_value = mock_credentials
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        # Test
        self.instance.add_user("new_user", "user")

        # Assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("/path/to/eimer/config/users.json")
        mock_blob.upload_from_string.assert_called_once()

    def test_add_user_user_exists(self) -> None:
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.add_user("admin_user", "admin")
        self.assertEqual(str(context.exception), "User admin_user already exists!")

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_remove_user_success(
        self, mock_storage_client: Mock, mock_fetch_credentials: Mock
    ) -> None:
        # Setup
        mock_credentials = Mock()
        mock_fetch_credentials.return_value = mock_credentials
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        # Test
        self.instance.remove_user("admin_user")

        # Assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("/path/to/eimer/config/users.json")
        mock_blob.upload_from_string.assert_called_once()

    def test_remove_user_user_not_exists(self) -> None:
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.remove_user("new_user")
        self.assertEqual(str(context.exception), "User new_user does not exist.")
