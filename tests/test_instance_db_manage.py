from copy import copy
from unittest.mock import Mock
from unittest.mock import patch

from tests.test_instance_base import DEFAULT_TABLES_IN_TEST
from tests.test_instance_base import TestEimerDBInstanceBase


class TestEimerDBInstanceDbManage(TestEimerDBInstanceBase):

    # verify that instance is set up correctly
    def test_init_admin(self) -> None:
        self.assertEqual(self.instance.bucket_name, "test_bucket")
        self.assertEqual(self.instance.eimerdb_name, "test_eimerdb")
        self.assertEqual(self.instance.path, "path/to/config")
        self.assertEqual(self.instance.eimer_path, "path/to/eimer")
        self.assertEqual(self.instance.created_by, "admin_user")
        self.assertEqual(self.instance.time_created, "2024-03-09T12:00:00Z")
        self.assertEqual(self.instance.users, {"admin_user": "admin"})
        self.assertEqual(
            self.instance.role_groups, {"admin_user": {"admin_group": ["admin_user"]}}
        )
        self.assertEqual(True, self.instance.is_admin)

    def test_that_getters_returns_immutable_copies_of_dicts(self) -> None:
        # Setup
        users = self.instance.users
        users.update({"new_user": "user"})
        self.assertEqual({"admin_user": "admin", "new_user": "user"}, users)

        tables = self.instance.tables
        tables.update({"new_table": {}})
        expected_tables = copy(DEFAULT_TABLES_IN_TEST)
        expected_tables.update({"new_table": {}})
        self.assertEqual(expected_tables, tables)

        role_groups = self.instance.role_groups
        self.assertIsNotNone(role_groups)
        if role_groups is not None:  # required for mypy
            role_groups.update({"new_group": "group"})
            self.assertEqual(
                {"admin_user": {"admin_group": ["admin_user"]}, "new_group": "group"},
                role_groups,
            )

        # Assertions
        self.assertNotEqual(users, self.instance.users)
        self.assertNotEqual(tables, self.instance.tables)
        self.assertNotEqual(role_groups, self.instance.role_groups)

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_add_user_expect_added_user(
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
        self.assertEqual(
            {
                "admin_user": "admin",
                "new_user": "user",
            },
            self.instance.users,
        )

        # Mock Assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("path/to/eimer/config/users.json")
        mock_blob.upload_from_string.assert_called_once()

    def test_add_user_given_user_exists_expect_exception(self) -> None:
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.add_user("admin_user", "admin")
        self.assertEqual("User admin_user already exists!", str(context.exception))

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_remove_user_expect_removed_user(
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
        self.assertEqual({}, self.instance.users)

        # Mock assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("path/to/eimer/config/users.json")
        mock_blob.upload_from_string.assert_called_once()

    def test_remove_user_user_not_exists_expect_exception(self) -> None:
        # Test & Assertion
        with self.assertRaises(ValueError) as context:
            self.instance.remove_user("new_user")
        self.assertEqual("User new_user does not exist.", str(context.exception))

    @patch("eimerdb.instance.AuthClient.fetch_google_credentials")
    @patch("eimerdb.instance.storage.Client")
    def test_create_table_as_admin_expect_added_table(
        self, mock_storage_client: Mock, mock_fetch_credentials: Mock
    ) -> None:
        # Setup
        mock_credentials = Mock()
        mock_fetch_credentials.return_value = mock_credentials

        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        table_name = "table3"
        schema = [{"name": "field1", "type": "int8", "label": "Field 1"}]

        # Test
        self.instance.create_table(table_name, schema)

        expected = copy(DEFAULT_TABLES_IN_TEST)
        expected.update(
            {
                table_name: {
                    "bucket": "test_bucket",
                    "created_by": "user",
                    "editable": True,
                    "partition_columns": None,
                    "schema": [
                        {"label": "Unique row ID", "name": "row_id", "type": "string"},
                        {"label": "Field 1", "name": "field1", "type": "int8"},
                    ],
                    "table_path": "path/to/eimer/table3",
                }
            }
        )

        self.assertEqual(
            expected,
            self.instance.tables,
        )

        # Mock assertions
        mock_fetch_credentials.assert_called_once()
        mock_storage_client.assert_called_once_with(credentials=mock_credentials)
        mock_bucket.blob.assert_called_once_with("path/to/eimer/config/tables.json")
        mock_blob.upload_from_string.assert_called_once()
