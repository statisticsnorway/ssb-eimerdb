import unittest
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pyarrow as pa

from eimerdb.functions import arrow_schema_from_json
from eimerdb.functions import create_eimerdb
from eimerdb.functions import get_datetime
from eimerdb.functions import get_initials
from eimerdb.functions import get_json
from eimerdb.functions import parse_sql_query


class TestFunctions(unittest.TestCase):

    def test_get_datetime(self) -> None:
        result = get_datetime()
        self.assertTrue(isinstance(result, str))
        # Check if the string matches the expected format
        self.assertRegex(result, r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+")

    @patch("eimerdb.functions.AuthClient.fetch_local_user_from_jupyter")
    def test_get_initials_with_mock(self, mock_fetch: Mock) -> None:
        mock_fetch.return_value = {"username": "john.doe@example.com"}
        result = get_initials()
        self.assertEqual(result, "john.doe")

    def test_get_initials_without_mock(self) -> None:
        result = get_initials()
        self.assertEqual(result, "user")

    @patch(
        "eimerdb.functions.AuthClient.fetch_google_credentials", return_value="token"
    )
    @patch("google.cloud.storage.Client")
    def test_get_json(self, mock_client: Mock, _: Mock) -> None:
        mock_blob = MagicMock()
        mock_blob.download_as_text.return_value = '{"key": "value"}'

        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        mock_client.return_value.get_bucket.return_value = mock_bucket

        result = get_json("bucket_name", "blob_path")
        self.assertEqual(result, {"key": "value"})

    def test_arrow_schema_from_json(self) -> None:
        json_schema = [
            {"name": "field1", "type": "int8", "label": "Field 1"},
            {"name": "field2", "type": "string", "label": "Field 2"},
            {"name": "field3", "type": "timestamp(us)", "label": "Field 3"},
        ]
        result = arrow_schema_from_json(json_schema)

        assert result == pa.schema(
            [
                pa.field("field1", pa.int8(), metadata={"label": "Field 1"}),
                pa.field("field2", pa.string(), metadata={"label": "Field 2"}),
                pa.field("field3", pa.timestamp("us"), metadata={"label": "Field 3"}),
            ]
        )

    def test_parse_sql_query(self) -> None:
        sql_query = "SELECT * FROM table WHERE condition"
        result = parse_sql_query(sql_query)
        assert result == {
            "columns": ["*"],
            "operation": "SELECT",
            "select_clause": "",
            "table_name": ["table"],
            "where_clause": "condition",
        }

    @patch("eimerdb.functions.AuthClient.fetch_google_credentials")
    @patch("google.cloud.storage.Client")
    def test_create_eimerdb(self, mock_client: Mock, mock_fetch: Mock) -> None:
        mock_fetch.return_value = "token"
        mock_blob = MagicMock()
        mock_bucket = MagicMock()

        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        create_eimerdb("bucket_name", "db_name")
