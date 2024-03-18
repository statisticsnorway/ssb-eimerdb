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

    #
    # get_initials
    #

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
            {
                "name": "field4",
                "type": "dictionary",
                "indices": pa.int32(),
                "values": pa.string(),
                "label": "Field 4",
            },
        ]

        result = arrow_schema_from_json(json_schema)

        assert result == pa.schema(
            [
                pa.field("field1", pa.int8(), metadata={"label": "Field 1"}),
                pa.field("field2", pa.string(), metadata={"label": "Field 2"}),
                pa.field("field3", pa.timestamp("us"), metadata={"label": "Field 3"}),
                pa.field(
                    "field4",
                    pa.dictionary(pa.int32(), pa.string()),
                    metadata={"label": "Field 4"},
                ),
            ]
        )

    #
    # parse_sql_query
    #

    def test_parse_sql_query_invalid_operation_expect_exception(self) -> None:
        sql_query = "INVALID OPERATION"
        with self.assertRaises(ValueError) as context:
            parse_sql_query(sql_query)
        self.assertEqual(
            "Error parsing sql-query. Syntax error or query not supported.",
            str(context.exception),
        )

    def test_parse_sql_query_select(self) -> None:
        sql_query = "SELECT * FROM table WHERE condition"
        assert parse_sql_query(sql_query) == {
            "columns": ["*"],
            "operation": "SELECT",
            "select_clause": "",
            "table_name": ["table"],
            "where_clause": "condition",
        }

    def test_parse_sql_query_update(self) -> None:
        sql_query = "UPDATE table1 SET field1='1' WHERE row_id=1"
        assert parse_sql_query(sql_query) == {
            "operation": "UPDATE",
            "set_clause": "field1='1'",
            "table_name": "table1",
            "where_clause": "row_id=1",
        }

    def test_parse_sql_query_delete(self) -> None:
        sql_query = "DELETE FROM table1 WHERE row_id=1"
        assert parse_sql_query(sql_query) == {
            "operation": "DELETE",
            "table_name": "table1",
            "where_clause": "row_id=1",
        }

    def test_parse_sql_query_select_complex_query(self) -> None:
        sql_query = """
            SELECT
                current_year.inntektsaar,
                current_year.id,
                current_year.felt,
                previous_year.beloep as forrige_aar,
                current_year.beloep as dette_aar,
                current_year.endret_dato,
                current_year.endret_bruker,
                aarsak_endring.beskrivelse AS aarsak_editering
            FROM
                (
                    SELECT *
                    FROM resultatregnskap
                    WHERE inntektsaar = 2022 AND id = '123'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY felt ORDER BY endret_dato DESC) = 1
                ) AS current_year
                LEFT JOIN (
                    SELECT id, felt, beloep
                    FROM resultatregnskap
                    WHERE inntektsaar = 2021 AND id = '123'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY felt ORDER BY endret_dato DESC) = 1
                ) AS previous_year ON current_year.felt = previous_year.felt
                JOIN aarsak_endring ON current_year.aarsak_kode = aarsak_endring.id
            ORDER BY current_year.felt
        """
        assert parse_sql_query(sql_query) == {
            "columns": ["*"],
            "operation": "SELECT",
            "select_clause": "",
            "table_name": ["aarsak_endring", "resultatregnskap"],
            "where_clause": None,
        }

    def test_update_query_multiline(self) -> None:
        sql_query = """
            UPDATE resultatregnskap_less
            SET beloep = 42.0
            WHERE
                inntektsaar = 2022
                AND id = '1234'
                AND versjon = '1'
                AND felt = '3600'
            """

        assert parse_sql_query(sql_query) == {
            "operation": "UPDATE",
            "set_clause": "beloep = 42.0",
            "table_name": "resultatregnskap_less",
            "where_clause": "inntektsaar = 2022 AND id = '1234' AND versjon = '1' AND "
            "felt = '3600'",
        }

    def test_delete_query_multiline(self) -> None:
        sql_query = """
            DELETE FROM table1
            WHERE row_id=1
            """

        assert parse_sql_query(sql_query) == {
            "operation": "DELETE",
            "table_name": "table1",
            "where_clause": "row_id=1",
        }

    #
    # create_eimerdb
    #

    @patch("eimerdb.functions.AuthClient.fetch_google_credentials")
    @patch("google.cloud.storage.Client")
    def test_create_eimerdb(self, mock_client: Mock, mock_fetch: Mock) -> None:
        mock_fetch.return_value = "token"
        mock_blob = MagicMock()
        mock_bucket = MagicMock()

        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        create_eimerdb("bucket_name", "db_name")
