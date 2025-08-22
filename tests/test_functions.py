import unittest
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pyarrow as pa
import pytest
from parameterized import parameterized

from eimerdb.functions import arrow_schema_from_json
from eimerdb.functions import create_eimerdb
from eimerdb.functions import filter_partition_select_on_table
from eimerdb.functions import get_datetime
from eimerdb.functions import get_initials
from eimerdb.functions import get_json
from eimerdb.functions import is_daplalab
from eimerdb.functions import parse_sql_query


class TestFunctions(unittest.TestCase):

    @parameterized.expand(
        [
            ("table1", None, None),
            ("table1", {"col1": ["value1", "value2"]}, {"col1": ["value1", "value2"]}),
            (
                "table1",
                {"table1": {"col1": ["value1", "value2"]}},
                {"col1": ["value1", "value2"]},
            ),
            ("table2", {"table1": {"col1": ["value1", "value2"]}}, None),
        ]
    )
    def test_filter_partition_select_on_table(
        self,
        table_name: str,
        partition_select: dict[str, Any],
        expected: dict[str, Any] | None,
    ) -> None:
        # Call the function under test
        result = filter_partition_select_on_table(table_name, partition_select)

        # Assert result
        self.assertEqual(expected, result)

    def test_get_datetime(self) -> None:
        # Call the function under test
        result = get_datetime()

        # Assert that the result is a string
        self.assertIsInstance(result, str)

        # Assert that the string matches the expected format
        self.assertRegex(result, r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+")

    #
    # get_initials
    #

    def test_get_initials_without_mock(self) -> None:
        # Call the function under test
        result = get_initials()

        # Assert result
        if is_daplalab():
            self.assertNotEqual("user", result)
        else:
            self.assertEqual("user", result)

    def test_get_json(self) -> None:
        mock_blob = MagicMock()
        mock_blob.download_as_text.return_value = '{"key": "value"}'

        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        with patch("google.cloud.storage.Client") as mock_storage_client:
            # Setup mocks
            mock_storage_client.return_value.get_bucket.return_value = mock_bucket

            # Call the function under test
            result = get_json("bucket_name", "blob_path")

            # Assert result
            self.assertEqual({"key": "value"}, result)

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

        # Call the function under test
        result = arrow_schema_from_json(json_schema)

        expected_pa_schema = pa.schema(
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

        self.assertEqual(expected_pa_schema, result)

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

        # Call the function under test and assert the result
        self.assertEqual(
            {
                "columns": ["*"],
                "operation": "SELECT",
                "select_clause": "",
                "table_name": ["table"],
                "where_clause": "condition",
            },
            parse_sql_query(sql_query),
        )

    def test_parse_sql_query_update(self) -> None:
        sql_query = "UPDATE table1 SET field1='1' WHERE row_id=1"

        # Call the function under test and assert the result
        self.assertEqual(
            {
                "operation": "UPDATE",
                "set_clause": "field1='1'",
                "table_name": "table1",
                "where_clause": "row_id=1",
            },
            parse_sql_query(sql_query),
        )

    def test_parse_sql_query_delete(self) -> None:
        sql_query = "DELETE FROM table1 WHERE row_id=1"

        # Call the function under test and assert the result
        self.assertEqual(
            {
                "operation": "DELETE",
                "table_name": "table1",
                "where_clause": "row_id=1",
            },
            parse_sql_query(sql_query),
        )

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

        # Call the function under test and assert the result
        self.assertEqual(
            {
                "columns": ["*"],
                "operation": "SELECT",
                "select_clause": "",
                "table_name": ["aarsak_endring", "resultatregnskap"],
                "where_clause": None,
            },
            parse_sql_query(sql_query),
        )

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

        # Call the function under test and assert the result
        self.assertEqual(
            {
                "operation": "UPDATE",
                "set_clause": "beloep = 42.0",
                "table_name": "resultatregnskap_less",
                "where_clause": "inntektsaar = 2022 AND id = '1234' AND versjon = '1' AND "
                "felt = '3600'",
            },
            parse_sql_query(sql_query),
        )

    def test_delete_query_multiline(self) -> None:
        sql_query = """
            DELETE FROM table1
            WHERE row_id=1
            """

        # Call the function under test and assert the result
        self.assertEqual(
            {
                "operation": "DELETE",
                "table_name": "table1",
                "where_clause": "row_id=1",
            },
            parse_sql_query(sql_query),
        )

    #
    # create_eimerdb
    #

    @pytest.mark.skipif(not is_daplalab(), reason="Only works on Daplalab")
    def test_create_eimerdb(self) -> None:
        mock_blob = MagicMock()
        mock_bucket = MagicMock()

        mock_bucket.blob.return_value = mock_blob

        with patch("google.cloud.storage.Client") as mock_storage_client:
            mock_storage_client.return_value.bucket.return_value = mock_bucket

            # Call the method under test
            create_eimerdb(bucket_name="bucket_name", db_name="db_name")

            # Mock assertions

            expected_calls = [
                call("eimerdb/db_name/config/about.json"),
                call("eimerdb/db_name/config/users.json"),
                call("eimerdb/db_name/config/role_groups.json"),
                call("eimerdb/db_name/config/tables.json"),
            ]

            mock_bucket.blob.assert_has_calls(expected_calls, any_order=True)
