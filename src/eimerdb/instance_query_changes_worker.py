import logging
from typing import Any
from typing import Optional
from typing import Union

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dapla import FileClient
from pyarrow import Table

from .abstract_db_instance import AbstractDbInstance
from .eimerdb_constants import ARROW_OUTPUT_FORMAT
from .eimerdb_constants import BUCKET_KEY
from .eimerdb_constants import CHANGES_ALL
from .eimerdb_constants import CHANGES_RECENT
from .eimerdb_constants import COLUMNS_KEY
from .eimerdb_constants import DUCKDB_DEFAULT_CONFIG
from .eimerdb_constants import EDITABLE_KEY
from .eimerdb_constants import OPERATION_KEY
from .eimerdb_constants import PANDAS_OUTPUT_FORMAT
from .eimerdb_constants import PARTITION_COLUMNS_KEY
from .eimerdb_constants import TABLE_NAME_KEY
from .functions import parse_sql_query
from .query import filter_partitions

logger = logging.getLogger(__name__)


class QueryChangesWorker:
    """Internal class for offloading query_changes.

    Do not create an instance of this class directly.
    """

    def __init__(self, db_instance: AbstractDbInstance) -> None:
        """Initialize QueryChangesWorker.

        Args:
            db_instance: The EimerDBInstance instance.
        """
        self.db_instance = db_instance

    def query_changes(
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
        changes_output: str = CHANGES_ALL,
    ) -> Optional[Union[pd.DataFrame, pa.Table]]:
        """Query changes made in the database table.

        Args:
            sql_query (str): The SQL query to execute.
            partition_select (Dict, optional):
                Dictionary containing partition selection criteria. Defaults to None.
            unedited (bool):
                Flag indicating whether to retrieve unedited changes. Defaults to False.
            output_format (str):
                The desired output format ('pandas' or 'arrow'). Defaults to 'pandas'.
            changes_output (str):
                The changes that are to be retrieved ('recent' or 'all'). Defaults to 'all'.

        Returns:
            Optional[pd.DataFrame, pa.Table]:
                Returns a pandas DataFrame if 'pandas' output format is specified,
                an arrow Table if 'arrow' output format is specified,
                or None if operation is different from SELECT.

        Raises:
            ValueError: If the output format is invalid.
        """
        if output_format not in (PANDAS_OUTPUT_FORMAT, ARROW_OUTPUT_FORMAT):
            raise ValueError(f"Invalid output format: {output_format}")

        # Validate changes_output
        if changes_output not in (CHANGES_ALL, CHANGES_RECENT):
            raise ValueError(f"Invalid changes output: {changes_output}")

        parsed_query = parse_sql_query(sql_query)
        # Check if the operation is SELECT
        if parsed_query[OPERATION_KEY] != "SELECT":
            raise ValueError(
                f"Operation {parsed_query[OPERATION_KEY]} is not supported."
            )

        table_name = parsed_query[TABLE_NAME_KEY][0]
        table_config = self.db_instance.tables[table_name]

        def get_partition_levels() -> str:
            partitions = table_config[PARTITION_COLUMNS_KEY]
            partitions_len = len(partitions) if partitions is not None else 0
            return "**/" * partitions_len + "*"

        def get_columns(_local_changes_output: str) -> Optional[list[str]]:
            if _local_changes_output == CHANGES_RECENT:
                return None

            columns = parsed_query.get(COLUMNS_KEY)
            if columns == ["*"]:
                return None

            return columns

        def get_duckdb_query(_local_changes_output: str) -> str:
            modified_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")

            if _local_changes_output == CHANGES_RECENT:
                return modified_query

            if (
                get_columns(_local_changes_output) is not None
                and table_config[EDITABLE_KEY] is True
                and unedited is not True
            ):
                # add row_id to the select clause
                return modified_query.replace(" FROM", ", row_id FROM")

            return modified_query

        fs = FileClient.get_gcs_file_system()

        def get_change_dataset(local_changes_output: str) -> Optional[Table]:
            changes_suffix = (
                "changes_all" if local_changes_output == CHANGES_ALL else "changes"
            )

            changes_files = fs.glob(
                f"gs://{table_config[BUCKET_KEY]}/eimerdb/{self.db_instance.eimerdb_name}/"
                f"{table_name}_{changes_suffix}/{get_partition_levels()}"
            )

            try:
                max_depth = max(obj.count("/") for obj in changes_files)
            except ValueError:
                return None

            changes_files_max_depth = [
                obj for obj in changes_files if obj.count("/") == max_depth
            ]

            if partition_select is not None:
                changes_files_max_depth = filter_partitions(
                    table_files=changes_files_max_depth,
                    partition_select=partition_select,
                )

            # noinspection PyTypeChecker
            dataset = pq.read_table(
                source=changes_files_max_depth,
                schema=self.db_instance.get_arrow_schema(table_name, True),
                filesystem=fs,
                columns=get_columns(local_changes_output),
            )

            return dataset if dataset.num_rows > 0 else None

        def get_changes_query_result(
            local_changes_output: str,
        ) -> Optional[Union[pd.DataFrame, pa.Table]]:
            dataset = get_change_dataset(local_changes_output)
            if dataset is None:
                return None

            conn = duckdb.connect(config=DUCKDB_DEFAULT_CONFIG)
            query_result = conn.query(get_duckdb_query(local_changes_output))
            if output_format == PANDAS_OUTPUT_FORMAT:
                return query_result.df()
            else:
                column_order = [
                    field.name
                    for field in self.db_instance.get_arrow_schema(table_name, True)
                ]
                return query_result.arrow().select(column_order)

        def cast_if_arrow(
            table: Optional[Union[pd.DataFrame, pa.Table]]
        ) -> Optional[Union[pd.DataFrame, pa.Table]]:
            if table is None or output_format == PANDAS_OUTPUT_FORMAT:
                return table

            return table.cast(self.db_instance.get_arrow_schema(table_name, True))

        def concat_changes(
            first: Optional[Union[pd.DataFrame, pa.Table]],
            second: Optional[Union[pd.DataFrame, pa.Table]],
        ) -> Optional[Union[pd.DataFrame, pa.Table]]:
            if first is None and second is None:
                return None

            if first is None:
                return cast_if_arrow(second)

            if second is None:
                return cast_if_arrow(first)

            if output_format == PANDAS_OUTPUT_FORMAT:
                return pd.concat([first, second])
            else:
                table = pa.concat_tables([first, second])
                return cast_if_arrow(table)

        # method body
        if changes_output == CHANGES_ALL:
            return concat_changes(
                first=get_changes_query_result(CHANGES_ALL),
                second=get_changes_query_result(CHANGES_RECENT),
            )
        else:
            return cast_if_arrow(get_changes_query_result(changes_output))
