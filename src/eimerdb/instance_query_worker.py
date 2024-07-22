import logging
from datetime import datetime
from typing import Any
from typing import Optional
from typing import Union
from uuid import uuid4

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dapla import FileClient
from gcsfs import GCSFileSystem

from .abstract_db_instance import AbstractDbInstance
from .eimerdb_constants import ARROW_OUTPUT_FORMAT
from .eimerdb_constants import BUCKET_KEY
from .eimerdb_constants import CHANGES_ALL
from .eimerdb_constants import CHANGES_RECENT
from .eimerdb_constants import DEFAULT_COMPRESSION
from .eimerdb_constants import DEFAULT_MIN_ROWS_PER_GROUP
from .eimerdb_constants import DUCKDB_DEFAULT_CONFIG
from .eimerdb_constants import EDITABLE_KEY
from .eimerdb_constants import PANDAS_OUTPUT_FORMAT
from .eimerdb_constants import PARTITION_COLUMNS_KEY
from .eimerdb_constants import SELECT_STAR_QUERY
from .eimerdb_constants import TABLE_NAME_KEY
from .eimerdb_constants import TABLE_PATH_KEY
from .eimerdb_constants import WHERE_CLAUSE_KEY
from .functions import filter_partition_select_on_table
from .functions import get_datetime
from .functions import get_initials
from .functions import parse_sql_query
from .query import filter_partitions
from .query import get_partitioned_files
from .query import update_pyarrow_table

logger = logging.getLogger(__name__)


class QueryWorker:
    """Internal class for offloading query.

    Do not create an instance of this class directly.
    """

    def __init__(self, db_instance: AbstractDbInstance) -> None:
        """Initialize QueryWorker.

        Args:
            db_instance: The AbstractDbInstance instance.
        """
        self._db_instance = db_instance

    @staticmethod
    def _timetravel_filter(
        target_table: pa.Table,
        timetravel: Optional[str],
    ) -> pa.Table:
        if timetravel is None:
            return target_table

        timetravel_datetime = pa.scalar(
            datetime.strptime(timetravel, "%Y-%m-%d %H:%M:%S"), type=pa.timestamp("ns")
        )

        result_table = target_table.filter(
            pc.less_equal(target_table["datetime"], timetravel_datetime)
        )

        return result_table.drop(["user", "operation", "datetime"])

    def query_select(
        self,
        parsed_query: dict[str, Any],
        sql_query: str,
        partition_select: Optional[dict[str, Any]],
        unedited: bool,
        output_format: str,
        fs: GCSFileSystem,
        timetravel: Optional[str] = None,
    ) -> Union[pd.DataFrame, pa.Table]:
        """Query the database.

        Args:
            parsed_query (dict): The parsed query.
            sql_query (str): The SQL query to execute.
            partition_select (dict, optional): Dictionary containing partition selection criteria. Defaults to None.
            unedited (bool): Flag indicating whether to retrieve unedited data. Defaults to False.
            output_format (str): Desired output format ('pandas' or 'arrow'). Defaults to PANDAS_OUTPUT_FORMAT.
            timetravel (str, optional): A string with the date and time in the format '2024-04-15 00:00:00'. Defaults to None.
            fs (GCSFileSystem): The GCSFileSystem instance.

        Returns:
            Union[pd.DataFrame, pa.Table]: Returns a pandas DataFrame if 'pandas' output format is specified,
        """
        con = duckdb.connect(config=DUCKDB_DEFAULT_CONFIG)
        tables = parsed_query[TABLE_NAME_KEY]

        for table_name in tables:
            table_config = self._db_instance.tables[table_name]
            current_partition_select = filter_partition_select_on_table(
                table_name=table_name, partition_select=partition_select
            )
            table_files = get_partitioned_files(
                table_name=table_name,
                instance_name=self._db_instance.eimerdb_name,
                table_config=table_config,
                suffix="_raw",
                fs=fs,
                timetravel=timetravel,
                partition_select=current_partition_select,
                unedited=unedited,
            )

            table = QueryWorker._timetravel_filter(
                target_table=pq.read_table(table_files, filesystem=fs),
                timetravel=timetravel,
            )

            if table_config[EDITABLE_KEY] is True and unedited is False:
                changes_table = self.query_changes(
                    sql_query=f"{SELECT_STAR_QUERY} {table_name}",
                    partition_select=current_partition_select,
                    output_format=ARROW_OUTPUT_FORMAT,
                    changes_output=CHANGES_RECENT,
                    unedited=False,
                )

                if changes_table is not None and changes_table.num_rows > 0:
                    table = update_pyarrow_table(table, changes_table, timetravel)

            con.register(table_name, table)
            del table

        query_result = con.execute(sql_query)
        return (
            query_result.df()
            if output_format == PANDAS_OUTPUT_FORMAT
            else query_result.arrow()
        )

    @staticmethod
    def _add_meta_data(target_df: pd.DataFrame, operation: str) -> pd.DataFrame:
        """Add metadata to the DataFrame.

        Args:
            target_df (pd.DataFrame): The target DataFrame.
            operation (str): The operation.

        Returns:
            pd.DataFrame: The DataFrame with added metadata.
        """
        target_df["user"] = get_initials()
        target_df["datetime"] = get_datetime()
        target_df["operation"] = operation
        return target_df

    def query_update_or_delete(
        self,
        parsed_query: dict[str, Any],
        update_sql_query: Optional[str],
        partition_select: Optional[dict[str, Any]],
        fs: GCSFileSystem,
    ) -> str:
        """Query the database to update or delete records.

        Args:
            parsed_query (dict): The parsed query.
            update_sql_query (str): The SQL query to execute. When defines, assumes update operation.
            partition_select (Dict, optional): Dictionary containing partition selection criteria.
            fs (GCSFileSystem): The GCSFileSystem instance.

        Returns:
            str: String containing number of rows updated.

        Raises:
            ValueError: If the table is not editable.
        """
        table_name = parsed_query[TABLE_NAME_KEY]
        where_clause = parsed_query[WHERE_CLAUSE_KEY]
        table_config = self._db_instance.tables[table_name]

        if table_config[EDITABLE_KEY] is not True:
            raise ValueError(f"The table {table_name} is not editable!")

        arrow_schema = self._db_instance.get_arrow_schema(table_name, True)

        select_query = f"{SELECT_STAR_QUERY} {table_name} WHERE {where_clause}"
        df_change_results: pd.DataFrame = self.query_select(
            parsed_query=parse_sql_query(select_query),
            sql_query=select_query,
            partition_select=partition_select,
            unedited=False,
            output_format=PANDAS_OUTPUT_FORMAT,
            fs=fs,
        )

        is_update = update_sql_query is not None
        df_change_results = QueryWorker._add_meta_data(
            target_df=df_change_results, operation="update" if is_update else "delete"
        )
        dataset = pa.Table.from_pandas(df_change_results, schema=arrow_schema)

        con = duckdb.connect()
        con.register("dataset", dataset)

        if update_sql_query is not None:
            con.execute(f"CREATE TABLE updates AS FROM dataset WHERE {where_clause}")
            local_sql_query = update_sql_query.replace(
                f"UPDATE {table_name}", "UPDATE updates"
            )
            con.execute(local_sql_query)
            changes_df: pd.DataFrame = con.table("updates").df()
        else:
            con.execute(f"CREATE TABLE deletes AS FROM dataset WHERE {where_clause}")
            changes_df = con.table("deletes").df()

        changes_table = pa.Table.from_pandas(changes_df, schema=arrow_schema)
        table_path = self._db_instance.tables[table_name][TABLE_PATH_KEY] + "_changes"

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=changes_table,
            root_path=f"gs://{self._db_instance.bucket_name}/{table_path}",
            partition_cols=table_config[PARTITION_COLUMNS_KEY],
            basename_template=f"commit_{uuid4()}_{{i}}.parquet",
            compression=DEFAULT_COMPRESSION,
            min_rows_per_group=DEFAULT_MIN_ROWS_PER_GROUP,
            schema=arrow_schema if is_update else None,
            filesystem=fs,
        )
        operation = "updated" if is_update else "deleted"
        return f"{changes_df.shape[0]} rows {operation} by {get_initials()}"

    def _cast_if_arrow(
        self,
        table: Optional[Union[pd.DataFrame, pa.Table]],
        table_name: str,
        output_format: str,
    ) -> Optional[Union[pd.DataFrame, pa.Table]]:
        if table is None or output_format == PANDAS_OUTPUT_FORMAT:
            return table

        return table.cast(self._db_instance.get_arrow_schema(table_name, True))

    def _concat_changes(
        self,
        first: Optional[Union[pd.DataFrame, pa.Table]],
        second: Optional[Union[pd.DataFrame, pa.Table]],
        table_name: str,
        output_format: str,
    ) -> Optional[Union[pd.DataFrame, pa.Table]]:
        if first is None and second is None:
            return None

        if first is None or second is None:
            return self._cast_if_arrow(
                table=first if first is not None else second,
                table_name=table_name,
                output_format=output_format,
            )

        if output_format == PANDAS_OUTPUT_FORMAT:
            return pd.concat([first, second])

        return self._cast_if_arrow(
            table=pa.concat_tables([first, second]),
            table_name=table_name,
            output_format=output_format,
        )

    def query_changes(
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]],
        unedited: bool,
        output_format: str,
        changes_output: str,
    ) -> Optional[Union[pd.DataFrame, pa.Table]]:
        """Query changes made in the database table.

        Args:
            sql_query (str): The SQL query to execute.
            partition_select (dict):
                Dictionary containing partition selection criteria. Defaults to None.
            unedited (bool):
                Flag indicating whether to retrieve unedited changes.
            output_format (str):
                The desired output format ('pandas' or 'arrow').
            changes_output (str):
                The changes that are to be retrieved ('recent' or 'all').

        Returns:
            Optional[pd.DataFrame, pa.Table]:
                Returns a pandas DataFrame if 'pandas' output format is specified,
                an arrow Table if 'arrow' output format is specified,
                or None if operation is different from SELECT.
        """
        parsed_query = parse_sql_query(sql_query)
        table_name = parsed_query[TABLE_NAME_KEY][0]
        table_config = self._db_instance.tables[table_name]

        def get_partition_levels() -> str:
            partitions = table_config[PARTITION_COLUMNS_KEY]
            partitions_len = len(partitions) if partitions is not None else 0
            return "**/" * partitions_len + "*"

        def get_duckdb_query(_local_changes_output: str) -> str:
            modified_query = sql_query.replace(f"FROM {table_name}", "FROM dataset")

            if _local_changes_output == CHANGES_RECENT:
                return modified_query

            if table_config[EDITABLE_KEY] is True and unedited is not True:
                # add row_id to the select clause
                return modified_query.replace(" FROM", ", row_id FROM")

            return modified_query

        fs = FileClient.get_gcs_file_system()

        def get_change_dataset(local_changes_output: str) -> Optional[pa.Table]:
            changes_suffix = (
                "changes_all" if local_changes_output == CHANGES_ALL else "changes"
            )

            changes_files = fs.glob(
                f"gs://{table_config[BUCKET_KEY]}/eimerdb/{self._db_instance.eimerdb_name}/"
                f"{table_name}_{changes_suffix}/"
                f"{get_partition_levels()}"
            )

            if len(changes_files) < 1:
                return None

            max_depth = max(obj.count("/") for obj in changes_files)

            changes_files_at_max_depth = [
                obj for obj in changes_files if obj.count("/") == max_depth
            ]

            table_level_partition_select = filter_partition_select_on_table(
                table_name=table_name, partition_select=partition_select
            )
            if table_level_partition_select is not None:
                changes_files_at_max_depth = filter_partitions(
                    table_files=changes_files_at_max_depth,
                    partition_select=table_level_partition_select,
                )

            # noinspection PyTypeChecker
            dataset = pq.read_table(
                source=changes_files_at_max_depth,
                schema=self._db_instance.get_arrow_schema(table_name, True),
                filesystem=fs,
                columns=None,
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
                    for field in self._db_instance.get_arrow_schema(table_name, True)
                ]
                return query_result.arrow().select(column_order)

        # method body
        if changes_output == CHANGES_ALL:
            return self._concat_changes(
                first=get_changes_query_result(CHANGES_ALL),
                second=get_changes_query_result(CHANGES_RECENT),
                table_name=table_name,
                output_format=output_format,
            )

        return self._cast_if_arrow(
            table=get_changes_query_result(changes_output),
            table_name=table_name,
            output_format=output_format,
        )
