import logging
from typing import Any
from typing import Optional
from typing import Union
from uuid import uuid4

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from gcsfs import GCSFileSystem

from .abstract_db_instance import AbstractDbInstance
from .eimerdb_constants import DUCKDB_DEFAULT_CONFIG
from .eimerdb_constants import EDITABLE_KEY
from .eimerdb_constants import PANDAS_OUTPUT_FORMAT
from .eimerdb_constants import PARTITION_COLUMNS_KEY
from .eimerdb_constants import SELECT_STAR_QUERY
from .eimerdb_constants import TABLE_NAME_KEY
from .eimerdb_constants import TABLE_PATH_KEY
from .eimerdb_constants import WHERE_CLAUSE_KEY
from .functions import get_datetime
from .functions import get_initials
from .functions import parse_sql_query
from .instance_query_changes_worker import QueryChangesWorker
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
        self.db_instance = db_instance
        self.query_changes_worker = QueryChangesWorker(db_instance)

    def query_select(
        self,
        fs: GCSFileSystem,
        parsed_query: dict[str, Any],
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
    ) -> Union[pd.DataFrame, pa.Table]:
        """Query the database.

        Args:
            fs (GCSFileSystem): The GCSFileSystem instance.
            parsed_query (dict): The parsed query.
            sql_query (str): The SQL query to execute.
            partition_select (dict, optional): Dictionary containing partition selection criteria. Defaults to None.
            unedited (bool): Flag indicating whether to retrieve unedited data. Defaults to False.
            output_format (str): Desired output format ('pandas' or 'arrow'). Defaults to PANDAS_OUTPUT_FORMAT.

        Returns:
            Union[pd.DataFrame, pa.Table]: Returns a pandas DataFrame if 'pandas' output format is specified,
        """
        con = duckdb.connect(config=DUCKDB_DEFAULT_CONFIG)
        tables = parsed_query[TABLE_NAME_KEY]

        for table_name in tables:
            table_config = self.db_instance.tables[table_name]

            table_files = get_partitioned_files(
                table_name=table_name,
                instance_name=self.db_instance.eimerdb_name,
                table_config=table_config,
                suffix="_raw",
                fs=fs,
                partition_select=partition_select,
                unedited=unedited,
            )

            # noinspection PyTypeChecker
            df = pq.read_table(table_files, filesystem=fs)
            df_changes = None
            editable = table_config[EDITABLE_KEY]

            if editable is True and unedited is False:
                select_query = SELECT_STAR_QUERY
                df_changes = self.query_changes_worker.query_changes(
                    f"{select_query} {table_name}", partition_select
                )

            if editable is True and unedited is False and df_changes is not None:
                if df_changes is not None and df_changes.num_rows != 0:
                    df = update_pyarrow_table(df, df_changes)

            con.register(table_name, df)
            del df

        if output_format == PANDAS_OUTPUT_FORMAT:
            return con.execute(sql_query).df()
        else:
            return con.execute(sql_query).arrow()

    def query_update(
        self,
        fs: GCSFileSystem,
        parsed_query: dict[str, Any],
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
    ) -> str:
        """Query the database to update records.

        Args:
            fs (GCSFileSystem): The GCSFileSystem instance.
            parsed_query (dict): The parsed query.
            sql_query (str): The SQL query to execute.
            partition_select (Dict, optional): Dictionary containing partition selection criteria. Defaults to None.

        Returns:
            str: String containing number of rows updated.

        Raises:
            ValueError: If the table is not editable.
        """
        table_name = parsed_query[TABLE_NAME_KEY]
        where_clause = parsed_query[WHERE_CLAUSE_KEY]

        table_config = self.db_instance.tables[table_name]
        partitions = table_config[PARTITION_COLUMNS_KEY]

        if table_config[EDITABLE_KEY] is not True:
            raise ValueError(f"The table {table_name} is not editable!")

        select_query = f"{SELECT_STAR_QUERY} {table_name} WHERE {where_clause}"
        df_update_results: pd.DataFrame = self.query_select(
            fs=fs,
            parsed_query=parse_sql_query(select_query),
            sql_query=select_query,
            partition_select=partition_select,
        )

        df_update_results["user"] = get_initials()
        df_update_results["datetime"] = get_datetime()
        df_update_results["operation"] = "update"

        arrow_schema = self.db_instance.get_arrow_schema(table_name, True)

        dataset = pa.Table.from_pandas(df_update_results, schema=arrow_schema)
        con = duckdb.connect()
        con.register("dataset", dataset)
        con.execute(f"CREATE TABLE updates AS FROM dataset WHERE {where_clause}")

        sql_query = sql_query.replace(f"UPDATE {table_name}", "UPDATE updates")
        con.execute(sql_query)
        df_updates_commits = con.table("updates").df()

        table_path = self.db_instance.tables[table_name][TABLE_PATH_KEY] + "_changes"
        update_table = pa.Table.from_pandas(df_updates_commits, schema=arrow_schema)

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=update_table,
            root_path=f"gs://{self.db_instance.bucket_name}/{table_path}",
            partition_cols=partitions,
            basename_template=f"commit_{uuid4()}_{{i}}.parquet",
            schema=self.db_instance.get_arrow_schema(table_name, True),
            filesystem=fs,
        )
        return f"{df_updates_commits.shape[0]} rows updated by {get_initials()}"

    def query_delete(
        self,
        fs: GCSFileSystem,
        parsed_query: dict[str, Any],
        partition_select: Optional[dict[str, Any]] = None,
    ) -> str:
        """Query the database to delete records.

        Args:
            fs (GCSFileSystem): The GCSFileSystem instance.
            parsed_query (dict): The parsed query.
            partition_select (dict, optional): Dictionary containing partition selection criteria. Defaults to None.

        Returns:
            str: String containing number of rows deleted.

        Raises:
            ValueError: If the table is not editable.
        """
        table_name = parsed_query[TABLE_NAME_KEY]
        where_clause = parsed_query[WHERE_CLAUSE_KEY]

        table_config = self.db_instance.tables[table_name]

        if table_config[EDITABLE_KEY] is not True:
            raise ValueError(f"The table {table_name} is not editable!")

        partitions = table_config[PARTITION_COLUMNS_KEY]

        select_query = f"{SELECT_STAR_QUERY} {table_name} WHERE {where_clause}"
        df_delete_results: pd.DataFrame = self.query_select(
            fs=fs,
            parsed_query=parse_sql_query(select_query),
            sql_query=select_query,
            partition_select=partition_select,
        )

        df_delete_results["user"] = get_initials()
        df_delete_results["datetime"] = get_datetime()
        df_delete_results["operation"] = "delete"

        arrow_schema = self.db_instance.get_arrow_schema(table_name, True)

        dataset = pa.Table.from_pandas(df_delete_results, schema=arrow_schema)
        con = duckdb.connect()
        con.register("dataset", dataset)
        con.execute(f"CREATE TABLE deletes AS FROM dataset WHERE {where_clause}")

        df_deletions = con.table("deletes").df()
        table_path = table_config[TABLE_PATH_KEY] + "_changes"
        deletion_table = pa.Table.from_pandas(df_deletions, schema=arrow_schema)

        # noinspection PyTypeChecker
        pq.write_to_dataset(
            table=deletion_table,
            root_path=f"gs://{self.db_instance.bucket_name}/{table_path}",
            partition_cols=partitions,
            basename_template=f"commit_{uuid4()}_{{i}}.parquet",
            filesystem=fs,
        )
        return f"{df_deletions.shape[0]} rows deleted by {get_initials()}"
