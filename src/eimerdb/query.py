from datetime import datetime
from typing import Any
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
from gcsfs import GCSFileSystem

from eimerdb.eimerdb_constants import BUCKET_KEY
from eimerdb.eimerdb_constants import PARTITION_COLUMNS_KEY


def get_partitioned_files(
    table_name: str,
    instance_name: str,
    table_config: dict[str, Any],
    suffix: str,
    fs: GCSFileSystem,
    timetravel: Optional[str] = None,
    partition_select: Optional[dict[str, Any]] = None,
    unedited: bool = False,
) -> list[str]:
    """Retrieve the paths of partitioned files for a given table.

    Args:
        table_name (str): The name of the table.
        instance_name (str): The name of the instance.
        table_config (dict[str, Any]): Configuration details for the table.
        suffix (str): The suffix to be appended to the table name.
        fs (GCSFileSystem): The filesystem object.
        timetravel (str, optional): A string with the date and time in the format '2024-04-15 00:00:00'. Defaults to None.
        partition_select (Optional[dict[str, Any]]): Optional dictionary specifying partition
            selection criteria. Defaults to None.
        unedited (bool): Flag indicating whether the file paths should include
            the suffix or not. Defaults to False.

    Returns:
        list[str]: A list of file paths corresponding to the partitioned files of the table.

    """
    partitions = table_config[PARTITION_COLUMNS_KEY]
    bucket_name = table_config[BUCKET_KEY]
    partitions_len = len(partitions) if partitions is not None else 0
    partition_levels = "**/" * partitions_len + "*"

    if unedited is True or timetravel is not None:
        table_name_parts = table_name + f"{suffix}"
    else:
        table_name_parts = table_name

    table_files: list[str] = fs.glob(
        f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name_parts}/{partition_levels}"
    )

    max_depth = max(obj.count("/") for obj in table_files)
    filtered_files: list[str] = [
        obj for obj in table_files if obj.count("/") == max_depth
    ]

    if partition_select is None:
        return filtered_files

    return filter_partitions(
        table_files=filtered_files, partition_select=partition_select
    )


def filter_partitions(
    table_files: list[str],
    partition_select: dict[str, Any],
) -> list[str]:
    """Filter the list of partitioned files based on specified partition selection criteria.

    Args:
        table_files (list[str]): List of file paths corresponding to partitioned files.
        partition_select (dict[str, Any]): Dictionary specifying partition selection criteria,
            where keys are partition column names and values are lists of values to match.

    Returns:
        list[str]: A filtered list of file paths that match the specified partition selection criteria.

    """
    filtered_files = []

    for file in table_files:
        parts = file.split("/")
        all_matches = True

        for key, values in partition_select.items():
            match_found = any(f"{key}={value}" in parts for value in values)

            if not match_found:
                all_matches = False
                break

        if all_matches:
            filtered_files.append(file)

    return filtered_files


def update_pyarrow_table(
    target_table: pa.Table, changes_table: pa.Table, timetravel: Optional[str] = None
) -> pa.Table:
    """Apply changes from a PyArrow table of updates and deletions to another PyArrow table.

    Args:
        target_table (pa.Table): The original PyArrow table to be updated.
        changes_table (pa.Table): The PyArrow table containing updates and deletions.
        timetravel (str, optional): A string with the date and time in the format '2024-04-15 00:00:00'. Defaults to None.

    Returns:
        pa.Table: A new PyArrow table with the changes applied.
    """
    timestamp_column = changes_table["datetime"].cast(pa.timestamp("ns"))
    changes_table = changes_table.drop(["datetime"])

    changes_table = changes_table.add_column(
        len(changes_table.column_names),
        pa.field("datetime", pa.timestamp("ns")),
        timestamp_column,
    )
    if timetravel is not None:
        timetravel_datetime = pa.scalar(
            datetime.strptime(timetravel, "%Y-%m-%d %H:%M:%S"), type=pa.timestamp("ns")
        )

        changes_table = changes_table.filter(
            pc.less_equal(changes_table["datetime"], timetravel_datetime)
        )

    # Aggregate max datetime per row_id
    row_id_max: pa.Table = changes_table.group_by("row_id").aggregate(
        [("datetime", "max")]
    )
    row_id_max = row_id_max.select(["row_id", "datetime_max"])
    row_id_max = row_id_max.rename_columns(["row_id", "datetime"])

    # Join df_changes with row_id_max to get the latest changes
    changes_table = changes_table.join(
        row_id_max, ["row_id", "datetime"], join_type="inner"
    ).combine_chunks()

    def filter_on_operation_and_drop_columns(operation: str) -> pa.Table:
        _df = changes_table.filter(pa.compute.field("operation") == operation)
        return _df.drop(["datetime", "operation", "user"])

    # Separate updates and deletions
    df_updates = filter_on_operation_and_drop_columns("update")
    df_deletes = filter_on_operation_and_drop_columns("delete")

    # Filter out rows to be deleted
    filter_array = pa.compute.invert(
        pa.compute.is_in(target_table["row_id"], changes_table["row_id"])
    )
    df_filtered = pa.compute.filter(target_table, filter_array)
    column_order = [field.name for field in df_updates.schema]

    # Select relevant columns and update schema
    df_filtered = df_filtered.select(column_order).cast(df_updates.schema)

    # Combine filtered data with updates
    df_updated = pa.concat_tables([df_filtered, df_updates])

    # Filter out rows marked for deletion
    filter_array_deletes = pa.compute.invert(
        pa.compute.is_in(df_updated["row_id"], df_deletes["row_id"])
    )
    df_output: pa.Table = pa.compute.filter(df_updated, filter_array_deletes)

    return df_output
