from typing import Any
from typing import Optional

import pyarrow as pa
from dapla import FileClient


def get_partitioned_files(
    table_name: str,
    instance_name: str,
    table_config: dict[str, Any],
    suffix: str,
    fs: Any,
    partition_select: Optional[dict[str, Any]] = None,
    unedited: Optional[bool] = False,
) -> list[str]:
    """Retrieve the paths of partitioned files for a given table.

    Args:
        table_name (str): The name of the table.
        instance_name (str): The name of the instance.
        table_config (dict[str, Any]): Configuration details for the table.
        suffix (str): The suffix to be appended to the table name.
        fs (Any): The filesystem object.
        partition_select (Optional[dict[str, Any]]): Optional dictionary specifying partition
            selection criteria. Defaults to None.
        unedited (Optional[bool]): Optional flag indicating whether the file paths should include
            the suffix or not. Defaults to False.

    Returns:
        list[str]: A list of file paths corresponding to the partitioned files of the table.

    """
    partitions = table_config["partition_columns"]
    bucket_name = table_config["bucket"]
    partitions_len = len(partitions)
    partition_levels = "**/" * partitions_len + "*"
    fs = FileClient.get_gcs_file_system()
    if unedited is True:
        table_name_parts = table_name + f"{suffix}"
    else:
        table_name_parts = table_name
    table_files = fs.glob(
        f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name_parts}/{partition_levels}"
    )
    max_depth = max(obj.count("/") for obj in table_files)
    table_files = [obj for obj in table_files if obj.count("/") == max_depth]
    return table_files  # type: ignore


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
        table_files = filtered_files
    return table_files


def update_pyarrow_table(df: pa.Table, df_changes: pa.Table) -> pa.Table:
    """Apply changes from a PyArrow table of updates and deletions to another PyArrow table.

    Args:
        df (pa.Table): The original PyArrow table to be updated.
        df_changes (pa.Table): The PyArrow table containing updates and deletions.

    Returns:
        pa.Table: A new PyArrow table with the changes applied.

    """
    timestamp_column = df_changes["datetime"].cast(pa.timestamp("ns"))

    df_changes = df_changes.drop(["datetime"])  # type: ignore

    df_changes = df_changes.add_column(
        len(df_changes.column_names),
        pa.field("datetime", pa.timestamp("ns")),
        timestamp_column,
    )

    row_id_max = df_changes.group_by("row_id").aggregate([("datetime", "max")])  # type: ignore

    new_names = ["row_id", "datetime"]
    row_id_max = row_id_max.select(["row_id", "datetime_max"])

    row_id_max = row_id_max.rename_columns(new_names)  # type: ignore

    df_changes = df_changes.join(
        row_id_max, ["row_id", "datetime"], join_type="inner"
    ).combine_chunks()

    df_updates = df_changes.filter(pa.compute.field("operation") == "update")

    df_deletes = df_changes.filter(pa.compute.field("operation") == "delete")

    df_updates = df_updates.drop(["datetime", "operation", "user"])  # type: ignore
    df_deletes = df_deletes.drop(["datetime", "operation", "user"])  # type: ignore

    row_id_updates = df_changes["row_id"]
    filter_array = pa.compute.invert(pa.compute.is_in(df["row_id"], row_id_updates))  # type: ignore

    df_filtered = pa.compute.filter(df, filter_array)  # type: ignore

    df_filtered = df_filtered.cast(df_updates.schema)

    df_updated = pa.concat_tables([df_filtered, df_updates])

    row_id_deletes = df_deletes["row_id"]
    filter_array_deletes = pa.compute.invert(  # type: ignore
        pa.compute.is_in(df_updated["row_id"], row_id_deletes)  # type: ignore
    )
    df_output: pa.Table = pa.compute.filter(df_updated, filter_array_deletes)  # type: ignore
    return df_output
