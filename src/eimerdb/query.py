from typing import Any
from typing import Optional
from typing import Union
from dapla import FileClient
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

def get_partitioned_files(
    table_name: str,
    instance_name: str,
    table_config: dict,
    suffix: str,
    fs,
    partition_select: Optional[dict[str, Any]] = None,
    unedited: Optional[bool] = False,
    ) -> list:
    """ABC.

    Returns:
        ABC.

    """
    editable = table_config["editable"]
    partitions = table_config["partition_columns"]
    bucket_name = table_config["bucket"]
    partitions_len = len(partitions)
    partition_levels = "**/" * partitions_len + "*"
    fs = FileClient.get_gcs_file_system()
    if unedited is True:
        table_name_parts = table_name + f"{suffix}" # _raw, _changes. _changes_all
    else:
        table_name_parts = table_name
    table_files = fs.glob(
        f"gs://{bucket_name}/eimerdb/{instance_name}/{table_name_parts}/{partition_levels}"
    )
    max_depth = max(obj.count("/") for obj in table_files)
    table_files = [
        obj for obj in table_files if obj.count("/") == max_depth
    ]
    return table_files


def filter_partitions(
        table_files: list,
        partition_select: dict,
):
    """ABC.

    Returns:
        ABC.

    """
    filtered_files = []
    for file in table_files:
        parts = file.split("/")

        all_matches = True

        for key, values in partition_select.items():
            match_found = any(
                f"{key}={value}" in parts for value in values
            )

            if not match_found:
                all_matches = False
                break
        if all_matches:
            filtered_files.append(file)
        table_files = filtered_files
    return table_files

def update_pyarrow_table(df: pa.Table, df_changes: pa.Table) -> pa.Table:
    """ABC.

    Returns:
        ABC.

    """
    timestamp_column = df_changes["datetime"].cast(
        pa.timestamp("ns")
    )

    df_changes = df_changes.drop(["datetime"])

    df_changes = df_changes.add_column(
        len(df_changes.column_names),
        pa.field("datetime", pa.timestamp("ns")),
        timestamp_column,
    )

    row_id_max = df_changes.group_by("row_id").aggregate(
        [("datetime", "max")]
    )

    new_names = ["row_id", "datetime"]
    row_id_max = row_id_max.select(["row_id", "datetime_max"])

    row_id_max = row_id_max.rename_columns(new_names)

    df_changes = df_changes.join(
        row_id_max, ["row_id", "datetime"], join_type="inner"
    ).combine_chunks()

    df_updates = df_changes.filter(
        pa.compute.field("operation") == "update"
    )

    df_deletes = df_changes.filter(
        pa.compute.field("operation") == "delete"
    )

    df_updates = df_updates.drop(["datetime", "operation", "user"])
    df_deletes = df_deletes.drop(["datetime", "operation", "user"])

    row_id_updates = df_changes["row_id"]
    filter_array = pa.compute.invert(
        pa.compute.is_in(df["row_id"], row_id_updates)
    )

    df_filtered = pa.compute.filter(df, filter_array)

    df_filtered = df_filtered.cast(df_updates.schema)

    df_updated = pa.concat_tables([df_filtered, df_updates])

    row_id_deletes = df_deletes["row_id"]
    filter_array_deletes = pa.compute.invert(
        pa.compute.is_in(df_updated["row_id"], row_id_deletes)
    )
    df_output = pa.compute.filter(df_updated, filter_array_deletes)
    return df_output
