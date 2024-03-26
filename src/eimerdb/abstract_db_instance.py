from abc import ABC
from abc import ABCMeta
from abc import abstractmethod
from copy import copy
from typing import Any
from typing import Optional
from typing import Union

import pandas as pd
import pyarrow as pa
from docstring_inheritance import GoogleDocstringInheritanceMeta

from eimerdb.eimerdb_constants import CHANGES_ALL
from eimerdb.eimerdb_constants import PANDAS_OUTPUT_FORMAT


class Meta(ABCMeta, GoogleDocstringInheritanceMeta):
    """Metaclass for AbstractDbInstance."""

    pass


class AbstractDbInstance(ABC, metaclass=Meta):
    """Abstract class for database instance.

    All database instance classes must inherit from this class.
    """

    def __init__(
        self,
        bucket_name: str,
        eimerdb_name: str,
        path: str,
        eimer_path: str,
        created_by: str,
        time_created: str,
        tables: dict[str, Any],
        users: dict[str, Any],
        role_groups: Optional[dict[str, Any]],
        is_admin: bool,
    ) -> None:
        """Initialize AbstractDbInstance.

        Args:
            bucket_name (str): Name of the bucket.
            eimerdb_name (str): Name of the EimerDB.
            path (str): Path to the EimerDB configuration file.
            eimer_path (str): Path to the EimerDB.
            created_by (str): Name of the user who created the EimerDB.
            time_created (str): Time when the EimerDB was created.
            tables (dict): Dictionary containing the tables in the EimerDB.
            users (dict): Dictionary containing the users in the EimerDB.
            role_groups (dict, optional): Dictionary containing the role groups in the EimerDB.
            is_admin (bool): Indicates whether the current user is an admin.
        """
        self._bucket_name: str = bucket_name
        self._eimerdb_name: str = eimerdb_name
        self._path: str = path
        self._eimer_path: str = eimer_path
        self._created_by: str = created_by
        self._time_created: str = time_created
        self._tables: dict[str, Any] = tables
        self._users: dict[str, Any] = users
        self._role_groups: Optional[dict[str, Any]] = role_groups
        self._is_admin: bool = is_admin

    @property
    def bucket_name(self) -> str:
        """Get the bucket name."""
        return self._bucket_name

    @property
    def eimerdb_name(self) -> str:
        """Get the EimerDB name."""
        return self._eimerdb_name

    @property
    def path(self) -> str:
        """Get the path to the EimerDB configuration file."""
        return self._path

    @property
    def eimer_path(self) -> str:
        """Get the path to the EimerDB."""
        return self._eimer_path

    @property
    def created_by(self) -> str:
        """Get the name of the user who created the EimerDB."""
        return self._created_by

    @property
    def time_created(self) -> str:
        """Get the time when the EimerDB was created."""
        return self._time_created

    @property
    def tables(self) -> dict[str, Any]:
        """Get the tables in the EimerDB."""
        return copy(self._tables)

    @property
    def users(self) -> dict[str, Any]:
        """Get the users in the EimerDB."""
        return copy(self._users)

    @property
    def role_groups(self) -> Optional[dict[str, Any]]:
        """Get the role groups in the EimerDB."""
        return copy(self._role_groups) if self._role_groups is not None else None

    @property
    def is_admin(self) -> bool:
        """Get whether the current user is an admin."""
        return self._is_admin

    @abstractmethod
    def add_user(self, username: str, role: Any) -> None:
        """Add a user with a specified role.

        Args:
            username (str): Name of the user to add.
            role (Any): Role to assign (admin or user).

        Raises:
            PermissionError: If the user is not an admin.
            ValueError: If the user already exists.
        """

    @abstractmethod
    def remove_user(self, username: str) -> None:
        """Remove a users access to the database.

        Args:
            username (str): Name of the user to remove.

        Raises:
            PermissionError: If the user is not an admin.
            ValueError: If the user does not exist.
        """

    @abstractmethod
    def create_table(
        self,
        table_name: str,
        schema: list[dict[str, Any]],
        partition_columns: Optional[list[str]] = None,
        editable: Optional[bool] = True,
    ) -> None:
        """Create a new table in EimerDB.

        Args:
            table_name (str): Name of the new table.
            schema (str): JSON schema for the table.
            partition_columns (list, optional): List of partition columns.
            editable (bool, optional): Indicates if the table is editable.

        Raises:
            PermissionError: If the current user is not an admin.
        """

    @abstractmethod
    def insert(self, table_name: str, df: pd.DataFrame) -> list[str]:
        """Insert unedited data into a main table.

        Args:
            table_name (str): Name of the table to insert data into.
            df (pandas.DataFrame): DataFrame containing the data to insert

        Returns:
            list[str]: A list of row IDs for the inserted data.

        Raises:
            PermissionError: If the current user is not an admin.
        """

    @abstractmethod
    def combine_changes(self, table_name: str) -> None:
        """Combines the files containing the changes of the table into one file.

        Args:
            table_name (str): The name of the table for which changes are to be merged.
        """

    @abstractmethod
    def combine_inserts(self, table_name: str, raw: bool) -> None:
        """Combines the files containing the inserts of the table into one file.

        Args:
            table_name (str): The name of the table.
            raw (bool): Indicates whether to retrieve the raw schema.
        """

    @abstractmethod
    def get_arrow_schema(
        self,
        table_name: str,
        raw: bool,
    ) -> pa.Schema:
        """Get the arrow schema for a specified table.

        Args:
            table_name (str): The name of the table.
            raw (bool): Indicates whether to retrieve the raw schema.

        Returns:
            pa.Schema: The arrow schema for the specified table.
        """

    @abstractmethod
    def query(
        self,
        sql_query: str,
        partition_select: Optional[dict[str, Any]] = None,
        unedited: bool = False,
        output_format: str = PANDAS_OUTPUT_FORMAT,
    ) -> Union[pd.DataFrame, pa.Table, str]:
        """Execute an SQL query on an EimerDB table.

        Args:
            sql_query (str): SQL query to execute.
            partition_select (dict, optional): Dictionary specifying partition filters.
            unedited (bool): Indicates whether to include unedited data.
            output_format (str): Desired output format ('pandas' or 'arrow').

        Returns:
            pandas.DataFrame, pyarrow.Table, str: The result of the SQL query.

        Raises:
            ValueError: If the output format is invalid, table is not editable, or invalid query.
        """

    @abstractmethod
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
