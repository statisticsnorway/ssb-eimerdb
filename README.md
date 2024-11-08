# EimerDB

[![PyPI](https://img.shields.io/pypi/v/ssb-eimerdb.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/ssb-eimerdb.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-eimerdb)][pypi status]
[![License](https://img.shields.io/pypi/l/ssb-eimerdb)][license]

[![Documentation](https://github.com/statisticsnorway/ssb-eimerdb/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/ssb-eimerdb/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-eimerdb&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-eimerdb&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/ssb-eimerdb/
[documentation]: https://statisticsnorway.github.io/ssb-eimerdb
[tests]: https://github.com/statisticsnorway/ssb-eimerdb/actions?workflow=Tests

[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-eimerdb
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-eimerdb
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

## About

EimerDB is a python package that gives database-like functionality to parquet files stored in google cloud storage.
It achieves this by organising the parquet files in a certain way, reads and combines them with pyarrow and then query the combined pyarrow tables with
duckdb. For use as a part of the statistical production process at Statistics Norway.

## Features

### Create and connect to a database

Create a new database by specifying the bucket name and a database name.

```python
import eimerdb as db

db.create_eimerdb(bucket_name="bucket-name", db_name="prodcombasen")
```

Connect to your EimerDB database.

```python
prodcombasen = db.EimerDBInstance("bucket-name", "prodcombasen")
```

### Table Management

You can create a new table with the create_table method. Specify the table name, the schema, the partition columns and set if the
table is editable or not. Define the columns in the schema, with a column name, type and a label.

```python
schema = [
    {
        "name": "aar",
        "type": "int16",
        "label": "Årgangen."
    },
    {
        "name": "ident",
        "type": "string",
        "label": "Foretakets identifikator."
    },
    {
        "name": "skjemaversjon",
        "type": "string",
        "label": "Skjemaets versjon."
    },
    {
        "name": "råvarekode",
        "type": "string",
        "label": "Prefillet råvarekode. Disse kodene lages av NR."
    },
    {
        "name": "beskrivelse",
        "type": "string",
        "label": "Prefillet råvarebeskrivelse. Disse beskrivelsene lages av NR."
    },
    {
        "name": "forbruk",
        "type": "int64",
        "label": "Oppgitt forbruk (i 1 000 NOK) til den tilhørende råvarekoden."
    },
]

prodcombasen.create_table(
    table_name="prefill_prod",
    schema,
    partition_columns=["aar"],
    editable=True
)
```

Partitioning the table by one or more columns will help improve query performance

### SQL Query Support

Query your tables with SQL syntax. You can optionally specify the partition to be queried.

```python
prodcombasen.query(
    """SELECT *
    FROM prodcom_prefill
    WHERE produktkode = '10.13.11.20'""",
    partition_select = {
        "aar": [2022, 2021]
        }
```

### Updates

Perform updates using SQL statements
Each update is saved as a separate parquet file for versioning. The update files includes a username column and
a datetime column for when the update happened.

```python
prodcombasen.query(
    """UPDATE prodcom_prefill
    SET mengde = 123
    WHERE ident = '123456'
    AND produktkode = '10.13.11.20'""",
    partition_select = partitions
)
```

### Easily access the unedited version of a table

Retrieve the unedited version of your data by specifying unedited=True.

```python
prodcombasen.query(
    """SELECT *
    FROM prodcom_prefill""",
    unedited=True
)
```

### Query the changes made to a table

You can query alle the changes made to the table with the query_changes method.

```python
prodcombasen.query_changes(
    """SELECT *
    FROM prodcom_prefill""",
    unedited=True
)
```

### Query multiple tables

Query multiple tables using JOIN and subquery.

```python
prodcombasen.query(
    f"""SELECT
            t1.aar,
            t1.produktkode,
            t1.beskrivelse,
            SUM(t1.mengde) AS mengde
        FROM
            prefill_prod AS t1
        JOIN (
            SELECT
                t2.aar,
                t2.ident,
                t2.skjemaversjon,
                MAX(t2.dato_mottatt) AS newest_dato_mottatt
            FROM
                skjemainfo AS t2
            GROUP BY
                t2.aar,
                t2.ident,
                t2.skjemaversjon
        ) AS subquery ON
            t1.aar = subquery.aar
            AND t1.ident = subquery.ident
            AND t1.skjemaversjon = subquery.skjemaversjon
        WHERE
            t1.mengde IS NOT NULL
        GROUP BY
            t1.aar,
            t1.produktkode,
            t1.beskrivelse;""",
        partition_select={
            "aar": [2022, 2021, 2020]
        },
    )
```

### User Management (in development)

Add and remove users from your instance.
Assign specific roles to users for access control.

```python
prodcombasen.add_user(username="newuser", role="admin")
prodcombasen.remove_user(username="olduser")
```

## Requirements

- TODO

## Installation

You can install _EimerDB_ via [pip] from [PyPI]:

```console
pip install ssb-eimerdb
```

## Usage

Please see the [Reference Guide] for details.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_EimerDB_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/ssb-eimerdb/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/ssb-eimerdb/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/ssb-eimerdb/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/ssb-eimerdb/reference.html
