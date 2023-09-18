# EimerDB

[![PyPI](https://img.shields.io/pypi/v/ssb-eimerdb.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/ssb-eimerdb.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-eimerdb)][pypi status]
[![License](https://img.shields.io/pypi/l/ssb-eimerdb)][license]

[![Documentation](https://github.com/statisticsnorway/ssb-eimerdb/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/ssb-eimerdb/workflows/Tests/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-eimerdb&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-eimerdb&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/ssb-eimerdb/
[documentation]: https://statisticsnorway.github.io/ssb-eimerdb
[tests]: https://github.com/statisticsnorway/ssb-eimerdb/actions?workflow=Tests
[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-eimerdb
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-eimerdb
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

## Features

### Google Cloud Storage Integration

Create your custom instance for data storage
```python
create_eimerdb(bucket="bucket-name", db_name="mvabasen")
```
Seamlessly connect to instances hosted on Google Cloud Storage.
```python
eimerdb = EimerDBInstance("bucket-name", "myinstance")
```

### Table Management

Easily create tables with defined schemas.
```python
mvabasen.create_table(
    table_name="Hovedtabell", schema, partition_columns=["aar", "termin"], editable=True
)
```
Partition tables for efficient data organization.

### SQL Query Support

Execute SQL queries directly on your data.
```python
mvabasen.query("SELECT * FROM hovedtabell WHERE substr(nace,1,3) = '479'")
```
    

### Data Updates

Perform data updates using SQL statements, including inserts, updates, and deletes.
Changes are recorded as commits for versioning.
```python
mvabasen.query(
    "UPDATE hovedtabell SET omsetning = 999 WHERE aar = '2022' AND termin = '2' AND orgb = '987654321'"
)
```

### Unedited Data Access

Retrieve the unedited version of your data for auditing or historical purposes.
```python
mvabasen.query("SELECT * FROM hovedtabell WHERE orgb = '987654321'", unedited=True)
```

### Partition Filtering

Filter data based on partition keys, allowing for precise data selection.
Optimize query performance by narrowing down search criteria.
```python
partitions = {
    "aar": ["2022", "2023"],
    "termin": ["2", "3"]
}

mvabasen.query("SELECT * FROM hovedtabell WHERE orgb = '911803259'", partition_select=partitions)
```

### User Management

Add and remove users from your instance.
Assign specific roles to users for access control.
```python
mvabasen.add_user(username="newuser", role="admin")
```

## Requirements

- TODO

## Installation

You can install _EimerDB_ via [pip] from [PyPI]:

```console
$ pip install ssb-eimerdb
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
