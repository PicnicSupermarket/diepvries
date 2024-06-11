# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.1] - 2024-06-11
### Fixed
- Stop using invalid `password` authenticator.

## [1.0.0] - 2024-06-05
### Added
- Add support for external browser authentication in `SnowflakeDeserializer`.
- Add GitHub actions workflow for creating GitHub releases.

### Changed
- Use `pyproject.toml` for setuptools config.

## [0.9.1] - 2023-09-13
### Changed
- Go back 4 hours in the calculation of minimum record timestamp to avoid issues in concurrent loads.

## [0.9.0] - 2023-09-04
### Changed
- Add extra filter on the effectivity satellite's query to reduce the number of scanned
  records on the parent link.

## [0.8.0] - 2023-08-28
### Added
- Add method `sql_load_scripts_by_group` to `DataVaultLoad`.

### Changed
- Add filter to `MERGE` clauses to reduce the number of scanned records. DML queries now
  consist of two statements, a `SET` and a `MERGE`, instead of just a `MERGE` query.

## [0.7.0] - 2023-05-26
### Added
- Add validation for driving keys (#41)

### Changed
- Exclude more recent records from satellites based on hashkey or driving key (#42)

## [0.6.4] - 2023-03-01
### Added
- Add support for python 3.10 (#37)

### Changed
- Update `snowflake-connector-python` to `~=3.0` (#37)

## [0.6.3] - 2022-08-25
### Added
- Add `StagingTable` class (#30).
- Add the pre-commit tool (#34).

### Changed
- Changed CI/CD from Travis to Github Actions (#33).

## [0.6.2] - 2021-08-20
### Added
- Apply LISTAGG to r_source in link/hub DML (#23).

## [0.6.1] - 2021-08-19
### Added
- Force CAST to in hash generation when the target field is TEXT (#21).

## [0.6.0] - 2021-08-18
### Added
- Add `CHANGELOG.md` (#16).

### Changed
- Merge similar DML queries (#15).
- Make hashkey and hashdiff generation deterministic (#17).
- Add DISTINCT to filtered_staging in satellite MERGE statement (#18).
- Change link/hub DML to apply a DISTINCT instead of a MIN (#19).

### Removed
- Remove deduplication based on hashdiffs (#14).

## [0.5.2] - 2021-07-30
### Changed
- Grammar issue in documentation.
- Graphviz installation in Travis for documentation website.

## [0.5.1] - 2021-07-29
### Changed
- Correct URLs for the Pypi page.
- Serving of all files in Github pages.

### Removed
- Deprecated documentation page `introduction.rst`.
- Bash script to release package (now done through tox).

## [0.5.0] - 2021-07-02
### Added
- Bash script to build and upload Python package.
- Travis pipeline for CI.
- `py.typed` for Mypy.
- CD pipeline for documentation website.
- `README.md`.
- CD pipeline for Pypi artifacts.

### Changed
- This Python package is now called `diepvries` (previously `picnic-data-vault`).
