# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
