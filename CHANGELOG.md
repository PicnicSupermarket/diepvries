# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Merge similar DML queries (#15).
- Remove deduplication based on hashdiffs (#14).
- Add `CHANGELOG.md` (#16).

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