# diepvries

[![PyPI version shields.io](https://img.shields.io/pypi/v/diepvries.svg)](https://pypi.python.org/pypi/diepvries/)
[![PyPI license](https://img.shields.io/pypi/l/diepvries.svg)](https://pypi.python.org/pypi/diepvries/)
[![Travis build](https://img.shields.io/travis/com/PicnicSupermarket/diepvries/master)](https://travis.com/PicnicSupermarket/diepvries)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

Welcome to `diepvries`, a Python library to generate
[Data Vault](https://en.wikipedia.org/wiki/Data_vault_modeling) SQL statements.

## What does it do?

`diepvries` takes as input a Data Vault model (a list of hubs, links and satellites tables),
and generates SQL statements to load data in those tables. For that purpose, it relies
on naming conventions for tables and columns. There are 2 ways to feed a Data Vault
model to `diepvries`:

- Either declaratively, by enumerating tables and columns;
- Or automatically, by building these structures using the database metadata.

At the moment, `diepvries` is only compatible with
[Snowflake](https://www.snowflake.com/).

## Getting started

`diepvries` is distributed as a Python wheel on PyPI. In a virtual environment, you can
grab the latest version by running:

```shell
pip install diepvries
```

and in a Python console:

```python3
from diepvries.hub import Hub
help(Hub)
```

If you see the help page for the `Hub` class, you're all set! :rocket:

## Continue the journey

The best way to start using `diepvries` is by reading
[its documentation website](https://diepvries.picnic.tech). You'll find a tutorial, a
list of naming conventions, and more!

## Contributing

Want to fix a bug, improve the docs, or add a new feature? That's awesome! Please read
the [contributing document](https://github.com/PicnicSupermarket/diepvries/blob/master/CONTRIBUTING.md).

## Changelog

You can find the changelog of this package in
[`CHANGELOG.md`](https://github.com/PicnicSupermarket/diepvries/blob/master/CHANGELOG.md).
