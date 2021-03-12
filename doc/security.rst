Security
========

This framework generates queries using string substitutions. This is
necessary, as server-side variable binding does not work with all
parts of SQL queries. For example, it is possible to pass parameters
to Snowflake as the ``VALUES`` of an ``INSERT`` query, but it is not
possible to use variables for a table name without resorting to
``IDENTIFIER()``.

The raw data loaded in a Data Vault model has to be stored in a table
somewhere for the framework to pick it up. Hence, it is assumed it has
already been sanitized against potential SQL injections.
