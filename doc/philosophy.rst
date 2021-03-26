Philosophy
==========

diepvries has been designed with the following in mind:

Convention over configuration
-----------------------------

Strong :doc:`naming conventions <naming-conventions>` for table and
column names allow diepvries to magically know the role of a given
entity. For example, ``h_customer`` is a hub, ``hs_customer`` is a hub
satellite, and ``h_customer_hashkey`` is the hashkey of the customer
hub. These conventions have been carefully crafted to remove
ambiguities and make everything predictable. On top of this, Data
Vault tables are easier to analyze when they are sorted by type, which
comes for free with the table prefixes.

Relying on conventions rather than configuration allows focusing on
business logic and does not clutter source code with configuration
files, which are hard to maintain and allow inconsistencies to creep
in.

Automatic field mapping
-----------------------

Field mapping (between source systems and Data Vault entities) is
often manually done and error-prone. In an extraction table, diepvries
assumes the field names are the same as in the Data Vault tables,
allowing a natural mapping based on identical names.

No external dependencies
------------------------

With the exception of the Python Snowflake connector (to be able to
automatically deserialize tables into Python objects), diepvries
doesn't depend on anything but a Python runtime. This is a standalone
tool that does not bring outdated or conflicting packages into your
dependencies tree. As a result, it is very lightweight.

Restricted scope
----------------

diepvries is not a full-blown solution to manage your Data
Vault. Instead, it focuses on one thing and does it well: generating
SQL to load data in a Data Vault. This makes it a very flexible tool
to integrate into your environment: it does not care how data reaches
extraction tables, or how the generated SQL is executed.
