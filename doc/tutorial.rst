Tutorial
========

In this tutorial, we will go over the creation of Data Vault
structures, and their automatic loading through the framework.

Some pre-requisites are required to get started:

- General knowledge about the `Data Vault 2.0
  <https://en.wikipedia.org/wiki/Data_vault_modeling>`_ modeling
  technique.

- :doc:`A working installation of diepvries <installation>`.

- An access to a `Snowflake instance
  <https://docs.snowflake.com/en/user-guide-getting-started.html>`_.

You should also create a database and schemas to host the various
structures and data:

.. code-block:: sql

   CREATE DATABASE diepvries_tutorial;
   USE DATABASE diepvries_tutorial;
   CREATE SCHEMA dv;
   CREATE SCHEMA dv_extract;
   CREATE SCHEMA dv_staging;

Three schemas are used in this example:

- ``dv``: Where the Data Vault model is stored.

- ``dv_extract``: Where the raw, extracted data lies. The tables it
  contains are used as input to the framework.

- ``dv_staging``: Staging area for data, used internally by the
  framework.

Data model
----------

Let's assume a simple data model consisting of customers and
orders. We need Data Vault entities to represent them, let's dive into
their creation and some naming conventions.

Customers
+++++++++

We start with customers:

.. literalinclude:: snippets/h_customer.sql
   :language: sql

We created two tables:

- ``dv.h_customer``: A hub, whose table name is prefixed with
  ``h_``. It contains multiple columns:

  - ``h_customer_hashkey``: The hashkey for this entity. It is the
    primary key for this table.

  - ``r_timestamp``: The creation timestamp for a record.

  - ``r_source``: The data source for a record.

  - ``customer_id``: The business key for a record.

- ``dv.hs_customer``: A hub satellite, whose table name is prefixed
  with ``hs_``. It contains multiple columns:

  - ``h_customer_hashkey``: The foreign key pointing to the hub.

  - ``s_hashdiff``: The hashdiff for a record.

  - ``r_timestamp``: The start timestamp for a record, it represents
    the beginning of that record's validity.

  - ``r_timestamp_end``: The end timestamp for a record, it represents
    the end of that record's validity.

  - ``r_source``: The data source for a record.

  - ``firstname`` and ``lastname``: Some actual values, and not
    metadata. They are the first and last names of a customer.

The primary key for the hub satellite is ``(h_customer_hashkey,
r_timestamp)``: The combination of a hub record and a given
timestamp.

At this point, you might have noticed the strong use of :doc:`naming
conventions <naming-conventions>`, with prefixes and suffixes.

Orders
++++++

Following those principles, let's create a hub and a hub satellite to
represent orders:

.. literalinclude:: snippets/h_order.sql
   :language: sql

This is of course a simplified model: we assume a business selling
only one product to customers; who can buy a different quantity of
that product.

Link
++++

The next step is to link customers and orders together. Let's create a
link:

.. literalinclude:: snippets/l_order_customer.sql
   :language: sql

Of course, if that link had fields associated with it, we would create
a link satellite. But for the purpose of this tutorial we will skip
this part.

Extraction table
----------------

The framework assumes you have raw data somewhere in a table,
extracted from an operational system. For the purpose of this
tutorial, let's generate some data. We'll pretend it comes from the
operational system handling customers and orders.

.. literalinclude:: snippets/extract.sql
   :language: sql

Notice the last item: ``customer_id`` and ``lastname`` are
``NULL``. diepvries graciously handles ``NULL`` values, as we'll see
further in this tutorial.

Loading data
------------

Now comes the interesting part. We have prepared hubs and hub
satellites for customers and orders, and have raw data waiting to be
ingested in the Data Vault! Everything is set up for the framework to
do its magic.

Let's move from SQL to Python, and have a look at this script:

.. literalinclude:: snippets/dv_load.py
   :language: python

This is the source code to ingest into the Data Vault the data coming
from our (fake) operational system. It concerns both entities,
customers, and orders, hence will load data into both hubs and both
satellites.

A few things are going on in this script:

- We prepare a
  :class:`~diepvries.deserializers.snowflake_deserializer.DatabaseConfiguration`
  to hold the Snowflake parameters.

- We instantiate a
  :class:`~diepvries.deserializers.snowflake_deserializer.SnowflakeDeserializer`. This
  object will deserialize the given tables into Python objects,
  through Snowflake introspection. We gave as a parameter the list of
  target tables we're interested in, the 2 hubs and the 2 hub
  satellites.

- We instantiate a
  :class:`~diepvries.data_vault_load.DataVaultLoad`. This is
  the main entry point to the framework. Let's look at its parameters:

  - ``extract_schema``: The schema where the raw data lives,
    ``dv_extract`` in our case.

  - ``extract_table``: The table that actually contains the raw data,
    the one we created earlier: ``order_customer``.

  - ``staging_schema``: The stating aread, used internally by the
    framework. We created ``dv_staging`` for that purpose.

  - ``staging_table``: The table used for storing staging data. It
    needs to be unique for each Data Vault script, in our case we
    chose the name ``order_customer``.

  - ``extract_start_timestamp``: The extract timestamp, i.e. now, the
    time when the data is loaded into the Data Vault. It needs to be
    timezone-aware.

  - ``target_tables``: The list of target tables. We computed it
    earlier with the Snowflake deserializer.

  - ``source``: The source of the data.

After filling in the correct credentials for your Snowflake account,
you can run this script. Be sure to execute it in the virtual
environment where diepvries is installed, so it can be imported.

Et voilà! The Python script will print out all the SQL statements
needed to load ``h_customer``, ``hs_customer``, ``h_order`` and
``hs_order`` with the data from ``dv_extract.order_customer``.

After executing those, you should inspect the content of the tables:

.. code-block:: sql

   SELECT * FROM dv.h_customer;
   SELECT * FROM dv.hs_customer;
   SELECT * FROM dv.h_order;
   SELECT * FROM dv.hs_order;

Notice how ``h_customer`` has a row with the value ``dv_unknown`` for
the column ``customer_id``. This is the ghost record, automatically
created by diepvries due to a row in the input data having ``NULL`` as
``customer_id``. This happens for business IDs; other NULLable columns
will retain ``NULL`` values.

Most of the content of the Python script is unrelated to the data
being loaded and the targeted Data Vault structures. This is the basic
recipe you can use for loading data from multiple sources to multiple
targets; with a few adaptations it can easily be configured to load
all your data.

How you organize and orchestrate multiple sources is up to you;
diepvries provides the building blocks, allowing more flexibility into
what you build with it.

Next steps
----------

Hungry for more? Head over the :doc:`advanced topics
<advanced-topics>` or dive into the :doc:`API documentation
<api/modules>`!
