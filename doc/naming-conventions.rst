Naming conventions
==================

diepvries relies on naming conventions for tables and fields. This
brings many advantages, such as a simple system to easily organize and
filter hundreds of tables, and the ease of string manipulations to
generate queries.

Tables
------

Hubs
++++

Hubs are prefixed with ``h_`` and are named after the entity they
represent: ``h_<entity>``. Conventionally, entity names are singular.

.. topic:: Example

   A hub for customers would be named: ``h_customer``.

Role-playing hubs are implemented as views on top of the physical hub,
and are suffixed with what they represent.

.. topic:: Example

   A role-playing hub for non-human customers could be named:
   ``h_customer_robot``.

Links
+++++

Links are prefixed with ``l_`` and are named after all the entities
they link: ``l_<entity_1>_<entity_2>...<entity_n>``.

.. topic:: Example

   A link for customers and orders would be named:
   ``l_order_customer``.

The order of entities in a link depend on the level of detail. For a
1-N relationship, N comes first. In the example above, one order is
linked to one customer (but one customer can have multiple orders),
hence ``order`` comes first. For 1-1 and N-M relationships, the first
entity is usually the main entity populated by a certain process.

Satellites
++++++++++

Hub satellites are prefixed with ``hs_`` and are named after the hub
they link to, with an optional suffix: ``hs_<entity>[_suffix]``.

.. topic:: Example

   A hub satellite for customers would be named: ``hs_customer``

Using ``suffix`` is a recommended practice, for traceability purposes,
as well as for distinguishing multiple satellites linked to the same
hub.

.. topic:: Example

   A hub satellite for customers' address details would be named:
   ``hs_customer_address_details``.

Link satellites are prefixed with ``ls_`` and are named after the link
they link to, with an optional suffix:
``ls_<entity_1>_<entity_2>...<entity_n>[_suffix]``.

.. topic:: Example

   A link satellite for customers and orders would be named:
   ``ls_order_customer``.

The same suffix recommendations apply for hub and link satellites.

Finally, effectivity satellites are suffixed with ``_eff``.

Fields
------

Fields also follow naming conventions. Moreover, they are always
ordered in the same way: hashkeys, business keys, child keys, metadata
fields, descriptive fields.

Hashkeys
++++++++

The hashkey of an entity is named after the table, and suffixed with
``_hashkey``: ``<table_name>_hashkey``.

.. topic:: Example

   The hashkey for the customer hub would be named:
   ``h_customer_hashkey``.

.. topic:: Example

   The hashkey for the customer/order link would be named:
   ``l_order_customer_hashkey``.

Links and satellites reference hashkeys from different parent entities
(as foreign keys), sharing the same hashkey name.

.. topic:: Example

   The customer hub satellite would have a column named
   ``h_customer_hashkey``, referencing
   ``h_customer.h_customer_hashkey``.

Business keys
+++++++++++++

The business key of an entity is named after the entity, and suffixed
with ``_id``: ``<entity>_id``.

.. topic:: Example

   The customer business key would be named ``customer_id``.

In the case of multiple business keys for a single entity, additional
words should be used: ``<entity>_<business_key_name>_id``.

.. topic:: Example

   Multiple customer business keys could be named
   ``customer_systemfoo_id``, ``customer_systembar_id``.

Child keys
++++++++++

The child keys of an entity are named after their field names, and
prefixed with ``ck_``: ``ck_<field_name>``.

Child keys are optional.

Metadata fields
+++++++++++++++

Multiple metadata fields are in use for various Data Vault
entities. They are all prefixed with ``r_``, except for
``s_hashdiff``, which is a bit special.

- ``r_timestamp``: Used in every entity, it represents the record
  creation timestamp.

- ``r_timestamp_end``: Used in satellites, it represents the record end
  of validity timestamp.

- ``r_source``: Used in every entity, it stores the source of the
  record, i.e. from where it was captured.

- ``s_hashdiff``: Used in satellites, it stores the hashdiff of the
  record.

Descriptive fields
++++++++++++++++++

Descriptive fields are all the other fields. Their names should be the
same as the source field names, except if a name conflicts with an SQL
reserved keyword, or one of the fields described above.
