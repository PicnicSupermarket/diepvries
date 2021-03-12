Model naming conventions
========================

diepvries relies on naming conventions for tables and fields. This
brings many advantages, such as a simple system to easily organize and
filter hundreds of tables, and the ease of string manipulations to
generate queries.

Tables
------

- **Hub**: ``h_<entity name>``.

.. topic:: Example

   Hub customer would be named: ``h_customer``.

- **Link**: ``l_<entity name 1>_<entity name 2>_<entity name n>``.

.. topic:: Example

   Link customer order would be named: ``l_customer_order``.

- **Link satellite**: ``ls_<entity name>_<suffix>``.

- **Hub satellite**: ``hs_<entity name>_<suffix>``.

  ``suffix`` should represent the specifics of the data stored in the
   satellite and is only mandatory when you have more than one
   satellite linked to the same hub/link.

.. topic:: Example

   ``hs_customer_address_details`` could be the name of a satellite
    that stores the customer's address data.

In case you're creating an effectivity satellite, it's a good practice
to include ``eff`` in the suffix.

Fields
------

- **Hashkey**: ``<name of the table>_hashkey``,

- **Hashdiff**: ``s_hashdiff``,

- **Child key**: ``ck_<name of the field>``,

- **Business key**: ``<name of the entity>_id``,

- **Descriptive fields**: should be the same as the source field names
  when possible.
