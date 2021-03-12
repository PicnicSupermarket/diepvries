Glossary
========

Table Types
-----------

- **Extraction table:** A table storing all data extracted from a
  source. This is the input for the framework.

- **Staging table:** The table created from the extraction table and
  containing all DV specific fields used to load every target table
  (hubs, links and satellites).

- **Hub:** Table that keeps record of all distinct business keys of a
  specific entity.

.. topic:: Example

   Hub Customer will have one record per customer.

- **Link:** Table that keeps record of all distinct relationships
  between one or more hubs.

.. topic:: Example

   Link Customer Activity will store one record per relationship
   between Customer and Activity.

- **Satellite:** Table that contains all properties of a link or a
  satellite. The data in this table is totally historized. Each row
  has a start and end timestamp. If a change is detected, a new record
  is inserted and the start and end timestamps adjusted accordingly.

  - **Effectivity Satellite:** A special type of link satellite, that
    defines which link relationship is active at a given point in
    time, based on a subset of link fields.

.. topic:: Example

   Let's imagine Link Customer Activity presented in Link table
   example above. If we would want to keep just the latest activity
   performed by the customer as active (open version in the satellite)
   we would need an effectivity satellite, with ``h_customer_hashkey``
   defined as ``driving_key``.

Field Types
-----------

- **Business key:** Operational identifier of an entity.

.. topic:: Example

   ``customer_id``.

- **Child key:** A field that exists in the source system, is not a
  business key, but is used to define a relationship between two
  entities.

.. topic:: Example

   A link between a Customer and an Activity may have a timestamp that
   represents the time in which the customer started doing that
   activity. If a customer does the same activity three times a day,
   we record three relationships. If we did not have this field as
   child key and the Customer would do the same activity during a
   whole month, we would record one new relationship in that month.

- **Hashkey:** generated identifier, equivalent to a hashed version of
  the table business keys.

.. topic:: Example

   ``h_customer_hashkey`` will correspond to an hashed version of
   ``customer_id``, and will be used as primary key of Hub Customer
   (``h_customer``).

- **Hashdiff:** generated field, equivalent to a data row identifier
  (hashed version of all business keys + child_keys + descriptive
  field). This field is used to identify which records were changed
  between loads.

- **Metadata:** Fields that represent Data Vault specific metadata:
  for now, the three fields defined as metadata are the following:

  - ``r_timestamp``: start timestamp of a record.

  - ``r_timestamp_end``: end timestamp of a record (only applicable to
    satellites).

  - ``r_source``: source system (API/database/etc).
