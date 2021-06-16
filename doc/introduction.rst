Introduction
============

Description
-----------

The Picnic Data Vault framework should be able to generate a full Data
Vault model SQL load script, based on one extraction table.

In order to perform a full Data Vault load, the framework will
generate the following queries:

1. **Staging table creation**: the generation of the script will:

   - **Calculate hashkeys and hashdiffs**.

   - **Apply default values to business keys**: every NULL business
     key received in extraction table will be defaulted to
     ``dv_unknown``.

   - **Verify all datatypes**: even though the extraction table may
     store fields as strings (depending on how the "load" step of an
     ELT is implemented), the staging table field datatypes will be
     enforced.

2. **Data Vault load script generation (from staging table), for all
   table types**:

   - *Hub*: ``INSERT`` command that appends new hub records to the
     target table (ensuring only new business keys are inserted).

   - *Link*: ``INSERT`` command that appends new link records to the
     target table (ensuring only new combination of business keys are
     inserted).

   - *Satellite and Effectivity Satellite*: ``MERGE`` command that
     merges new satellite records with the target table, ensuring:

     - Only new records are inserted (if trying to insert a new
       hashdiff).

     - No records are inserted if the start timestamp in staging table
       (``r_timestamp``) is lower that the maximum ``r_timestamp`` in
       target table.

     - If the staging table contains more than one record for the same
       business key combination, these records are deduplicated.

.. warning:: Deduplication will be handled differently.

Expected inputs
---------------

In order to generate a full Data Vault load script, the framework
expects the following inputs:

1. **Extraction start timestamp**: timestamp in which the extraction
   started.

2. **Extraction table schema**: schema where the extraction table is
   stored.

3. **Extraction table name**: table that will be used as baseline for
   staging table creation. This table should contain values for the
   following column roles:

   - business_key,

   - child_key (if applicable),

   - descriptive fields.

4. **Staging schema**: schema where the staging table should be
   stored.

5. **Staging table**: staging table functional name (the framework
   will create this table, suffixed by the extraction timestamp), but
   the input should always be the same, for the same DV model.

6. **Target tables list**: list of :class:`diepvries.table.Table`,
   representing all target tables to be populated in current Data
   Vault load.

Expected outputs
----------------

The expected output of the DataVault framework (DataVaultLoad class
object) is a list of strings, each of them representing a SQL command
needed to load the DataVault model.

The SQL commands will be chained in the following order:

1. Creation of staging table,

2. Population of hubs,

3. Population of links,

4. Population of satellites.
