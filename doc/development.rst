Development
===========

Classes diagram
---------------

.. Generated with `pyreverse data_vault/` and adapted.

.. graphviz::

   digraph "classes" {

   // Generated

   charset="utf-8"
   rankdir=BT
   "0" [label="{DataVaultLoad|extract_schema\lextract_start_timestamp\lextract_table\lsource\lsql_load_script\lstaging_create_sql_statement\lstaging_schema\lstaging_table\ltarget_tables\l}", shape="record"];
   "1" [label="{DataVaultField|data_type\lddl_in_staging\lis_mandatory\llength\lname\lname_in_staging\lparent_table_name\lparent_table_type\lposition\lprecision\lprefix\lrole\lscale\lsuffix\l}", shape="record"];
   "2" [label="{DataVaultTable|fields\lfields_by_name\lfields_by_role\lhashkey_sql\lloading_order\lname\lschema\lsql_load_statement\lsql_placeholders\lstaging_schema\lstaging_table\l}", shape="record"];
   "3" [label="{DrivingKeyField|\l}", shape="record"];
   "4" [label="{EffectivitySatellite|driving_keys\lsql_load_statement\lsql_placeholders\l}", shape="record"];
   "5" [label="{Hub|entity_name\lloading_order\lprefix\lsql_load_statement\lsql_placeholders\l}", shape="record"];
   "6" [label="{Link|loading_order\lparent_hub_names\lsql_load_statement\lsql_placeholders\l}", shape="record"];
   "7" [label="{RolePlayingHub|parent_table\lsql_load_statement\lsql_placeholders\l}", shape="record"];
   "8" [label="{Satellite|hashdiff_sql\lloading_order\lparent_table\lparent_table_name\lsql_load_statement\lsql_placeholders\l}", shape="record"];
   "4" -> "8" [arrowhead="empty", arrowtail="none"];
   "5" -> "2" [arrowhead="empty", arrowtail="none"];
   "6" -> "2" [arrowhead="empty", arrowtail="none"];
   "7" -> "5" [arrowhead="empty", arrowtail="none"];
   "8" -> "2" [arrowhead="empty", arrowtail="none"];

   // Manually added; pyreverse doesn't understand type hints

   "1" -> "2" [arrowhead="diamond", arrowtail="none", label="fields"];
   "2" -> "0" [arrowhead="diamond", arrowtail="none", label="target_tables"];
   }
