from typing import List, Dict

from . import HASH_DELIMITER, TEMPLATES_DIR, FieldRole, FIELD_SUFFIX, METADATA_FIELDS
from .data_vault_field import DataVaultField
from .data_vault_table import DataVaultTable
from .driving_key_field import DrivingKeyField
from .template_sql.sql_formulas import (
    BUSINESS_KEY_SQL_TEMPLATE,
    CHILD_KEY_SQL_TEMPLATE,
    DESCRIPTIVE_FIELD_SQL_TEMPLATE,
    END_OF_TIME_SQL_TEMPLATE,
    HASHDIFF_SQL_TEMPLATE,
    format_fields_for_join,
    format_fields_for_select,
    RECORD_END_TIMESTAMP_SQL_TEMPLATE,
)


class Satellite(DataVaultTable):
    _parent_table = None

    def __init__(
        self,
        schema: str,
        name: str,
        fields: List[DataVaultField],
        driving_keys: List[DrivingKeyField] = None,
    ):
        """
        Instantiates a Satellite.

        Satellite: Data Vault table that contains all properties of a link or a satellite.
        The data in this table is totally historized. Each row has a start and end timestamp.
        There are no deletes in this type of tables, if something changes, a new record is inserted
        and the start and end timestamps adjusted accordingly.

        Example: Customer Satellite will hold all customer properties: name,
        date of registration, address, etc...

        In case we are dealing with a link satellite, we might need an effectivity satellite.
        An effectivity satellite is a special type of satellite with the purpose of keeping
        "versions open" based on a subset of fields in the link.

        Example: we have a link between a Customer, and a Contact and we want to keep only
        one contact version per customer at a given point in time. This means that, if a customer
        changes contacts, we only keep the latest relationship between a Customer and its
        Contact as an active relationship. For this specific example, Hub Customer's hashkey would be
        our driving key.

        The distinction between a regular and an effectivity satellite is made based on the existence
        of driving keys.

        Args:
            schema (str): Data Vault schema name.
            name (str): Satellite name.
            fields (List[DataVaultField]): List of fields that this Hub holds.
            driving_keys (List[DrivingKeyField]): Defines the set of link (parent table) fields
                that should be used as driving keys (in the example presented above, the driving
                key would be the h_customer_hashkey). Only applicable for effectivity satellites.
        """
        super().__init__(schema, name, fields)
        self.driving_keys = driving_keys
        self.parent_table = None

    @property
    def loading_order(self) -> int:
        """
        Gets loading order (satellites are the third and last tables to be loaded).

        Returns:
            int: Table loading order.
        """
        return 3

    def _validate(self):
        """
        Performs Satellite specific checks (besides common ones - check parent class):
        1. Table has one hashkey for parent table (hub or link);
        2. Table has one end timestamp field (r_timestamp_end);
        3. Table has one hashdiff field (s_hashdiff).

        Raises:
            RuntimeError: If one of the checks fails.
        """
        super()._validate()

        if not self.fields_by_role.get(FieldRole.HASHKEY_PARENT):
            raise RuntimeError(f"'{self.name}': No hashkeys for parent table found")

        if not self.fields_by_name.get(METADATA_FIELDS["record_end_timestamp"]):
            raise RuntimeError(
                f"'{self.name}': No field named '{METADATA_FIELDS['record_end_timestamp']}' found"
            )

        hashdiff_name = f"s_{FIELD_SUFFIX[FieldRole.HASHDIFF]}"
        if not self.fields_by_name.get(hashdiff_name):
            raise RuntimeError(f"'{self.name}': No field named '{hashdiff_name}' found")

    @property
    def sql_load_statement(self) -> str:
        """
        Generate the SQL query to populate current satellite.

        This method is prepared to load both regular and effectivity satellites.
        The distinction between both is made based on the existence of driving key fields.

        All needed placeholders are calculated, in order to match template SQL:
         - If regular satellite: check template_sql.satellite_dml.sql;
         - If effectivity satellite: check template_sql.effectivity_satellite_dml.sql.

        Returns:
            str: SQL query to load target satellite.
        """
        if self.driving_keys:
            sql_load_statement = self._effectivity_satellite_sql
        else:
            sql_load_statement = self._satellite_sql

        return sql_load_statement

    @property
    def parent_table_name(self) -> str:
        """
        Gets the name of current table's parent, following this rule:
        - Name of table's hashkey field, without the suffix.

        Returns:
            str: Parent table name.
        """
        parent_table_name = next(
            field.name
            for field in self.fields
            if field.name.endswith(FIELD_SUFFIX[FieldRole.HASHKEY])
        ).replace(f"_{FIELD_SUFFIX[FieldRole.HASHKEY]}", "")

        return parent_table_name

    @property
    def _satellite_sql(self) -> str:
        """
        Generate the SQL query to populate current satellite
        (if table does not have driving keys - regular satellite).

        All needed placeholders are calculated, in order to match template SQL (check
        template_sql.satellite_dml.sql).

        Returns:
            str: SQL query to load target satellite.
        """
        record_end_timestamp = RECORD_END_TIMESTAMP_SQL_TEMPLATE.format(
            key_fields=self.satellite_common_sql_placeholders["hashkey_field"]
        )

        sql_load_statement = (
            (TEMPLATES_DIR / "satellite_dml.sql")
            .read_text()
            .format(
                **self.sql_placeholders,
                **self.satellite_common_sql_placeholders,
                record_end_timestamp_expression=record_end_timestamp,
            )
        )

        self._logger.info("Loading SQL for satellite (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement

    @property
    def _effectivity_satellite_sql(self) -> str:
        """
        Generate the SQL query to populate current effectivity satellite
        (if table has driving keys - effectivity satellite).

        All needed placeholders are calculated, in order to match template SQL (check
        template_sql.effectivity_satellite_dml.sql).

        Returns:
            str: SQL query to load target satellite.
        """

        sql_load_statement = (
            (TEMPLATES_DIR / "effectivity_satellite_dml.sql")
            .read_text()
            .format(
                **self.sql_placeholders,
                **self.satellite_common_sql_placeholders,
                **self.effectivity_satellite_sql_placeholders,
            )
        )

        self._logger.info(
            "Loading SQL for effectivity satellite (%s) generated.", self.name
        )
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement

    @property
    def hashdiff_sql(self) -> str:
        """
        Generate the SQL expression that should be used to calculate a hashdiff field.

        The hashdiff formula is the following:
            - MD5(business_key_1 + |~~| + business_key_n + |~~| + child_key_1 + |~~|
        descriptive_field_1). To assure that a hashdiff does not change if a new field
        is added to the table, we assure that all |~~| character sequence in the end of the
        string is removed.

        Returns:
            str: Hashdiff SQL expression.
        """
        hashdiff = next(
            hashdiff for hashdiff in self.fields_by_role.get(FieldRole.HASHDIFF)
        )
        business_keys_sql = [
            BUSINESS_KEY_SQL_TEMPLATE.format(business_key=field)
            for field in format_fields_for_select(
                fields=self.parent_table.fields_by_role.get(FieldRole.BUSINESS_KEY)
            )
        ]
        child_keys_sql = [
            CHILD_KEY_SQL_TEMPLATE.format(child_key=field)
            for field in format_fields_for_select(
                fields=self.parent_table.fields_by_role[FieldRole.CHILD_KEY]
            )
        ]
        descriptive_fields_sql = [
            DESCRIPTIVE_FIELD_SQL_TEMPLATE.format(descriptive_field=field)
            for field in format_fields_for_select(
                fields=self.fields_by_role.get(FieldRole.DESCRIPTIVE)
            )
        ]
        fields_for_hashdiff = business_keys_sql

        if child_keys_sql:
            fields_for_hashdiff.extend(child_keys_sql)

        if descriptive_fields_sql:
            fields_for_hashdiff.extend(descriptive_fields_sql)

        hashdiff_expression = f"||'{HASH_DELIMITER}'||".join(fields_for_hashdiff)

        hashdiff_sql = HASHDIFF_SQL_TEMPLATE.format(
            hashdiff_expression=hashdiff_expression, hashdiff=hashdiff.name_in_staging
        )
        self._logger.debug(
            "Hashdiff SQL expression for table (%s) is (%s)", self.name, hashdiff_sql
        )

        return hashdiff_sql

    @property
    def satellite_common_sql_placeholders(self) -> Dict[str, str]:
        """
        Calculates common placeholders needed to generate SQL for all Satellites.

        As satellites can either be effectivity or regular satellites, the load can be done
        using one of two different template SQL files.
        In this function we calculate the placeholders that are common between both template SQL.

        Returns:
            Dict[str, str]: Common placeholders to use in all Satellite SQL scripts.
        """
        hashkey = next(
            hashkey
            for hashkey in format_fields_for_select(
                fields=self.fields_by_role.get(FieldRole.HASHKEY_PARENT)
            )
        )

        hashdiff = next(
            hashdiff for hashdiff in self.fields_by_role.get(FieldRole.HASHDIFF)
        )

        descriptive_fields = self.fields_by_role.get(FieldRole.DESCRIPTIVE)
        satellite_descriptive_fields = ",".join(
            format_fields_for_select(fields=descriptive_fields, table_alias="satellite")
        )
        satellite_descriptive_fields_sql = (
            f",{satellite_descriptive_fields}" if satellite_descriptive_fields else ""
        )
        staging_descriptive_fields = ",".join(
            format_fields_for_select(fields=descriptive_fields, table_alias="staging")
        )
        staging_descriptive_fields_sql = (
            f",{staging_descriptive_fields}" if staging_descriptive_fields else ""
        )

        descriptive_fields_sql = ",".join(
            format_fields_for_select(fields=descriptive_fields)
        )
        descriptive_fields_sql = (
            f",{descriptive_fields_sql}" if descriptive_fields_sql else ""
        )

        query_args = {
            "hashkey_field": hashkey,
            "hashdiff_field": hashdiff.name,
            "staging_hashdiff_field": hashdiff.name_in_staging,
            "staging_descriptive_fields": staging_descriptive_fields_sql,
            "satellite_descriptive_fields": satellite_descriptive_fields_sql,
            "descriptive_fields": descriptive_fields_sql,
            "end_of_time": END_OF_TIME_SQL_TEMPLATE,
            "record_end_timestamp_name": METADATA_FIELDS["record_end_timestamp"],
        }

        return query_args

    @property
    def effectivity_satellite_sql_placeholders(self) -> Dict[str, str]:
        """
        Calculates effectivity satellite specific placeholders, needed to generate SQL.

        Returns:
            Dict[str, str]: Effectivity satellite specific placeholders,
                to use in effectivity satellites.
        """
        record_end_timestamp = RECORD_END_TIMESTAMP_SQL_TEMPLATE.format(
            key_fields=",".join(format_fields_for_select(fields=self.driving_keys))
        )
        # Handle driving_keys fields query placeholders.
        driving_keys_sql = ",".join(format_fields_for_select(fields=self.driving_keys))
        satellite_driving_keys_sql = ",".join(
            format_fields_for_select(fields=self.driving_keys, table_alias="satellite")
        )
        staging_driving_keys_sql = ",".join(
            format_fields_for_select(fields=self.driving_keys, table_alias="staging")
        )

        link_driving_keys_sql = ",".join(
            format_fields_for_select(fields=self.driving_keys, table_alias="l")
        )

        # Handle driving key condition query placeholder.
        satellite_driving_key_condition = " AND ".join(
            format_fields_for_join(
                fields=self.driving_keys,
                table_1_alias="satellite",
                table_2_alias="staging",
            )
        )
        link_driving_key_condition = " AND ".join(
            format_fields_for_join(
                fields=self.driving_keys, table_1_alias="l", table_2_alias="staging"
            )
        )
        query_args = {
            "link_table": self.parent_table.name,
            "driving_keys": driving_keys_sql,
            "satellite_driving_keys": satellite_driving_keys_sql,
            "staging_driving_keys": staging_driving_keys_sql,
            "link_driving_keys": link_driving_keys_sql,
            "satellite_driving_key_condition": satellite_driving_key_condition,
            "link_driving_key_condition": link_driving_key_condition,
            "record_end_timestamp_expression": record_end_timestamp,
        }

        return query_args
