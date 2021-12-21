"""A Satellite."""

from typing import Dict, Optional, Union

from . import FIELD_SUFFIX, HASH_DELIMITER, METADATA_FIELDS, TEMPLATES_DIR, FieldRole
from .hub import Hub
from .link import Link
from .table import DataVaultTable
from .template_sql.sql_formulas import (
    END_OF_TIME_SQL_TEMPLATE,
    HASHDIFF_SQL_TEMPLATE,
    RECORD_END_TIMESTAMP_SQL_TEMPLATE,
    format_fields_for_select,
)


class Satellite(DataVaultTable):
    """A Satellite.

    A Satellite is a Data Vault table that contains all properties of a link or a
    hub. The data in this table is totally historized. Each row has a start and end
    timestamp. There are no deletes in this type of tables. If something changes,
    a new record is inserted and the start and end timestamps adjusted accordingly.

    Example: Customer Satellite will hold all customer properties: name,
    date of registration, address, etc...
    """

    # Parent table is set after instantiation.
    parent_table: Optional[Union[Link, Hub]] = None

    @property
    def loading_order(self) -> int:
        """Get loading order (satellites are the third and last tables to be loaded).

        Returns:
           Table loading order.
        """
        return 3

    def _validate(self):
        """Perform Satellite specific checks (besides common ones - check parent class).

        1. Table has one hashkey for parent table (hub or link);
        2. Table has one end timestamp field (r_timestamp_end);
        3. Table has one hashdiff field (s_hashdiff).

        Raises:
            KeyError: If any of the checks fail.
        """
        super()._validate()

        try:
            self.fields_by_role[FieldRole.HASHKEY_PARENT]
        except KeyError as e:
            raise KeyError(f"'{self.name}': No hashkeys for parent table found") from e

        try:
            self.fields_by_name[METADATA_FIELDS["record_end_timestamp"]]
        except KeyError as e:
            raise KeyError(
                f"'{self.name}': No field named "
                f"'{METADATA_FIELDS['record_end_timestamp']}' found"
            ) from e

        hashdiff_name = f"s_{FIELD_SUFFIX[FieldRole.HASHDIFF]}"
        try:
            self.fields_by_name[hashdiff_name]
        except KeyError as e:
            raise KeyError(
                f"'{self.name}': No field named '{hashdiff_name}' found"
            ) from e

    @property
    def sql_load_statement(self) -> str:
        """Get the SQL query to populate the satellite.

        All needed placeholders are calculated, in order to match template SQL (check
        template_sql.satellite_dml.sql).

        Returns:
            SQL query to load target satellite.
        """
        record_end_timestamp = RECORD_END_TIMESTAMP_SQL_TEMPLATE.format(
            key_fields=self.sql_placeholders["hashkey_field"]
        )

        sql_load_statement = (
            (TEMPLATES_DIR / "satellite_dml.sql")
            .read_text()
            .format(
                **self.sql_placeholders,
                record_end_timestamp_expression=record_end_timestamp,
            )
        )

        self._logger.info("Loading SQL for satellite (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement

    @property
    def parent_table_name(self) -> str:
        """Get the name the parent table.

        It is calculated by removing the _hashkey suffix from the table's hashkey field.

        Returns:
            Parent table name.
        """
        parent_table_name = next(
            field.name
            for field in self.fields
            if field.name.endswith(FIELD_SUFFIX[FieldRole.HASHKEY])
        ).replace(f"_{FIELD_SUFFIX[FieldRole.HASHKEY]}", "")

        return parent_table_name

    @property
    def hashdiff_sql(self) -> str:
        """Get the SQL expression that should be used to calculate a hashdiff field.

        The hashdiff formula is the following:::

            MD5(business_key_1   + |~~|
                + business_key_n + |~~|
                + child_key_1    + |~~|
                + descriptive_field_1)

        To ensure that a hashdiff does not change if a new field is added to the
        table, it is assumed that all ``|~~|`` character sequences placed at the end of
        the string are removed.

        Returns:
            Hashdiff SQL expression.
        """
        hashdiff = next(
            hashdiff for hashdiff in self.fields_by_role[FieldRole.HASHDIFF]
        )
        fields_for_hashdiff = [
            field.hash_concatenation_sql
            for field in self.parent_table.fields_by_role[FieldRole.BUSINESS_KEY]
        ]
        fields_for_hashdiff.extend(
            [
                field.hash_concatenation_sql
                for field in self.parent_table.fields_by_role[FieldRole.CHILD_KEY]
            ]
        )
        fields_for_hashdiff.extend(
            [
                field.hash_concatenation_sql
                for field in self.fields_by_role[FieldRole.DESCRIPTIVE]
            ]
        )

        hashdiff_expression = f"||'{HASH_DELIMITER}'||".join(fields_for_hashdiff)

        hashdiff_sql = HASHDIFF_SQL_TEMPLATE.format(
            hashdiff_expression=hashdiff_expression, hashdiff=hashdiff.name_in_staging
        )
        self._logger.debug(
            "Hashdiff SQL expression for table (%s) is (%s)", self.name, hashdiff_sql
        )

        return hashdiff_sql

    @property
    def sql_placeholders(self) -> Dict[str, str]:
        """Satellite specific SQL placeholders.

        These placeholders are used to format the Satellite loading query.

        The results are joined with the results from super().sql_placeholders(), as all
        placeholders calculated in Table (parent class) are applicable in a
        Satellite.

        Returns:
            Satellite specific SQL placeholders.
        """
        hashkey = next(
            hashkey
            for hashkey in format_fields_for_select(
                fields=self.fields_by_role[FieldRole.HASHKEY_PARENT]
            )
        )

        hashdiff = next(
            hashdiff for hashdiff in self.fields_by_role[FieldRole.HASHDIFF]
        )
        fields = ", ".join(format_fields_for_select(fields=self.fields))
        descriptive_fields = ", ".join(
            format_fields_for_select(fields=self.fields_by_role[FieldRole.DESCRIPTIVE])
        )
        satellite_descriptive_fields = ", ".join(
            format_fields_for_select(
                fields=self.fields_by_role[FieldRole.DESCRIPTIVE],
                table_alias="satellite",
            )
        )
        staging_descriptive_fields = ", ".join(
            format_fields_for_select(
                fields=self.fields_by_role[FieldRole.DESCRIPTIVE], table_alias="staging"
            )
        )

        if self.fields_by_role[FieldRole.DESCRIPTIVE]:
            descriptive_fields = f", {descriptive_fields}"
            satellite_descriptive_fields = f", {satellite_descriptive_fields}"
            staging_descriptive_fields = f", {staging_descriptive_fields}"

        sql_placeholders = {
            "fields": fields,
            "hashkey_field": hashkey,
            "hashdiff_field": hashdiff.name,
            "staging_hashdiff_field": hashdiff.name_in_staging,
            "staging_descriptive_fields": staging_descriptive_fields,
            "satellite_descriptive_fields": satellite_descriptive_fields,
            "descriptive_fields": descriptive_fields,
            "end_of_time": END_OF_TIME_SQL_TEMPLATE,
            "record_end_timestamp_name": METADATA_FIELDS["record_end_timestamp"],
        }
        sql_placeholders.update(super().sql_placeholders)

        return sql_placeholders
