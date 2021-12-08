"""A Hub."""

from typing import Dict

from . import FIELD_SUFFIX, METADATA_FIELDS, TEMPLATES_DIR, FieldRole
from .table import DataVaultTable
from .template_sql.sql_formulas import format_fields_for_select


class Hub(DataVaultTable):
    """A hub."""

    @property
    def prefix(self) -> str:
        """Get table prefix.

        Returns:
            Table prefix.

        """
        return next(split_part for split_part in self.name.split("_"))

    @property
    def loading_order(self) -> int:
        """Get loading order (hubs are the first tables to be loaded).

        Returns:
           Table loading order.
        """
        return 1

    @property
    def entity_name(self) -> str:
        """Get entity name for current Hub.

        Entity name corresponds to the name of the table, without the prefix.

        Returns:
            Name of the entity.
        """
        return "_".join(self.name.split("_")[1:])

    def _validate(self):
        """Perform Hub specific checks (besides common ones - check parent class).

        1. Table has one field with role = FieldRole.HASHKEY;
        3. Table has one field with role = FieldRole.BUSINESS_KEY;

        Raises:
            KeyError: If the table does not have any business key or hashkey.
            RuntimeError: If the table has more that one business key.
        """
        super()._validate()
        hashkey_name = f"{self.name}_{FIELD_SUFFIX[FieldRole.HASHKEY]}"

        try:
            self.fields_by_name[hashkey_name]
        except KeyError as e:
            raise KeyError(f"{self.name}: No field named '{hashkey_name}' found") from e

        business_keys = [
            field.name for field in self.fields_by_role[FieldRole.BUSINESS_KEY]
        ]
        if len(business_keys) > 1:
            raise RuntimeError(
                f"{self.name}: More than one business key detected: "
                f"({','.join(business_keys)})"
            )

    @property
    def sql_placeholders(self) -> Dict[str, str]:
        """Hub specific SQL placeholders.

        These placeholders are used to format the hub loading query.

        The results are joined with the results from super().sql_placeholders(), as all
        placeholders calculated in Table (parent class) are applicable in a Satellite.

        Returns:
            Satellite specific SQL placeholders.
        """
        hashkey = next(hashkey for hashkey in self.fields_by_role[FieldRole.HASHKEY])

        target_fields = ", ".join(format_fields_for_select(fields=self.fields))
        staging_fields = ", ".join(
            format_fields_for_select(fields=self.fields, table_alias="staging")
        )
        source_fields = ", ".join(
            [
                field.name
                for field in self.fields
                if field.role != FieldRole.HASHKEY
                and field.name != METADATA_FIELDS["record_source"]
            ]
        )

        sql_placeholders = {
            "source_hashkey_field": hashkey.name,
            "target_hashkey_field": hashkey.name,
            "record_source_field": METADATA_FIELDS["record_source"],
            "source_fields": source_fields,
            "target_fields": target_fields,
            "staging_source_fields": staging_fields,
        }
        sql_placeholders.update(super().sql_placeholders)

        return sql_placeholders

    @property
    def sql_load_statement(self) -> str:
        """Get the SQL query to populate current hub.

        All needed placeholders are calculated, in order to match template SQL
        (check template_sql.hub_link_dml.sql).

        Returns:
            SQL query to load target hub.
        """
        sql_load_statement = (
            (TEMPLATES_DIR / "hub_link_dml.sql")
            .read_text()
            .format(**self.sql_placeholders)
        )

        self._logger.info("Loading SQL for hub (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement
