"""A Hub."""

from typing import Dict

from . import FIELD_SUFFIX, TEMPLATES_DIR, FieldRole
from .table import Table
from .template_sql.sql_formulas import (
    FIELDS_AGGREGATION_SQL_TEMPLATE,
    format_fields_for_join,
    format_fields_for_select,
)


class Hub(Table):
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

        business_key_condition = "AND".join(
            format_fields_for_join(
                fields=self.fields_by_role[FieldRole.BUSINESS_KEY],
                table_1_alias="hub",
                table_2_alias="staging",
            )
        )

        non_hashkey_fields = [
            field for field in self.fields if field.role != FieldRole.HASHKEY
        ]
        non_hashkey_fields_aggregation = ",".join(
            [
                FIELDS_AGGREGATION_SQL_TEMPLATE.format(field=field)
                for field in format_fields_for_select(fields=non_hashkey_fields)
            ]
        )
        hub_non_hashkey_fields = ",".join(
            format_fields_for_select(fields=non_hashkey_fields)
        )
        staging_non_hashkey_fields = ",".join(
            format_fields_for_select(fields=non_hashkey_fields, table_alias="staging")
        )

        sql_placeholders = {
            "hashkey_field": hashkey.name,
            "non_hashkey_fields": hub_non_hashkey_fields,
            "non_hashkey_fields_aggregation": non_hashkey_fields_aggregation,
            "business_key_condition": business_key_condition,
            "staging_non_hashkey_fields": staging_non_hashkey_fields,
        }
        sql_placeholders.update(super().sql_placeholders)

        return sql_placeholders

    @property
    def sql_load_statement(self) -> str:
        """Get the SQL query to populate current hub.

        All needed placeholders are calculated, in order to match template SQL
        (check template_sql.hub_dml.sql).

        Returns:
            SQL query to load target hub.
        """
        sql_load_statement = (
            (TEMPLATES_DIR / "hub_dml.sql").read_text().format(**self.sql_placeholders)
        )

        self._logger.info("Loading SQL for hub (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement
