from . import FIELD_SUFFIX, TEMPLATES_DIR, FieldRole
from .data_vault_table import DataVaultTable
from .template_sql.sql_formulas import (
    FIELDS_AGGREGATION_SQL_TEMPLATE,
    format_fields_for_join,
    format_fields_for_select,
)


class Hub(DataVaultTable):
    @property
    def prefix(self):
        """
        Get table prefix.

        Returns:
            str: Table prefix.

        """
        return next(split_part for split_part in self.name.split("_"))

    @property
    def loading_order(self) -> int:
        """
        Get loading order (hubs are the first tables to be loaded).

        Returns:
            int: Table loading order.
        """
        return 1

    @property
    def entity_name(self) -> str:
        """
        Get entity name for current Hub.

        Entity name corresponds to the name of the table, without the prefix.

        Returns:
            str: Name of the entity.
        """
        return "_".join(self.name.split("_")[1:])

    def _validate(self):
        """
        Perform Hub specific checks (besides common ones - check parent class):
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
        except KeyError:
            raise KeyError(f"{self.name}: No field named '{hashkey_name}' found")

        business_key_name = f"{self.entity_name}_{FIELD_SUFFIX[FieldRole.BUSINESS_KEY]}"

        try:
            business_keys = self.fields_by_name[business_key_name]
        except KeyError:
            raise KeyError(f"{self.name}: No field named '{business_key_name}' found")

        if len(self.fields_by_role[FieldRole.BUSINESS_KEY]) > 1:
            raise RuntimeError(
                f"{self.name}: More than one business key detected: ({business_keys})"
            )

    @property
    def sql_load_statement(self) -> str:
        """
         Generate the SQL query to populate current hub.

         All needed placeholders are calculated, in order to match template SQL
         (check template_sql.hub_dml.sql).

         Returns:
             str: SQL query to load target hub.
         """
        hashkey = next(
            hashkey for hashkey in self.fields_by_role.get(FieldRole.HASHKEY)
        )

        business_keys = format_fields_for_join(
            fields=self.fields_by_role.get(FieldRole.BUSINESS_KEY),
            table_1_alias="hub",
            table_2_alias="staging",
        )
        business_key_condition = "AND".join(business_keys)

        non_hashkey_fields = [
            field for field in self.fields if field.role != FieldRole.HASHKEY
        ]
        non_hashkey_fields_sql = ",".join(
            format_fields_for_select(fields=non_hashkey_fields)
        )

        non_hashkey_fields_aggregation = [
            FIELDS_AGGREGATION_SQL_TEMPLATE.format(field=field)
            for field in format_fields_for_select(fields=non_hashkey_fields)
        ]
        non_hashkey_fields_aggregation_sql = ",".join(non_hashkey_fields_aggregation)

        sql_load_statement = (
            (TEMPLATES_DIR / "hub_dml.sql")
            .read_text()
            .format(
                **self.sql_placeholders,
                hashkey_field=hashkey.name,
                non_hashkey_fields=non_hashkey_fields_sql,
                non_hashkey_fields_aggregation=non_hashkey_fields_aggregation_sql,
                business_key_condition=business_key_condition,
            )
        )

        self._logger.info("Loading SQL for hub (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement
