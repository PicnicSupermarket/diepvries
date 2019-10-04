from . import FieldRole, FIELD_SUFFIX, TEMPLATES_DIR
from .data_vault_table import DataVaultTable
from .template_sql.sql_formulas import format_fields_for_join


class Hub(DataVaultTable):
    @property
    def prefix(self):
        """
        Gets table prefix.

        Returns:
            str: Table prefix.

        """
        return next(split_part for split_part in self.name.split("_"))

    @property
    def loading_order(self) -> int:
        """
        Gets loading order (hubs are the first tables to be loaded).

        Returns:
            int: Table loading order.
        """
        return 1

    @property
    def entity_name(self) -> str:
        """
        Gets entity name for current Hub.

        Entity name corresponds to the name of the table, without the prefix.

        Returns:
            str: Name of the entity.
        """
        return "_".join(self.name.split("_")[1:])

    def _validate(self):
        """
        Performs Hub specific checks (besides common ones - check parent class):
        1. Table has one field with role = FieldRole.HASHKEY;
        3. Table has one field with role = FieldRole.BUSINESS_KEY;

        Raises:
            RuntimeError: If one of the checks fails.
        """
        super()._validate()
        hashkey_name = f"{self.name}_{FIELD_SUFFIX[FieldRole.HASHKEY]}"

        if not self.fields_by_name.get(hashkey_name):
            raise RuntimeError(f"{self.name}: No field named '{hashkey_name}' found")

        business_keys = self.fields_by_role.get(FieldRole.BUSINESS_KEY)
        business_key_name = f"{self.entity_name}_{FIELD_SUFFIX[FieldRole.BUSINESS_KEY]}"

        if not business_keys:
            raise RuntimeError(
                f"{self.name}: No field named '{business_key_name}' found"
            )

        if len(business_keys) > 1:
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
        business_keys = format_fields_for_join(
            fields=self.fields_by_role.get(FieldRole.BUSINESS_KEY),
            table_1_alias="hub",
            table_2_alias="staging",
        )
        business_key_condition = "AND".join(business_keys)

        sql_load_statement = (
            (TEMPLATES_DIR / "hub_dml.sql")
            .read_text()
            .format(
                **self.sql_placeholders, business_key_condition=business_key_condition
            )
        )

        self._logger.info("Loading SQL for hub (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement
