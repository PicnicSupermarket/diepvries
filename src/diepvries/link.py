from typing import List

from . import FieldRole, TEMPLATES_DIR, FIELD_SUFFIX
from .data_vault_table import DataVaultTable
from .template_sql.sql_formulas import format_fields_for_join


class Link(DataVaultTable):
    @property
    def loading_order(self) -> int:
        """
        Get loading order (links are the second tables to be loaded).

        Returns:
            int: Table loading order.
        """
        return 2

    def _validate(self):
        """
        Perform Link specific checks (besides common ones - check parent class):
        1. Table has one field with role = FieldRole.HASHKEY;
        2. Field with role = FieldRole.HASHKEY is placed in the first column of the table;
        3. Table has at least two business keys for connected hubs;
        4. Table has at least two hashkeys for connected hubs;
        5. Table has the same number of business keys and hashkeys for connected hubs.

        Raises:
            RuntimeError: If one of the checks fails.
        """
        super()._validate()
        hashkey_name = f"{self.name}_{FIELD_SUFFIX[FieldRole.HASHKEY]}"

        if not self.fields_by_name.get(hashkey_name):
            raise RuntimeError(f"'{self.name}': No field named '{hashkey_name}' found")

        business_keys = self.fields_by_role.get(FieldRole.BUSINESS_KEY)
        hashkey_parents = self.fields_by_role.get(FieldRole.HASHKEY_PARENT)

        if not business_keys:
            raise RuntimeError(
                f"'{self.name}': No business keys for connected hubs found"
            )

        if not hashkey_parents:
            raise RuntimeError(f"'{self.name}': No hashkeys for connected hubs found")

        if len(business_keys) != len(hashkey_parents):
            raise RuntimeError(
                (
                    f"'{self.name}': Number of hashkeys and business keys for connected "
                    f"hubs should be the same"
                )
            )

        if len(business_keys) == 1:
            raise RuntimeError(
                (
                    f"'{self.name}': At least two business keys for connected hubs are needed, only one found"
                )
            )

        if len(hashkey_parents) == 1:
            raise RuntimeError(
                (
                    f"'{self.name}': At least two hashkeys for connected hubs are needed, only one found"
                )
            )

    @property
    def sql_load_statement(self) -> str:
        """
        Generate the SQL query to populate current link.

        All needed placeholders are calculated, in order to match template SQL
        (check template_sql.link_dml.sql).

        Returns:
            str: SQL query to load target link.
        """
        hashkeys = format_fields_for_join(
            fields=self.fields_by_role.get(FieldRole.HASHKEY),
            table_1_alias="lnk",
            table_2_alias="staging",
        )
        hashkey_condition = "AND".join(hashkeys)

        sql_load_statement = (
            (TEMPLATES_DIR / "link_dml.sql")
            .read_text()
            .format(**self.sql_placeholders, hashkey_condition=hashkey_condition)
        )

        self._logger.info("Loading SQL for link (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement

    @property
    def parent_hub_names(self) -> List[str]:
        """
        Get a list of parent hub names.

        Returns:
            List[str]: Parent hub names.
        """
        parent_hub_names = []

        for hashkey_parent in self.fields_by_role.get(FieldRole.HASHKEY_PARENT):
            parent_hub_names.append(
                hashkey_parent.name.replace(f"_{FIELD_SUFFIX[FieldRole.HASHKEY]}", "")
            )

        return parent_hub_names
