"""A role playing Hub."""

from typing import Dict, List, Optional

from . import TEMPLATES_DIR, FieldRole
from .field import Field
from .hub import Hub
from .template_sql.sql_formulas import format_fields_for_select


class RolePlayingHub(Hub):
    """A role playing Hub.

    A role playing Hub is a hub that is not materialized as a table, but is a view
    pointing to a main Hub. The concept is essentially the same as a role playing
    dimension in a star schema.

    One conceptual example of a Role Playing Hub can be a Hub that represents an
    account, that can assume different roles (supplier, transporter, administrator,
    etc...). All of them are accounts, but each of them represent a different role.
    Using this example, only the Hub account would be materialized and populated (as
    a table), as all other hubs (Hub Account Supplier/Transporter/etc...) would be
    views pointing to the main hub.
    """

    def __init__(self, schema: str, name: str, fields: List[Field]):
        """Instantiate a role RolePlayingHub.

        Args:
            schema: Data Vault schema name.
            name: Role playing hub name.
            fields: List of fields that this Hub holds.
        """
        super().__init__(schema, name, fields)
        # Parent table is set just after instantiation.
        self.parent_table: Optional[Hub] = None

    @property
    def sql_placeholders(self) -> Dict[str, str]:
        """Role playing hub specific SQL placeholders.

        These placeholders are used to format the RolePlayingHub loading query.

        The results are joined with the results from super().sql_placeholders(),
        as most placeholders calculated in Table (parent class) are applicable in a
        RolePlayingHub. The only placeholder that is calculated in the parent class
        and replaced in this method is target_table, that points to the parent
        hub in this case.

        Returns:
            Role playing hub specific SQL placeholders.
        """
        sql_placeholders = super().sql_placeholders

        target_hashkey = next(
            hashkey for hashkey in self.parent_table.fields_by_role[FieldRole.HASHKEY]
        )

        target_fields = ", ".join(
            format_fields_for_select(fields=self.parent_table.fields)
        )

        new_sql_placeholders = {
            "target_table": self.parent_table.name,
            "target_hashkey_field": target_hashkey.name,
            "target_fields": target_fields,
        }
        sql_placeholders.update(new_sql_placeholders)

        return sql_placeholders

    @property
    def sql_load_statement(self) -> str:
        """Get the SQL query to populate the current role playing hub.

        If table has a parent table - role playing hub.

        All needed placeholders are calculated, in order to match template SQL (check
        template_sql.hub_link_dml.sql).

        Returns:
            SQL query to load target hub.
        """
        sql_load_statement = (
            (TEMPLATES_DIR / "hub_link_dml.sql")
            .read_text()
            .format(**self.sql_placeholders)
        )

        self._logger.info("Loading SQL for role playing hub (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement
