"""A link."""

from typing import Dict, List

from . import FIELD_SUFFIX, TEMPLATES_DIR, FieldRole
from .table import Table
from .template_sql.sql_formulas import (
    FIELDS_AGGREGATION_SQL_TEMPLATE,
    format_fields_for_select,
)


class Link(Table):
    """A link."""

    @property
    def loading_order(self) -> int:
        """Get loading order (links are the second tables to be loaded).

        Returns:
            Table loading order.
        """
        return 2

    def _validate(self):
        """Perform Link specific checks (besides common ones - check parent class).

        1. Table has one field with role = FieldRole.HASHKEY;
        2. Field with role = FieldRole.HASHKEY is placed in the first column of the
        table;
        3. Table has at least two business keys for connected hubs;
        4. Table has at least two hashkeys for connected hubs;
        5. Table has the same number of business keys and hashkeys for connected hubs.

        Raises:
            KeyError: If the table has no hashkey.
            RuntimeError: If any other check fails.
        """
        super()._validate()
        hashkey_name = f"{self.name}_{FIELD_SUFFIX[FieldRole.HASHKEY]}"

        try:
            self.fields_by_name[hashkey_name]
        except KeyError as e:
            raise KeyError(
                f"'{self.name}': No field named '{hashkey_name}' found"
            ) from e

        try:
            business_keys = self.fields_by_role[FieldRole.BUSINESS_KEY]
        except KeyError as e:
            raise KeyError(
                f"'{self.name}': No business keys for connected hubs found"
            ) from e
        try:
            hashkey_parents = self.fields_by_role[FieldRole.HASHKEY_PARENT]
        except KeyError as e:
            raise KeyError(
                f"'{self.name}': No hashkeys for connected hubs found"
            ) from e

        if len(business_keys) != len(hashkey_parents):
            raise RuntimeError(
                (
                    f"'{self.name}': Number of hashkeys and business keys for "
                    f"connected hubs should be the same"
                )
            )

        if not business_keys:
            raise RuntimeError(
                (
                    f"'{self.name}': At least one business key for connected hub(s) is "
                    f"needed, none found"
                )
            )

        if not hashkey_parents:
            raise RuntimeError(
                (
                    f"'{self.name}': At least one hashkey for connected hub(s) is "
                    f"needed, none found"
                )
            )

    @property
    def sql_placeholders(self) -> Dict[str, str]:
        """Link specific SQL placeholders.

        These placeholders are used to format the Link loading query.

        The results are joined with the results from super().sql_placeholders(), as all
        placeholders calculated in Table (parent class) are applicable in a Link.

        Returns:
            Link specific SQL placeholders.
        """
        hashkey = next(hashkey for hashkey in self.fields_by_role[FieldRole.HASHKEY])

        hashkey_condition = f"link.{hashkey.name} = staging.{hashkey.name}"

        non_hashkey_fields = [
            field for field in self.fields if field.role != FieldRole.HASHKEY
        ]
        non_hashkey_fields_aggregation = ",".join(
            [
                FIELDS_AGGREGATION_SQL_TEMPLATE.format(field=field)
                for field in format_fields_for_select(fields=non_hashkey_fields)
            ]
        )
        link_non_hashkey_fields = ",".join(
            format_fields_for_select(fields=non_hashkey_fields)
        )
        staging_non_hashkey_fields = ",".join(
            format_fields_for_select(fields=non_hashkey_fields, table_alias="staging")
        )

        sql_placeholders = {
            "hashkey_field": hashkey.name,
            "non_hashkey_fields": link_non_hashkey_fields,
            "non_hashkey_fields_aggregation": non_hashkey_fields_aggregation,
            "hashkey_condition": hashkey_condition,
            "staging_non_hashkey_fields": staging_non_hashkey_fields,
        }
        sql_placeholders.update(super().sql_placeholders)

        return sql_placeholders

    @property
    def sql_load_statement(self) -> str:
        """Get the SQL query to populate current link.

        All needed placeholders are calculated, in order to match template SQL
        (check template_sql.link_dml.sql).

        Returns:
            SQL query to load target link.
        """
        sql_load_statement = (
            (TEMPLATES_DIR / "link_dml.sql").read_text().format(**self.sql_placeholders)
        )

        self._logger.info("Loading SQL for link (%s) generated.", self.name)
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement

    @property
    def parent_hub_names(self) -> List[str]:
        """Get the list of parent hub names.

        Returns:
            Parent hub names.
        """
        parent_hub_names = []

        for hashkey_parent in self.fields_by_role[FieldRole.HASHKEY_PARENT]:
            parent_hub_names.append(
                hashkey_parent.name.replace(f"_{FIELD_SUFFIX[FieldRole.HASHKEY]}", "")
            )

        return parent_hub_names
