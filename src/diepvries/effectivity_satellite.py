"""An effectivity satellite."""

from typing import Dict, List

from . import TEMPLATES_DIR
from .driving_key_field import DrivingKeyField
from .field import Field
from .satellite import Satellite
from .template_sql.sql_formulas import (
    RECORD_END_TIMESTAMP_SQL_TEMPLATE,
    format_fields_for_join,
    format_fields_for_select,
)


class EffectivitySatellite(Satellite):
    """Effectivity satellite.

    An effectivity satellite is a special type of satellite with the purpose of keeping
    "versions open" based on a subset of fields in the parent link.

    Example: A Data Vault model that has a link between a Customer and a Contact. It is
    required (for this specific example) to keep only one contact, per customer open at
    a given point in time. This means that, if a customer changes contacts, only the
    latest relationship between the Customer and its Contact is kept as an open
    relationship. Hub Customer's hashkey would be the driving key.
    """

    def __init__(
        self,
        schema: str,
        name: str,
        fields: List[Field],
        driving_keys: List[DrivingKeyField],
        *args,
        **kwargs,
    ):
        """Instantiate an EffectivitySatellite.

        Args:
            schema: Data Vault schema name.
            name: Satellite name.
            fields: List of fields that this Satellite holds.
            driving_keys: Define the set of link (parent table) fields that should be
                used as driving keys (in the example presented above, the driving key
                would be the h_customer_hashkey).
            args: Unused here, useful for children classes.
            kwargs: Unused here, useful for children classes.
        """
        super().__init__(schema, name, fields, *args, **kwargs)
        self.driving_keys = driving_keys

    @property
    def sql_load_statement(self) -> str:
        """Get the SQL query to populate current effectivity satellite.

        All needed placeholders are calculated, in order to match template SQL (check
        template_sql.effectivity_satellite_dml.sql).

        Returns:
            SQL query to load target satellite.
        """
        sql_load_statement = (
            (TEMPLATES_DIR / "effectivity_satellite_dml.sql")
            .read_text()
            .format(**self.sql_placeholders)
        )

        self._logger.info(
            "Loading SQL for effectivity satellite (%s) generated.", self.name
        )
        self._logger.debug("\n(%s)", sql_load_statement)

        return sql_load_statement

    @property
    def sql_placeholders(self) -> Dict[str, str]:
        """Calculate effectivity satellite specific placeholders.

        They are needed to generate SQL.

        The results are joined with the results from super().sql_placeholders(), as
        all placeholders calculated in Satellite (parent class) are applicable in
        an EffectivitySatellite.

        Returns:
            Effectivity satellite specific placeholders, to use in effectivity
                satellites.
        """
        record_end_timestamp = RECORD_END_TIMESTAMP_SQL_TEMPLATE.format(
            key_fields=", ".join(format_fields_for_select(fields=self.driving_keys))
        )
        # Handle driving_keys fields query placeholders.
        driving_keys_sql = ", ".join(format_fields_for_select(fields=self.driving_keys))
        satellite_driving_keys_sql = ", ".join(
            format_fields_for_select(fields=self.driving_keys, table_alias="satellite")
        )
        staging_driving_keys_sql = ", ".join(
            format_fields_for_select(fields=self.driving_keys, table_alias="staging")
        )

        link_driving_keys_sql = ", ".join(
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
        sql_placeholders = {
            "link_table": self.parent_table.name,
            "driving_keys": driving_keys_sql,
            "satellite_driving_keys": satellite_driving_keys_sql,
            "staging_driving_keys": staging_driving_keys_sql,
            "link_driving_keys": link_driving_keys_sql,
            "satellite_driving_key_condition": satellite_driving_key_condition,
            "link_driving_key_condition": link_driving_key_condition,
            "record_end_timestamp_expression": record_end_timestamp,
        }

        sql_placeholders.update(super().sql_placeholders)

        return sql_placeholders
