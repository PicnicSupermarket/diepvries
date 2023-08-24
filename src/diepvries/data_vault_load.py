"""Module for a Data Vault load."""

import itertools
import logging
from datetime import datetime
from functools import lru_cache
from typing import List, Optional

from pytz import timezone

from . import METADATA_FIELDS, TEMPLATES_DIR, FieldRole, FixedPrefixLoggerAdapter
from .field import Field
from .hub import Hub
from .link import Link
from .satellite import Satellite
from .table import DataVaultTable, StagingTable
from .template_sql.sql_formulas import (
    ALIASED_BUSINESS_KEY_SQL_TEMPLATE,
    RECORD_START_TIMESTAMP_SQL_TEMPLATE,
    SOURCE_SQL_TEMPLATE,
)


class DataVaultLoad:
    """Load data in a Data Vault."""

    _target_tables = None

    def __init__(
        self,
        extract_schema: str,
        extract_table: str,
        staging_schema: str,
        staging_table: str,
        extract_start_timestamp: datetime,
        target_tables: List[DataVaultTable],
        source: Optional[str] = None,
    ):
        """Instantiate a DataVaultLoad object and calculate additional fields.

        Args:
            extract_schema: Schema where the extraction table is stored.
            extract_table: Name of the extraction table.
            staging_schema: Schema where the staging table should be created.
            staging_table: Name of the staging table.
            extract_start_timestamp: Moment when the extraction started (when we started
                fetching data from source).
            target_tables: Tables that will be populated by current staging table.
            source: Source system/API/database. If source is not passed as argument, the
                process will assume that a source (field named according to
                METADATA_FIELDS naming conventions) will exist in target table.

        Raises:
            ValueError: When the extract_start_timestamp is not linked to a timezone.
        """
        self.extract_schema = extract_schema
        self.extract_table = extract_table
        self.staging_table = StagingTable(
            schema=staging_schema,
            name=staging_table,
            extract_start_timestamp=extract_start_timestamp,
        )

        # Check if extract_start_timestamp is timezone-aware.
        if extract_start_timestamp.tzinfo is None:
            raise ValueError(
                "extract_start_timestamp should be timezone-aware (timezone=UTC)"
            )

        # Convert extract_start_timestamp from its timezone to UTC.
        self.extract_start_timestamp = extract_start_timestamp.astimezone(
            timezone("UTC")
        )
        self.target_tables = target_tables
        self.source = source
        self._logger = FixedPrefixLoggerAdapter(logging.getLogger(__name__), str(self))

        self._logger.info("Created DataVaultLoad instance (%s).", str(self))

    def __str__(self) -> str:
        """Representation of a DataVaultLoad object as a string.

        This helps with the tracking of logging events per entity.

        Returns:
            String representation of this DataVaultLoad instance.
        """
        return f"{type(self).__name__}: staging_table={self.staging_table.name}"

    @property
    def target_tables(self) -> List[DataVaultTable]:
        """Get target tables.

        Returns:
            List of target tables.
        """
        return self._target_tables

    @target_tables.setter
    def target_tables(self, target_tables: List[DataVaultTable]):
        """Set target tables.

        Perform the following actions:
            1. Sort target_tables by loading order and name.
            2. Define staging_table and staging schema for all target_tables: physical
                name of the staging table, including extract_start_timestamp as suffix.
            3. Build relationship between each Satellite and its parent table.
            4. Check if all parent hub names exist in target_tables - applicable for
                links only.

        Args:
            target_tables: List of tables to be populated.

        Raises:
            StopIteration: If a parent table (both from Link and Satellite) is missing
                in self.target_tables.
        """
        self._target_tables = sorted(
            target_tables, key=lambda x: (x.loading_order, x.name)
        )
        for target_table in self._target_tables:
            target_table.staging_table = self.staging_table
            if isinstance(target_table, Satellite):
                try:
                    target_table.parent_table = self._get_target_table(
                        target_table.parent_table_name
                    )
                except StopIteration as e:
                    raise StopIteration(
                        f"{target_table}: Parent table "
                        f"'{target_table.parent_table_name}' missing in target_tables "
                        "configuration."
                    ) from e
            if isinstance(target_table, Link):
                for parent_hub in target_table.parent_hub_names:
                    try:
                        self._get_target_table(parent_hub)
                    except StopIteration as e:
                        raise StopIteration(
                            f"{target_table}: Parent hub '{parent_hub}' missing in "
                            f"target_tables configuration."
                        ) from e

    @property
    def staging_create_sql_statement(self) -> str:
        """Generate the SQL query to create the staging table.

        All needed placeholders are calculated, in order to match template SQL (check
        template_sql/staging_table_ddl.sql).

        Returns:
            SQL query to create staging table.
        """
        fields_dml = []
        fields_ddl = []

        # Produce the list of fields that should exist in the staging table. As
        # common field names can appear in multiple target tables and it is not
        # possible to have duplicated field names in the staging table, seen fields
        # are kept in a set to avoid duplicates; while the list is built iteratively
        # to maintain ordering.
        staging_fields = []
        seen_fields = set()
        for table in self.target_tables:
            for field in table.fields:
                # Record end timestamp is calculated during Data Vault model load
                # (not needed in the staging table).
                if field.name == METADATA_FIELDS["record_end_timestamp"]:
                    continue
                # Avoid duplicates.
                if field in seen_fields:
                    continue
                seen_fields.add(field)
                staging_fields.append(field)

        # Iterate over all staging fields and produce the DML and DDL expressions
        # that should be used to create the staging table.
        for field in staging_fields:
            fields_dml.append(
                self._get_staging_dml_expression(
                    field, self._get_target_table(field.parent_table_name)
                )
            )
            fields_ddl.append(field.ddl_in_staging)

        query_args = {
            "staging_schema": self.staging_table.schema,
            "staging_table": self.staging_table.name,
            "fields_dml": ", ".join(fields_dml),
            "fields_ddl": ", ".join(fields_ddl),
            "extract_schema_name": self.extract_schema,
            "extract_table_name": self.extract_table,
        }

        staging_table_create_sql = (
            (TEMPLATES_DIR / "staging_table_ddl.sql").read_text().format(**query_args)
        )

        self._logger.info(
            "Loading SQL for staging table (%s) generated.",
            self.staging_table.name,
        )
        self._logger.debug("\n(%s)", staging_table_create_sql)

        return staging_table_create_sql

    @property
    def sql_load_script(self) -> List[str]:
        """Generate the SQL script to load current Data Vault model.

         It is a list of SQL commands.

        Returns:
            SQL script that should be executed to load current Data Vault model - one
                entry per table to load.
        """
        return itertools.chain.from_iterable(self.sql_load_scripts_by_group)

    @property
    def sql_load_scripts_by_group(self) -> List[List[str]]:
        """Generate the SQL scripts to load current Data Vault model.

        Scripts are grouped by their loading order. Within a group, queries can be run
        in parallel.
        """
        result = [[self.staging_create_sql_statement]]
        for _, group in itertools.groupby(
            self.target_tables, key=lambda x: x.loading_order
        ):
            result.append([table.sql_load_statement for table in group])
        return result

    def _get_staging_dml_expression(self, field: Field, table: DataVaultTable) -> str:
        """Get the SQL expression to represent a field in the staging table.

        Args:
            field: Field to calculate SQL expression.
            table: Field parent table.

        Returns:
            SQL expression that should be used in staging creation.
        """
        if field.name_in_staging == METADATA_FIELDS["record_start_timestamp"]:
            return RECORD_START_TIMESTAMP_SQL_TEMPLATE.format(
                extract_start_timestamp=self.extract_start_timestamp.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            )
        if (
            field.name_in_staging == METADATA_FIELDS["record_source"]
            and self.source is not None
        ):
            return SOURCE_SQL_TEMPLATE.format(source=self.source)
        if field.role == FieldRole.BUSINESS_KEY:
            return ALIASED_BUSINESS_KEY_SQL_TEMPLATE.format(business_key=field.name)
        if field.role == FieldRole.HASHKEY and isinstance(table, (Hub, Link)):
            return table.hashkey_sql
        if field.role == FieldRole.HASHDIFF and isinstance(table, Satellite):
            return table.hashdiff_sql
        return field.name_in_staging

    @lru_cache
    def _get_target_table(self, target_table_name: str) -> DataVaultTable:
        """Get a Table object from target tables.

        Args:
            target_table_name: Name of the table to be returned.

        Returns:
            Table instance with the name given as argument.

        Raises:
            StopIteration: If target_table passed as argument does not exist in
                target_tables.
        """
        try:
            target_table = next(
                filter(lambda x: x.name == target_table_name, self.target_tables)
            )
        except StopIteration as e:
            raise StopIteration(
                f"Table '{target_table_name}' missing in target_tables"
            ) from e

        return target_table
