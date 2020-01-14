import logging
from datetime import datetime
from functools import lru_cache
from typing import List

from ordered_set import OrderedSet
from pytz import timezone

from . import METADATA_FIELDS, TEMPLATES_DIR, FieldRole, FixedPrefixLoggerAdapter
from .data_vault_field import DataVaultField
from .data_vault_table import DataVaultTable
from .hub import Hub
from .link import Link
from .satellite import Satellite
from .template_sql.sql_formulas import (
    ALIASED_BUSINESS_KEY_SQL_TEMPLATE,
    RECORD_START_TIMESTAMP_SQL_TEMPLATE,
    SOURCE_SQL_TEMPLATE,
    STAGING_PHYSICAL_NAME_SQL_TEMPLATE,
)


class DataVaultLoad:
    _target_tables = None
    _staging_table = None

    def __init__(
        self,
        extract_schema: str,
        extract_table: str,
        staging_schema: str,
        staging_table: str,
        extract_start_timestamp: datetime,
        target_tables: List[DataVaultTable],
        source: str = None,
    ):
        """
        Instantiate a DataVaultLoad object and calculate additional fields (not passed
        upon DataVaultLoad creation).

        Args:
            extract_schema (str): Schema where the extraction table is stored.
            extract_table (str): Name of the extraction table.
            staging_schema (str): Schema where the staging table should be created.
            staging_table (str): Name of the staging table (functional name,
                as the physical name will be calculated in staging_table property
                getter).
            target_tables (List[DataVaultTable]): Tables that will be populated by
                current staging table.
            extract_start_timestamp (datetime): Moment when the extraction started
                (when we started fetching data from source).
            source (str): Source system/API/database. If source is not passed as
                argument, the process will assume that a source (field named according
                to METADATA_FIELDS naming conventions) will exist in target table.
        """
        self.extract_schema = extract_schema
        self.extract_table = extract_table
        self.staging_schema = staging_schema
        self.staging_table = staging_table

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
        """
        Define the representation of a DataVaultLoad object as a string.
        This helps with the tracking of logging events per entity.

        Returns:
            str: String representation of this DataVaultLoad instance.
        """
        return f"{type(self).__name__}: staging_table={self.staging_table}"

    @property
    def target_tables(self):
        """
        Get _target_tables (defined in target_tables.setter).

        Returns:
            List[DataVaultTable]: List of target tables.
        """
        return self._target_tables

    @target_tables.setter
    def target_tables(self, target_tables):
        """
        Perform the following actions:
            1. Sort target_tables by loading order and name.
            2. Define staging_table and staging schema for all target_tables: physical
                name of the staging table, including extract_start_timestamp as suffix.
            3. Build relationship between each Satellite and its parent table.
            4. Check if all parent hub names exist in target_tables - applicable for links
                only.
        Args:
            target_tables (List[DataVaultTable]): List of tables to be populated in
                current DataVaultLoad.

        Raises:
            StopIteration: If a parent table (both from Link and Satellite)
                is missing in self.target_tables.
        """
        self._target_tables = sorted(
            target_tables, key=lambda x: (x.loading_order, x.name)
        )
        for target_table in self._target_tables:
            target_table.staging_schema = self.staging_schema
            target_table.staging_table = self.staging_table
            try:
                if isinstance(target_table, Satellite):
                    target_table.parent_table = self._get_target_table(
                        target_table.parent_table_name
                    )
            except StopIteration:
                raise StopIteration(
                    f"{target_table}: Parent table "
                    f"'{target_table.parent_table_name}' missing in "
                    f"target_tables configuration"
                )
            if isinstance(target_table, Link):
                for parent_hub in target_table.parent_hub_names:
                    try:
                        self._get_target_table(parent_hub)
                    except StopIteration:
                        raise StopIteration(
                            f"{target_table}: Parent hub '{parent_hub}' missing in "
                            f"target_tables configuration"
                        )

    @property
    def staging_table(self):
        """
        Name of the staging table in the database.

        Returns:
            str: Name of the staging table (suffixed with extract_start_timestamp).

        """
        return STAGING_PHYSICAL_NAME_SQL_TEMPLATE.format(
            staging_table=self._staging_table,
            staging_table_suffix=self.extract_start_timestamp.strftime("%Y%m%d_%H%M%S"),
        )

    @staging_table.setter
    def staging_table(self, staging_table):
        """
        Set the name of the staging table in the database.
        """
        self._staging_table = staging_table

    @property
    def staging_create_sql_statement(self) -> str:
        """
        Generate the SQL query to create the staging table.

        All needed placeholders are calculated, in order to match template SQL (check
        template_sql.staging_table_ddl.sql).

        Returns:
            str: SQL query to create staging table.
        """
        fields_dml = []
        fields_ddl = []

        # Produce the list of fields that should exist in the staging table.
        # As common field names can appear in multiple target tables we cannot have
        # duplicated field names in the staging table, it's assured that each field is
        # only considered once.
        staging_fields = OrderedSet(
            [
                field
                for table in self.target_tables
                for field in table.fields
                # Record end timestamp is calculated during Data Vault model load
                # (not needed in the staging table).
                if field.name != METADATA_FIELDS["record_end_timestamp"]
                # Fields with HASHKEY_PARENT role are not needed in the staging table
                # (the same field will exist with role = HASHKEY in the parent hub).
                and field.role != FieldRole.HASHKEY_PARENT
            ]
        )

        # Iterate over all staging fields and produces the DML and DDL expressions
        # that should be used to create the staging table.
        for field in staging_fields:
            fields_dml.append(
                self._get_staging_dml_expression(
                    field, self._get_target_table(field.parent_table_name)
                )
            )
            fields_ddl.append(field.ddl_in_staging)

        query_args = {
            "staging_schema": self.staging_schema,
            "staging_table": self.staging_table,
            "fields_dml": ", ".join(fields_dml),
            "fields_ddl": ", ".join(fields_ddl),
            "extract_schema_name": self.extract_schema,
            "extract_table_name": self.extract_table,
        }

        staging_table_create_sql = (
            (TEMPLATES_DIR / "staging_table_ddl.sql").read_text().format(**query_args)
        )

        self._logger.info(
            "Loading SQL for staging table (%s) generated.", self.staging_table
        )
        self._logger.debug("\n(%s)", staging_table_create_sql)

        return staging_table_create_sql

    @property
    def sql_load_script(self) -> List[str]:
        """
        Generate the SQL script to load current Data Vault model (list of SQL commands).

        Returns:
            List[str]: SQL script that should be executed to load current Data Vault
                model - one entry per table to load.
        """
        data_vault_sql = [self.staging_create_sql_statement]
        for table in self.target_tables:
            data_vault_sql.append(table.sql_load_statement)

        return data_vault_sql

    def _get_staging_dml_expression(
        self, field: DataVaultField, table: DataVaultTable
    ) -> str:
        """
        Calculate the SQL expression that should be used to represent the field passed
        as argument in the staging table SQL script.

        Args:
            field (DataVaultField): Field to calculate SQL expression.
            table (DataVaultTable): Field parent table.

        Returns:
            str: SQL expression that should be used in staging creation.
        """
        if field.name_in_staging == METADATA_FIELDS["record_start_timestamp"]:
            formatted_timestamp = self.extract_start_timestamp.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            sql_expression = RECORD_START_TIMESTAMP_SQL_TEMPLATE.format(
                extract_start_timestamp=formatted_timestamp
            )
        elif (
            field.name_in_staging == METADATA_FIELDS["record_source"]
            and self.source is not None
        ):
            source_expression = SOURCE_SQL_TEMPLATE.format(source=self.source)
            sql_expression = source_expression
        elif field.role == FieldRole.BUSINESS_KEY:
            business_key_expression = ALIASED_BUSINESS_KEY_SQL_TEMPLATE.format(
                business_key=field.name
            )
            sql_expression = business_key_expression
        elif field.role == FieldRole.HASHKEY and isinstance(table, (Hub, Link)):
            sql_expression = table.hashkey_sql
        elif field.role == FieldRole.HASHDIFF and isinstance(table, Satellite):
            sql_expression = table.hashdiff_sql
        else:
            sql_expression = field.name_in_staging

        return sql_expression

    @lru_cache(1)
    def _get_target_table(self, target_table_name: str) -> DataVaultTable:
        """
        Get a DataVaultTable object from self.target_tables, given its name as argument.

        Args:
            target_table_name (str): Name of the table to be returned.

        Returns:
            DataVaultTable: Table instance with the name given as argument.

        Raises:
            StopIteration: If target_table passed as argument does not exist in
                target_tables.
        """
        try:
            target_table = next(
                table for table in self.target_tables if table.name == target_table_name
            )
        except StopIteration:
            raise StopIteration(f"Table '{target_table_name}' missing in target_tables")

        return target_table
