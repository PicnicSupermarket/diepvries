"""Data Vault table."""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from typing import Dict, List

from . import HASH_DELIMITER, METADATA_FIELDS, FieldRole, FixedPrefixLoggerAdapter
from .field import Field
from .template_sql.sql_formulas import HASHKEY_SQL_TEMPLATE


class Table(ABC):
    """A generic table.

    Abstract class Table. Holds common properties between all database tables.
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, schema: str, name: str, *_args, **_kwargs):
        """Instantiate a table.

        Args:
            schema: Schema name.
            name: Table name.
            _args: Unused, useful for child classes.
            _kwargs: Unused, useful for child classes.
        """
        self.schema = schema
        self.name = name.lower()

        self._logger = FixedPrefixLoggerAdapter(logging.getLogger(__name__), str(self))

        self._logger.info("Instance of (%s) created", type(self))

    def __str__(self) -> str:
        """Representation of a Table object as a string.

        This helps the tracking of logging events per entity.

        Returns:
            String representation of a Table.
        """
        return f"{type(self).__name__}: {self.schema}.{self.name}"


class StagingTable(Table):
    """A table used for staging."""

    # pylint: disable=too-few-public-methods

    def __init__(self, schema: str, name: str, extract_start_timestamp: datetime):
        """Instantiate a StagingTable.

        Args:
             schema: Schema name.
             name: Table name.
             extract_start_timestamp: Extract start timestamp.
        """
        staging_table_suffix = extract_start_timestamp.strftime("%Y%m%d_%H%M%S")
        physical_name = f"{name}_{staging_table_suffix}"

        super().__init__(schema=schema, name=physical_name)


class DataVaultTable(Table):
    """A Data Vault table.

    Abstract class DataVaultTable. It holds common properties between all subclasses:
    Hub, Link and Satellite.
    """

    _fields = None

    # Table used for staging. Set in DataVaultLoad.
    staging_table: StagingTable

    def __init__(self, schema: str, name: str, fields: List[Field], *_args, **_kwargs):
        """Instantiate a Data Vault table.

        Besides setting each __init__ argument as instance attributes, it also performs
        the following actions:

        - Calculate fields_by_name: dictionary with each Field as values and its name
          as key;
        - Calculate fields_by_role: dictionary with a list of Field as values and its
          role as key;
        - Check if table structure is valid: in this class, only generic checks
          (applicable to all Table subclasses). Each subclass will call
          ``super()._validate`` before starting each specific test (only applicable to
          instances of the subclass).

        Args:
            schema: Data Vault schema name.
            name: Data Vault table name.
            fields: List of fields that this table holds.
            _args: Unused here, useful for children classes.
            _kwargs: Unused here, useful for children classes.
        """
        super().__init__(schema=schema, name=name)
        self.fields = fields

        # Check if table structure is valid. Each subclass has its own implementation
        # (with its specific tests + the tests performed in this abstract class).
        self._validate()

    @property
    @abstractmethod
    def loading_order(self) -> int:
        """Get loading order."""

    @property
    def fields(self) -> List[Field]:
        """Get fields list for the current table.

        Returns:
            Fields for the current table.
        """
        return self._fields

    @fields.setter
    def fields(self, fields: List[Field]):
        """Set fields.

        Besides the setting of fields property, this method also sorts current table
        fields by position (in the database table). This sorting is crucial to ensure
        that hashdiffs/hashkeys are always generated following the same field order
        (check hashkey_sql and hashdiff_sql for more detail about hash fields
        generation).

        Args:
            fields: Fields list that the current table holds.
        """
        self._fields = sorted(fields, key=lambda x: x.position)

    @cached_property
    def fields_by_name(self) -> Dict[str, Field]:
        """Get a dictionary of fields, indexed by their names.

        Returns:
            Dictionary of fields, indexed by their names.
        """
        fields_by_name_as_dict = {}

        for field in self.fields:
            fields_by_name_as_dict[field.name] = field

        return fields_by_name_as_dict

    @cached_property
    def fields_by_role(self) -> Dict[FieldRole, List[Field]]:
        """Get a dictionary of fields, indexed by their roles.

        See _fields_by_role_as_dict.

        Returns:
            Dictionary of fields, indexed by their roles.
        """
        fields_by_role_as_dict = {
            FieldRole.HASHKEY: [],
            FieldRole.HASHKEY_PARENT: [],
            FieldRole.HASHDIFF: [],
            FieldRole.BUSINESS_KEY: [],
            FieldRole.CHILD_KEY: [],
            FieldRole.DRIVING_KEY: [],
            FieldRole.DESCRIPTIVE: [],
            FieldRole.METADATA: [],
        }

        for field in self.fields:
            fields_by_role_as_dict[field.role].append(field)

        return fields_by_role_as_dict

    @property
    @abstractmethod
    def sql_load_statement(self) -> str:
        """Get SQL script to load current table.

        Returns:
           SQL script to load current table.
        """

    @property
    def sql_placeholders(self) -> Dict[str, str]:
        """Get common placeholders needed to generate SQL for this Table.

        Returns:
            Common placeholders to be used in all Table SQL scripts.
        """
        query_args = {
            "target_schema": self.schema,
            "target_table": self.name,
            "staging_schema": self.staging_table.schema,
            "staging_table": self.staging_table.name,
            "record_start_timestamp": METADATA_FIELDS["record_start_timestamp"],
            "record_source": METADATA_FIELDS["record_source"],
        }

        return query_args

    def _validate(self):
        """Validate table fields.

        Perform the following checks (common to all Table subclasses):
        1. Table has one start_timestamp field - name defined in METADATA_FIELDS.
        2. Table has one source field - name defined in METADATA_FIELDS.

        Raises:
            KeyError: If one of the checks fails.
        """
        try:
            self.fields_by_name[METADATA_FIELDS["record_start_timestamp"]]
        except KeyError as e:
            raise KeyError(
                f"{self.name}: No field named "
                f"'{METADATA_FIELDS['record_start_timestamp']}' found"
            ) from e
        try:
            self.fields_by_name[METADATA_FIELDS["record_source"]]
        except KeyError as e:
            raise KeyError(
                f"{self.name}: No field named '{METADATA_FIELDS['record_source']}' "
                f"found"
            ) from e

    @property
    def hashkey_sql(self) -> str:
        """Get SQL expression to calculate hashkey fields.

        The hashkey formula is the following:
        `MD5(business_key_1 + |~~| + business_key_n + |~~| child_key_1)`.

        Returns:
            Hashkey SQL expression.
        """
        hashkey = next(hashkey for hashkey in self.fields_by_role[FieldRole.HASHKEY])
        fields_for_hashkey = [
            field.hash_concatenation_sql
            for field in self.fields_by_role[FieldRole.BUSINESS_KEY]
        ]
        fields_for_hashkey.extend(
            [
                field.hash_concatenation_sql
                for field in self.fields_by_role[FieldRole.CHILD_KEY]
            ]
        )

        hashkey_sql = HASHKEY_SQL_TEMPLATE.format(
            hashkey_expression=f"||'{HASH_DELIMITER}'||".join(fields_for_hashkey),
            hashkey=hashkey.name,
        )
        self._logger.debug(
            "Hashkey SQL expression for table (%s) is '(%s)'", self.name, hashkey_sql
        )

        return hashkey_sql
