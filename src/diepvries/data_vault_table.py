import logging
from abc import abstractmethod, ABC
from functools import lru_cache
from typing import List, Dict

from . import FieldRole, FixedPrefixLoggerAdapter, HASH_DELIMITER, METADATA_FIELDS
from .data_vault_field import DataVaultField
from .template_sql.sql_formulas import (
    BUSINESS_KEY_SQL_TEMPLATE,
    CHILD_KEY_SQL_TEMPLATE,
    HASHKEY_SQL_TEMPLATE,
    format_fields_for_select,
)


class DataVaultTable(ABC):
    _fields = None
    _fields_by_name = None
    _fields_by_role = None

    def __init__(self, schema: str, name: str, fields: List[DataVaultField]):
        """
        Abstract class DataVaultTable. It holds common properties between all
        subclasses: Hub, Link and Satellite.

        Besides setting each __init__ argument as class attributes, it also performs the following actions:
        - Calculates fields_by_name: dictionary with each DataVaultField as values and its name as key;
        - Calculates fields_by_role: dictionary with a list of DataVaultField as values and its role as key;
        - Checks if table structure is valid: in this class, only generic checks (applicable to all DataVaultTable
        subclasses). Each subclass will call super()._validate before starting each specific test (only applicable
        to instances of the subclass).

        Args:
            schema (str): Data Vault schema name.
            name (str): Data Vault table name.
            fields (List[DataVaultField]): List of fields that this table holds.
        """
        self.schema = schema
        self.name = name.lower()
        self.fields = fields

        # Checks if table structure is valid. Each subclass has its own implementation (with its specific tests +
        # the tests performed in this abstract class).
        self._validate()

        # Variables set in DataVaultLoad
        self.staging_schema = None
        self.staging_table = None

        self._logger = FixedPrefixLoggerAdapter(logging.getLogger(__name__), str(self))

        self._logger.info("Instance of (%s) created", type(self))

    def __str__(self) -> str:
        """
        Defines the representation of a DataVaultTable object as a string.
        This helps the tracking of logging events per entity.

        Returns:
            str: String representation of a DataVaultTable.
        """
        return f"{type(self).__name__}: {self.schema}.{self.name}"

    @property
    @abstractmethod
    def loading_order(self) -> int:
        """
        Implemented in subclasses.
        """

    @property
    def fields(self) -> List[DataVaultField]:
        """
        Returns field list for current table.

        Returns:
            List[DataVaultField]: Fields for current table.
        """
        return self._fields

    @fields.setter
    def fields(self, fields: List[DataVaultField]):
        """
        Besides the setting of fields property, this method also sorts current table fields by position (in
        the database table). This sorting is crucial to assure that hashdiffs/hashkeys are always generated following
        the same field order (check hashkey_sql and hashdiff_sql for more detail about hash fields generation).

        Args:
            fields (List[DataVaultField]): Field list that the current table holds.
        """
        self._fields = sorted(fields, key=lambda x: x.position)

    @property
    @lru_cache(1)
    def fields_by_name(self) -> Dict[str, DataVaultField]:
        """
        Returns a dictionary of DataVaultField, indexed by its name.

        Returns:
            Dict[str, DataVaultField]: Dictionary of fields, indexed by its name.

        """
        fields_by_name_as_dict = {}

        for field in self.fields:
            fields_by_name_as_dict[field.name] = field

        return fields_by_name_as_dict

    @property
    @lru_cache(1)
    def fields_by_role(self) -> Dict[str, List[DataVaultField]]:
        """
        Returns a dictionary of DataVaultField, indexed by its role (check _fields_by_role_as_dict).

        Returns:
            Dict[str, DataVaultField]: Dictionary of fields, indexed by its role.
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
        """
        Implemented in subclasses.

        Returns:
            str: SQL script that should be executed to load current table.
        """

    @property
    def sql_placeholders(self) -> Dict[str, str]:
        """
        Calculates common placeholders needed to generate SQL for all DataVaultTable.

        Returns:
            Dict[str, str]: Common placeholders to be used in all DataVaultTable SQL scripts.
        """
        fields = ",".join(format_fields_for_select(fields=self.fields))
        query_args = {
            "target_schema": self.schema,
            "data_vault_table": self.name,
            "fields": fields,
            "staging_schema": self.staging_schema,
            "staging_table": self.staging_table,
            "record_start_timestamp": METADATA_FIELDS["record_start_timestamp"],
            "record_source": METADATA_FIELDS["record_source"],
        }

        return query_args

    def _validate(self):
        """
        Performs the following checks (common to all DataVaultTable subclasses):
        1. Table has one start_timestamp field - name defined in METADATA_FIELDS.
        2. Table has one source field - name defined in METADATA_FIELDS.

        Raises:
            RuntimeError: If one of the checks fails.
        """
        if not self.fields_by_name.get(METADATA_FIELDS["record_start_timestamp"]):
            raise RuntimeError(
                f"{self.name}: No field named '{METADATA_FIELDS['record_start_timestamp']}' found"
            )
        if not self.fields_by_name.get(METADATA_FIELDS["record_source"]):
            raise RuntimeError(
                f"{self.name}: No field named '{METADATA_FIELDS['record_source']}' found"
            )

    @property
    def hashkey_sql(self) -> str:
        """
        Generate SQL expression that should be used to calculate hashkey fields.

        The hashkey formula is the following: MD5(business_key_1 + |~~| + business_key_n + |~~| child_key_1).

        Returns:
            str: Hashkey SQL expression.
        """
        hashkey = next(
            hashkey
            for hashkey in format_fields_for_select(
                fields=self.fields_by_role.get(FieldRole.HASHKEY)
            )
        )
        business_keys_sql = [
            BUSINESS_KEY_SQL_TEMPLATE.format(business_key=field)
            for field in format_fields_for_select(
                fields=self.fields_by_role.get(FieldRole.BUSINESS_KEY)
            )
        ]
        child_keys_sql = [
            CHILD_KEY_SQL_TEMPLATE.format(child_key=field)
            for field in format_fields_for_select(
                fields=self.fields_by_role.get(FieldRole.CHILD_KEY)
            )
        ]
        fields_for_hashkey = business_keys_sql

        if child_keys_sql:
            fields_for_hashkey.extend(child_keys_sql)

        hashkey_expression = f"||'{HASH_DELIMITER}'||".join(fields_for_hashkey)
        hashkey_sql = HASHKEY_SQL_TEMPLATE.format(
            hashkey_expression=hashkey_expression, hashkey=hashkey
        )
        self._logger.debug(
            "Hashkey SQL expression for table (%s) is '(%s)'", self.name, hashkey_sql
        )

        return hashkey_sql
