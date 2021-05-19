"""Module for a Data Vault field."""
from typing import Optional

from . import (
    FIELD_PREFIX,
    FIELD_SUFFIX,
    METADATA_FIELDS,
    TABLE_PREFIXES,
    FieldDataType,
    FieldRole,
    TableType,
)


class Field:
    """A field in a Data Vault model."""

    def __init__(
        self,
        parent_table_name: str,
        name: str,
        data_type: FieldDataType,
        position: int,
        is_mandatory: bool,
        precision: int = None,
        scale: int = None,
        length: int = None,
    ):
        """Instantiate a Field.

        Convert both name and parent_table_name to lower case.

        Args:
            parent_table_name: Name of parent table in the database.
            name: Column name in the database.
            data_type: Column data type in the database.
            position: Column position in the database.
            is_mandatory: Column is mandatory in the database.
            precision: Numeric precision (maximum number of digits before the decimal
                separator). Only applicable when `self.data_type==FieldDataType.NUMBER`.
            scale: Numeric scale (maximum number of digits after the decimal
                separator). Only applicable when `self.data_type==FieldDataType.NUMBER`.
            length: Character length (maximum number of characters allowed). Only
                applicable when `self.data_type==FieldDataType.TEXT`.
        """
        self.parent_table_name = parent_table_name.lower()
        self.name = name.lower()
        self.data_type = data_type
        self.position = position
        self.is_mandatory = is_mandatory
        self.precision = precision
        self.scale = scale
        self.length = length

    def __hash__(self):
        """Hash of a Data Vault field."""
        return hash(self.name_in_staging)

    def __eq__(self, other):
        """Equality of a Data Vault field."""
        return self.name_in_staging == other.name_in_staging

    def __str__(self) -> str:
        """Representation of a Field object as a string.

        This helps the tracking of logging events per entity.

        Returns:
            String representation for the `Field` object.
        """
        return f"{type(self).__name__}: {self.name}"

    @property
    def suffix(self) -> str:
        """Get field suffix.

        Returns:
            Field suffix.
        """
        return self.name.split("_").pop()

    @property
    def prefix(self) -> str:
        """Get field prefix.

        Returns:
           Field prefix.
        """
        return next(split_part for split_part in self.name.split("_"))

    @property
    def parent_table_type(self) -> TableType:
        """Get parent table type, based on table prefix.

        Returns:
            Table type (HUB, LINK or SATELLITE).
        """
        table_prefix = next(
            split_part for split_part in self.parent_table_name.split("_")
        )
        if table_prefix in TABLE_PREFIXES[TableType.LINK]:
            return TableType.LINK
        if table_prefix in TABLE_PREFIXES[TableType.SATELLITE]:
            return TableType.SATELLITE
        return TableType.HUB

    @property
    def name_in_staging(self) -> str:
        """Get the name that this field should have, when created in a staging table.

        In most cases this function will return `self.name`, but for hashdiffs the name
        is <parent_table_name>_hashdiff (every Satellite has one hashdiff field, named
        s_hashdiff).

        Returns:
            Name of the field in staging.
        """
        if self.role == FieldRole.HASHDIFF:
            return f"{self.parent_table_name}_{FIELD_SUFFIX[FieldRole.HASHDIFF]}"
        return self.name

    @property
    def ddl_in_staging(self) -> str:
        """Get DDL expression to create this field in the staging table.

        Returns:
            The DDL expression for this field.
        """
        if (
            self.data_type == FieldDataType.NUMBER
            and self.scale is not None
            and self.precision is not None
        ):
            return (
                f"{self.name_in_staging} {self.data_type.value} "
                f"({self.precision}, {self.scale}) "
                f"{'NOT NULL' if self.is_mandatory else ''}"
            )
        if self.data_type == FieldDataType.TEXT and self.length:
            return (
                f"{self.name_in_staging} {self.data_type.value} "
                f"({self.length}) {'NOT NULL' if self.is_mandatory else ''}"
            )
        return (
            f"{self.name_in_staging} {self.data_type.name} "
            f"{'NOT NULL' if self.is_mandatory else ''}"
        )

    @property
    def role(self) -> FieldRole:
        """Get the role of the field in a Data Vault model.

         See `FieldRole` enum for more information.

        Returns:
            Field role in a Data Vault model.

        Raises:
            RuntimeError: When no field role can be attributed.
        """
        found_role: Optional[FieldRole] = None

        if self.name in METADATA_FIELDS.values():
            found_role = FieldRole.METADATA
        elif (
            self.name == f"{self.parent_table_name}_{self.suffix}"
            and self.suffix == FIELD_SUFFIX[FieldRole.HASHKEY]
        ):
            found_role = FieldRole.HASHKEY
        elif self.suffix == FIELD_SUFFIX[FieldRole.HASHKEY]:
            found_role = FieldRole.HASHKEY_PARENT
        elif self.prefix == FIELD_PREFIX[FieldRole.CHILD_KEY]:
            found_role = FieldRole.CHILD_KEY
        elif (
            self.parent_table_type != TableType.SATELLITE
            and self.prefix not in FIELD_PREFIX.values()
            and self.position != 1
        ):
            found_role = FieldRole.BUSINESS_KEY
        elif self.suffix == FIELD_SUFFIX[FieldRole.HASHDIFF]:
            found_role = FieldRole.HASHDIFF
        elif self.parent_table_type == TableType.SATELLITE:
            found_role = FieldRole.DESCRIPTIVE

        if found_role is not None:
            return found_role

        raise RuntimeError(
            (
                f"{self.name}: It was not possible to assign a valid field role "
                f" (validate FieldRole and FIELD_PREFIXES configuration)"
            )
        )
