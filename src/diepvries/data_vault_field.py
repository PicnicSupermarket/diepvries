from dataclasses import dataclass

from . import (
    FieldRole,
    METADATA_FIELDS,
    TableType,
    FIELD_SUFFIX,
    FIELD_PREFIX,
    TABLE_PREFIXES,
)


@dataclass
class DataVaultField:
    """
    Class that represents a field in a Data Vault model.
    """

    #: Name of parent table in the database.
    parent_table_name: str
    #: Column name in the database.
    name: str
    #: Column datatype in the database.
    datatype: str
    #: Column position in the database.
    position: int
    #: Column is mandatory in the database.
    is_mandatory: bool

    def __post_init__(self):
        """
        Converts name and parent_table_name to lower case.
        """
        self.name = self.name.lower()
        self.parent_table_name = self.parent_table_name.lower()

    def __str__(self) -> str:
        """
        Defines the representation of a DataVaultField object as a string.
        This helps the tracking of logging events per entity.

        Returns:
            str: String representation for the `DataVaultField` object.
        """
        return f"{type(self).__name__}: {self.name}"

    @property
    def suffix(self) -> str:
        """
        Gets field suffix.

        Returns:
            str: Field suffix.
        """
        return self.name.split("_").pop()

    @property
    def prefix(self) -> str:
        """
        Gets field prefix.

        Returns:
            str: Field prefix.
        """
        return next(split_part for split_part in self.name.split("_"))

    @property
    def parent_table_type(self) -> TableType:
        """
        Defines parent table type, based on table prefix.

        Returns:
            TableType: Table type (HUB, LINK or SATELLITE).
        """
        table_prefix = next(
            split_part for split_part in self.parent_table_name.split("_")
        )
        if table_prefix in TABLE_PREFIXES[TableType.LINK]:
            return TableType.LINK
        elif table_prefix in TABLE_PREFIXES[TableType.SATELLITE]:
            return TableType.SATELLITE
        else:
            return TableType.HUB

    @property
    def name_in_staging(self) -> str:
        """
        Defines the name that this field should have, whenever created in a staging table.

        In most cases this function will return self.name, but for hashdiffs the name
        is <parent_table_name>_hashdiff (every Satellite has one hashdiff field, named s_hashdiff).

        Returns:
            str: Name of the field in staging.
        """
        if self.role == FieldRole.HASHDIFF:
            name_in_staging = (
                f"{self.parent_table_name}_{FIELD_SUFFIX[FieldRole.HASHDIFF]}"
            )
        else:
            name_in_staging = self.name

        return name_in_staging

    @property
    def ddl_in_staging(self) -> str:
        """
        Calculates the DDL expression that should be used while creating this
        field in the staging table.

        Returns:
            str: The DDL expression for this field.
        """
        return f"{self.name_in_staging} {self.datatype} {'NOT NULL' if self.is_mandatory else ''}"

    @property
    def role(self) -> FieldRole:
        """
        Calculates the role of the field in a Data Vault model (check FieldRole enum).

        Returns:
            FieldRole: Field role in a Data Vault model.

        Raises:
            RuntimeError: If a field role could be attributed.
        """
        name_no_prefix_or_suffix = self.name.replace(f"_{self.suffix}", "").replace(
            f"{self.prefix}_", ""
        )

        if self.name in METADATA_FIELDS.values():
            return FieldRole.METADATA
        elif (
            self.name == f"{self.parent_table_name}_{self.suffix}"
            and self.suffix in FIELD_SUFFIX[FieldRole.HASHKEY]
        ):
            return FieldRole.HASHKEY
        elif self.suffix in FIELD_SUFFIX[FieldRole.HASHKEY]:
            return FieldRole.HASHKEY_PARENT
        elif self.prefix in FIELD_PREFIX[FieldRole.CHILD_KEY]:
            return FieldRole.CHILD_KEY
        elif (
            self.suffix in FIELD_SUFFIX[FieldRole.BUSINESS_KEY]
            and name_no_prefix_or_suffix in self.parent_table_name
        ):
            return FieldRole.BUSINESS_KEY
        elif self.suffix in FIELD_SUFFIX[FieldRole.HASHDIFF]:
            return FieldRole.HASHDIFF
        elif self.parent_table_type == TableType.SATELLITE:
            return FieldRole.DESCRIPTIVE
        else:
            raise RuntimeError(
                (
                    f"{self.name}: It was not possible to assign a valid field role "
                    f" (validate FieldRole and FIELD_PREFIXES configuration)"
                )
            )
