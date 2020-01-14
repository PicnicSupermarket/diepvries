from . import (
    FIELD_PREFIX,
    FIELD_SUFFIX,
    METADATA_FIELDS,
    TABLE_PREFIXES,
    FieldDataType,
    FieldRole,
    TableType,
)


class DataVaultField:
    """
    Class that represents a field in a Data Vault model.
    """

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
        """
        Instantiate a DataVaultField object and converts both name and parent_table_name
        to lower case.

        Args:
            parent_table_name (str): Name of parent table in the database.
            name (str): Column name in the database.
            data_type (FieldDataType): Column data type in the database.
            position (int): Column position in the database.
            is_mandatory (bool): Column is mandatory in the database.
            precision (int): numeric precision (maximum number of digits before the
                decimal separator). Only applicable when
                self.data_type==FieldDataType.NUMBER.
            scale (int): numeric scale (maximum number of digits after the
                decimal separator). Only applicable when
                self.data_type==FieldDataType.NUMBER.
            length (int): character length (maximum number of characters allowed).
                Only applicable when self.data_type==FieldDataType.TEXT.
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
        return hash(self.name_in_staging)

    def __eq__(self, other):
        return self.name_in_staging == other.name_in_staging

    def __str__(self) -> str:
        """
        Define the representation of a DataVaultField object as a string.
        This helps the tracking of logging events per entity.

        Returns:
            str: String representation for the `DataVaultField` object.
        """
        return f"{type(self).__name__}: {self.name}"

    @property
    def suffix(self) -> str:
        """
        Get field suffix.

        Returns:
            str: Field suffix.
        """
        return self.name.split("_").pop()

    @property
    def prefix(self) -> str:
        """
        Get field prefix.

        Returns:
            str: Field prefix.
        """
        return next(split_part for split_part in self.name.split("_"))

    @property
    def parent_table_type(self) -> TableType:
        """
        Define parent table type, based on table prefix.

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
        Define the name that this field should have, whenever created in a staging
        table.

        In most cases this function will return self.name, but for hashdiffs the name
        is <parent_table_name>_hashdiff (every Satellite has one hashdiff field, named
        s_hashdiff).

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
        Calculate the DDL expression that should be used while creating this
        field in the staging table.

        Returns:
            str: The DDL expression for this field.
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
        elif self.data_type == FieldDataType.TEXT and self.length:
            return (
                f"{self.name_in_staging} {self.data_type.value} "
                f"({self.length}) {'NOT NULL' if self.is_mandatory else ''}"
            )
        else:
            return (
                f"{self.name_in_staging} {self.data_type.name} "
                f"{'NOT NULL' if self.is_mandatory else ''}"
            )

    @property
    def role(self) -> FieldRole:
        """
        Calculate the role of the field in a Data Vault model (check FieldRole enum).

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
