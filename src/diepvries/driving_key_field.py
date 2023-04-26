"""Field for Driving Key."""

from dataclasses import dataclass

from . import TABLE_PREFIXES, TableType


@dataclass
class DrivingKeyField:
    """A driving key.

    A driving key is a field used in effectivity satellites to calculate the
    `r_timestamp_end` (`PARTITION BY` clause in DML) when the link unique key is
    defined by a subset of hashkeys.
    """

    def __init__(self, parent_table_name: str, name: str, satellite_name: str):
        """Instantiate a DrivingKeyField.

        Args:
            parent_table_name: Name of parent table in the database (always a Link).
            name: Column name in the database.
            satellite_name: Name of the satellite where this field
                represents a driving key.
        """
        self.parent_table_name = parent_table_name
        self.name = name
        self.satellite_name = satellite_name

        self._validate()

    def __str__(self) -> str:
        """Representation of a DrivingKeyField object as a string.

        This helps the tracking of logging events per entity.

        Returns:
            String representation for the `DrivingKeyField` object.
        """
        return (
            f"{type(self).__name__}: field_name={self.name}, "
            f"satellite={self.satellite_name}"
        )

    def _validate(self):
        """Validate driving key.

        Perform the following checks:
        1. Satellite name has a prefix within TABLE_PREFIXES[TableType.SATELLITE].
        2. Parent table name has a prefix within TABLE_PREFIXES[TableType.LINK].

        Raises:
            RuntimeError: If one of the checks fails.
        """
        satellite_name_prefix = next(
            split_part for split_part in self.satellite_name.split("_")
        )
        if satellite_name_prefix not in TABLE_PREFIXES[TableType.SATELLITE]:
            raise RuntimeError(
                f"'{self.satellite_name}': Satellite name with incorrect prefix. "
                f"Got '{satellite_name_prefix}', "
                f"but expects '{TABLE_PREFIXES[TableType.SATELLITE]}'."
            )

        parent_table_name_prefix = next(
            split_part for split_part in self.parent_table_name.split("_")
        )
        if parent_table_name_prefix not in TABLE_PREFIXES[TableType.LINK]:
            raise RuntimeError(
                f"'{self.satellite_name}': Parent table with incorrect prefix. "
                f"Got '{parent_table_name_prefix}', "
                f"but expects '{TABLE_PREFIXES[TableType.LINK]}'."
            )
