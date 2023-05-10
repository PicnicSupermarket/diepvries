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

    #: Name of parent table in the database (always a Link).
    parent_table_name: str
    #: Column name in the database.
    name: str
    #: Name of the satellite where this field represents a driving key.
    satellite_name: str

    def __post_init__(self):
        """Validate the driving key."""
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
            AssertionError: If one of the checks fails.
        """
        satellite_name_prefix = next(
            split_part for split_part in self.satellite_name.split("_")
        )
        assert satellite_name_prefix in TABLE_PREFIXES[TableType.SATELLITE], (
            f"'{self.satellite_name}': Satellite name with incorrect prefix. "
            f"Got '{satellite_name_prefix}', "
            f"but expects '{TABLE_PREFIXES[TableType.SATELLITE]}'."
        )

        parent_table_name_prefix = next(
            split_part for split_part in self.parent_table_name.split("_")
        )
        assert parent_table_name_prefix in TABLE_PREFIXES[TableType.LINK], (
            f"'{self.satellite_name}': Parent table with incorrect prefix. "
            f"Got '{parent_table_name_prefix}', "
            f"but expects '{TABLE_PREFIXES[TableType.LINK]}'."
        )
