"""Field for Driving Key."""

from dataclasses import dataclass


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
