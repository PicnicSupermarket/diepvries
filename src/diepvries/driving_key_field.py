from dataclasses import dataclass


@dataclass
class DrivingKeyField:
    """
    Class that represents a driving key (field used in effectivity satellites to point
    that is should be used to calculate the r_timestamp_end - PARTITION BY clause in DML).
    """

    #: Name of parent table in the database (always a Link).
    parent_table_name: str
    #: Column name in the database.
    name: str
    #: Name of the satellite where this field represents a driving key.
    satellite_name: str

    def __str__(self) -> str:
        """
        Defines the representation of a DrivingKeyField object as a string.
        This helps the tracking of logging events per entity.

        Returns:
            str: String representation for the `DrivingKeyField` object.
        """
        return f"{type(self).__name__}: field_name={self.name}, satellite={self.satellite_name}"
