"""Unit test for DrivingKeyField."""

import pytest

from diepvries.driving_key_field import DrivingKeyField


def test_valid_driving_key():
    """Check that no errors are raised when instantiating a
    DrivingKeyField with valid values.
    """
    DrivingKeyField(
        name="h_customer_hashkey",
        parent_table_name="l_order_customer",
        satellite_name="ls_order_customer_eff",
    )


def test_invalid_satellite_name():
    """Check that an `AssertionError` is raised when an invalid
    satellite_name is provided.
    """
    with pytest.raises(AssertionError):
        DrivingKeyField(
            name="h_customer_hashkey",
            parent_table_name="l_order_customer",
            satellite_name="invalid_satellite_name",
        )


def test_invalid_parent_table_name():
    """Check that an `AssertionError` is raised when an invalid
    parent_table_name is provided.
    """
    with pytest.raises(AssertionError):
        DrivingKeyField(
            name="h_customer_hashkey",
            parent_table_name="invalid_parent_table_name",
            satellite_name="ls_order_customer_eff",
        )
