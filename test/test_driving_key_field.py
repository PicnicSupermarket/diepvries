"""Unit test for DrivingKeyField."""
from diepvries import TableType, TABLE_PREFIXES
from diepvries.driving_key_field import DrivingKeyField


def test_satellite_name_prefix(ls_order_customer_eff_driving_keys: DrivingKeyField):
    """Assert correctness of satellite table names prefix in list of driving keys
    ls_order_customer_eff_driving_keys.

    Args:
        ls_order_customer_eff_driving_keys: ls_order_customer_eff_driving_keys
        fixture value.
    """
    for driving_key in ls_order_customer_eff_driving_keys:
        table_name_prefix = next(
            split_part for split_part in driving_key.satellite_name.split("_")
        )
        assert table_name_prefix in TABLE_PREFIXES[TableType.SATELLITE]


def test_parent_table_name_prefix(ls_order_customer_eff_driving_keys: DrivingKeyField):
    """Assert correctness of parent table names prefix in list of driving keys
    ls_order_customer_eff_driving_keys.

    Args:
        ls_order_customer_eff_driving_keys: ls_order_customer_eff_driving_keys
        fixture value.
    """
    for driving_key in ls_order_customer_eff_driving_keys:
        table_name_prefix = next(
            split_part for split_part in driving_key.parent_table_name.split("_")
        )
        assert table_name_prefix in TABLE_PREFIXES[TableType.LINK]
