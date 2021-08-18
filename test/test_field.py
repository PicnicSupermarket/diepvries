"""Unit tests for Field."""

import pytest

from diepvries import METADATA_FIELDS, FieldDataType, FieldRole, TableType
from diepvries.field import Field


@pytest.mark.parametrize(
    ("input_field", "output_string"),
    [
        (
            Field(
                parent_table_name="some_table",
                name="test_text_with_length",
                data_type=FieldDataType.TEXT,
                position=1,
                length=32,
                is_mandatory=False,
            ),
            "TEXT (32)",
        ),
        (
            Field(
                parent_table_name="some_table",
                name="test_text_without_length",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            "TEXT",
        ),
        (
            Field(
                parent_table_name="some_table",
                name="test_number",
                data_type=FieldDataType.NUMBER,
                position=1,
                is_mandatory=False,
                precision=18,
                scale=8,
            ),
            "NUMBER (18, 8)",
        ),
        (
            Field(
                parent_table_name="some_table",
                name="test_array",
                data_type=FieldDataType.ARRAY,
                position=1,
                is_mandatory=False,
            ),
            "ARRAY",
        ),
    ],
)
def test_data_type_sql(input_field, output_string):
    """Test ``data_type_sql`` property."""
    assert input_field.data_type_sql == output_string


@pytest.mark.parametrize(
    ("input_field", "output_string"),
    [
        (
            Field(
                parent_table_name="hs_customer",
                name="test_array",
                data_type=FieldDataType.ARRAY,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(CAST(CAST(test_array AS ARRAY) AS TEXT), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_boolean",
                data_type=FieldDataType.BOOLEAN,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(CAST(CAST(test_boolean AS BOOLEAN) AS TEXT), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_date",
                data_type=FieldDataType.DATE,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(TO_CHAR(CAST(test_date AS DATE), 'yyyy-mm-dd'), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_geography",
                data_type=FieldDataType.GEOGRAPHY,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(ST_ASTEXT(TO_GEOGRAPHY(test_geography)), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_number",
                data_type=FieldDataType.NUMBER,
                position=1,
                precision=18,
                scale=8,
                is_mandatory=False,
            ),
            "COALESCE(CAST(CAST(test_number AS NUMBER (18, 8)) AS TEXT), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_object",
                data_type=FieldDataType.OBJECT,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(CAST(CAST(test_object AS OBJECT) AS TEXT), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_real",
                data_type=FieldDataType.REAL,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(CAST(CAST(test_real AS REAL) AS TEXT), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_text",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(CAST(test_text AS TEXT), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_time",
                data_type=FieldDataType.TIME,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(TO_CHAR(CAST(test_time AS TIME), 'hh24:mi:ss.ff9'), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_timestamp_ltz",
                data_type=FieldDataType.TIMESTAMP_LTZ,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(TO_CHAR(CAST(test_timestamp_ltz AS TIMESTAMP_LTZ), "
            "'yyyy-mm-dd hh24:mi:ss.ff9 tzhtzm'), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_timestamp_ntz",
                data_type=FieldDataType.TIMESTAMP_NTZ,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(TO_CHAR(CAST(test_timestamp_ntz AS TIMESTAMP_NTZ), "
            "'yyyy-mm-dd hh24:mi:ss.ff9'), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_timestamp_tz",
                data_type=FieldDataType.TIMESTAMP_TZ,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(TO_CHAR(CAST(test_timestamp_tz AS TIMESTAMP_TZ), "
            "'yyyy-mm-dd hh24:mi:ss.ff9 tzhtzm'), '')",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="test_variant",
                data_type=FieldDataType.VARIANT,
                position=1,
                is_mandatory=False,
            ),
            "COALESCE(CAST(CAST(test_variant AS VARIANT) AS TEXT), '')",
        ),
        (
            Field(
                parent_table_name="h_customer",
                name="customer_id",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=False,
            ),
            "COALESCE(CAST(customer_id AS TEXT), 'dv_unknown')",
        ),
    ],
)
def test_hash_concatenation_sql(input_field, output_string):
    """Test ``hash_concatenation_sql`` property."""
    assert input_field.hash_concatenation_sql == output_string


@pytest.mark.parametrize(
    ("input_field", "suffix"),
    [
        (
            Field(
                parent_table_name="h_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            "hashkey",
        ),
        (
            Field(
                parent_table_name="h_customer",
                name="customer_id",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=False,
            ),
            "id",
        ),
    ],
)
def test_suffix(input_field, suffix):
    """Test ``suffix`` property."""
    assert input_field.suffix == suffix


@pytest.mark.parametrize(
    ("input_field", "prefix"),
    [
        (
            Field(
                parent_table_name="h_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            "h",
        ),
        (
            Field(
                parent_table_name="l_order_customer",
                name="l_order_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            "l",
        ),
    ],
)
def test_prefix(input_field, prefix):
    """Test ``prefix`` property."""
    assert input_field.prefix == prefix


@pytest.mark.parametrize(
    ("input_field", "table_type"),
    [
        (
            Field(
                parent_table_name="h_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            TableType.HUB,
        ),
        (
            Field(
                parent_table_name="l_order_customer",
                name="l_order_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            TableType.LINK,
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            TableType.SATELLITE,
        ),
        (
            Field(
                parent_table_name="ls_order_customer",
                name="l_order_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            TableType.SATELLITE,
        ),
    ],
)
def test_parent_table_type(input_field, table_type):
    """Test ``parent_table_type`` property."""
    assert input_field.parent_table_type == table_type


@pytest.mark.parametrize(
    ("input_field", "field_name_in_staging"),
    [
        (
            Field(
                parent_table_name="h_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            "h_customer_hashkey",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="s_hashdiff",
                data_type=FieldDataType.TEXT,
                position=3,
                is_mandatory=False,
            ),
            "hs_customer_hashdiff",
        ),
    ],
)
def test_name_in_staging(input_field, field_name_in_staging):
    """Test ``name_in_staging`` property."""
    assert input_field.name_in_staging == field_name_in_staging


@pytest.mark.parametrize(
    ("input_field", "ddl_in_staging"),
    [
        (
            Field(
                parent_table_name="hs_customer",
                name="mandatory_field",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=True,
            ),
            "mandatory_field TEXT NOT NULL",
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="non_mandatory_field",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=False,
            ),
            "non_mandatory_field TEXT",
        ),
    ],
)
def test_ddl_in_staging(input_field, ddl_in_staging):
    """Test ``ddl_in_staging`` property."""
    assert input_field.ddl_in_staging == ddl_in_staging


@pytest.mark.parametrize(
    ("input_field", "role"),
    [
        (
            Field(
                parent_table_name="hs_customer",
                name=METADATA_FIELDS["record_source"],
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=True,
            ),
            FieldRole.METADATA,
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name=METADATA_FIELDS["record_start_timestamp"],
                data_type=FieldDataType.TIMESTAMP_NTZ,
                position=1,
                is_mandatory=True,
            ),
            FieldRole.METADATA,
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name=METADATA_FIELDS["record_end_timestamp"],
                data_type=FieldDataType.TIMESTAMP_NTZ,
                position=1,
                is_mandatory=True,
            ),
            FieldRole.METADATA,
        ),
        (
            Field(
                parent_table_name="l_order_customer",
                name="l_order_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=True,
            ),
            FieldRole.HASHKEY,
        ),
        (
            Field(
                parent_table_name="h_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=1,
                is_mandatory=True,
            ),
            FieldRole.HASHKEY,
        ),
        (
            Field(
                parent_table_name="l_order_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=True,
            ),
            FieldRole.HASHKEY_PARENT,
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="h_customer_hashkey",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=True,
            ),
            FieldRole.HASHKEY_PARENT,
        ),
        (
            Field(
                parent_table_name="l_order_customer",
                name="order_id",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=True,
            ),
            FieldRole.BUSINESS_KEY,
        ),
        (
            Field(
                parent_table_name="h_customer",
                name="customer_id",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=True,
            ),
            FieldRole.BUSINESS_KEY,
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="s_hashdiff",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=True,
            ),
            FieldRole.HASHDIFF,
        ),
        (
            Field(
                parent_table_name="l_order_customer",
                name="ck_timestamp",
                data_type=FieldDataType.TIMESTAMP_NTZ,
                position=2,
                is_mandatory=True,
            ),
            FieldRole.CHILD_KEY,
        ),
        (
            Field(
                parent_table_name="hs_customer",
                name="some_field",
                data_type=FieldDataType.TEXT,
                position=2,
                is_mandatory=False,
            ),
            FieldRole.DESCRIPTIVE,
        ),
    ],
)
def test_role(input_field, role):
    """Test ``role`` property."""
    assert input_field.role == role
