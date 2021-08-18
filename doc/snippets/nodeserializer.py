from datetime import datetime, timezone

from diepvries import FieldDataType
from diepvries.data_vault_load import DataVaultLoad
from diepvries.field import Field
from diepvries.hub import Hub
from diepvries.satellite import Satellite


def prepare_sql():
    # Instantiate structures.
    hub_foo = Hub(
        schema="dv",
        name="h_foo",
        fields=[
            Field(
                parent_table_name="h_foo",
                name="h_foo_hashkey",
                data_type=FieldDataType.NUMBER,
                position=1,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_foo",
                name="r_source",
                data_type=FieldDataType.NUMBER,
                position=2,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_foo",
                name="r_timestamp",
                data_type=FieldDataType.NUMBER,
                position=3,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_foo",
                name="foo_id",
                data_type=FieldDataType.TEXT,
                position=4,
                is_mandatory=True,
            ),
        ],
    )

    hub_satellite_foo = Satellite(
        schema="dv",
        name="hs_foo",
        fields=[
            Field(
                parent_table_name="hs_foo",
                name="h_foo_hashkey",
                data_type=FieldDataType.NUMBER,
                position=1,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="hs_foo",
                name="s_hashdiff",
                data_type=FieldDataType.NUMBER,
                position=2,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="hs_foo",
                name="r_timestamp",
                data_type=FieldDataType.NUMBER,
                position=4,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="hs_foo",
                name="r_timestamp_end",
                data_type=FieldDataType.NUMBER,
                position=5,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="hs_foo",
                name="r_source",
                data_type=FieldDataType.NUMBER,
                position=3,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="hs_foo",
                name="some_foo_property",
                data_type=FieldDataType.TEXT,
                position=6,
                is_mandatory=True,
            ),
        ],
    )

    # Prepare data load.
    dv_load = DataVaultLoad(
        extract_schema="dv_extract",
        extract_table="foobar",
        staging_schema="dv_staging",
        staging_table="foobar",
        extract_start_timestamp=datetime.utcnow().replace(tzinfo=timezone.utc),
        target_tables=[hub_foo, hub_satellite_foo],
        source="some_source",
    )

    # Show generated SQL statements.
    for statement in dv_load.sql_load_script:
        print(statement)


if __name__ == "__main__":
    prepare_sql()
