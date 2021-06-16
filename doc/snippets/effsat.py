from datetime import datetime, timezone

from diepvries import FieldDataType
from diepvries.data_vault_load import DataVaultLoad
from diepvries.driving_key_field import DrivingKeyField
from diepvries.effectivity_satellite import EffectivitySatellite
from diepvries.field import Field
from diepvries.hub import Hub
from diepvries.satellite import Satellite


def prepare_sql():
    # Instantiate structures.
    hub_foo = Hub(...)
    hub_bar = Hub(...)
    satellite_foo_bar = Satellite(...)

    eff_satellite_foo_bar = EffectivitySatellite(
        schema="dv",
        name="ls_foo_bar_eff",
        driving_keys=[
            DrivingKeyField(
                name="h_foo_hashkey",
                parent_table_name="ls_foo_bar",
                satellite_name="ls_foo_eff",
            )
        ],
        fields=[
            Field(
                parent_table_name="ls_foo_bar_eff",
                name="h_foo_hashkey",
                data_type=FieldDataType.NUMBER,
                position=1,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="ls_foo_bar_eff",
                name="s_hashdiff",
                data_type=FieldDataType.NUMBER,
                position=2,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="ls_foo_bar_eff",
                name="r_timestamp",
                data_type=FieldDataType.NUMBER,
                position=4,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="ls_foo_bar_eff",
                name="r_timestamp_end",
                data_type=FieldDataType.NUMBER,
                position=5,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="ls_foo_bar_eff",
                name="r_source",
                data_type=FieldDataType.NUMBER,
                position=3,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="ls_foo_bar_eff",
                name="some_property",
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
        target_tables=[hub_foo, hub_bar, satellite_foo_bar, eff_satellite_foo_bar],
        source="some_source",
    )

    # Show generated SQL statements.
    for statement in dv_load.sql_load_script:
        print(statement)


if __name__ == "__main__":
    prepare_sql()
