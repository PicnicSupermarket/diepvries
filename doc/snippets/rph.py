from datetime import datetime, timezone

from diepvries import FieldDataType
from diepvries.data_vault_load import DataVaultLoad
from diepvries.field import Field
from diepvries.hub import Hub
from diepvries.role_playing_hub import RolePlayingHub
from diepvries.satellite import Satellite


def prepare_sql():
    # Instantiate structures.
    hub_account = Hub(
        schema="dv",
        name="h_account",
        fields=[
            Field(
                parent_table_name="h_account",
                name="h_account_hashkey",
                data_type=FieldDataType.NUMBER,
                position=1,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_account",
                name="r_source",
                data_type=FieldDataType.NUMBER,
                position=2,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_account",
                name="r_timestamp",
                data_type=FieldDataType.NUMBER,
                position=3,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_account",
                name="account_id",
                data_type=FieldDataType.TEXT,
                position=4,
                is_mandatory=True,
            ),
        ],
    )

    role_playing_hub_account_supplier = RolePlayingHub(
        schema="dv",
        name="h_account_supplier",
        fields=[
            Field(
                parent_table_name="h_account_supplier",
                name="h_account_supplier_hashkey",
                data_type=FieldDataType.NUMBER,
                position=1,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_account_supplier",
                name="r_source",
                data_type=FieldDataType.NUMBER,
                position=2,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_account_supplier",
                name="r_timestamp",
                data_type=FieldDataType.NUMBER,
                position=3,
                is_mandatory=True,
            ),
            Field(
                parent_table_name="h_account_supplier",
                name="account_id",
                data_type=FieldDataType.TEXT,
                position=4,
                is_mandatory=True,
            ),
        ],
    )
    role_playing_hub_account_supplier.parent_table = hub_account

    # Prepare data load.
    dv_load = DataVaultLoad(
        extract_schema="dv_extract",
        extract_table="foobar",
        staging_schema="dv_staging",
        staging_table="foobar",
        extract_start_timestamp=datetime.utcnow().replace(tzinfo=timezone.utc),
        target_tables=[hub_account, role_playing_hub_account_supplier],
        source="some_source",
    )

    # Show generated SQL statements.
    for statement in dv_load.sql_load_script:
        print(statement)


if __name__ == "__main__":
    prepare_sql()
