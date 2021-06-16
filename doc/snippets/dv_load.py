from datetime import datetime, timezone

from diepvries.data_vault_load import DataVaultLoad
from diepvries.deserializers.snowflake_deserializer import (
    DatabaseConfiguration,
    SnowflakeDeserializer,
)


def get_load_sql():
    # Configuration for the Snowflake connection
    database_configuration = DatabaseConfiguration(
        database="diepvries_tutorial",
        user="<YOUR USER>",
        password="<YOUR PASSWORD>",
        warehouse="<YOUR WAREHOUSE>",
        account="<YOUR SNOWFLAKE ACCOUNT NAME>",
    )

    # Automatically deserialize known Data Vault tables
    deserializer = SnowflakeDeserializer(
        target_schema="dv",
        target_tables=["h_customer", "hs_customer", "h_order", "hs_order"],
        database_configuration=database_configuration,
    )

    # Prepare data load
    dv_load = DataVaultLoad(
        extract_schema="dv_extract",
        extract_table="order_customer",
        staging_schema="dv_staging",
        staging_table="order_customer",
        extract_start_timestamp=datetime.utcnow().replace(tzinfo=timezone.utc),
        target_tables=deserializer.deserialized_target_tables,
        source="Data from diepvries tutorial",
    )

    # Show generated SQL statements
    for statement in dv_load.sql_load_script:
        print(statement)


if __name__ == "__main__":
    get_load_sql()
