from diepvries.deserializers.snowflake_deserializer import (
    DatabaseConfiguration,
    SnowflakeDeserializer,
)


def deserialize():
    database_configuration = DatabaseConfiguration(
        database="<DB>",
        user="<USER>",
        password="<PASSWORD>",
        warehouse="<WAREHOUSE>",
        account="<ACCOUNT>",
    )

    deserializer = SnowflakeDeserializer(
        target_schema="dv",
        target_tables=["h_account", "h_account_supplier", "h_account_transporter"],
        database_configuration=database_configuration,
        role_playing_hubs={
            "h_account_supplier": "h_account",
            "h_account_transporter": "h_account",
        },
    )

    print(deserializer.deserialized_target_tables)
    print([x.name for x in deserializer.deserialized_target_tables])


if __name__ == "__main__":
    deserialize()
