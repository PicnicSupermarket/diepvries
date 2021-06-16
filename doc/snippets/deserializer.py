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
        target_tables=["h_customer", "hs_customer"],
        database_configuration=database_configuration,
    )

    print(deserializer.deserialized_target_tables)
    print([x.name for x in deserializer.deserialized_target_tables])


if __name__ == "__main__":
    deserialize()
