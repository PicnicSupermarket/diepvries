from diepvries.deserializers.snowflake_deserializer import (
    DatabaseConfiguration,
    SnowflakeDeserializer,
)
from diepvries.driving_key_field import DrivingKeyField


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
        target_tables=["l_foo_bar", "ls_foo_bar_eff"],
        database_configuration=database_configuration,
        driving_keys=[
            DrivingKeyField(
                name="h_foo_hashkey",
                parent_table_name="l_foo_bar",
                satellite_name="ls_foo_bar_eff",
            )
        ],
    )

    print(deserializer.deserialized_target_tables)
    print([x.name for x in deserializer.deserialized_target_tables])


if __name__ == "__main__":
    deserialize()
