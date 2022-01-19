import os

from snowflake.sqlalchemy import URL, MergeInto
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy import (
    Column,
    MetaData,
    Table,
    and_,
    cast,
    column,
    create_engine,
    func,
    literal,
    or_,
    select,
    text,
)
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.functions import coalesce, concat
from sqlalchemy.types import TEXT, TIMESTAMP


def connect():
    url = URL()
    engine = create_engine(url)
    connection = engine.connect()
    return engine, connection


def disconnect(engine, connection):
    connection.close()
    engine.dispose()


def hashdiff(*columns):
    # Cast to text (TODO: different casting functions for different types)
    columns = [cast(x, TEXT) for x in columns]
    # Coalesce (TODO: coalesce to dv_unknown for business key
    columns = [coalesce(x, "") for x in columns]

    sep = literal("|~~|")
    columns_with_sep = []
    for i, c in enumerate(columns):
        columns_with_sep.append(c)
        if i < len(columns):
            columns_with_sep.append(sep)
    return func.md5(func.regexp_replace(concat(*columns_with_sep)))


def main():
    engine, connection = connect()

    # Reflect state of DB.
    # TODO: check if the reflection loads the whole schema. Can be super slow.
    # TODO: does it support geography fields?
    metadata = MetaData(connection)
    metadata.reflect(schema="test_dv", only=["h_customer", "hs_customer"])
    metadata.reflect(schema="test_dv_extract", only=["order_customer"])

    # Reflect target tables
    dv_h_customer = metadata.tables["test_dv.h_customer"]
    dv_hs_customer = metadata.tables["test_dv.hs_customer"]

    # Reflect extract table
    dv_extract_order_customer = metadata.tables["test_dv_extract.order_customer"]

    # Build staging table
    staging_table_order_customer = select(
        coalesce(dv_extract_order_customer.c.customer_id, "dv_unknown").label(
            "customer_id"
        ),
        func.md5(
            coalesce(cast(dv_extract_order_customer.c.customer_id, TEXT), "dv_unknown")
        ).label("h_customer_hashkey"),
        coalesce(dv_extract_order_customer.c.order_id, "dv_unknown").label("order_id"),
        func.md5(
            coalesce(cast(dv_extract_order_customer.c.order_id, TEXT), "dv_unknown")
        ).label("h_order_hashkey"),
        cast(literal("2021-11-12T10:05:16.761205Z"), TIMESTAMP).label("r_timestamp"),
        literal("Data from diepvries tutorial").label("r_source"),
        hashdiff(
            dv_extract_order_customer.c.customer_id,
            dv_extract_order_customer.c.firstname,
            dv_extract_order_customer.c.lastname,
        ).label("hs_customer_hashdiff"),
        dv_extract_order_customer.c.firstname.label("firstname"),
        dv_extract_order_customer.c.lastname.label("lastname"),
        hashdiff(
            dv_extract_order_customer.c.order_id,
            dv_extract_order_customer.c.create_ts,
            dv_extract_order_customer.c.quantity,
        ).label("hs_order_hashdiff"),
        dv_extract_order_customer.c.create_ts.label("create_ts"),
        dv_extract_order_customer.c.quantity.label("quantity"),
    )
    # To actually create the table ("CREATE TABLE AS SELECT ..."), see
    # https://github.com/sqlalchemy/sqlalchemy/issues/5687
    # https://stackoverflow.com/questions/30575111/how-to-create-a-new-table-from-select-statement-in-sqlalchemy
    # Or first create the table, then INSERT INTO ... SELECT ...

    # Staging table definition
    staging_table = Table(
        "order_customer_20211112_100516",
        metadata,
        Column("customer_id", TEXT),
        Column("h_customer_hashkey", TEXT),
        Column("order_id", TEXT),
        Column("h_order_hashkey", TEXT),
        Column("r_timestamp", TEXT),
        Column("r_source", TEXT),
        Column("hs_customer_hashdiff", TEXT),
        Column("firstname", TEXT),
        Column("lastname", TEXT),
        Column("hs_order_hashdiff", TEXT),
        Column("create_ts", TEXT),
        Column("quantity", TEXT),
        schema="test_dv_staging",
    )

    # Build MERGE INTO statement for h_customer
    h_customer_statement = select(
        [
            staging_table.c.h_customer_hashkey.label("h_customer_hashkey"),
            (
                func.listagg(staging_table.c.r_source.distinct(), ",")
                .within_group(staging_table.c.r_source)
                .over(partition_by=staging_table.c.h_customer_hashkey)
                .label("r_source")
            ),
            staging_table.c.r_timestamp.label("r_timestamp"),
            staging_table.c.customer_id.label("customer_id"),
        ],
        distinct=True,
    ).subquery(name="staging")

    # TODO: 2 issues with MergeInto
    # - Doesn't support subquery as source, i.e. no parentheses for USING (...).
    # - Unclear how compilation/repr works, i.e. we're forced to give it a string as
    #   source, otherwise the literal_binds are not applied in the final result.
    h_customer_statement_merge = MergeInto(
        target=dv_h_customer,
        source=h_customer_statement.compile(compile_kwargs={"literal_binds": True}),
        on=dv_h_customer.c.h_customer_hashkey
        == h_customer_statement.c.h_customer_hashkey,
    )
    h_customer_statement_merge.when_not_matched_then_insert().values(
        h_customer_hashkey=h_customer_statement.c.h_customer_hashkey,
        r_timestamp=h_customer_statement.c.r_timestamp,
        r_source=h_customer_statement.c.r_source,
        customer_id=h_customer_statement.c.customer_id,
    )

    # Build MERGE INTO statement for hs_customer
    _max_satellite_timestamp = select(
        func.max(dv_hs_customer.c.r_timestamp).label("max_r_timestamp")
    ).subquery("max_satellite_timestamp")

    _hs_customer_filtered_staging = (
        select(
            [
                staging_table.c.h_customer_hashkey,
                staging_table.c.hs_customer_hashdiff,
                staging_table.c.r_timestamp,
                staging_table.c.r_source,
                staging_table.c.firstname,
                staging_table.c.lastname,
            ],
            distinct=True,
        )
        .join(_max_satellite_timestamp, literal(True))  # CROSS JOIN
        .where(
            staging_table.c.r_timestamp
            >= coalesce(
                _max_satellite_timestamp.c.max_r_timestamp, "1970-01-01 00:00:00"
            )
        )
        .cte(name="filtered_staging")
    )

    _hs_customer_staging_satellite_affected_records = (
        select(
            [
                _hs_customer_filtered_staging.c.h_customer_hashkey,
                _hs_customer_filtered_staging.c.hs_customer_hashdiff,
                _hs_customer_filtered_staging.c.r_timestamp,
                _hs_customer_filtered_staging.c.r_source,
                _hs_customer_filtered_staging.c.firstname,
                _hs_customer_filtered_staging.c.lastname,
            ]
        )
        .outerjoin(
            dv_hs_customer,
            and_(
                _hs_customer_filtered_staging.c.h_customer_hashkey
                == dv_hs_customer.c.h_customer_hashkey,
                dv_hs_customer.c.r_timestamp_end
                == cast(literal("9999-12-31T00:00:00.000000Z"), TIMESTAMP),
            ),
        )
        .where(
            or_(
                dv_hs_customer.c.h_customer_hashkey == None,
                dv_hs_customer.c.s_hashdiff
                != _hs_customer_filtered_staging.c.hs_customer_hashdiff,
            )
        )
        # TODO: UNION ALL. Useful for MERGE INTO, or can be done differently with
        #  different DBs.
        .cte(name="staging_satellite_affected_records")
    )

    hs_customer_statement = select(
        [
            _hs_customer_staging_satellite_affected_records.c.h_customer_hashkey,
            _hs_customer_staging_satellite_affected_records.c.hs_customer_hashdiff,
            _hs_customer_staging_satellite_affected_records.c.r_timestamp,
            (
                func.lead(
                    func.dateadd(
                        text("MILLISECONDS"),
                        -1,
                        _hs_customer_staging_satellite_affected_records.c.r_timestamp,
                    ),
                    1,
                    cast(literal("9999-12-31T00:00:00.000000Z"), TIMESTAMP),
                )
                .over(
                    partition_by=_hs_customer_staging_satellite_affected_records.c.h_customer_hashkey,
                    order_by=_hs_customer_staging_satellite_affected_records.c.r_timestamp,
                )
                .label("r_timestamp_end")
            ),
            _hs_customer_staging_satellite_affected_records.c.r_source,
            _hs_customer_staging_satellite_affected_records.c.firstname,
            _hs_customer_staging_satellite_affected_records.c.lastname,
        ]
    )
    # TODO hs_customer_statement_merge

    print(hs_customer_statement.compile(compile_kwargs={"literal_binds": True}))


if __name__ == "__main__":
    main()
