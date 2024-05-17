from sqlalchemy import create_engine
import yaml

with open("params.yaml", "r") as yamlfile:
    parmas = yaml.load(yamlfile, Loader=yaml.FullLoader)

engine = create_engine(
    "snowflake://{user}:{password}@{account_identifier}/diepvries_tutorial".format(
        user=parmas["user"],
        password=parmas["password"],
        account_identifier=parmas["account"],

    )
)
# try:
#     connection = engine.connect()
#     results = connection.execute("select current_version()").fetchone()
#     print(results[0])
# finally:
#     connection.close()
#     engine.dispose()


# def set_minimum_timestamp(engine):
#     min_timestamp = engine.execute(
#         """SELECT
#                       DATEADD(HOUR, -4, COALESCE(MIN(target.{record_start_timestamp}), CURRENT_TIMESTAMP()))
#                     FROM {staging_schema}.{staging_table} AS staging
#                       INNER JOIN {target_schema}.{target_table} AS target
#                                  ON (staging.{source_hashkey_field} = target.{target_hashkey_field})
#                     )"""
#     )
#     return min_timestamp


connection = engine.connect()

def fetch_timestamp(engine):
    min_timestamp = engine.execute(
        """SELECT create_ts from dv_extract.order_customer"""
    ).fetchone()
    return min_timestamp


def fetch_timestamp_placeholder(engine, params):
    min_timestamp = engine.execute(
        """SELECT create_ts from dv_extract.{table}""".format(**params)
    ).fetchone()
    return min_timestamp




params = {"table": "order_customer"}
print(fetch_timestamp_placeholder(engine=engine, params= params))




# def merge_SQL(min_timestamp):
#     SQL = """MERGE INTO {target_schema}.{target_table} AS target
#   USING (
#         SELECT DISTINCT
#           {source_hashkey_field},
#           -- If multiple sources for the same hashkey are received, their values
#           -- are concatenated using a comma.
#           LISTAGG(DISTINCT {record_source_field}, ',')
#                   WITHIN GROUP (ORDER BY {record_source_field})
#                   OVER (PARTITION BY {source_hashkey_field}) AS {record_source_field},
#           {source_fields}
#         FROM {staging_schema}.{staging_table}
#         ) AS staging ON (target.{target_hashkey_field} = staging.{source_hashkey_field}
#     AND target.{record_start_timestamp} >= min_timestamp)
#   WHEN NOT MATCHED THEN INSERT ({target_fields})
#     VALUES ({staging_source_fields})"""


# sql_load_statement = (
#     (TEMPLATES_DIR / "hub_link_dml.sql")
#     .read_text()
#     .format(**self.sql_placeholders)
# )