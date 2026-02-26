from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_handelse",
    source_entity="AR_DIM_HANDELSE",
    table="ar_dim_handelse",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="HANDELSE", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE_ORDNING", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="HANDELSE_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[HANDELSE] AS HANDELSE,
	[HANDELSE2_ID_TEXT] AS HANDELSE2_ID_TEXT,
	[HANDELSE2_TEXT] AS HANDELSE2_TEXT,
	[HANDELSE_ID_TEXT] AS HANDELSE_ID_TEXT,
	[HANDELSE_ORDNING] AS HANDELSE_ORDNING,
	[HANDELSE_TEXT] AS HANDELSE_TEXT
    FROM [raindance_udp].[udp_150].[AR_DIM_HANDELSE]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8510", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")