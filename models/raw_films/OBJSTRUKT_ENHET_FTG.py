from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="objstrukt_enhet_ftg",
    source_entity="OBJSTRUKT_ENHET_FTG",
    table="objstrukt_enhet_ftg",
    schema="raindance_raw_8010",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ENHET", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_FTG", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['films', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[OBJ_ENHET] AS OBJ_ENHET,
	[OBJ_FTG] AS OBJ_FTG
    FROM [utdata].[utdata801].[OBJSTRUKT_ENHET_FTG]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8010", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")