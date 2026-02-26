from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_kund_msg",
    source_entity="RK_DIM_KUND_MSG",
    table="rk_dim_kund_msg",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BIT_PAF", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="ENVELOPE_TRS", data_type=PostgresType.TEXT),
        PostgresColumn(name="FORMATV_RDF", data_type=PostgresType.TEXT),
        PostgresColumn(name="FREEVALUE", data_type=PostgresType.TEXT),
        PostgresColumn(name="LOGICALVALUE", data_type=PostgresType.TEXT),
        PostgresColumn(name="MEDIA_TRS", data_type=PostgresType.TEXT),
        PostgresColumn(name="MSGKEY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MSGTYPEV", data_type=PostgresType.TEXT),
        PostgresColumn(name="MSGWAY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="PART", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="SBID", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[BIT_PAF] AS BIT_PAF,
	[ENVELOPE_TRS] AS ENVELOPE_TRS,
	[FORMATV_RDF] AS FORMATV_RDF,
	[FREEVALUE] AS FREEVALUE,
	[LOGICALVALUE] AS LOGICALVALUE,
	[MEDIA_TRS] AS MEDIA_TRS,
	[MSGKEY] AS MSGKEY,
	[MSGTYPEV] AS MSGTYPEV,
	[MSGWAY] AS MSGWAY,
	[PART] AS PART,
	[SBID] AS SBID
    FROM [utdata].[utdata150].[RK_DIM_KUND_MSG]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1500", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")