from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_beloppstyp",
    source_entity="AR_DIM_BELOPPSTYP",
    table="ar_dim_beloppstyp",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BELOPPSTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP_ORDNING", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP_TEXT", data_type=PostgresType.TEXT),
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
	[BELOPPSTYP] AS BELOPPSTYP,
	[BELOPPSTYP2_ID_TEXT] AS BELOPPSTYP2_ID_TEXT,
	[BELOPPSTYP2_TEXT] AS BELOPPSTYP2_TEXT,
	[BELOPPSTYP_ID_TEXT] AS BELOPPSTYP_ID_TEXT,
	[BELOPPSTYP_ORDNING] AS BELOPPSTYP_ORDNING,
	[BELOPPSTYP_TEXT] AS BELOPPSTYP_TEXT
    FROM [raindance_udp].[udp_150].[AR_DIM_BELOPPSTYP]

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