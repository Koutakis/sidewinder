from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_boktyp",
    source_entity="RK_DIM_BOKTYP",
    table="rk_dim_boktyp",
    schema="raindance_raw_2900",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BOKTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_NR", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['patn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[BOKTYP] AS BOKTYP,
	[BOKTYP_ID] AS BOKTYP_ID,
	[BOKTYP_ID_TEXT] AS BOKTYP_ID_TEXT,
	[BOKTYP_NR] AS BOKTYP_NR,
	[BOKTYP_TEXT] AS BOKTYP_TEXT
    FROM [utdata].[utdata290].[RK_DIM_BOKTYP]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2900", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")