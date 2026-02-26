from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_tab_agf",
    source_entity="RK_DIM_TAB_AGF",
    table="rk_dim_tab_agf",
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_AGF", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_AGF_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_AGF_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VARDE1", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VARDE2", data_type=PostgresType.NUMERIC),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ftsl', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[DUMMY2] AS DUMMY2,
	[TAB_AGF] AS TAB_AGF,
	[TAB_AGF_ID_TEXT] AS TAB_AGF_ID_TEXT,
	[TAB_AGF_TEXT] AS TAB_AGF_TEXT,
	[VARDE1] AS VARDE1,
	[VARDE2] AS VARDE2
    FROM [ftvudp].[ftv_400].[RK_DIM_TAB_AGF]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8810", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")