from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_kund_attr",
    source_entity="RK_DIM_KUND_ATTR",
    table="rk_dim_kund_attr",
    schema="raindance_raw_8020",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTR_KEY_PAT", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="ATTRIBUTE", data_type=PostgresType.TEXT),
        PostgresColumn(name="SBID", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sllin', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[ATTR_KEY_PAT] AS ATTR_KEY_PAT,
	[ATTRIBUTE] AS ATTRIBUTE,
	[SBID] AS SBID
    FROM [utdata].[utdata802].[RK_DIM_KUND_ATTR]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8020", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")