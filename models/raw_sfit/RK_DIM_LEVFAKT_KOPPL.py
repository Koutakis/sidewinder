from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_levfakt_koppl",
    source_entity="RK_DIM_LEVFAKT_KOPPL",
    table="rk_dim_levfakt_koppl",
    schema="raindance_raw_2940",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KORR1", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR2", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR3", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR4", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR5", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR6", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR7", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR8", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR9", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KORR10", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="NR", data_type=PostgresType.NUMERIC),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sfit', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[KORR1] AS KORR1,
	[KORR2] AS KORR2,
	[KORR3] AS KORR3,
	[KORR4] AS KORR4,
	[KORR5] AS KORR5,
	[KORR6] AS KORR6,
	[KORR7] AS KORR7,
	[KORR8] AS KORR8,
	[KORR9] AS KORR9,
	[KORR10] AS KORR10,
	[NR] AS NR
    FROM [utdata].[utdata294].[RK_DIM_LEVFAKT_KOPPL]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2940", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")