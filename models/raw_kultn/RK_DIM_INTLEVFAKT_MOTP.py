from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_intlevfakt_motp",
    source_entity="RK_DIM_INTLEVFAKT_MOTP",
    table="rk_dim_intlevfakt_motp",
    schema="raindance_raw_3610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="INTKUNDID", data_type=PostgresType.TEXT),
        PostgresColumn(name="INTLEVID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KUND_PÅLOGG_FTG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KUNDFAKTNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="LEV_PÅLOGG_FTG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MOTPKOMB", data_type=PostgresType.TEXT),
        PostgresColumn(name="NR", data_type=PostgresType.NUMERIC),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kultn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[INTKUNDID] AS INTKUNDID,
	[INTLEVID] AS INTLEVID,
	[KUND_PÅLOGG_FTG] AS KUND_PÅLOGG_FTG,
	[KUNDFAKTNR] AS KUNDFAKTNR,
	[LEV_PÅLOGG_FTG] AS LEV_PÅLOGG_FTG,
	[MOTPKOMB] AS MOTPKOMB,
	[NR] AS NR
    FROM [utdata].[utdata361].[RK_DIM_INTLEVFAKT_MOTP]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_3610", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")