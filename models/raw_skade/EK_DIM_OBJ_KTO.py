from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_kto",
    source_entity="EK_DIM_OBJ_KTO",
    table="ek_dim_obj_kto",
    schema="raindance_raw_2990",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRANGO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRANGO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRANGO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRANGO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRANGO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRANGO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GKTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GKTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GKTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="GKTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GKTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="GKTO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGRUPP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KGRUPP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KGRUPP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGRUPP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGRUPP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KGRUPP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KKL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KTO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADNR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RADNR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RADNR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADNR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADNR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RADNR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TSIK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TSIK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TSIK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['skade', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([FRANGO_GILTIG_FOM], '1899-12-31 00:00:00') AS FRANGO_GILTIG_FOM,
	COALESCE([FRANGO_GILTIG_TOM], '1899-12-31 00:00:00') AS FRANGO_GILTIG_TOM,
	[FRANGO_ID] AS FRANGO_ID,
	[FRANGO_ID_TEXT] AS FRANGO_ID_TEXT,
	[FRANGO_PASSIV] AS FRANGO_PASSIV,
	[FRANGO_TEXT] AS FRANGO_TEXT,
	COALESCE([GKTO_GILTIG_FOM], '1899-12-31 00:00:00') AS GKTO_GILTIG_FOM,
	COALESCE([GKTO_GILTIG_TOM], '1899-12-31 00:00:00') AS GKTO_GILTIG_TOM,
	[GKTO_ID] AS GKTO_ID,
	[GKTO_ID_TEXT] AS GKTO_ID_TEXT,
	[GKTO_PASSIV] AS GKTO_PASSIV,
	[GKTO_TEXT] AS GKTO_TEXT,
	COALESCE([KGRUPP_GILTIG_FOM], '1899-12-31 00:00:00') AS KGRUPP_GILTIG_FOM,
	COALESCE([KGRUPP_GILTIG_TOM], '1899-12-31 00:00:00') AS KGRUPP_GILTIG_TOM,
	[KGRUPP_ID] AS KGRUPP_ID,
	[KGRUPP_ID_TEXT] AS KGRUPP_ID_TEXT,
	[KGRUPP_PASSIV] AS KGRUPP_PASSIV,
	[KGRUPP_TEXT] AS KGRUPP_TEXT,
	COALESCE([KKL_GILTIG_FOM], '1899-12-31 00:00:00') AS KKL_GILTIG_FOM,
	COALESCE([KKL_GILTIG_TOM], '1899-12-31 00:00:00') AS KKL_GILTIG_TOM,
	[KKL_ID] AS KKL_ID,
	[KKL_ID_TEXT] AS KKL_ID_TEXT,
	[KKL_PASSIV] AS KKL_PASSIV,
	[KKL_TEXT] AS KKL_TEXT,
	COALESCE([KTO_GILTIG_FOM], '1899-12-31 00:00:00') AS KTO_GILTIG_FOM,
	COALESCE([KTO_GILTIG_TOM], '1899-12-31 00:00:00') AS KTO_GILTIG_TOM,
	[KTO_ID] AS KTO_ID,
	[KTO_ID_TEXT] AS KTO_ID_TEXT,
	[KTO_PASSIV] AS KTO_PASSIV,
	[KTO_TEXT] AS KTO_TEXT,
	COALESCE([RADNR_GILTIG_FOM], '1899-12-31 00:00:00') AS RADNR_GILTIG_FOM,
	COALESCE([RADNR_GILTIG_TOM], '1899-12-31 00:00:00') AS RADNR_GILTIG_TOM,
	[RADNR_ID] AS RADNR_ID,
	[RADNR_ID_TEXT] AS RADNR_ID_TEXT,
	[RADNR_PASSIV] AS RADNR_PASSIV,
	[RADNR_TEXT] AS RADNR_TEXT,
	COALESCE([TSIK_GILTIG_FOM], '1899-12-31 00:00:00') AS TSIK_GILTIG_FOM,
	COALESCE([TSIK_GILTIG_TOM], '1899-12-31 00:00:00') AS TSIK_GILTIG_TOM,
	[TSIK_ID] AS TSIK_ID,
	[TSIK_ID_TEXT] AS TSIK_ID_TEXT,
	[TSIK_PASSIV] AS TSIK_PASSIV,
	[TSIK_TEXT] AS TSIK_TEXT
    FROM [utdata].[utdata299].[EK_DIM_OBJ_KTO]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2990", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")