from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_konto",
    source_entity="EK_DIM_OBJ_KONTO",
    table="ek_dim_obj_konto",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG2_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG2_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG2_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRG2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRG2_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRG2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="K1_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="K1_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="K1_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="K1_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="K1_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="K1_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="K2_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="K2_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="K2_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="K2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="K2_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="K2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KONTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KONTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KONTO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TSIK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TSIK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TSIK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UHK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UHK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UHK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="UHK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UHK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="UHK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([FRG2_GILTIG_FOM], '1899-12-31 00:00:00') AS FRG2_GILTIG_FOM,
	COALESCE([FRG2_GILTIG_TOM], '1899-12-31 00:00:00') AS FRG2_GILTIG_TOM,
	[FRG2_ID] AS FRG2_ID,
	[FRG2_ID_TEXT] AS FRG2_ID_TEXT,
	[FRG2_PASSIV] AS FRG2_PASSIV,
	[FRG2_TEXT] AS FRG2_TEXT,
	COALESCE([FRG_GILTIG_FOM], '1899-12-31 00:00:00') AS FRG_GILTIG_FOM,
	COALESCE([FRG_GILTIG_TOM], '1899-12-31 00:00:00') AS FRG_GILTIG_TOM,
	[FRG_ID] AS FRG_ID,
	[FRG_ID_TEXT] AS FRG_ID_TEXT,
	[FRG_PASSIV] AS FRG_PASSIV,
	[FRG_TEXT] AS FRG_TEXT,
	COALESCE([FRK_GILTIG_FOM], '1899-12-31 00:00:00') AS FRK_GILTIG_FOM,
	COALESCE([FRK_GILTIG_TOM], '1899-12-31 00:00:00') AS FRK_GILTIG_TOM,
	[FRK_ID] AS FRK_ID,
	[FRK_ID_TEXT] AS FRK_ID_TEXT,
	[FRK_PASSIV] AS FRK_PASSIV,
	[FRK_TEXT] AS FRK_TEXT,
	COALESCE([K1_GILTIG_FOM], '1899-12-31 00:00:00') AS K1_GILTIG_FOM,
	COALESCE([K1_GILTIG_TOM], '1899-12-31 00:00:00') AS K1_GILTIG_TOM,
	[K1_ID] AS K1_ID,
	[K1_ID_TEXT] AS K1_ID_TEXT,
	[K1_PASSIV] AS K1_PASSIV,
	[K1_TEXT] AS K1_TEXT,
	COALESCE([K2_GILTIG_FOM], '1899-12-31 00:00:00') AS K2_GILTIG_FOM,
	COALESCE([K2_GILTIG_TOM], '1899-12-31 00:00:00') AS K2_GILTIG_TOM,
	[K2_ID] AS K2_ID,
	[K2_ID_TEXT] AS K2_ID_TEXT,
	[K2_PASSIV] AS K2_PASSIV,
	[K2_TEXT] AS K2_TEXT,
	COALESCE([KONTO_GILTIG_FOM], '1899-12-31 00:00:00') AS KONTO_GILTIG_FOM,
	COALESCE([KONTO_GILTIG_TOM], '1899-12-31 00:00:00') AS KONTO_GILTIG_TOM,
	[KONTO_ID] AS KONTO_ID,
	[KONTO_ID_TEXT] AS KONTO_ID_TEXT,
	[KONTO_PASSIV] AS KONTO_PASSIV,
	[KONTO_TEXT] AS KONTO_TEXT,
	COALESCE([TSIK_GILTIG_FOM], '1899-12-31 00:00:00') AS TSIK_GILTIG_FOM,
	COALESCE([TSIK_GILTIG_TOM], '1899-12-31 00:00:00') AS TSIK_GILTIG_TOM,
	[TSIK_ID] AS TSIK_ID,
	[TSIK_ID_TEXT] AS TSIK_ID_TEXT,
	[TSIK_PASSIV] AS TSIK_PASSIV,
	[TSIK_TEXT] AS TSIK_TEXT,
	COALESCE([UHK_GILTIG_FOM], '1899-12-31 00:00:00') AS UHK_GILTIG_FOM,
	COALESCE([UHK_GILTIG_TOM], '1899-12-31 00:00:00') AS UHK_GILTIG_TOM,
	[UHK_ID] AS UHK_ID,
	[UHK_ID_TEXT] AS UHK_ID_TEXT,
	[UHK_PASSIV] AS UHK_PASSIV,
	[UHK_TEXT] AS UHK_TEXT
    FROM [udpb4].[udpb4_100].[EK_DIM_OBJ_KONTO]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1100", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")