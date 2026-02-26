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
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
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
        PostgresColumn(name="SLLKTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SLLKTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SLLKTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SLLKTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SLLKTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SLLKTO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ÅRL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ÅRL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ÅRL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ÅRL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ÅRL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ÅRL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ste', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
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
	COALESCE([SLLKTO_GILTIG_FOM], '1899-12-31 00:00:00') AS SLLKTO_GILTIG_FOM,
	COALESCE([SLLKTO_GILTIG_TOM], '1899-12-31 00:00:00') AS SLLKTO_GILTIG_TOM,
	[SLLKTO_ID] AS SLLKTO_ID,
	[SLLKTO_ID_TEXT] AS SLLKTO_ID_TEXT,
	[SLLKTO_PASSIV] AS SLLKTO_PASSIV,
	[SLLKTO_TEXT] AS SLLKTO_TEXT,
	COALESCE([ÅRL_GILTIG_FOM], '1899-12-31 00:00:00') AS ÅRL_GILTIG_FOM,
	COALESCE([ÅRL_GILTIG_TOM], '1899-12-31 00:00:00') AS ÅRL_GILTIG_TOM,
	[ÅRL_ID] AS ÅRL_ID,
	[ÅRL_ID_TEXT] AS ÅRL_ID_TEXT,
	[ÅRL_PASSIV] AS ÅRL_PASSIV,
	[ÅRL_TEXT] AS ÅRL_TEXT
    FROM [steudp].[udp_600].[EK_DIM_OBJ_KONTO]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8530", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")