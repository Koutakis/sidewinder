from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_obj_proj",
    source_entity="AR_DIM_OBJ_PROJ",
    table="ar_dim_obj_proj",
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="BE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="BE_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PANS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PANS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PANS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PANS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PANS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PANS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PKAT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PKAT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PKAT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PKAT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PKAT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PKAT_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROJ_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PTYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PTYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PTYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PTYP_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([BE_GILTIG_FOM], '1899-12-31 00:00:00') AS BE_GILTIG_FOM,
	COALESCE([BE_GILTIG_TOM], '1899-12-31 00:00:00') AS BE_GILTIG_TOM,
	[BE_ID] AS BE_ID,
	[BE_ID_TEXT] AS BE_ID_TEXT,
	[BE_PASSIV] AS BE_PASSIV,
	[BE_TEXT] AS BE_TEXT,
	COALESCE([PANS_GILTIG_FOM], '1899-12-31 00:00:00') AS PANS_GILTIG_FOM,
	COALESCE([PANS_GILTIG_TOM], '1899-12-31 00:00:00') AS PANS_GILTIG_TOM,
	[PANS_ID] AS PANS_ID,
	[PANS_ID_TEXT] AS PANS_ID_TEXT,
	[PANS_PASSIV] AS PANS_PASSIV,
	[PANS_TEXT] AS PANS_TEXT,
	COALESCE([PKAT_GILTIG_FOM], '1899-12-31 00:00:00') AS PKAT_GILTIG_FOM,
	COALESCE([PKAT_GILTIG_TOM], '1899-12-31 00:00:00') AS PKAT_GILTIG_TOM,
	[PKAT_ID] AS PKAT_ID,
	[PKAT_ID_TEXT] AS PKAT_ID_TEXT,
	[PKAT_PASSIV] AS PKAT_PASSIV,
	[PKAT_TEXT] AS PKAT_TEXT,
	COALESCE([PROJ_GILTIG_FOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_FOM,
	COALESCE([PROJ_GILTIG_TOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_TOM,
	[PROJ_ID] AS PROJ_ID,
	[PROJ_ID_TEXT] AS PROJ_ID_TEXT,
	[PROJ_PASSIV] AS PROJ_PASSIV,
	[PROJ_TEXT] AS PROJ_TEXT,
	COALESCE([PTYP_GILTIG_FOM], '1899-12-31 00:00:00') AS PTYP_GILTIG_FOM,
	COALESCE([PTYP_GILTIG_TOM], '1899-12-31 00:00:00') AS PTYP_GILTIG_TOM,
	[PTYP_ID] AS PTYP_ID,
	[PTYP_ID_TEXT] AS PTYP_ID_TEXT,
	[PTYP_PASSIV] AS PTYP_PASSIV,
	[PTYP_TEXT] AS PTYP_TEXT
    FROM [steudp].[udp_600].[AR_DIM_OBJ_PROJ]

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