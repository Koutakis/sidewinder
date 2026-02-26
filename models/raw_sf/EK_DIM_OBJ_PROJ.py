from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_proj",
    source_entity="EK_DIM_OBJ_PROJ",
    table="ek_dim_obj_proj",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVSLÅR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVSLÅR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVSLÅR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVSLÅR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVSLÅR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVSLÅR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="EKTYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="EKTYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="EKTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EKTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="EKTYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="EKTYP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FIFORM_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FIFORM_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FIFORM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FIFORM_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FIFORM_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FIFORM_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="LEVANS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="LEVANS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="LEVANS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="LEVANS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="LEVANS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="LEVANS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PRADM_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PRADM_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PRADM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PRADM_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PRADM_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PRADM_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PRJINR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PRJINR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PRJINR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PRJINR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PRJINR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PRJINR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROENH_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROENH_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROENH_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROENH_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROENH_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROENH_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROJ_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROJL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([AVSLÅR_GILTIG_FOM], '1899-12-31 00:00:00') AS AVSLÅR_GILTIG_FOM,
	COALESCE([AVSLÅR_GILTIG_TOM], '1899-12-31 00:00:00') AS AVSLÅR_GILTIG_TOM,
	[AVSLÅR_ID] AS AVSLÅR_ID,
	[AVSLÅR_ID_TEXT] AS AVSLÅR_ID_TEXT,
	[AVSLÅR_PASSIV] AS AVSLÅR_PASSIV,
	[AVSLÅR_TEXT] AS AVSLÅR_TEXT,
	COALESCE([EKTYP_GILTIG_FOM], '1899-12-31 00:00:00') AS EKTYP_GILTIG_FOM,
	COALESCE([EKTYP_GILTIG_TOM], '1899-12-31 00:00:00') AS EKTYP_GILTIG_TOM,
	[EKTYP_ID] AS EKTYP_ID,
	[EKTYP_ID_TEXT] AS EKTYP_ID_TEXT,
	[EKTYP_PASSIV] AS EKTYP_PASSIV,
	[EKTYP_TEXT] AS EKTYP_TEXT,
	COALESCE([FIFORM_GILTIG_FOM], '1899-12-31 00:00:00') AS FIFORM_GILTIG_FOM,
	COALESCE([FIFORM_GILTIG_TOM], '1899-12-31 00:00:00') AS FIFORM_GILTIG_TOM,
	[FIFORM_ID] AS FIFORM_ID,
	[FIFORM_ID_TEXT] AS FIFORM_ID_TEXT,
	[FIFORM_PASSIV] AS FIFORM_PASSIV,
	[FIFORM_TEXT] AS FIFORM_TEXT,
	COALESCE([LEVANS_GILTIG_FOM], '1899-12-31 00:00:00') AS LEVANS_GILTIG_FOM,
	COALESCE([LEVANS_GILTIG_TOM], '1899-12-31 00:00:00') AS LEVANS_GILTIG_TOM,
	[LEVANS_ID] AS LEVANS_ID,
	[LEVANS_ID_TEXT] AS LEVANS_ID_TEXT,
	[LEVANS_PASSIV] AS LEVANS_PASSIV,
	[LEVANS_TEXT] AS LEVANS_TEXT,
	COALESCE([PRADM_GILTIG_FOM], '1899-12-31 00:00:00') AS PRADM_GILTIG_FOM,
	COALESCE([PRADM_GILTIG_TOM], '1899-12-31 00:00:00') AS PRADM_GILTIG_TOM,
	[PRADM_ID] AS PRADM_ID,
	[PRADM_ID_TEXT] AS PRADM_ID_TEXT,
	[PRADM_PASSIV] AS PRADM_PASSIV,
	[PRADM_TEXT] AS PRADM_TEXT,
	COALESCE([PRJINR_GILTIG_FOM], '1899-12-31 00:00:00') AS PRJINR_GILTIG_FOM,
	COALESCE([PRJINR_GILTIG_TOM], '1899-12-31 00:00:00') AS PRJINR_GILTIG_TOM,
	[PRJINR_ID] AS PRJINR_ID,
	[PRJINR_ID_TEXT] AS PRJINR_ID_TEXT,
	[PRJINR_PASSIV] AS PRJINR_PASSIV,
	[PRJINR_TEXT] AS PRJINR_TEXT,
	COALESCE([PROENH_GILTIG_FOM], '1899-12-31 00:00:00') AS PROENH_GILTIG_FOM,
	COALESCE([PROENH_GILTIG_TOM], '1899-12-31 00:00:00') AS PROENH_GILTIG_TOM,
	[PROENH_ID] AS PROENH_ID,
	[PROENH_ID_TEXT] AS PROENH_ID_TEXT,
	[PROENH_PASSIV] AS PROENH_PASSIV,
	[PROENH_TEXT] AS PROENH_TEXT,
	COALESCE([PROJ_GILTIG_FOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_FOM,
	COALESCE([PROJ_GILTIG_TOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_TOM,
	[PROJ_ID] AS PROJ_ID,
	[PROJ_ID_TEXT] AS PROJ_ID_TEXT,
	[PROJ_PASSIV] AS PROJ_PASSIV,
	[PROJ_TEXT] AS PROJ_TEXT,
	COALESCE([PROJL_GILTIG_FOM], '1899-12-31 00:00:00') AS PROJL_GILTIG_FOM,
	COALESCE([PROJL_GILTIG_TOM], '1899-12-31 00:00:00') AS PROJL_GILTIG_TOM,
	[PROJL_ID] AS PROJL_ID,
	[PROJL_ID_TEXT] AS PROJL_ID_TEXT,
	[PROJL_PASSIV] AS PROJL_PASSIV,
	[PROJL_TEXT] AS PROJL_TEXT
    FROM [utdata].[utdata298].[EK_DIM_OBJ_PROJ]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2985", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")