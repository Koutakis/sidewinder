from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_kst",
    source_entity="EK_DIM_OBJ_KST",
    table="ek_dim_obj_kst",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ENH_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="NSO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="NSO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="NSO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="NSO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="NSO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="NSO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RE_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SA_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SA_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SA_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SA_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SA_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VT_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([ENH_GILTIG_FOM], '1899-12-31 00:00:00') AS ENH_GILTIG_FOM,
	COALESCE([ENH_GILTIG_TOM], '1899-12-31 00:00:00') AS ENH_GILTIG_TOM,
	[ENH_ID] AS ENH_ID,
	[ENH_ID_TEXT] AS ENH_ID_TEXT,
	[ENH_PASSIV] AS ENH_PASSIV,
	[ENH_TEXT] AS ENH_TEXT,
	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
	[KST_ID] AS KST_ID,
	[KST_ID_TEXT] AS KST_ID_TEXT,
	[KST_PASSIV] AS KST_PASSIV,
	[KST_TEXT] AS KST_TEXT,
	COALESCE([NSO_GILTIG_FOM], '1899-12-31 00:00:00') AS NSO_GILTIG_FOM,
	COALESCE([NSO_GILTIG_TOM], '1899-12-31 00:00:00') AS NSO_GILTIG_TOM,
	[NSO_ID] AS NSO_ID,
	[NSO_ID_TEXT] AS NSO_ID_TEXT,
	[NSO_PASSIV] AS NSO_PASSIV,
	[NSO_TEXT] AS NSO_TEXT,
	COALESCE([RE_GILTIG_FOM], '1899-12-31 00:00:00') AS RE_GILTIG_FOM,
	COALESCE([RE_GILTIG_TOM], '1899-12-31 00:00:00') AS RE_GILTIG_TOM,
	[RE_ID] AS RE_ID,
	[RE_ID_TEXT] AS RE_ID_TEXT,
	[RE_PASSIV] AS RE_PASSIV,
	[RE_TEXT] AS RE_TEXT,
	COALESCE([SA_GILTIG_FOM], '1899-12-31 00:00:00') AS SA_GILTIG_FOM,
	COALESCE([SA_GILTIG_TOM], '1899-12-31 00:00:00') AS SA_GILTIG_TOM,
	[SA_ID] AS SA_ID,
	[SA_ID_TEXT] AS SA_ID_TEXT,
	[SA_PASSIV] AS SA_PASSIV,
	[SA_TEXT] AS SA_TEXT,
	COALESCE([VG_GILTIG_FOM], '1899-12-31 00:00:00') AS VG_GILTIG_FOM,
	COALESCE([VG_GILTIG_TOM], '1899-12-31 00:00:00') AS VG_GILTIG_TOM,
	[VG_ID] AS VG_ID,
	[VG_ID_TEXT] AS VG_ID_TEXT,
	[VG_PASSIV] AS VG_PASSIV,
	[VG_TEXT] AS VG_TEXT,
	COALESCE([VT_GILTIG_FOM], '1899-12-31 00:00:00') AS VT_GILTIG_FOM,
	COALESCE([VT_GILTIG_TOM], '1899-12-31 00:00:00') AS VT_GILTIG_TOM,
	[VT_ID] AS VT_ID,
	[VT_ID_TEXT] AS VT_ID_TEXT,
	[VT_PASSIV] AS VT_PASSIV,
	[VT_TEXT] AS VT_TEXT
    FROM [udpb4].[udpb4_100].[EK_DIM_OBJ_KST]

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