from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_obj_kst",
    source_entity="AR_DIM_OBJ_KST",
    table="ar_dim_obj_kst",
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DI_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DI_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DI_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DI_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DI_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DI_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SEK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VER_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VER_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VER_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VER_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VER_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VER_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([DI_GILTIG_FOM], '1899-12-31 00:00:00') AS DI_GILTIG_FOM,
	COALESCE([DI_GILTIG_TOM], '1899-12-31 00:00:00') AS DI_GILTIG_TOM,
	[DI_ID] AS DI_ID,
	[DI_ID_TEXT] AS DI_ID_TEXT,
	[DI_PASSIV] AS DI_PASSIV,
	[DI_TEXT] AS DI_TEXT,
	COALESCE([KL_GILTIG_FOM], '1899-12-31 00:00:00') AS KL_GILTIG_FOM,
	COALESCE([KL_GILTIG_TOM], '1899-12-31 00:00:00') AS KL_GILTIG_TOM,
	[KL_ID] AS KL_ID,
	[KL_ID_TEXT] AS KL_ID_TEXT,
	[KL_PASSIV] AS KL_PASSIV,
	[KL_TEXT] AS KL_TEXT,
	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
	[KST_ID] AS KST_ID,
	[KST_ID_TEXT] AS KST_ID_TEXT,
	[KST_PASSIV] AS KST_PASSIV,
	[KST_TEXT] AS KST_TEXT,
	COALESCE([SEK_GILTIG_FOM], '1899-12-31 00:00:00') AS SEK_GILTIG_FOM,
	COALESCE([SEK_GILTIG_TOM], '1899-12-31 00:00:00') AS SEK_GILTIG_TOM,
	[SEK_ID] AS SEK_ID,
	[SEK_ID_TEXT] AS SEK_ID_TEXT,
	[SEK_PASSIV] AS SEK_PASSIV,
	[SEK_TEXT] AS SEK_TEXT,
	COALESCE([VER_GILTIG_FOM], '1899-12-31 00:00:00') AS VER_GILTIG_FOM,
	COALESCE([VER_GILTIG_TOM], '1899-12-31 00:00:00') AS VER_GILTIG_TOM,
	[VER_ID] AS VER_ID,
	[VER_ID_TEXT] AS VER_ID_TEXT,
	[VER_PASSIV] AS VER_PASSIV,
	[VER_TEXT] AS VER_TEXT
    FROM [steudp].[udp_600].[AR_DIM_OBJ_KST]

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