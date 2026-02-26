from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_avt_enh_60",
    source_entity="EK_DIM_OBJ_AVT_ENH_60",
    table="ek_dim_obj_avt_enh_60",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVT_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SHA_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SHA_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SHA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SHA_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SHA_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SHA_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([AVT_GILTIG_FOM], '1899-12-31 00:00:00') AS AVT_GILTIG_FOM,
	COALESCE([AVT_GILTIG_TOM], '1899-12-31 00:00:00') AS AVT_GILTIG_TOM,
	[AVT_ID] AS AVT_ID,
	[AVT_ID_TEXT] AS AVT_ID_TEXT,
	[AVT_PASSIV] AS AVT_PASSIV,
	[AVT_TEXT] AS AVT_TEXT,
	COALESCE([SHA_GILTIG_FOM], '1899-12-31 00:00:00') AS SHA_GILTIG_FOM,
	COALESCE([SHA_GILTIG_TOM], '1899-12-31 00:00:00') AS SHA_GILTIG_TOM,
	[SHA_ID] AS SHA_ID,
	[SHA_ID_TEXT] AS SHA_ID_TEXT,
	[SHA_PASSIV] AS SHA_PASSIV,
	[SHA_TEXT] AS SHA_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_AVT_ENH_60]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1500", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")