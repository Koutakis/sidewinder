from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_stä",
    source_entity="EK_DIM_OBJ_STÄ",
    table="ek_dim_obj_stä",
    schema="raindance_raw_8090",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RE_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="STÄ_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="STÄ_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="STÄ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="STÄ_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="STÄ_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="STÄ_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['medic', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([AVD_GILTIG_FOM], '1899-12-31 00:00:00') AS AVD_GILTIG_FOM,
	COALESCE([AVD_GILTIG_TOM], '1899-12-31 00:00:00') AS AVD_GILTIG_TOM,
	[AVD_ID] AS AVD_ID,
	[AVD_ID_TEXT] AS AVD_ID_TEXT,
	[AVD_PASSIV] AS AVD_PASSIV,
	[AVD_TEXT] AS AVD_TEXT,
	COALESCE([RE_GILTIG_FOM], '1899-12-31 00:00:00') AS RE_GILTIG_FOM,
	COALESCE([RE_GILTIG_TOM], '1899-12-31 00:00:00') AS RE_GILTIG_TOM,
	[RE_ID] AS RE_ID,
	[RE_ID_TEXT] AS RE_ID_TEXT,
	[RE_PASSIV] AS RE_PASSIV,
	[RE_TEXT] AS RE_TEXT,
	COALESCE([STÄ_GILTIG_FOM], '1899-12-31 00:00:00') AS STÄ_GILTIG_FOM,
	COALESCE([STÄ_GILTIG_TOM], '1899-12-31 00:00:00') AS STÄ_GILTIG_TOM,
	[STÄ_ID] AS STÄ_ID,
	[STÄ_ID_TEXT] AS STÄ_ID_TEXT,
	[STÄ_PASSIV] AS STÄ_PASSIV,
	[STÄ_TEXT] AS STÄ_TEXT
    FROM [MediCarrierUDP].[utdata100].[EK_DIM_OBJ_STÄ]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8090", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")