from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_valuta",
    source_entity="EK_DIM_OBJ_VALUTA",
    table="ek_dim_obj_valuta",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VALUTA_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VALUTA_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VALUTA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VALUTA_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VALUTA_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VALUTA_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([VALUTA_GILTIG_FOM], '1899-12-31 00:00:00') AS VALUTA_GILTIG_FOM,
	COALESCE([VALUTA_GILTIG_TOM], '1899-12-31 00:00:00') AS VALUTA_GILTIG_TOM,
	[VALUTA_ID] AS VALUTA_ID,
	[VALUTA_ID_TEXT] AS VALUTA_ID_TEXT,
	[VALUTA_PASSIV] AS VALUTA_PASSIV,
	[VALUTA_TEXT] AS VALUTA_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_VALUTA]

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