from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_obj_enhet",
    source_entity="AR_DIM_OBJ_ENHET",
    table="ar_dim_obj_enhet",
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSVAR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSVAR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANSVAR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENHET_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENHET_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENHET_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENHET_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENHET_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ENHET_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TVC_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TVC_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TVC_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TVC_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TVC_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TVC_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKRS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERKRS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERKRS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKRS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKRS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VERKRS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERKS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERKS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VERKS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VGREN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ftsl', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([ANSVAR_GILTIG_FOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_FOM,
	COALESCE([ANSVAR_GILTIG_TOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_TOM,
	[ANSVAR_ID] AS ANSVAR_ID,
	[ANSVAR_ID_TEXT] AS ANSVAR_ID_TEXT,
	[ANSVAR_PASSIV] AS ANSVAR_PASSIV,
	[ANSVAR_TEXT] AS ANSVAR_TEXT,
	COALESCE([ENHET_GILTIG_FOM], '1899-12-31 00:00:00') AS ENHET_GILTIG_FOM,
	COALESCE([ENHET_GILTIG_TOM], '1899-12-31 00:00:00') AS ENHET_GILTIG_TOM,
	[ENHET_ID] AS ENHET_ID,
	[ENHET_ID_TEXT] AS ENHET_ID_TEXT,
	[ENHET_PASSIV] AS ENHET_PASSIV,
	[ENHET_TEXT] AS ENHET_TEXT,
	COALESCE([TVC_GILTIG_FOM], '1899-12-31 00:00:00') AS TVC_GILTIG_FOM,
	COALESCE([TVC_GILTIG_TOM], '1899-12-31 00:00:00') AS TVC_GILTIG_TOM,
	[TVC_ID] AS TVC_ID,
	[TVC_ID_TEXT] AS TVC_ID_TEXT,
	[TVC_PASSIV] AS TVC_PASSIV,
	[TVC_TEXT] AS TVC_TEXT,
	COALESCE([VERKRS_GILTIG_FOM], '1899-12-31 00:00:00') AS VERKRS_GILTIG_FOM,
	COALESCE([VERKRS_GILTIG_TOM], '1899-12-31 00:00:00') AS VERKRS_GILTIG_TOM,
	[VERKRS_ID] AS VERKRS_ID,
	[VERKRS_ID_TEXT] AS VERKRS_ID_TEXT,
	[VERKRS_PASSIV] AS VERKRS_PASSIV,
	[VERKRS_TEXT] AS VERKRS_TEXT,
	COALESCE([VERKS_GILTIG_FOM], '1899-12-31 00:00:00') AS VERKS_GILTIG_FOM,
	COALESCE([VERKS_GILTIG_TOM], '1899-12-31 00:00:00') AS VERKS_GILTIG_TOM,
	[VERKS_ID] AS VERKS_ID,
	[VERKS_ID_TEXT] AS VERKS_ID_TEXT,
	[VERKS_PASSIV] AS VERKS_PASSIV,
	[VERKS_TEXT] AS VERKS_TEXT,
	COALESCE([VGREN_GILTIG_FOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_FOM,
	COALESCE([VGREN_GILTIG_TOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_TOM,
	[VGREN_ID] AS VGREN_ID,
	[VGREN_ID_TEXT] AS VGREN_ID_TEXT,
	[VGREN_PASSIV] AS VGREN_PASSIV,
	[VGREN_TEXT] AS VGREN_TEXT
    FROM [ftvudp].[ftv_400].[AR_DIM_OBJ_ENHET]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8810", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")