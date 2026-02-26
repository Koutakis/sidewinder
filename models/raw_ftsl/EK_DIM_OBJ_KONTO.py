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
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRANGO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRANGO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRANGO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRANGO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRANGO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRANGO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HKRAD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="HKRAD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="HKRAD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="HKRAD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HKRAD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="HKRAD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KGR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KGR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KGR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KKL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLIRAD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KLIRAD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KLIRAD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLIRAD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLIRAD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KLIRAD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KONTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KONTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KONTO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SRU_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SRU_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SRU_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SRU_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SRU_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SRU_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="STYRAD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="STYRAD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="STYRAD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="STYRAD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="STYRAD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="STYRAD_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([FRANGO_GILTIG_FOM], '1899-12-31 00:00:00') AS FRANGO_GILTIG_FOM,
	COALESCE([FRANGO_GILTIG_TOM], '1899-12-31 00:00:00') AS FRANGO_GILTIG_TOM,
	[FRANGO_ID] AS FRANGO_ID,
	[FRANGO_ID_TEXT] AS FRANGO_ID_TEXT,
	[FRANGO_PASSIV] AS FRANGO_PASSIV,
	[FRANGO_TEXT] AS FRANGO_TEXT,
	COALESCE([HKRAD_GILTIG_FOM], '1899-12-31 00:00:00') AS HKRAD_GILTIG_FOM,
	COALESCE([HKRAD_GILTIG_TOM], '1899-12-31 00:00:00') AS HKRAD_GILTIG_TOM,
	[HKRAD_ID] AS HKRAD_ID,
	[HKRAD_ID_TEXT] AS HKRAD_ID_TEXT,
	[HKRAD_PASSIV] AS HKRAD_PASSIV,
	[HKRAD_TEXT] AS HKRAD_TEXT,
	COALESCE([KGR_GILTIG_FOM], '1899-12-31 00:00:00') AS KGR_GILTIG_FOM,
	COALESCE([KGR_GILTIG_TOM], '1899-12-31 00:00:00') AS KGR_GILTIG_TOM,
	[KGR_ID] AS KGR_ID,
	[KGR_ID_TEXT] AS KGR_ID_TEXT,
	[KGR_PASSIV] AS KGR_PASSIV,
	[KGR_TEXT] AS KGR_TEXT,
	COALESCE([KKL_GILTIG_FOM], '1899-12-31 00:00:00') AS KKL_GILTIG_FOM,
	COALESCE([KKL_GILTIG_TOM], '1899-12-31 00:00:00') AS KKL_GILTIG_TOM,
	[KKL_ID] AS KKL_ID,
	[KKL_ID_TEXT] AS KKL_ID_TEXT,
	[KKL_PASSIV] AS KKL_PASSIV,
	[KKL_TEXT] AS KKL_TEXT,
	COALESCE([KLIRAD_GILTIG_FOM], '1899-12-31 00:00:00') AS KLIRAD_GILTIG_FOM,
	COALESCE([KLIRAD_GILTIG_TOM], '1899-12-31 00:00:00') AS KLIRAD_GILTIG_TOM,
	[KLIRAD_ID] AS KLIRAD_ID,
	[KLIRAD_ID_TEXT] AS KLIRAD_ID_TEXT,
	[KLIRAD_PASSIV] AS KLIRAD_PASSIV,
	[KLIRAD_TEXT] AS KLIRAD_TEXT,
	COALESCE([KONTO_GILTIG_FOM], '1899-12-31 00:00:00') AS KONTO_GILTIG_FOM,
	COALESCE([KONTO_GILTIG_TOM], '1899-12-31 00:00:00') AS KONTO_GILTIG_TOM,
	[KONTO_ID] AS KONTO_ID,
	[KONTO_ID_TEXT] AS KONTO_ID_TEXT,
	[KONTO_PASSIV] AS KONTO_PASSIV,
	[KONTO_TEXT] AS KONTO_TEXT,
	COALESCE([SRU_GILTIG_FOM], '1899-12-31 00:00:00') AS SRU_GILTIG_FOM,
	COALESCE([SRU_GILTIG_TOM], '1899-12-31 00:00:00') AS SRU_GILTIG_TOM,
	[SRU_ID] AS SRU_ID,
	[SRU_ID_TEXT] AS SRU_ID_TEXT,
	[SRU_PASSIV] AS SRU_PASSIV,
	[SRU_TEXT] AS SRU_TEXT,
	COALESCE([STYRAD_GILTIG_FOM], '1899-12-31 00:00:00') AS STYRAD_GILTIG_FOM,
	COALESCE([STYRAD_GILTIG_TOM], '1899-12-31 00:00:00') AS STYRAD_GILTIG_TOM,
	[STYRAD_ID] AS STYRAD_ID,
	[STYRAD_ID_TEXT] AS STYRAD_ID_TEXT,
	[STYRAD_PASSIV] AS STYRAD_PASSIV,
	[STYRAD_TEXT] AS STYRAD_TEXT
    FROM [ftvudp].[ftv_400].[EK_DIM_OBJ_KONTO]

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