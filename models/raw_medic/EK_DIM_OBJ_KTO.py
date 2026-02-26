from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_kto",
    source_entity="EK_DIM_OBJ_KTO",
    table="ek_dim_obj_kto",
    schema="raindance_raw_8090",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FR01_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FR01_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FR01_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FR01_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FR01_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FR01_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KKL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KTO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTOG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KTOG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KTOG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTOG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTOG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KTOG_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['medic', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FR01_GILTIG_FOM], '1899-12-31 00:00:00') AS FR01_GILTIG_FOM,
    	COALESCE([FR01_GILTIG_TOM], '1899-12-31 00:00:00') AS FR01_GILTIG_TOM,
    	[FR01_ID] AS FR01_ID,
    	[FR01_ID_TEXT] AS FR01_ID_TEXT,
    	[FR01_PASSIV] AS FR01_PASSIV,
    	[FR01_TEXT] AS FR01_TEXT,
    	COALESCE([KKL_GILTIG_FOM], '1899-12-31 00:00:00') AS KKL_GILTIG_FOM,
    	COALESCE([KKL_GILTIG_TOM], '1899-12-31 00:00:00') AS KKL_GILTIG_TOM,
    	[KKL_ID] AS KKL_ID,
    	[KKL_ID_TEXT] AS KKL_ID_TEXT,
    	[KKL_PASSIV] AS KKL_PASSIV,
    	[KKL_TEXT] AS KKL_TEXT,
    	COALESCE([KTO_GILTIG_FOM], '1899-12-31 00:00:00') AS KTO_GILTIG_FOM,
    	COALESCE([KTO_GILTIG_TOM], '1899-12-31 00:00:00') AS KTO_GILTIG_TOM,
    	[KTO_ID] AS KTO_ID,
    	[KTO_ID_TEXT] AS KTO_ID_TEXT,
    	[KTO_PASSIV] AS KTO_PASSIV,
    	[KTO_TEXT] AS KTO_TEXT,
    	COALESCE([KTOG_GILTIG_FOM], '1899-12-31 00:00:00') AS KTOG_GILTIG_FOM,
    	COALESCE([KTOG_GILTIG_TOM], '1899-12-31 00:00:00') AS KTOG_GILTIG_TOM,
    	[KTOG_ID] AS KTOG_ID,
    	[KTOG_ID_TEXT] AS KTOG_ID_TEXT,
    	[KTOG_PASSIV] AS KTOG_PASSIV,
    	[KTOG_TEXT] AS KTOG_TEXT
    FROM [MediCarrierUDP].[utdata100].[EK_DIM_OBJ_KTO]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8090')