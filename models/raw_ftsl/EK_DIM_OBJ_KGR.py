from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_kgr",
    source_entity="EK_DIM_OBJ_KGR",
    table="ek_dim_obj_kgr",
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
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
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ftsl', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
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
    	[KKL_TEXT] AS KKL_TEXT
    FROM [ftvudp].[ftv_400].[EK_DIM_OBJ_KGR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8810')