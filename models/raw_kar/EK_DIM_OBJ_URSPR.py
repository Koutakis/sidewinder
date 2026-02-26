from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_urspr",
    source_entity="EK_DIM_OBJ_URSPR",
    table="ek_dim_obj_urspr",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="URSPR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="URSPR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="URSPR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="URSPR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VTAM_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VTAM_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VTAM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VTAM_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VTAM_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VTAM_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ÖVTYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ÖVTYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ÖVTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ÖVTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ÖVTYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ÖVTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([URSPR_GILTIG_FOM], '1899-12-31 00:00:00') AS URSPR_GILTIG_FOM,
    	COALESCE([URSPR_GILTIG_TOM], '1899-12-31 00:00:00') AS URSPR_GILTIG_TOM,
    	[URSPR_ID] AS URSPR_ID,
    	[URSPR_ID_TEXT] AS URSPR_ID_TEXT,
    	[URSPR_PASSIV] AS URSPR_PASSIV,
    	[URSPR_TEXT] AS URSPR_TEXT,
    	COALESCE([VTAM_GILTIG_FOM], '1899-12-31 00:00:00') AS VTAM_GILTIG_FOM,
    	COALESCE([VTAM_GILTIG_TOM], '1899-12-31 00:00:00') AS VTAM_GILTIG_TOM,
    	[VTAM_ID] AS VTAM_ID,
    	[VTAM_ID_TEXT] AS VTAM_ID_TEXT,
    	[VTAM_PASSIV] AS VTAM_PASSIV,
    	[VTAM_TEXT] AS VTAM_TEXT,
    	COALESCE([ÖVTYP_GILTIG_FOM], '1899-12-31 00:00:00') AS ÖVTYP_GILTIG_FOM,
    	COALESCE([ÖVTYP_GILTIG_TOM], '1899-12-31 00:00:00') AS ÖVTYP_GILTIG_TOM,
    	[ÖVTYP_ID] AS ÖVTYP_ID,
    	[ÖVTYP_ID_TEXT] AS ÖVTYP_ID_TEXT,
    	[ÖVTYP_PASSIV] AS ÖVTYP_PASSIV,
    	[ÖVTYP_TEXT] AS ÖVTYP_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_URSPR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')