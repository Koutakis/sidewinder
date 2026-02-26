from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_motp_enh_60",
    source_entity="EK_DIM_OBJ_MOTP_ENH_60",
    table="ek_dim_obj_motp_enh_60",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTFRA_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTFRA_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTFRA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTFRA_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTFRA_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTFRA_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([MOTFRA_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTFRA_GILTIG_FOM,
    	COALESCE([MOTFRA_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTFRA_GILTIG_TOM,
    	[MOTFRA_ID] AS MOTFRA_ID,
    	[MOTFRA_ID_TEXT] AS MOTFRA_ID_TEXT,
    	[MOTFRA_PASSIV] AS MOTFRA_PASSIV,
    	[MOTFRA_TEXT] AS MOTFRA_TEXT,
    	COALESCE([MOTP_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_FOM,
    	COALESCE([MOTP_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_TOM,
    	[MOTP_ID] AS MOTP_ID,
    	[MOTP_ID_TEXT] AS MOTP_ID_TEXT,
    	[MOTP_PASSIV] AS MOTP_PASSIV,
    	[MOTP_TEXT] AS MOTP_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_MOTP_ENH_60]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')