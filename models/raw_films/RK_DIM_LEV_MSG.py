from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_lev_msg",
    source_entity="RK_DIM_LEV_MSG",
    table="rk_dim_lev_msg",
    schema="raindance_raw_8010",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BIT_PAF", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="ENVELOPE_TRS", data_type=PostgresType.TEXT),
        PostgresColumn(name="FORMATV_RDF", data_type=PostgresType.TEXT),
        PostgresColumn(name="FREEVALUE", data_type=PostgresType.TEXT),
        PostgresColumn(name="LOGICALVALUE", data_type=PostgresType.TEXT),
        PostgresColumn(name="MEDIA_TRS", data_type=PostgresType.TEXT),
        PostgresColumn(name="MSGKEY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MSGTYPEV", data_type=PostgresType.TEXT),
        PostgresColumn(name="MSGWAY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="PART", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="SBID", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['films', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[BIT_PAF] AS BIT_PAF,
    	[ENVELOPE_TRS] AS ENVELOPE_TRS,
    	[FORMATV_RDF] AS FORMATV_RDF,
    	[FREEVALUE] AS FREEVALUE,
    	[LOGICALVALUE] AS LOGICALVALUE,
    	[MEDIA_TRS] AS MEDIA_TRS,
    	[MSGKEY] AS MSGKEY,
    	[MSGTYPEV] AS MSGTYPEV,
    	[MSGWAY] AS MSGWAY,
    	[PART] AS PART,
    	[SBID] AS SBID
    FROM [utdata].[utdata801].[RK_DIM_LEV_MSG]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8010')