from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_externid",
    source_entity="EK_DIM_EXTERNID",
    table="ek_dim_externid",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DELSYS", data_type=PostgresType.TEXT),
        PostgresColumn(name="DELSYS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DOKUMENTTYP", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="EXTERNID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID2", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID_GRUPP", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="NAMN2", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DELSYS] AS DELSYS,
    	[DELSYS_TEXT] AS DELSYS_TEXT,
    	[DOKUMENTTYP] AS DOKUMENTTYP,
    	[EXTERNID] AS EXTERNID,
    	[EXTERNID2] AS EXTERNID2,
    	[EXTERNID2_ID_TEXT] AS EXTERNID2_ID_TEXT,
    	[EXTERNID_GRUPP] AS EXTERNID_GRUPP,
    	[EXTERNID_ID_TEXT] AS EXTERNID_ID_TEXT,
    	[EXTERNID_TEXT] AS EXTERNID_TEXT,
    	[NAMN2] AS NAMN2
    FROM [raindance_udp].[udp_150].[EK_DIM_EXTERNID]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')