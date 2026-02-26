from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_lev_attr",
    source_entity="RK_DIM_LEV_ATTR",
    table="rk_dim_lev_attr",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTR_KEY_PAT", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="ATTRIBUTE", data_type=PostgresType.TEXT),
        PostgresColumn(name="SBID", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ATTR_KEY_PAT] AS ATTR_KEY_PAT,
    	[ATTRIBUTE] AS ATTRIBUTE,
    	[SBID] AS SBID
    FROM [utdata].[utdata150].[RK_DIM_LEV_ATTR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')