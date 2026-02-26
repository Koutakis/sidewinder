from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_intlevfakt_motp",
    source_entity="RK_DIM_INTLEVFAKT_MOTP",
    table="rk_dim_intlevfakt_motp",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="INTKUNDID", data_type=PostgresType.TEXT),
        PostgresColumn(name="INTLEVID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KUND_PÅLOGG_FTG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KUNDFAKTNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="LEV_PÅLOGG_FTG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MOTPKOMB", data_type=PostgresType.TEXT),
        PostgresColumn(name="NR", data_type=PostgresType.NUMERIC),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[INTKUNDID] AS INTKUNDID,
    	[INTLEVID] AS INTLEVID,
    	[KUND_PÅLOGG_FTG] AS KUND_PÅLOGG_FTG,
    	[KUNDFAKTNR] AS KUNDFAKTNR,
    	[LEV_PÅLOGG_FTG] AS LEV_PÅLOGG_FTG,
    	[MOTPKOMB] AS MOTPKOMB,
    	[NR] AS NR
    FROM [utdata].[utdata150].[RK_DIM_INTLEVFAKT_MOTP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')