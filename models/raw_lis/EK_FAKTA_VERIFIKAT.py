from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_fakta_verifikat",
    source_entity="EK_FAKTA_VERIFIKAT",
    table="ek_fakta_verifikat",
    schema="raindance_raw_8410",
    write_mode=WriteMode.MERGE,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTDATUM1", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTDATUM2", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTSIGN1", data_type=PostgresType.TEXT),
        PostgresColumn(name="ATTESTSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="DEFDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEFSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="DOK_ANTAL", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DOKTYP", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DOKUMENTID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNANM", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNNR", data_type=PostgresType.TEXT),
        PostgresColumn(name="FORETAG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="FRI_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="HUVUDTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="IB", data_type=PostgresType.TEXT),
        PostgresColumn(name="INTERNVERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KATEGORI", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KONTSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MED", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PNYCKEL", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADTYPNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="REGDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="REGSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="STATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="URS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPRUNGS_VERIFIKAT", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UTF_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="UTILITY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VERDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERDOKREF", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VERRAD", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VERTYP", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['lis', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    if env.backfill and env.backfill.enabled:
        since = env.backfill.since.strftime("%Y-%m-%d")
        until = env.backfill.until.strftime("%Y-%m-%d")
    elif env.cron and env.cron.enabled:
        since = env.cron.since.strftime("%Y-%m-%d")
        until = env.cron.until.strftime("%Y-%m-%d")
    else:
        since = None
        until = None
    query=f"""SELECT * FROM (SELECT
    	CAST(VERDATUM AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ATTESTDATUM1], '1899-12-31 00:00:00') AS ATTESTDATUM1,
    	COALESCE([ATTESTDATUM2], '1899-12-31 00:00:00') AS ATTESTDATUM2,
    	[ATTESTSIGN1] AS ATTESTSIGN1,
    	[ATTESTSIGN2] AS ATTESTSIGN2,
    	COALESCE([DEFDATUM], '1899-12-31 00:00:00') AS DEFDATUM,
    	[DEFSIGN] AS DEFSIGN,
    	[DOK_ANTAL] AS DOK_ANTAL,
    	[DOKTYP] AS DOKTYP,
    	[DOKUMENTID] AS DOKUMENTID,
    	[EXTERNANM] AS EXTERNANM,
    	[EXTERNID] AS EXTERNID,
    	[EXTERNNR] AS EXTERNNR,
    	[FORETAG] AS FORETAG,
    	[FRI_ID] AS FRI_ID,
    	[HUVUDTEXT] AS HUVUDTEXT,
    	[IB] AS IB,
    	[INTERNVERNR] AS INTERNVERNR,
    	[KATEGORI] AS KATEGORI,
    	[KONTSIGN] AS KONTSIGN,
    	[KST_ID] AS KST_ID,
    	[KTO_ID] AS KTO_ID,
    	[MED] AS MED,
    	[MOTP_ID] AS MOTP_ID,
    	[PNYCKEL] AS PNYCKEL,
    	[PROJ_ID] AS PROJ_ID,
    	[RADTEXT] AS RADTEXT,
    	[RADTYPNR] AS RADTYPNR,
    	COALESCE([REGDATUM], '1899-12-31 00:00:00') AS REGDATUM,
    	[REGSIGN] AS REGSIGN,
    	[STATUS] AS STATUS,
    	[URS_ID] AS URS_ID,
    	[URSPRUNGS_VERIFIKAT] AS URSPRUNGS_VERIFIKAT,
    	[URSPTEXT] AS URSPTEXT,
    	[UTF_V] AS UTF_V,
    	[UTILITY] AS UTILITY,
    	COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
    	[VERDOKREF] AS VERDOKREF,
    	[VERNR] AS VERNR,
    	[VERRAD] AS VERRAD,
    	[VERTYP] AS VERTYP
    FROM [utdata].[utdata840].[EK_FAKTA_VERIFIKAT]) y
    WHERE _data_modified BETWEEN '{since}' AND '{until}'"""
    yield from read(query=query, env_var_name='RAINDANCE_8410')