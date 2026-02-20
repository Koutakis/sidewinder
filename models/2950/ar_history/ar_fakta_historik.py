from core import model, read, run_ingest, TableMode
import polars as pl


@model(
    name="ar_fakta_historik_2950",
    source_table="AR_FAKTA_HISTORIK",
    destination_table="ar_fakta_historik_2950",
    destination_schema="hq0x_sandbox",
    default_table_mode=TableMode.TRUNCATE_INSERT,
)
def execute(since=None, until=None) -> pl.DataFrame:
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ANLAGGNING] AS ANLAGGNING,
    	[ANLTYP_ID] AS ANLTYP_ID,
    	[ANSK] AS ANSK,
    	[BERAKNBELOPP] AS BERAKNBELOPP,
    	[HANDELSE] AS HANDELSE,
    	[KORRTYP] AS KORRTYP,
    	[KST_ID] AS KST_ID,
    	[PLAC2_ID] AS PLAC2_ID,
    	[PLAC_ID] AS PLAC_ID,
    	[PLAVSKR] AS PLAVSKR,
    	[PROJ_ID] AS PROJ_ID,
    	[REG_DATUM_TID] AS REG_DATUM_TID,
    	[REGSIGN] AS REGSIGN,
    	[UTILITY] AS UTILITY,
    	COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
    	[VERNR] AS VERNR
    FROM [utdata].[utdata295].[AR_FAKTA_HISTORIK] ) y
    WHERE 1=1"""
    return read("RAINDANCE_2950", query)


run_ingest(
    execute,
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    table_mode=TableMode.TRUNCATE_INSERT,
)
