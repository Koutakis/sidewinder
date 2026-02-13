from core import run_ingest, read, TableMode


def execute(start=None, end=None): 
    query=f"""SELECT * FROM (SELECT 
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ANLAGGNING] AS ANLAGGNING,
    	[ANLTYP_ID] AS ANLTYP_ID,
    	[ANSK] AS ANSK,
    	[BERAKNBELOPP] AS BERAKNBELOPP,
    	[FRI1_ID] AS FRI1_ID,
    	[HANDELSE] AS HANDELSE,
    	[KORRTYP] AS KORRTYP,
    	[KST_ID] AS KST_ID,
    	[PLAC_ID] AS PLAC_ID,
    	[PLAVSKR] AS PLAVSKR,
    	[PROJ_ID] AS PROJ_ID,
    	[REG_DATUM_TID] AS REG_DATUM_TID,
    	[REGSIGN] AS REGSIGN,
    	[UTILITY] AS UTILITY,
    	COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
    	[VERNR] AS VERNR
    FROM [utdata].[utdata298].[AR_FAKTA_HISTORIK] ) y
    WHERE 1=1"""

    return read("RAINDANCE_2985", query)


run_ingest(
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    dest_table="hq0x_sandbox.ar_fakta_historik", 
    execute=execute,
    table_mode=TableMode.FULL,
    schedule="0 6 * * *",
    start="2024-01-01",
    end="2026-02-12",
    force=True,
)

