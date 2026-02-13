from core import run_ingest, read, TableMode

def execute(start=None, end=None): 
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ANLTYP_ID] AS ANLTYP_ID,
    	[BELOPPSTYP] AS BELOPPSTYP,
    	[FLY] AS FLY,
    	[FORS] AS FORS,
    	[FRI1_ID] AS FRI1_ID,
    	[IB] AS IB,
    	[INV] AS INV,
    	[KOR] AS KOR,
    	[KST_ID] AS KST_ID,
    	[OMF] AS OMF,
    	[PAV] AS PAV,
    	COALESCE([PERIOD], '1899-12-31 00:00:00') AS PERIOD,
    	[PLAC_ID] AS PLAC_ID,
    	[PROJ_ID] AS PROJ_ID,
    	[STR] AS STR,
    	[UB] AS UB,
    	[UTF] AS UTF,
    	[UTILITY] AS UTILITY,
    	[UTR] AS UTR
    FROM [utdata].[utdata298].[AR_FAKTA_HISTTOT] ) y
    WHERE 1=1"""

    return read("RAINDANCE_2985", query)


run_ingest(
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    dest_table="hq0x_sandbox.ar_fakta_histtot", 
    execute=execute,
    table_mode=TableMode.FULL,
    schedule="0 6 * * *",
    start="2024-01-01",
    end="2026-02-12",
    force=True,
)



