from core import ModelConfig, TableMode, read


config = ModelConfig(
    name="ar_fakta_histtot_2950",
    source_table="AR_FAKTA_HISTTOT",
    source_env="RAINDANCE_2950",
    destination_table="ar_fakta_histtot_2950",
    destination_schema="hq0x_sandbox",
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    table_mode=TableMode.TRUNCATE_INSERT,
)


def execute(env, cfg=config):
    query = """
    SELECT
        CAST(GETDATE() AS DATE) as _data_modified,
        CAST(GETDATE() AS DATETIME2) as _metadata_modified,
        [ANLTYP_ID] AS ANLTYP_ID,
        [BELOPPSTYP] AS BELOPPSTYP,
        [FLY] AS FLY,
        [FORS] AS FORS,
        [IB] AS IB,
        [INV] AS INV,
        [KOR] AS KOR,
        [KST_ID] AS KST_ID,
        [OMF] AS OMF,
        [PAV] AS PAV,
        COALESCE([PERIOD], '1899-12-31 00:00:00') AS PERIOD,
        [PLAC2_ID] AS PLAC2_ID,
        [PLAC_ID] AS PLAC_ID,
        [PROJ_ID] AS PROJ_ID,
        [STR] AS STR,
        [UB] AS UB,
        [UTF] AS UTF,
        [UTILITY] AS UTILITY,
        [UTR] AS UTR
    FROM [utdata].[utdata295].[AR_FAKTA_HISTTOT]
    """
    yield from read(cfg.source_env, query)
