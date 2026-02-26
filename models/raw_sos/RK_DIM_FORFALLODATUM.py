from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_forfallodatum",
    source_entity="RK_DIM_FORFALLODATUM",
    table="rk_dim_forfallodatum",
    schema="raindance_raw_8570",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="AR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKFORINGSAR", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BOKFORINGSAR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKFORINGSARSLUT", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DAG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DAG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DATUM6_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DATUM6B_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DATUM8_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FORFALLODATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FORFALLODATUM_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KVARTAL", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KVARTAL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KVARTALNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KVARTALNR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MANAD", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MANAD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MANADNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MANADNR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MANADSNAMN", data_type=PostgresType.TEXT),
        PostgresColumn(name="PERIOD", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PERIOD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PERIODSLUT", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PERIODSTATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="PERIODSTATUS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TERTIAL", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="TERTIAL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TERTIALNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="TERTIALNR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VECKA", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VECKA_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VECKO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VECKODAG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VECKODAG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VECKONR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VECKONR_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[AR] AS AR,
	[AR_TEXT] AS AR_TEXT,
	COALESCE([BOKFORINGSAR], '1899-12-31 00:00:00') AS BOKFORINGSAR,
	[BOKFORINGSAR_TEXT] AS BOKFORINGSAR_TEXT,
	COALESCE([BOKFORINGSARSLUT], '1899-12-31 00:00:00') AS BOKFORINGSARSLUT,
	[DAG] AS DAG,
	[DAG_TEXT] AS DAG_TEXT,
	[DATUM6_TEXT] AS DATUM6_TEXT,
	[DATUM6B_TEXT] AS DATUM6B_TEXT,
	[DATUM8_TEXT] AS DATUM8_TEXT,
	COALESCE([FORFALLODATUM], '1899-12-31 00:00:00') AS FORFALLODATUM,
	[FORFALLODATUM_TEXT] AS FORFALLODATUM_TEXT,
	[KVARTAL] AS KVARTAL,
	[KVARTAL_TEXT] AS KVARTAL_TEXT,
	[KVARTALNR] AS KVARTALNR,
	[KVARTALNR_TEXT] AS KVARTALNR_TEXT,
	[MANAD] AS MANAD,
	[MANAD_TEXT] AS MANAD_TEXT,
	[MANADNR] AS MANADNR,
	[MANADNR_TEXT] AS MANADNR_TEXT,
	[MANADSNAMN] AS MANADSNAMN,
	COALESCE([PERIOD], '1899-12-31 00:00:00') AS PERIOD,
	[PERIOD_TEXT] AS PERIOD_TEXT,
	COALESCE([PERIODSLUT], '1899-12-31 00:00:00') AS PERIODSLUT,
	[PERIODSTATUS] AS PERIODSTATUS,
	[PERIODSTATUS_TEXT] AS PERIODSTATUS_TEXT,
	[TERTIAL] AS TERTIAL,
	[TERTIAL_TEXT] AS TERTIAL_TEXT,
	[TERTIALNR] AS TERTIALNR,
	[TERTIALNR_TEXT] AS TERTIALNR_TEXT,
	[VECKA] AS VECKA,
	[VECKA_TEXT] AS VECKA_TEXT,
	[VECKO_TEXT] AS VECKO_TEXT,
	[VECKODAG] AS VECKODAG,
	[VECKODAG_TEXT] AS VECKODAG_TEXT,
	[VECKONR] AS VECKONR,
	[VECKONR_TEXT] AS VECKONR_TEXT
    FROM [raindance_udp].[udp_220].[RK_DIM_FORFALLODATUM]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8570", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")