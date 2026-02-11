from orchestrator import run_ingest, read_query, TableMode, CronChecker

MODEL_NAME = "rk_dim_levfakt_koppl"
SCHEDULE = "0 2 * * *"


def execute(start=None, end=None):
    query = "SELECT * FROM [udpb4].[udpb4_100].[RK_DIM_LEVFAKT_KOPPL]"
    if start and end:
        query += f" WHERE date_column >= '{start}' AND date_column < '{end}'"
    return read_query("RAINDANCE_1100", query)


checker = CronChecker(MODEL_NAME, SCHEDULE)
checker.check_and_start()

run_ingest(
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    dest_table="hq0x_sandbox.rk_dim_levfakt_koppl",
    execute=execute,
    table_mode=TableMode.FULL,
    cron_checker=checker,
)
