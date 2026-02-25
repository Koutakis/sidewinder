from bollhav import Model, WriteMode
from roskarl import DSN


def get_max_date(cfg: Model, dest_dsn: DSN) -> str | None:
    from config.connections import get_postgres_connection

    conn = get_postgres_connection(dest_dsn)
    schema = cfg.schema
    table = cfg.table

    try:
        with conn:
            result = conn.execute(
                f'SELECT MAX("_data_modified")::text FROM {schema}.{table}'
            ).fetchone()
        return result[0] if result and result[0] else None
    except Exception:
        return None


def run(cfg: Model, fn, env, dest_dsn: DSN) -> None:
    from core.write import write

    if not dest_dsn:
        raise ValueError(f"{cfg.name}: dest_dsn must be set")

    since = None
    until = None

    if env.backfill and env.backfill.enabled:
        since = env.backfill.since.strftime("%Y-%m-%d")
        until = env.backfill.until.strftime("%Y-%m-%d")
    elif env.cron and env.cron.enabled:
        since = env.cron.since.strftime("%Y-%m-%d")
        until = env.cron.until.strftime("%Y-%m-%d")

    if cfg.write_mode == WriteMode.MERGE and not since:
        since = get_max_date(cfg, dest_dsn)

    total_rows = 0
    first_batch = True
    original_mode = cfg.write_mode

    for df in fn(env, cfg):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn, since=since, until=until)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)

    cfg.write_mode = original_mode

    if total_rows == 0:
        print(f"  ⏭ {cfg.name}: no data, skipping")
    else:
        print(f"  ✓ {cfg.name}: {total_rows:,} rows written")
