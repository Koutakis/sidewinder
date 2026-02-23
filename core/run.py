from core.model import ModelConfig, TableMode


def run(cfg: ModelConfig, fn, env) -> None:
    from core.write import write

    if cfg.table_mode == TableMode.MERGE and not (env.cron or env.backfill):
        raise ValueError(f"{cfg.name}: MERGE mode requires cron or backfill config")

    if not cfg.dest_env:
        raise ValueError(f"{cfg.name}: dest_env must be set")

    since = None
    until = None
    if env.cron and env.cron.since:
        since = str(env.cron.since)
        until = str(env.cron.until)
    elif env.backfill and env.backfill.since:
        since = str(env.backfill.since)
        until = str(env.backfill.until) if env.backfill.until else None

    total_rows = 0
    first_batch = True
    original_mode = cfg.table_mode

    for df in fn(env, cfg):
        if len(df) == 0:
            continue
        write(cfg, df, since=since, until=until)
        if first_batch:
            cfg.table_mode = TableMode.APPEND
            first_batch = False
        total_rows += len(df)

    cfg.table_mode = original_mode

    if total_rows == 0:
        print(f"  ⏭ {cfg.name}: no data, skipping")
    else:
        print(f"  ✓ {cfg.name}: {total_rows:,} rows written")
