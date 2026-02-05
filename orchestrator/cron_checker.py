from datetime import datetime, timezone
from croniter import croniter
import psycopg
import os
import sys
from typing import Optional

class CronChecker:
    def __init__(self, model_name: str, schedule: str, state_dsn: str = None):
        self.model_name = model_name
        self.schedule = schedule
        self.state_dsn = state_dsn or os.environ['BIG_EKONOMI_EXECUTION_PROD']
        self._ensure_table()
    
    def _ensure_table(self):
        with psycopg.connect(self.state_dsn) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS model_state (
                    model_name TEXT PRIMARY KEY,
                    last_run TIMESTAMPTZ,
                    status TEXT,
                    rows_processed BIGINT,
                    execution_time_seconds NUMERIC,
                    error TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.commit()
    
    def check_and_start(self) -> None:
        last_run = self._get_last_run()
        now = datetime.now(timezone.utc)  # Make timezone-aware
        
        if last_run is None:
            print(f"✓ Starting {self.model_name} (first run)")
            return
        
        cron = croniter(self.schedule, last_run)
        next_run = cron.get_next(datetime)
        
        if now >= next_run:
            print(f"✓ Starting {self.model_name} (last ran {last_run})")
            return
        else:
            print(f"⏭  {self.model_name} not due yet (last ran {last_run}, next run {next_run})")
            sys.exit(0)
    
    def _get_last_run(self) -> Optional[datetime]:
        with psycopg.connect(self.state_dsn) as conn:
            result = conn.execute(
                "SELECT last_run FROM model_state WHERE model_name = %s",
                (self.model_name,)
            ).fetchone()
            return result[0] if result else None
    
    def update_success(self, rows: int, duration: float):
        now = datetime.now(timezone.utc)
        with psycopg.connect(self.state_dsn) as conn:
            conn.execute("""
                INSERT INTO model_state 
                (model_name, last_run, status, rows_processed, execution_time_seconds)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (model_name) DO UPDATE SET
                    last_run = EXCLUDED.last_run,
                    status = EXCLUDED.status,
                    rows_processed = EXCLUDED.rows_processed,
                    execution_time_seconds = EXCLUDED.execution_time_seconds,
                    updated_at = NOW()
            """, (self.model_name, now, 'success', rows, duration))
            conn.commit()
        
        print(f"✓ {self.model_name} completed: {rows:,} rows in {duration:.2f}s")
    
    def update_failure(self, error: str, duration: float):
        now = datetime.now(timezone.utc)
        with psycopg.connect(self.state_dsn) as conn:
            conn.execute("""
                INSERT INTO model_state 
                (model_name, last_run, status, error, execution_time_seconds)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (model_name) DO UPDATE SET
                    last_run = EXCLUDED.last_run,
                    status = EXCLUDED.status,
                    error = EXCLUDED.error,
                    execution_time_seconds = EXCLUDED.execution_time_seconds,
                    updated_at = NOW()
            """, (self.model_name, now, 'failed', error, duration))
            conn.commit()
        
        print(f"✗ {self.model_name} failed: {error}")
