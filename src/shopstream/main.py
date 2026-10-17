"""Pipeline entry point.

    python -m shopstream.main --env dev --job all --run-date 2026-10-15

Exit code 0 = success, 1 = failure (so Airflow / ADF / Databricks Workflows mark the task failed).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import date, datetime, timezone

from shopstream.config import load_config
from shopstream.jobs import bronze_ingest, gold_aggregate, silver_transform
from shopstream.jobs.common import JobContext
from shopstream.logging_utils import get_logger, kv
from shopstream.spark_session import build_spark
from shopstream.storage import LakeStore

JOBS = {
    "bronze": bronze_ingest.run,
    "silver": silver_transform.run,
    "gold": gold_aggregate.run,
}
log = get_logger("shopstream")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ShopStream medallion lakehouse pipeline")
    p.add_argument("--job", choices=[*JOBS, "all"], default="all")
    p.add_argument("--env", default="dev")
    p.add_argument("--config", default="conf/pipeline.yaml")
    p.add_argument("--run-date", type=date.fromisoformat,
                   default=datetime.now(timezone.utc).date(), help="Logical date (YYYY-MM-DD)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config, args.env)
    run_id = f"{args.run_date:%Y%m%d}-{uuid.uuid4().hex[:8]}"
    spark = build_spark(cfg)
    ctx = JobContext(spark=spark, cfg=cfg, store=LakeStore(spark, cfg.storage_format),
                     run_date=args.run_date, run_id=run_id)

    selected = list(JOBS) if args.job == "all" else [args.job]
    log.info(kv(event="pipeline_start", run_id=run_id, run_date=args.run_date, env=cfg.env,
                format=cfg.storage_format, jobs=",".join(selected)))
    try:
        for name in selected:
            started = time.perf_counter()
            JOBS[name](ctx)
            log.info(kv(event="job_complete", job=name, seconds=round(time.perf_counter() - started, 1)))
    except Exception:
        log.exception(kv(event="pipeline_failed", run_id=run_id))
        return 1
    finally:
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:  # never stop a shared cluster session
            spark.stop()
    log.info(kv(event="pipeline_success", run_id=run_id))
    return 0


if __name__ == "__main__":
    sys.exit(main())
