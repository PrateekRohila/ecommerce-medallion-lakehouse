"""Landing -> Bronze: land raw files as-is, add lineage, never drop a record."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from shopstream.jobs.common import JobContext
from shopstream.logging_utils import kv
from shopstream.schemas import CORRUPT_RECORD_COL, SOURCES

JOB = "bronze_ingest"


def _read_landing(ctx: JobContext, fmt: str, schema, path: str) -> DataFrame:
    reader = (ctx.spark.read.schema(schema)
              .option("mode", "PERMISSIVE")                       # bad rows kept, not dropped
              .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL))
    if fmt == "csv":
        reader = reader.option("header", "true").option("escape", '"')
    return reader.format(fmt).load(path)


def run(ctx: JobContext) -> None:
    for entity, (fmt, schema) in SOURCES.items():
        src = ctx.cfg.path("landing", entity, f"ingest_date={ctx.run_date}")
        if not ctx.store.path_exists(src):
            ctx.log.warning(kv(event="no_landing_data", entity=entity, path=src))
            ctx.audit(JOB, entity, "SKIPPED", details=f"no files at {src}")
            continue

        df = (_read_landing(ctx, fmt, schema, src)
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_run_id", F.lit(ctx.run_id))
              .withColumn("ingest_date", F.lit(ctx.run_date)))

        target = ctx.cfg.path("bronze", entity)
        ctx.store.overwrite_partitions(df, target, "ingest_date", [ctx.run_date])  # idempotent re-runs

        written = ctx.store.read(target).filter(F.col("ingest_date") == F.lit(ctx.run_date))
        rows = written.count()
        corrupt = written.filter(F.col(CORRUPT_RECORD_COL).isNotNull()).count()
        ctx.audit(JOB, entity, rows_read=rows, rows_written=rows,
                  details=f"unparseable_rows={corrupt}")
