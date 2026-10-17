"""Shared job context and the run-audit table (operational observability)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DoubleType, LongType, StringType, StructField, StructType,
                               TimestampType)

from shopstream.config import PipelineConfig
from shopstream.logging_utils import get_logger, kv
from shopstream.storage import LakeStore

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("run_date", DateType()),
    StructField("job", StringType()),
    StructField("entity", StringType()),
    StructField("status", StringType()),
    StructField("rows_read", LongType()),
    StructField("rows_written", LongType()),
    StructField("rows_rejected", LongType()),
    StructField("reject_ratio", DoubleType()),
    StructField("details", StringType()),
    StructField("logged_at", TimestampType()),
])


@dataclass
class JobContext:
    spark: SparkSession
    cfg: PipelineConfig
    store: LakeStore
    run_date: date
    run_id: str

    @property
    def log(self):
        return get_logger("shopstream")

    def audit(self, job: str, entity: str, status: str = "SUCCESS", *, rows_read: int = 0,
              rows_written: int = 0, rows_rejected: int = 0, details: str = "") -> None:
        ratio = (rows_rejected / rows_read) if rows_read else 0.0
        row = (self.run_id, self.run_date, job, entity, status, rows_read, rows_written,
               rows_rejected, round(ratio, 4), details, datetime.now(timezone.utc).replace(tzinfo=None))
        self.store.append(self.spark.createDataFrame([row], AUDIT_SCHEMA), self.cfg.path("audit", "job_runs"))
        self.log.info(kv(event="audit", job=job, entity=entity, status=status, rows_read=rows_read,
                         rows_written=rows_written, rows_rejected=rows_rejected))
