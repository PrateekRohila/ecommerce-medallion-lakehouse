"""Table I/O layer.

All reads/writes go through `LakeStore`, so jobs never care which table format is
underneath:

* delta   -> ACID MERGE upserts, `replaceWhere` partition overwrites (production)
* parquet -> same semantics emulated with staging + atomic directory swap
             (local runs and unit tests without the Delta jars)

Every write is idempotent: re-running a job for the same run date produces the
same tables, which is what makes backfills and retries safe.
"""
from __future__ import annotations

import uuid
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from shopstream.logging_utils import get_logger, kv

log = get_logger(__name__)


class LakeStore:
    def __init__(self, spark: SparkSession, storage_format: str):
        if storage_format not in ("delta", "parquet"):
            raise ValueError(f"Unsupported storage format: {storage_format}")
        self.spark = spark
        self.fmt = storage_format

    # ------------------------------------------------------------------ reads
    def path_exists(self, path: str) -> bool:
        """Raw file-system check (works for landing folders on local, ADLS, S3, GCS)."""
        fs, hpath = self._fs(path)
        return fs.exists(hpath)

    def exists(self, path: str) -> bool:
        """True when a table (Delta or Parquet) has been created at `path`."""
        fs, hpath = self._fs(path)
        if self.fmt == "delta":
            return fs.exists(self._hpath(f"{path.rstrip('/')}/_delta_log"))
        return fs.exists(hpath)

    def read(self, path: str) -> DataFrame:
        return self.spark.read.format(self.fmt).load(path)

    # ----------------------------------------------------------------- writes
    def append(self, df: DataFrame, path: str) -> None:
        df.write.format(self.fmt).mode("append").option("mergeSchema", "true").save(path)

    def overwrite(self, df: DataFrame, path: str, partition_by: Sequence[str] = ()) -> None:
        """Replace the whole table. Safe even when `df` was derived from `path` itself."""
        if self.fmt == "delta":
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(path)
        else:
            self._swap_write(df, path, partition_by)
        log.info(kv(event="table_overwritten", path=path))

    def overwrite_partitions(self, df: DataFrame, path: str, partition_col: str, values: Iterable) -> None:
        """Replace only the given partitions, leaving the rest of the table untouched."""
        values = sorted({str(v) for v in values})
        if not values:
            return
        df = df.filter(F.col(partition_col).cast("string").isin(values))
        if self.fmt == "delta":
            writer = df.write.format("delta").partitionBy(partition_col)
            if self.exists(path):
                predicate = f"{partition_col} IN ({', '.join(repr(v) for v in values)})"
                writer.mode("overwrite").option("replaceWhere", predicate).save(path)
            else:
                writer.mode("errorifexists").save(path)
        else:
            (df.write.format("parquet").mode("overwrite").partitionBy(partition_col)
               .option("partitionOverwriteMode", "dynamic").save(path))
        log.info(kv(event="partitions_overwritten", path=path, column=partition_col, partitions=",".join(values)))

    def upsert(self, df: DataFrame, path: str, keys: Sequence[str], version_col: str) -> None:
        """Insert new keys; update existing keys only when the incoming version is newer or equal.

        `>=` (not `>`) keeps re-runs idempotent, while strictly older records arriving
        late (out-of-order CDC) never overwrite fresher data.
        """
        if not self.exists(path):
            self.overwrite(df, path)
            return

        if self.fmt == "delta":
            from delta.tables import DeltaTable

            on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
            (DeltaTable.forPath(self.spark, path).alias("t")
                .merge(df.alias("s"), on)
                .whenMatchedUpdateAll(condition=f"s.`{version_col}` >= t.`{version_col}`")
                .whenNotMatchedInsertAll()
                .execute())
        else:
            existing = self.read(path).withColumn("_src_priority", F.lit(0))
            incoming = df.withColumn("_src_priority", F.lit(1))
            w = Window.partitionBy(*keys).orderBy(F.col(version_col).desc(), F.col("_src_priority").desc())
            merged = (existing.unionByName(incoming, allowMissingColumns=True)
                      .withColumn("_rn", F.row_number().over(w))
                      .filter("_rn = 1")
                      .drop("_rn", "_src_priority"))
            self._swap_write(merged, path, ())
        log.info(kv(event="table_upserted", path=path, keys=",".join(keys)))

    # ---------------------------------------------------------------- helpers
    def _swap_write(self, df: DataFrame, path: str, partition_by: Sequence[str]) -> None:
        """Write to a staging dir, then swap it in (avoids reading and overwriting the same files)."""
        staging = f"{path.rstrip('/')}__staging_{uuid.uuid4().hex[:8]}"
        writer = df.write.format("parquet").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(staging)
        fs, target = self._fs(path)
        if fs.exists(target):
            fs.delete(target, True)
        if not fs.rename(self._hpath(staging), target):
            raise IOError(f"Could not move {staging} -> {path}")

    def _hpath(self, path: str):
        return self.spark._jvm.org.apache.hadoop.fs.Path(path)

    def _fs(self, path: str):
        hpath = self._hpath(path)
        return hpath.getFileSystem(self.spark._jsc.hadoopConfiguration()), hpath
