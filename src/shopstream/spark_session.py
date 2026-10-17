"""SparkSession factory.

* On Databricks / Synapse / Fabric an active session already exists -> it is reused.
* With spark-submit, pass Delta via `--packages io.delta:delta-spark_2.13:<ver>`.
* For local runs with storage_format=delta, the Delta jars are resolved from pip's
  `delta-spark` package automatically.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from shopstream.config import PipelineConfig

_DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def build_spark(cfg: PipelineConfig) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:  # managed platform (e.g. Databricks)
        for key, value in cfg.spark_conf.items():
            _try_set(active, key, value)
        return active

    builder = SparkSession.builder.appName(f"{cfg.app_name}-{cfg.env}")
    if cfg.spark_master:
        builder = builder.master(cfg.spark_master)
    for key, value in cfg.spark_conf.items():
        builder = builder.config(key, value)

    if cfg.is_delta:
        for key, value in _DELTA_CONF.items():
            builder = builder.config(key, value)
        if cfg.spark_master and cfg.spark_master.startswith("local"):
            from delta import configure_spark_with_delta_pip  # local-only dependency

            builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _try_set(spark: SparkSession, key: str, value: str) -> None:
    try:
        spark.conf.set(key, value)
    except Exception:  # some confs are static on managed clusters; ignore them
        pass
