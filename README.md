# E-commerce Medallion Lakehouse (PySpark + Delta Lake)

A production-style **medallion lakehouse pipeline** (Bronze → Silver → Gold) for an
e-commerce company, built on **Apache Spark / PySpark** and **Delta Lake**.

## The business problem

ShopStream receives three daily feeds in its landing zone:

| Feed      | Format | Source system | What's wrong with it in real life |
|-----------|--------|---------------|-----------------------------------|
| orders    | JSON   | order service | duplicates from retries, malformed lines, status updates arriving out of order, late orders for past days |
| customers | CSV    | CRM           | attributes change over time (tier, city), changes arrive late, no-op "changes", bad emails |
| products  | CSV    | catalogue     | occasional bad prices |

Finance and analytics need **trustworthy** daily revenue, cancellation and customer-value
numbers. Naively loading the files gives double-counted revenue (duplicates), wrong statuses
(stale updates overwriting newer ones), and wrong history ("what tier was this customer
*when they ordered*?"). This pipeline solves all of that.

## Architecture

```
landing/ (raw files)                     _audit/job_runs  ← one row per job/entity/run
   │                                            ▲
   ▼                                            │
BRONZE  raw, every column STRING, never drops a row, lineage cols, partitioned by ingest_date
   │
   ▼  type with try_cast → DQ rules → quarantine/ (rows + failed rule names)
SILVER  clean, typed, de-duplicated, upserted (newest version wins, stale updates ignored)
   │       orders · customer_changes · products
   ▼
GOLD    dim_customer (SCD Type 2, late-arrival safe)
        fact_orders  (point-in-time join to the customer version valid at order time)
        daily_sales_kpis  ·  customer_lifetime_metrics
```

## Industry practices implemented

| Practice | Where |
|---|---|
| Config-driven, multi-environment (local/dev/prod) with env-var overrides | `conf/pipeline.yaml`, `config.py` |
| Explicit schemas, PERMISSIVE parsing, `_corrupt_record` capture | `schemas.py`, `jobs/bronze_ingest.py` |
| Lineage columns (`_source_file`, `_ingest_ts`, `_run_id`) | bronze |
| Declarative data-quality rules + quarantine with reasons | `quality.py`, `transformations/silver.py` |
| Circuit breaker (fail run if reject ratio > threshold) | `quality.enforce_reject_threshold` |
| ANSI-safe casting (`try_cast`) — bad values never crash the job | `transformations/silver.py` |
| Window-function de-duplication | `dedupe_latest` |
| Delta `MERGE` upserts with out-of-order protection | `storage.LakeStore.upsert` |
| Idempotent writes (`replaceWhere` partition overwrite) — safe retries & backfills | `storage.py` |
| SCD Type 2 that handles **late-arriving** changes and collapses no-op changes | `transformations/scd2.py` |
| As-of (point-in-time) join, unknown-member handling (`customer_sk = -1`) | `transformations/gold.py` |
| Broadcast joins for small dimensions | gold |
| Incremental gold: only affected `order_date` partitions are rebuilt | `jobs/gold_aggregate.py` |
| Run-audit table for observability | `jobs/common.py` |
| Structured `key=value` logging, non-zero exit code on failure | `logging_utils.py`, `main.py` |
| Pure transformation functions (no I/O) → fully unit-tested | `tests/` |
| Orchestration with retries, backfill via logical date | `orchestration/airflow_dag.py` |

## Project layout

```
conf/pipeline.yaml                 configuration per environment
src/shopstream/
  main.py                          CLI entry point
  config.py  spark_session.py  storage.py  quality.py  schemas.py  logging_utils.py
  transformations/  silver.py  scd2.py  gold.py      ← business logic (pure)
  jobs/  bronze_ingest.py  silver_transform.py  gold_aggregate.py  common.py   ← I/O + orchestration
scripts/generate_sample_data.py    3 days of deliberately messy data
tests/                             pytest suite (12 tests)
orchestration/airflow_dag.py
```

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # needs Java 17+ (Spark 4) or 8/11/17 (Spark 3.5)

make test                                # unit tests
make run-delta                           # 3 days on real Delta Lake (downloads Delta jars once)
make run-local                           # same, Parquet fallback (offline / no Delta jars)
```

Single job / single day:

```bash
PYTHONPATH=src python -m shopstream.main --env dev --job silver --run-date 2026-10-15
```

**spark-submit:**

```bash
spark-submit --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --py-files dist/shopstream.zip src/shopstream/main.py --env prod --run-date 2026-10-15
```

**Databricks:** package as a wheel (`pip wheel .`), create a Workflow with three Python-wheel
tasks (`shopstream --job bronze|silver|gold --env prod --run-date {{job.start_time.iso_date}}`).
The existing Spark session is reused and Delta is built in.

## What the sample data proves (verified end-to-end)

* 5 bad order rows and 1 bad customer row quarantined with exact reasons; good rows flow on.
* 3 duplicate order lines removed.
* Order `O0010`: a stale `PLACED` update arriving after `SHIPPED` is **ignored**.
* Order `O0051` placed on 13 Oct arrives on 15 Oct → the 13 Oct partition is rebuilt.
* Customer `C001`: a change dated 14 Oct arrives on 16 Oct → inserted **in the middle** of
  SCD2 history, and order `O0001` (14 Oct, 15:00) is re-attributed from SILVER to GOLD.
* Customer `C005`: an update with no real attribute change creates **no** new version.
* Re-running any day produces identical tables (idempotency).

## Scaling notes for production

* Partition `fact_orders` by `order_date`; use liquid clustering / `ZORDER BY (customer_id)`.
* Schedule `OPTIMIZE` and `VACUUM` on Delta tables.
* Swap the batch landing read for Auto Loader / `readStream` + `trigger(availableNow=True)`
  to get exactly-once incremental file discovery with the same downstream code.
* Register tables in Unity Catalog / Hive metastore instead of path-based access.
