PY ?= python
export PYTHONPATH := src

.PHONY: install test sample run-local run-delta clean

install:
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest

sample:
	$(PY) scripts/generate_sample_data.py --base-path ./data

# No internet / no Delta jars: Parquet fallback
run-local: sample
	for d in 2026-10-14 2026-10-15 2026-10-16; do $(PY) -m shopstream.main --env local --run-date $$d || exit 1; done

# Real Delta Lake (downloads Delta jars on first run)
run-delta: sample
	for d in 2026-10-14 2026-10-15 2026-10-16; do $(PY) -m shopstream.main --env dev --run-date $$d || exit 1; done

clean:
	rm -rf data spark-warehouse metastore_db .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
