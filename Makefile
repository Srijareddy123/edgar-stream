PY ?= python3
STACK_DB ?= data/stack.duckdb
CLI = $(PY) -m edgar_stream.cli
DAYS ?= 2026-10-19 2026-10-20 2026-10-21 2026-10-22 2026-10-23

.PHONY: demo test lint eval stack-up stack-demo stack-down dashboard reprocess sample

demo:            ## run the committed sample end to end, no network, keys or containers
	$(CLI) demo
	@echo "open site/index.html"

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

eval:            ## fetch real EDGAR days (needs .env), replay them, write results/replay_results.json
	$(PY) scripts/measure.py --days $(DAYS)

stack-up:        ## Redpanda and MinIO
	docker compose up -d --wait redpanda minio

stack-demo: stack-up   ## the sample through Redpanda and MinIO, own database so it never reuses make demo
	rm -f $(STACK_DB)
	WAREHOUSE_PATH=$(STACK_DB) $(CLI) run --days-dir sample/days --bus kafka --store s3 --label stack --dup-rate 0.1 --shuffle 10
	WAREHOUSE_PATH=$(STACK_DB) $(CLI) transform
	WAREHOUSE_PATH=$(STACK_DB) $(CLI) report --store s3
	@echo "open site/index.html"

reprocess:
	$(CLI) reprocess

dashboard:
	$(PY) -m http.server 8501 -d site

stack-down:
	docker compose --profile airflow down -v
