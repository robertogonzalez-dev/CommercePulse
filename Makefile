.PHONY: help install ingest ingest-dataset ingest-debug list-datasets \
        transform transform-test transform-docs \
        api app test lint typecheck clean

PYTHON   := python
PYTEST   := pytest
DBT      := dbt
DATASET  ?= orders

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "CommercePulse — Makefile targets"
	@echo "────────────────────────────────────────────────────────"
	@echo "  Setup"
	@echo "    install            Install all Python dependencies"
	@echo ""
	@echo "  Ingestion (Bronze Layer)"
	@echo "    ingest             Run the full ingestion pipeline (all datasets)"
	@echo "    ingest-dataset     Run one dataset:  make ingest-dataset DATASET=orders"
	@echo "    ingest-debug       Run full pipeline with DEBUG logging"
	@echo "    list-datasets      List all available datasets"
	@echo ""
	@echo "  Transform (dbt — Silver & Gold layers)"
	@echo "    transform          Run all dbt models"
	@echo "    transform-test     Run dbt model tests"
	@echo "    transform-docs     Generate and serve dbt docs (localhost:8080)"
	@echo ""
	@echo "  Serve"
	@echo "    api                Start FastAPI server  (http://localhost:8000)"
	@echo "    app                Start Streamlit app   (http://localhost:8501)"
	@echo ""
	@echo "  Quality"
	@echo "    test               Run pytest suite"
	@echo "    lint               Run ruff linter + formatter check"
	@echo "    typecheck          Run mypy type checker"
	@echo ""
	@echo "  Housekeeping"
	@echo "    clean              Remove caches, logs, and build artefacts"
	@echo "────────────────────────────────────────────────────────"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

# ── Ingestion ─────────────────────────────────────────────────────────────────
ingest:
	$(PYTHON) run_pipeline.py

ingest-dataset:
	$(PYTHON) run_pipeline.py --datasets $(DATASET)

ingest-debug:
	$(PYTHON) run_pipeline.py --log-level DEBUG

list-datasets:
	$(PYTHON) run_pipeline.py --list

# ── dbt Transform ─────────────────────────────────────────────────────────────
transform:
	cd transform/dbt_project && $(DBT) run --profiles-dir .

transform-test:
	cd transform/dbt_project && $(DBT) test --profiles-dir .

transform-docs:
	cd transform/dbt_project && $(DBT) docs generate --profiles-dir .
	cd transform/dbt_project && $(DBT) docs serve --profiles-dir .

# ── Serve ─────────────────────────────────────────────────────────────────────
api:
	$(PYTHON) -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

app:
	$(PYTHON) -m streamlit run app/main.py

# ── Quality ───────────────────────────────────────────────────────────────────
test:
	$(PYTEST) tests/ -v --tb=short --cov=ingestion --cov-report=term-missing

lint:
	ruff check . && ruff format --check .

typecheck:
	mypy ingestion/ api/ --ignore-missing-imports

# ── Housekeeping ──────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache htmlcov .coverage .ruff_cache
	rm -rf logs/
