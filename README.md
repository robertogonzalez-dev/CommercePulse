# CommercePulse

An end-to-end e-commerce analytics warehouse built with Python, DuckDB, dbt, FastAPI, and Streamlit.

## Architecture

```
┌──────────────┐   Python loaders   ┌───────────────────────────────────────────┐
│  data/raw/   │ ─────────────────▶ │            DuckDB Warehouse               │
│  *.csv       │                    │  bronze.*  →  silver.*  →  gold.*         │
└──────────────┘                    │  (raw)        (clean)      (star schema)  │
                                    └────────────────────┬──────────────────────┘
                                                         │
                                             ┌───────────▼──────────┐
                                             │   FastAPI REST API   │
                                             └───────────┬──────────┘
                                                         │
                                             ┌───────────▼──────────┐
                                             │  Streamlit Dashboard │
                                             └──────────────────────┘
```

| Layer    | Tool            | Purpose                                  |
|----------|-----------------|------------------------------------------|
| Ingest   | Python + pandas | Config-driven CSV → DuckDB bronze loader |
| Bronze   | DuckDB          | Raw, immutable, audited tables           |
| Silver   | dbt             | Cleaned, typed, deduplicated models      |
| Gold     | dbt             | Star schema dimensions + facts + marts   |
| API      | FastAPI         | Parameterized metric endpoints           |
| App      | Streamlit       | Interactive analytics dashboard          |
| Tests    | pytest + httpx  | Unit + integration test suite            |
| CI       | GitHub Actions  | Lint, test, dbt compile on PR            |

---

## Quick Start

### Prerequisites

- Python 3.11+
- Git

### Setup

```bash
# 1. Clone and enter the repo
git clone https://github.com/robertogonzalez-dev/CommercePulse.git
cd CommercePulse

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt    # or: make install

# 4. (Optional) copy environment config
cp .env.example .env
```

### Run the ingestion pipeline

```bash
# Ingest all 9 datasets into DuckDB Bronze layer
python run_pipeline.py             # or: make ingest

# Ingest specific datasets only
python run_pipeline.py --datasets orders order_items payments

# Verbose output
python run_pipeline.py --log-level DEBUG

# List all available datasets
python run_pipeline.py --list
```

Expected output:

```
============================================================
CommercePulse Ingestion Pipeline
batch_id  : 3f7a1c2d-...
============================================================
   customers          status=success   loaded=25    duration=0.12s
   products           status=success   loaded=25    duration=0.08s
   inventory          status=success   loaded=25    duration=0.07s
   orders             status=success   loaded=30    duration=0.09s
   order_items        status=success   loaded=43    duration=0.08s
   payments           status=success   loaded=30    duration=0.08s
   refunds            status=success   loaded=5     duration=0.06s
   marketing_spend    status=success   loaded=12    duration=0.07s
   web_sessions       status=success   loaded=30    duration=0.11s
------------------------------------------------------------
Total: 9 datasets | 225 rows loaded | 0 failed
============================================================
```

### Run the test suite

```bash
make test
# or: pytest tests/ -v --tb=short
```

---

## Project Structure

```
CommercePulse/
├── data/
│   ├── raw/                  # Seed CSV files (9 datasets, tracked in git)
│   └── warehouse/            # DuckDB database (auto-created, gitignored)
├── ingestion/
│   ├── config/               # Per-dataset YAML configs
│   ├── loaders/
│   │   ├── base_loader.py    # Template-method loader + LoadResult
│   │   └── web_sessions_loader.py  # Custom subclass example
│   ├── schema/
│   │   └── bronze_ddl.sql    # CREATE TABLE statements for all bronze tables
│   ├── config_loader.py      # YAML → DatasetConfig dataclass
│   ├── logger_setup.py       # Rotating file + console logging
│   ├── pipeline.py           # Orchestrator
│   ├── validator.py          # Schema + quality checks + row hash
│   └── warehouse.py          # DuckDB connection manager + schema init
├── transform/                # dbt project (Phase 3)
├── api/                      # FastAPI service (Phase 5)
├── app/                      # Streamlit dashboard (Phase 6)
├── tests/
├── run_pipeline.py           # Top-level CLI entry point
├── Makefile
├── requirements.txt
└── .env.example
```

---

## Ingestion Framework Design

### Config-driven
Every dataset is described by a YAML file in `ingestion/config/`.
Adding a new standard CSV source requires only a new YAML + DDL entry — no Python changes.

### BaseLoader lifecycle

```
read() → validate() → transform() → filter_incremental() → write() → log_run()
```

Override `transform()` in a subclass for dataset-specific enrichment.
`WebSessionsLoader` demonstrates this pattern by computing `session_duration_seconds`.

### Load types

| Type | Behavior |
|------|-----------|
| `full` | Truncate-and-replace on every run |
| `incremental` | Append only rows newer than the high-water mark |

### Audit columns (appended to every Bronze row)

| Column | Description |
|--------|-------------|
| `_ingested_at` | UTC load timestamp |
| `_batch_id` | UUID shared across all datasets in a single pipeline run |
| `_source_file` | Source CSV filename |
| `_row_hash` | SHA-256 of all business columns (for change detection) |

### Retry logic
`_write_with_retry()` retries failed DuckDB writes up to **3 times**
with linear back-off (2s, 4s, 6s) before raising.

### Audit log
Every run writes to `bronze.ingestion_log` — success or failure — with
row counts, duration, and error messages for full pipeline observability.

---

## Adding a New Dataset

1. Drop the CSV in `data/raw/your_dataset.csv`

2. Create `ingestion/config/your_dataset.yaml`:

```yaml
dataset: your_dataset
source_file: data/raw/your_dataset.csv
target_schema: bronze
target_table: raw_your_dataset
load_type: full          # full | incremental
primary_key: id
watermark_column: null   # set to a date/timestamp column for incremental
expected_columns: [id, name, value]
not_null_columns: [id]
unique_columns: [id]
```

3. Add the `CREATE TABLE IF NOT EXISTS` block to `ingestion/schema/bronze_ddl.sql`

4. Run: `python run_pipeline.py --datasets your_dataset`

---

## Makefile Targets

```
make install          Install dependencies
make ingest           Run full ingestion pipeline
make ingest-dataset   Run one dataset  (DATASET=orders)
make ingest-debug     Run with DEBUG logging
make list-datasets    Print all registered datasets
make transform        Run all dbt models        (Phase 3)
make transform-test   Run dbt tests             (Phase 3)
make api              Start FastAPI server      (Phase 5 — localhost:8000)
make app              Start Streamlit dashboard (Phase 6 — localhost:8501)
make test             Run pytest
make lint             Run ruff linter + format check
make typecheck        Run mypy
make clean            Remove caches and logs
```

---

## Implementation Phases

| Phase | Status      | Description                    |
|-------|-------------|--------------------------------|
| 1     |  Complete | Blueprint & data model design  |
| 2     |  Complete | Foundation & Bronze ingestion  |
| 3     |  Next     | Silver dbt models              |
| 4     |  Planned  | Gold dimensional models        |
| 5     |  Planned  | FastAPI metric layer           |
| 6     |  Planned  | Streamlit dashboard            |
| 7     |  Planned  | CI/CD & documentation polish   |

---

## License

MIT — see [LICENSE](LICENSE).
