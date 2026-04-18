"""
Integration tests for the Bronze ingestion pipeline.

These tests use a real in-memory DuckDB instance (not mocks) to catch
schema mismatches and loader regressions early.

Run:
    pytest tests/test_ingestion.py -v
"""

from __future__ import annotations

# ── Fix import paths ─────────────────────────────────────────
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.config_loader import DatasetConfig, list_available_configs, load_config
from ingestion.loaders.base_loader import BaseLoader
from ingestion.validator import add_row_hash, validate

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def sample_customer_df() -> pd.DataFrame:
    csv = """customer_id,first_name,last_name,email,phone,gender,date_of_birth,city,state,country,zip_code,acquisition_channel,registration_date,is_active
CUST-TEST-01,Alice,Test,alice@test.com,+1-555-0001,F,1990-01-01,Austin,TX,US,78701,organic_search,2023-01-01,true
CUST-TEST-02,Bob,Sample,bob@sample.com,+1-555-0002,M,1985-06-15,Miami,FL,US,33101,email,2023-01-02,true"""
    return pd.read_csv(StringIO(csv))


@pytest.fixture
def customers_config() -> DatasetConfig:
    return load_config("customers")


# ─────────────────────────────────────────────────────────────
# Config loader tests
# ─────────────────────────────────────────────────────────────

class TestConfigLoader:
    def test_list_available_configs_returns_all_datasets(self):
        configs = list_available_configs()
        expected = {
            "customers", "products", "orders", "order_items",
            "payments", "refunds", "inventory", "marketing_spend", "web_sessions",
        }
        assert expected.issubset(set(configs)), f"Missing configs: {expected - set(configs)}"

    def test_load_customers_config(self, customers_config):
        assert customers_config.dataset == "customers"
        assert customers_config.target_table == "raw_customers"
        assert customers_config.target_schema == "bronze"
        assert customers_config.primary_key == "customer_id"
        assert "customer_id" in customers_config.not_null_columns

    def test_incremental_config_flag(self):
        orders_cfg = load_config("orders")
        assert orders_cfg.is_incremental is True
        assert orders_cfg.watermark_column == "order_date"

    def test_full_load_config_flag(self):
        products_cfg = load_config("products")
        assert products_cfg.is_incremental is False

    def test_missing_config_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent_dataset_xyz")


# ─────────────────────────────────────────────────────────────
# Validator tests
# ─────────────────────────────────────────────────────────────

class TestValidator:
    def test_valid_dataframe_passes(self, sample_customer_df, customers_config):
        result = validate(sample_customer_df, customers_config)
        assert result.passed is True
        assert len(result.errors) == 0

    def test_missing_required_column_fails(self, customers_config):
        df = pd.DataFrame({"customer_id": ["C1"], "email": ["x@y.com"]})
        result = validate(df, customers_config)
        assert result.passed is False
        assert any("Missing expected columns" in e for e in result.errors)

    def test_empty_dataframe_fails(self, customers_config):
        df = pd.DataFrame(columns=customers_config.expected_columns)
        result = validate(df, customers_config)
        assert result.passed is False
        assert any("zero rows" in e for e in result.errors)

    def test_duplicate_primary_key_warns(self, customers_config):
        df = pd.DataFrame({
            col: ["CUST-DUP", "CUST-DUP"] if col == "customer_id"
            else ["a@b.com", "c@d.com"] if col == "email"
            else ["val"] * 2
            for col in customers_config.expected_columns
        })
        result = validate(df, customers_config)
        assert any("duplicate" in w.lower() for w in result.warnings)

    def test_row_hash_is_added(self, sample_customer_df):
        df = add_row_hash(sample_customer_df)
        assert "_row_hash" in df.columns
        assert df["_row_hash"].notna().all()
        assert df["_row_hash"].nunique() == len(df)

    def test_row_hash_is_deterministic(self, sample_customer_df):
        df1 = add_row_hash(sample_customer_df.copy())
        df2 = add_row_hash(sample_customer_df.copy())
        pd.testing.assert_series_equal(df1["_row_hash"], df2["_row_hash"])


# ─────────────────────────────────────────────────────────────
# BaseLoader integration tests (in-memory DuckDB)
# ─────────────────────────────────────────────────────────────

class TestBaseLoader:
    """Uses a temp directory with a real DuckDB file for integration tests."""

    @pytest.fixture
    def temp_project(self, tmp_path: Path) -> Path:
        """Create a minimal project structure for testing."""
        # Copy raw data files
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        source = Path(__file__).parent.parent / "data" / "raw" / "customers.csv"
        if source.exists():
            (raw_dir / "customers.csv").write_text(source.read_text())
        return tmp_path

    @pytest.fixture(autouse=True)
    def set_db_env(self, temp_project: Path, monkeypatch):
        db_path = temp_project / "data" / "warehouse" / "test.duckdb"
        monkeypatch.setenv("COMMERCEPULSE_DB_PATH", str(db_path))

    def test_full_load_succeeds(self, temp_project: Path):
        from ingestion.warehouse import initialise_schema
        initialise_schema()

        config = load_config("customers")
        loader = BaseLoader(config, project_root=temp_project)
        result = loader.run()

        assert result.status == "success"
        assert result.rows_read > 0
        assert result.rows_loaded == result.rows_read

    def test_full_load_is_idempotent(self, temp_project: Path):
        from ingestion.warehouse import get_connection, initialise_schema
        initialise_schema()

        config = load_config("customers")
        loader = BaseLoader(config, project_root=temp_project)

        result1 = loader.run()
        result2 = loader.run()

        # Full load truncates and reloads — count should match source rows
        conn = get_connection()
        count = conn.execute("SELECT COUNT(*) FROM bronze.raw_customers").fetchone()[0]
        conn.close()

        assert count == result1.rows_read == result2.rows_read

    def test_audit_columns_are_present(self, temp_project: Path):
        from ingestion.warehouse import get_connection, initialise_schema
        initialise_schema()

        config = load_config("customers")
        loader = BaseLoader(config, project_root=temp_project)
        loader.run()

        conn = get_connection()
        row = conn.execute(
            "SELECT _ingested_at, _batch_id, _source_file, _row_hash "
            "FROM bronze.raw_customers LIMIT 1"
        ).fetchone()
        conn.close()

        assert row is not None
        _ingested_at, _batch_id, _source_file, _row_hash = row
        assert _ingested_at is not None
        assert _batch_id is not None
        assert _source_file == "customers.csv"
        assert len(_row_hash) == 64  # SHA-256 hex digest

    def test_ingestion_log_is_written(self, temp_project: Path):
        from ingestion.warehouse import get_connection, initialise_schema
        initialise_schema()

        config = load_config("customers")
        loader = BaseLoader(config, project_root=temp_project)
        loader.run()

        conn = get_connection()
        log = conn.execute(
            "SELECT status, rows_loaded FROM bronze.ingestion_log "
            "WHERE dataset = 'customers' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        conn.close()

        assert log is not None
        assert log[0] == "success"
        assert log[1] > 0

    def test_missing_source_file_produces_failed_result(self, temp_project: Path):
        from ingestion.warehouse import initialise_schema
        initialise_schema()

        config = load_config("customers")
        config.source_file = "data/raw/does_not_exist.csv"
        loader = BaseLoader(config, project_root=temp_project)
        result = loader.run()

        assert result.status == "failed"
        assert result.error_message is not None
