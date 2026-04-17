"""
BaseLoader — the reusable ingestion engine.

All dataset-specific loaders inherit from this class.
To add a new dataset: create a config YAML and (optionally) a thin
subclass that overrides `transform()` for custom pre-processing.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from ingestion.config_loader import DatasetConfig, load_config
from ingestion.validator import ValidationResult, add_row_hash, validate
from ingestion.warehouse import get_max_watermark, managed_connection

logger = logging.getLogger(__name__)

# How many times to retry a failed DB write before giving up
_MAX_RETRIES = 3
_RETRY_DELAY_SECONDS = 2.0


class LoadResult:
    """Carries the outcome of a single loader run."""

    def __init__(self, dataset: str, batch_id: str) -> None:
        self.dataset = dataset
        self.batch_id = batch_id
        self.status: str = "pending"
        self.rows_read: int = 0
        self.rows_loaded: int = 0
        self.rows_rejected: int = 0
        self.error_message: Optional[str] = None
        self.started_at: datetime = datetime.now(tz=timezone.utc)
        self.completed_at: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def finish(self, status: str, error: str | None = None) -> None:
        self.status = status
        self.error_message = error
        self.completed_at = datetime.now(tz=timezone.utc)

    def __str__(self) -> str:
        return (
            f"[{self.dataset}] status={self.status} "
            f"rows_read={self.rows_read} rows_loaded={self.rows_loaded} "
            f"rows_rejected={self.rows_rejected} "
            f"duration={self.duration_seconds:.2f}s"
        )


class BaseLoader:
    """
    Config-driven loader for any CSV dataset → DuckDB bronze table.

    Lifecycle per run:
        1. read()       — parse CSV into DataFrame
        2. validate()   — schema + quality checks
        3. transform()  — add audit columns (override for custom logic)
        4. filter_incremental() — drop rows already loaded (incremental only)
        5. write()      — INSERT INTO bronze table (with retry)
        6. log_run()    — write result to bronze.ingestion_log
    """

    def __init__(
        self,
        config: DatasetConfig,
        project_root: Path | None = None,
        batch_id: str | None = None,
    ) -> None:
        self.config = config
        self.project_root = project_root or Path.cwd()
        self.batch_id = batch_id or str(uuid.uuid4())

    # ──────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────

    def run(self) -> LoadResult:
        result = LoadResult(self.config.dataset, self.batch_id)
        logger.info(
            "Starting load | dataset=%s load_type=%s batch=%s",
            self.config.dataset,
            self.config.load_type,
            self.batch_id,
        )

        try:
            df = self._read()
            result.rows_read = len(df)

            validation = validate(df, self.config)
            result.rows_rejected = validation.rows_rejected
            if not validation.passed:
                raise ValueError(
                    f"Validation failed: {'; '.join(validation.errors)}"
                )

            df = self.transform(df)
            df = self._filter_incremental(df)

            rows_loaded = self._write_with_retry(df)
            result.rows_loaded = rows_loaded
            result.finish("success")
            logger.info("Completed | %s", result)

        except Exception as exc:
            result.finish("failed", error=str(exc))
            logger.error("FAILED | dataset=%s | %s", self.config.dataset, exc)

        finally:
            self._log_run(result)

        return result

    # ──────────────────────────────────────────────────────────
    # Step 1: Read
    # ──────────────────────────────────────────────────────────

    def _read(self) -> pd.DataFrame:
        source_path = self.project_root / self.config.source_file
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        df = pd.read_csv(
            source_path,
            delimiter=self.config.delimiter,
            encoding=self.config.encoding,
            dtype=str,          # read everything as str; typing happens in DuckDB
            keep_default_na=False,
            na_values=["", "NULL", "null", "N/A", "n/a"],
        )
        df.columns = [c.strip().lower() for c in df.columns]
        logger.debug("Read %d rows from %s", len(df), source_path)
        return df

    # ──────────────────────────────────────────────────────────
    # Step 2: Transform  (override in subclasses for custom logic)
    # ──────────────────────────────────────────────────────────

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add standard audit columns. Override to add dataset-specific logic."""
        now = datetime.now(tz=timezone.utc)
        source_file_name = Path(self.config.source_file).name

        df = add_row_hash(df)
        df["_ingested_at"] = now.isoformat()
        df["_batch_id"] = self.batch_id
        df["_source_file"] = source_file_name

        return df

    # ──────────────────────────────────────────────────────────
    # Step 3: Incremental filter
    # ──────────────────────────────────────────────────────────

    def _filter_incremental(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.is_incremental:
            return df

        wm_col = self.config.watermark_column
        if not wm_col or wm_col not in df.columns:
            logger.warning(
                "[%s] Incremental load configured but watermark column '%s' "
                "not found — loading all rows.",
                self.config.dataset,
                wm_col,
            )
            return df

        with managed_connection() as conn:
            max_wm = get_max_watermark(
                conn,
                self.config.target_schema,
                self.config.target_table,
                wm_col,
            )

        if max_wm is None:
            logger.info(
                "[%s] No existing watermark — loading all rows (initial load).",
                self.config.dataset,
            )
            return df

        before = len(df)
        df[wm_col] = pd.to_datetime(df[wm_col], errors="coerce")
        max_wm_ts = pd.to_datetime(max_wm)
        df = df[df[wm_col] > max_wm_ts].copy()
        logger.info(
            "[%s] Incremental filter: %d → %d rows (watermark > %s)",
            self.config.dataset, before, len(df), max_wm,
        )
        return df

    # ──────────────────────────────────────────────────────────
    # Step 4: Write (with retry)
    # ──────────────────────────────────────────────────────────

    def _write_with_retry(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.info("[%s] No new rows to load — skipping write.", self.config.dataset)
            return 0

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                return self._write(df)
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "[%s] Write attempt %d/%d failed: %s",
                    self.config.dataset, attempt, _MAX_RETRIES, exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAY_SECONDS * attempt)

        raise RuntimeError(
            f"All {_MAX_RETRIES} write attempts failed for {self.config.dataset}: {last_exc}"
        )

    def _write(self, df: pd.DataFrame) -> int:
        schema = self.config.target_schema
        table = self.config.target_table

        with managed_connection() as conn:
            if self.config.load_type == "full":
                conn.execute(f'DELETE FROM "{schema}"."{table}"')
                logger.debug("[%s] Full load: truncated existing rows.", self.config.dataset)

            # BY NAME matches DataFrame columns to DDL columns by name,
            # not by position — immune to column ordering differences.
            conn.execute(
                f'INSERT INTO "{schema}"."{table}" BY NAME SELECT * FROM df'
            )

        logger.info(
            "[%s] Wrote %d rows → %s.%s", self.config.dataset, len(df), schema, table
        )
        return len(df)

    # ──────────────────────────────────────────────────────────
    # Step 5: Audit log
    # ──────────────────────────────────────────────────────────

    def _log_run(self, result: LoadResult) -> None:
        try:
            log_row = {
                "log_id": str(uuid.uuid4()),
                "batch_id": result.batch_id,
                "dataset": result.dataset,
                "source_file": self.config.source_file,
                "target_table": self.config.full_table_name,
                "load_type": self.config.load_type,
                "status": result.status,
                "rows_read": result.rows_read,
                "rows_loaded": result.rows_loaded,
                "rows_rejected": result.rows_rejected,
                "error_message": result.error_message,
                "started_at": result.started_at.isoformat(),
                "completed_at": result.completed_at.isoformat() if result.completed_at else None,
                "duration_seconds": result.duration_seconds,
            }
            log_df = pd.DataFrame([log_row])
            with managed_connection() as conn:
                conn.execute(
                    "INSERT INTO bronze.ingestion_log SELECT * FROM log_df"
                )
        except Exception as exc:
            logger.warning("Failed to write ingestion log: %s", exc)


# ──────────────────────────────────────────────────────────────
# Factory function — eliminates the need for dataset-specific
# subclasses unless custom transform logic is needed.
# ──────────────────────────────────────────────────────────────

def make_loader(
    dataset: str,
    project_root: Path | None = None,
    batch_id: str | None = None,
) -> BaseLoader:
    """Return a configured BaseLoader for any registered dataset."""
    config = load_config(dataset)
    return BaseLoader(config, project_root=project_root, batch_id=batch_id)
