"""
Schema and data quality validator.

Runs lightweight checks on a DataFrame before it hits DuckDB.
Returns a ValidationResult — callers decide whether to abort or warn.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field

import pandas as pd

from ingestion.config_loader import DatasetConfig

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    rows_rejected: int = 0

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.passed = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def summary(self) -> str:
        parts = [f"passed={self.passed}"]
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        if self.warnings:
            parts.append(f"warnings={len(self.warnings)}")
        if self.rows_rejected:
            parts.append(f"rows_rejected={self.rows_rejected}")
        return ", ".join(parts)


def validate(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    result = ValidationResult()

    # 1. Column presence check
    if config.expected_columns:
        missing = set(config.expected_columns) - set(df.columns)
        extra = set(df.columns) - set(config.expected_columns)
        if missing:
            result.add_error(f"Missing expected columns: {sorted(missing)}")
        if extra:
            result.add_warning(f"Unexpected extra columns (will be loaded): {sorted(extra)}")

    # 2. Not-null checks
    for col in config.not_null_columns:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        if null_count > 0:
            result.add_warning(
                f"Column '{col}' has {null_count} null value(s) — "
                f"expected not-null per config."
            )

    # 3. Uniqueness checks (warn only — dedup happens in loader)
    for col in config.unique_columns:
        if col not in df.columns:
            continue
        dup_count = df[col].dropna().duplicated().sum()
        if dup_count > 0:
            result.add_warning(
                f"Column '{col}' has {dup_count} duplicate value(s) in source file."
            )

    # 4. Row count sanity
    if len(df) == 0:
        result.add_error("Source file contains zero rows after parsing.")

    if result.errors:
        logger.error(
            "[%s] Validation FAILED — %s", config.dataset, result.summary()
        )
        for err in result.errors:
            logger.error("  ERROR: %s", err)
    for warn in result.warnings:
        logger.warning("  WARN: %s", warn)

    return result


def add_row_hash(df: pd.DataFrame, exclude_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Compute a SHA-256 row hash over all business columns.
    Used for deduplication and change detection in incremental loads.
    """
    exclude = set(exclude_cols or [])
    hash_cols = [c for c in df.columns if c not in exclude and not c.startswith("_")]

    def _hash_row(row: pd.Series) -> str:
        raw = "|".join(str(v) if pd.notna(v) else "" for v in row[hash_cols])
        return hashlib.sha256(raw.encode()).hexdigest()

    df["_row_hash"] = df.apply(_hash_row, axis=1)
    return df
