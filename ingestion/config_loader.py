"""
Dataset configuration loader.

Reads YAML files from ingestion/config/ and returns validated
DatasetConfig dataclasses consumed by every loader.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class DatasetConfig:
    dataset: str
    source_file: str
    target_schema: str
    target_table: str
    load_type: str                     # full | incremental
    primary_key: str
    watermark_column: Optional[str]
    delimiter: str
    encoding: str
    expected_columns: list[str]
    column_types: dict[str, str]
    not_null_columns: list[str]
    unique_columns: list[str]

    # Derived helpers ─────────────────────────────────────────
    @property
    def full_table_name(self) -> str:
        return f"{self.target_schema}.{self.target_table}"

    @property
    def is_incremental(self) -> bool:
        return self.load_type == "incremental"


_CONFIG_DIR = Path(__file__).parent / "config"


def load_config(dataset: str) -> DatasetConfig:
    """Load and parse the YAML config for a named dataset."""
    config_path = _CONFIG_DIR / f"{dataset}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"No config found for dataset '{dataset}' at {config_path}"
        )

    with config_path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    return DatasetConfig(
        dataset=raw["dataset"],
        source_file=raw["source_file"],
        target_schema=raw["target_schema"],
        target_table=raw["target_table"],
        load_type=raw.get("load_type", "full"),
        primary_key=raw["primary_key"],
        watermark_column=raw.get("watermark_column") or None,
        delimiter=raw.get("delimiter", ","),
        encoding=raw.get("encoding", "utf-8"),
        expected_columns=raw.get("expected_columns", []),
        column_types=raw.get("column_types", {}),
        not_null_columns=raw.get("not_null_columns", []),
        unique_columns=raw.get("unique_columns", []),
    )


def list_available_configs() -> list[str]:
    """Return all dataset names that have a config file."""
    return sorted(p.stem for p in _CONFIG_DIR.glob("*.yaml"))


def load_all_configs() -> dict[str, DatasetConfig]:
    return {name: load_config(name) for name in list_available_configs()}
