"""
WebSessionsLoader — extends BaseLoader with session-specific transforms.

Demonstrates how to add a dataset-specific subclass when the base
transform isn't sufficient (e.g. derived columns, type coercions).
"""

from __future__ import annotations

import pandas as pd

from ingestion.config_loader import load_config
from ingestion.loaders.base_loader import BaseLoader
from pathlib import Path


class WebSessionsLoader(BaseLoader):
    """Adds a derived `session_duration_seconds` column before load."""

    def __init__(self, project_root: Path | None = None, batch_id: str | None = None) -> None:
        super().__init__(
            config=load_config("web_sessions"),
            project_root=project_root,
            batch_id=batch_id,
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Coerce timestamps before computing duration
        df["session_start"] = pd.to_datetime(df["session_start"], errors="coerce")
        df["session_end"] = pd.to_datetime(df["session_end"], errors="coerce")

        df["session_duration_seconds"] = (
            (df["session_end"] - df["session_start"])
            .dt.total_seconds()
            .clip(lower=0)
        )

        # Convert back to ISO strings for DuckDB TIMESTAMP columns.
        # Replace pandas NaT with None so DuckDB receives NULL, not the
        # literal string "NaT" which fails TIMESTAMP parsing.
        df["session_start"] = df["session_start"].dt.strftime("%Y-%m-%d %H:%M:%S").where(
            df["session_start"].notna(), other=None
        )
        df["session_end"] = df["session_end"].dt.strftime("%Y-%m-%d %H:%M:%S").where(
            df["session_end"].notna(), other=None
        )

        return super().transform(df)
