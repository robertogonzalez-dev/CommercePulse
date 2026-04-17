"""
CommercePulse Ingestion Pipeline Orchestrator.

Discovers all registered dataset configs and runs them in order.
Supports running a subset of datasets via the --datasets flag.

Usage:
    python -m ingestion.pipeline                          # run all datasets
    python -m ingestion.pipeline --datasets orders payments
    python -m ingestion.pipeline --datasets customers --log-level DEBUG
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from pathlib import Path
from typing import Optional

from ingestion.config_loader import list_available_configs
from ingestion.logger_setup import setup_logging
from ingestion.loaders.base_loader import BaseLoader, LoadResult, make_loader
from ingestion.loaders.web_sessions_loader import WebSessionsLoader
from ingestion.warehouse import initialise_schema

logger = logging.getLogger(__name__)

# Datasets that have custom loader subclasses
_CUSTOM_LOADERS: dict[str, type] = {
    "web_sessions": WebSessionsLoader,
}

# Ingestion execution order — respects FK dependencies
_LOAD_ORDER = [
    "customers",
    "products",
    "inventory",
    "orders",
    "order_items",
    "payments",
    "refunds",
    "marketing_spend",
    "web_sessions",
]


def build_loader(
    dataset: str,
    project_root: Path,
    batch_id: str,
) -> BaseLoader:
    """Instantiate the correct loader class for a dataset."""
    loader_cls = _CUSTOM_LOADERS.get(dataset)
    if loader_cls:
        return loader_cls(project_root=project_root, batch_id=batch_id)
    return make_loader(dataset, project_root=project_root, batch_id=batch_id)


def run_pipeline(
    datasets: Optional[list[str]] = None,
    project_root: Path | None = None,
    batch_id: str | None = None,
) -> dict[str, LoadResult]:
    """
    Execute the ingestion pipeline.

    Args:
        datasets:     List of dataset names to run. None = all registered.
        project_root: Repo root path. Defaults to cwd.
        batch_id:     Shared batch identifier for this run.

    Returns:
        Mapping of dataset name → LoadResult.
    """
    project_root = project_root or Path.cwd()
    batch_id = batch_id or str(uuid.uuid4())

    logger.info("=" * 60)
    logger.info("CommercePulse Ingestion Pipeline")
    logger.info("batch_id  : %s", batch_id)
    logger.info("root      : %s", project_root)
    logger.info("=" * 60)

    # Initialise bronze schema (idempotent)
    initialise_schema()

    # Determine which datasets to run, preserving load order
    available = set(list_available_configs())
    if datasets:
        unknown = set(datasets) - available
        if unknown:
            raise ValueError(f"Unknown dataset(s): {sorted(unknown)}. Available: {sorted(available)}")
        run_list = [d for d in _LOAD_ORDER if d in set(datasets)]
        # Append any requested datasets not in the ordered list
        run_list += [d for d in datasets if d not in run_list]
    else:
        run_list = [d for d in _LOAD_ORDER if d in available]
        run_list += [d for d in sorted(available) if d not in run_list]

    logger.info("Datasets to load (%d): %s", len(run_list), run_list)

    results: dict[str, LoadResult] = {}
    failed: list[str] = []

    for dataset in run_list:
        loader = build_loader(dataset, project_root, batch_id)
        result = loader.run()
        results[dataset] = result
        if result.status == "failed":
            failed.append(dataset)

    # ── Summary ───────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Pipeline Summary  |  batch_id=%s", batch_id)
    logger.info("-" * 60)
    total_rows = 0
    for name, res in results.items():
        icon = "✓" if res.status == "success" else "✗"
        logger.info(
            "  %s %-20s  status=%-8s  loaded=%-6d  duration=%.2fs",
            icon, name, res.status, res.rows_loaded,
            res.duration_seconds or 0,
        )
        total_rows += res.rows_loaded

    logger.info("-" * 60)
    logger.info(
        "Total: %d datasets | %d rows loaded | %d failed",
        len(results), total_rows, len(failed),
    )
    logger.info("=" * 60)

    if failed:
        logger.error("Failed datasets: %s", failed)
        sys.exit(1)

    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CommercePulse Bronze Layer Ingestion Pipeline"
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        metavar="DATASET",
        help="Run only these datasets (space-separated). Default: all.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Override the auto-generated batch UUID.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    setup_logging(log_level=args.log_level)
    run_pipeline(
        datasets=args.datasets,
        batch_id=args.batch_id,
    )
