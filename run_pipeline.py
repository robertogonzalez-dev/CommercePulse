#!/usr/bin/env python
"""
run_pipeline.py — top-level CLI entry point for CommercePulse.

Keeps the project root clean: users run `python run_pipeline.py`
instead of `python -m ingestion.pipeline`.

Examples:
    # Run all datasets
    python run_pipeline.py

    # Run specific datasets
    python run_pipeline.py --datasets orders payments

    # Verbose debug output
    python run_pipeline.py --log-level DEBUG

    # Dry-run: list available datasets without loading
    python run_pipeline.py --list
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the project root is on the path so `ingestion` is importable
# regardless of how the script is invoked.
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.config_loader import list_available_configs  # noqa: E402
from ingestion.logger_setup import setup_logging  # noqa: E402
from ingestion.pipeline import run_pipeline  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_pipeline",
        description="CommercePulse — Bronze Layer Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py
  python run_pipeline.py --datasets orders order_items
  python run_pipeline.py --log-level DEBUG
  python run_pipeline.py --list
        """,
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        metavar="DATASET",
        help="One or more dataset names to ingest. Omit to run all.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        metavar="UUID",
        help="Override the auto-generated batch UUID (useful for re-runs).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity. Default: INFO.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print all available dataset names and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.list:
        configs = list_available_configs()
        print("\nAvailable datasets:")
        for name in configs:
            print(f"  • {name}")
        print()
        return 0

    setup_logging(log_level=args.log_level)

    results = run_pipeline(
        datasets=args.datasets,
        project_root=PROJECT_ROOT,
        batch_id=args.batch_id,
    )

    failures = [r for r in results.values() if r.status == "failed"]
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
