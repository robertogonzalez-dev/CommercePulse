#!/usr/bin/env bash
# Pipeline entrypoint: Bronze ingestion → dbt Silver/Gold transform
set -euo pipefail

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

log "CommercePulse pipeline starting"
log "Database: ${CP_DB_PATH:-data/warehouse/commercepulse.duckdb}"

# ── Phase 1: Bronze ingestion ─────────────────────────────────────────────
log "Phase 1/2 — Data ingestion (Bronze layer)"
python run_pipeline.py
log "Phase 1/2 — Ingestion complete"

# ── Phase 2: dbt Silver + Gold transform ──────────────────────────────────
log "Phase 2/2 — dbt transformations (Silver + Gold)"
cd transform/dbt_project
dbt deps --profiles-dir . --quiet
dbt run  --profiles-dir . --no-partial-parse
dbt test --profiles-dir . --no-partial-parse
log "Phase 2/2 — dbt complete"

log "Pipeline finished successfully"
