"""
CommercePulse Analytics API
----------------------------
FastAPI application exposing the Gold/Reporting layer of the DuckDB warehouse.

Run:
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
    # or from project root:
    make api

Docs:
    http://localhost:8000/docs   (Swagger UI)
    http://localhost:8000/redoc  (ReDoc)
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import settings
from api.routers import health, kpis, reports

app = FastAPI(
    title="CommercePulse Analytics API",
    description=(
        "REST API for the CommercePulse e-commerce analytics warehouse. "
        "Serves Gold-layer and Reporting-layer data from DuckDB."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(health.router)
app.include_router(kpis.router)
app.include_router(reports.router)


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {
        "service": "CommercePulse Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
