"""Configuración por variables de entorno. Un solo lugar para rutas y parámetros."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    # Raíz del lake. Local: data/lake. En AWS (fase 2): s3a://finflow-<env>-lake
    lake: str = field(default_factory=lambda: os.getenv("FINFLOW_LAKE", str(PROJECT_ROOT / "data" / "lake")))
    # Zona donde los sistemas fuente dejan sus archivos (sale del alcance del pipeline).
    landing: str = field(default_factory=lambda: os.getenv("FINFLOW_LANDING", str(PROJECT_ROOT / "data" / "landing")))
    meta_db: str = field(default_factory=lambda: os.getenv(
        "FINFLOW_META_DB", str(PROJECT_ROOT / "data" / "meta" / "finflow.db")))
    duckdb: str = field(default_factory=lambda: os.getenv(
        "FINFLOW_DUCKDB", str(PROJECT_ROOT / "data" / "warehouse" / "finflow.duckdb")))
    seed: int = field(default_factory=lambda: int(os.getenv("FINFLOW_SEED", "42")))
    # Volumen diario base de pagos del generador (escala para pruebas rápidas o demos grandes).
    scale: float = field(default_factory=lambda: float(os.getenv("FINFLOW_SCALE", "1.0")))
    epoch: date = field(default_factory=lambda: date.fromisoformat(os.getenv("FINFLOW_EPOCH", "2026-09-01")))
    # Fecha desde la cual la fuente de pagos envía el esquema v2 (currency, channel).
    schema_v2_from: date = field(default_factory=lambda: date.fromisoformat(
        os.getenv("FINFLOW_SCHEMA_V2_FROM", "2026-09-15")))
    # Días hacia atrás que se aceptan como datos tardíos en silver.
    late_data_days: int = field(default_factory=lambda: int(os.getenv("FINFLOW_LATE_DAYS", "7")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("FINFLOW_MAX_RETRIES", "2")))
    shuffle_partitions: int = field(default_factory=lambda: int(os.getenv("FINFLOW_SHUFFLE_PARTITIONS", "8")))
    dbt_dir: str = field(default_factory=lambda: str(PROJECT_ROOT / "dbt"))

    def path(self, *parts: str) -> str:
        return "/".join([self.lake.rstrip("/"), *parts])


def get_settings() -> Settings:
    return Settings()
