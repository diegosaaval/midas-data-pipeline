"""Configuración por variables de entorno. Un solo lugar para rutas y parámetros."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    # Raíz del lake. Local: data/lake. En AWS (fase 2): s3a://midas-<env>-lake
    lake: str = field(default_factory=lambda: os.getenv("MIDAS_LAKE", str(PROJECT_ROOT / "data" / "lake")))
    # Zona donde los sistemas fuente dejan sus archivos (sale del alcance del pipeline).
    landing: str = field(default_factory=lambda: os.getenv("MIDAS_LANDING", str(PROJECT_ROOT / "data" / "landing")))
    meta_db: str = field(default_factory=lambda: os.getenv(
        "MIDAS_META_DB", str(PROJECT_ROOT / "data" / "meta" / "midas.db")))
    duckdb: str = field(default_factory=lambda: os.getenv(
        "MIDAS_DUCKDB", str(PROJECT_ROOT / "data" / "warehouse" / "midas.duckdb")))
    seed: int = field(default_factory=lambda: int(os.getenv("MIDAS_SEED", "42")))
    # Volumen diario base de pagos del generador (escala para pruebas rápidas o demos grandes).
    scale: float = field(default_factory=lambda: float(os.getenv("MIDAS_SCALE", "1.0")))
    epoch: date = field(default_factory=lambda: date.fromisoformat(os.getenv("MIDAS_EPOCH", "2026-09-01")))
    # Fecha desde la cual la fuente de pagos envía el esquema v2 (currency, channel).
    schema_v2_from: date = field(default_factory=lambda: date.fromisoformat(
        os.getenv("MIDAS_SCHEMA_V2_FROM", "2026-09-15")))
    # Días hacia atrás que se aceptan como datos tardíos en silver.
    late_data_days: int = field(default_factory=lambda: int(os.getenv("MIDAS_LATE_DAYS", "7")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MIDAS_MAX_RETRIES", "2")))
    shuffle_partitions: int = field(default_factory=lambda: int(os.getenv("MIDAS_SHUFFLE_PARTITIONS", "8")))
    dbt_dir: str = field(default_factory=lambda: str(PROJECT_ROOT / "dbt"))

    def path(self, *parts: str) -> str:
        return "/".join([self.lake.rstrip("/"), *parts])


def get_settings() -> Settings:
    return Settings()
