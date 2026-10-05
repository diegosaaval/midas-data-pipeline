"""Contrato con ATLAS: lo que ATLAS (conectores/finflow.yaml) espera leer de cada publicación.

Si este test falla, un cambio en FINFLOW rompería el monitoreo: hay que avisar para actualizar el conector de ATLAS.
"""

import json
from datetime import datetime
from pathlib import Path

import duckdb

# Columnas sobre las que ATLAS tiene reglas. Las tablas pueden tener más columnas; estas no pueden faltar.
ATLAS_COLUMNS = {
    "pagos_gold": {"id_pago", "id_cliente", "monto", "monto_cop", "estado", "medio_pago", "moneda"},
    "clientes_gold": {"id_cliente", "numero_documento", "tipo_documento", "segmento_riesgo"},
    "contracargos_gold": {"id_contracargo", "id_pago", "motivo", "monto", "dias_hasta_contracargo"},
    "indicadores_financieros": {"fecha", "pagos", "pagos_aprobados", "tasa_aprobacion", "tasa_devolucion",
                                "tasa_contracargos"},
}


def test_manifest_has_the_fields_atlas_reads(lake):
    s, _, _ = lake
    manifest = json.loads(Path(s.path("gold", "_manifest.json")).read_text(encoding="utf-8"))
    published = datetime.fromisoformat(manifest["published_at"])
    assert published.tzinfo is not None, "published_at debe ser ISO 8601 con zona horaria"
    assert manifest["dates"] and all(datetime.strptime(d, "%Y-%m-%d") for d in manifest["dates"])
    assert isinstance(manifest["run_id"], str) and manifest["run_id"]
    assert set(manifest["datasets"]) == set(ATLAS_COLUMNS)
    # Campos adicionales (opcionales para ATLAS): tipo de corrida y lo que silver apartó o corrigió.
    assert manifest["kind"] in {"incremental", "date", "backfill"} or manifest["kind"].startswith("task:")
    assert {"quarantined", "duplicates_removed", "late_rows", "quarantine_by_reason"} <= set(manifest["quality"])


def test_gold_tables_exist_with_the_columns_atlas_checks(lake):
    s, _, _ = lake
    for table, required in ATLAS_COLUMNS.items():
        path = Path(s.path("gold", f"{table}.parquet"))
        assert path.is_file(), f"falta {path.name}"
        columns = {r[0] for r in duckdb.sql(f"describe select * from read_parquet('{path}')").fetchall()}
        assert required <= columns, f"{table}: faltan {sorted(required - columns)}"
        if table != "contracargos_gold":  # los contracargos llegan 5-45 días después del pago: en 5 días puede no haber
            assert duckdb.sql(f"select count(*) from read_parquet('{path}')").fetchone()[0] > 0, f"{table} vacía"
