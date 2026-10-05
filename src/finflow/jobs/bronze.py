"""Landing -> Bronze.

Bronze es una copia fiel de lo que entregó la fuente: todo se lee como texto
(`primitivesAsString`) para no perder información por conversiones, se agregan
metadatos de linaje y se escribe en Parquet particionado por fecha de ingesta.

* Idempotente: reescribe solo la partición `ingest_date` del día (dynamic overwrite).
* Detecta cambios de esquema frente al contrato y los registra como eventos.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..config import Settings
from ..contracts import CONTRACTS
from ..metadata import JobResult, Metadata
from ..spark import path_exists

LINEAGE = ("_source_file", "_ingested_at", "_row_hash", "ingest_date")
CORRUPT = "_corrupt_record"


class MissingSourceError(RuntimeError):
    """La fuente no ha dejado el archivo del día (no tiene sentido reintentar de inmediato)."""


def run(spark: SparkSession, s: Settings, entity: str, day: date, meta: Metadata | None = None) -> JobResult:
    src = f"{s.landing}/{entity}/ingest_date={day.isoformat()}"
    if not path_exists(spark, src):
        raise MissingSourceError(f"No hay archivo de {entity} para {day} en landing")

    # cache: Spark no permite consultar solo la columna de registros corruptos sobre el JSON crudo
    raw = (spark.read.option("primitivesAsString", "true").option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", CORRUPT).json(src)
           .withColumn("_source_file", F.input_file_name())).cache()
    data_cols = [c for c in raw.columns if c not in (CORRUPT, "_source_file")]
    rows_read = raw.count()

    df = (raw
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_row_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(data_cols)])), 256))
          .withColumn("ingest_date", F.lit(day.isoformat())))
    if CORRUPT not in df.columns:
        df = df.withColumn(CORRUPT, F.lit(None).cast("string"))

    out = s.path("bronze", entity)
    if rows_read:
        df.write.mode("overwrite").partitionBy("ingest_date").parquet(out)

    # Contrato vs. lo recibido: columnas nuevas (no acordadas) o faltantes.
    contract = CONTRACTS[entity]
    expected = {c.name for c in contract.columns}
    required = {c.name for c in contract.columns if c.required}
    unknown = sorted(set(data_cols) - expected)
    missing = sorted(required - set(data_cols)) if rows_read else []
    details: dict = {"columns": sorted(data_cols)}
    if meta:
        if unknown:
            meta.schema_event(day.isoformat(), entity, "unknown_columns", unknown)
        if missing:
            meta.schema_event(day.isoformat(), entity, "missing_required_columns", missing)
        newer = [c.name for c in contract.columns if c.since > 1 and c.name in data_cols]
        if newer:
            details["contract_version"] = max(contract.column(c).since for c in newer)
    details |= {"unknown_columns": unknown, "missing_columns": missing}
    corrupt = df.where(F.col(CORRUPT).isNotNull()).count() if rows_read else 0
    details["corrupt_lines"] = corrupt
    return JobResult(rows_read=rows_read, rows_written=rows_read, partitions=[f"ingest_date={day}"], details=details)
