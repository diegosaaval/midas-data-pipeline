"""Bronze -> Silver: datos validados, tipados, deduplicados y listos para modelar.

Para cada lote (una fecha de ingesta):
1. Alinea al contrato: castea tipos, completa columnas de versiones nuevas con su
   valor por defecto y descarta columnas no acordadas (ya quedaron registradas en bronze).
2. Valida: obligatorios, tipos, catálogos, mínimos y reglas entre entidades. Lo que falla
   va a *quarantine* con la lista de motivos, nunca se pierde en silencio.
3. Deduplica por llave de negocio: gana el registro con `updated_at` más reciente.
4. Hechos (pagos, devoluciones, contracargos) se particionan por fecha del evento. Los
   datos tardíos se integran a su partición original: se leen solo las particiones
   afectadas (partition pruning), se unen con el lote, se deduplica y se reescriben solo
   esas particiones. Re-ejecutar el mismo día produce exactamente el mismo resultado.
5. Dimensiones (clientes, comercios) se mantienen como estado actual (último registro).
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..config import Settings
from ..contracts import CONTRACTS, Column, Contract
from ..metadata import JobResult
from ..spark import path_exists, table_readable
from .bronze import CORRUPT

CAST = {"string": "string", "long": "bigint", "double": "double", "timestamp": "timestamp", "date": "date"}
LINEAGE_OUT = ("_ingest_date", "_ingested_at", "_source_file")
REFERENCE_WINDOW = {"refunds": 30, "chargebacks": 60}  # días hacia atrás donde buscar el pago original


def _typed(col: Column) -> SparkColumn:
    """Conversión tolerante: un valor inválido queda en NULL (y la validación lo manda a cuarentena).

    Spark 4 activa el modo ANSI por defecto: un `cast` normal haría fallar todo el job por un
    solo registro malo. `try_cast` / `try_to_timestamp` devuelven NULL en su lugar.
    """
    raw = F.col(col.name)
    if col.type == "timestamp":
        return F.try_to_timestamp(raw)
    return raw.try_cast(CAST[col.type])


def align(df: DataFrame, contract: Contract) -> DataFrame:
    """Devuelve las columnas del contrato tipadas, más `_raw_<col>` para validar y poner en cuarentena."""
    cols = []
    for c in contract.columns:
        if c.name in df.columns:
            raw = F.col(c.name)
        else:  # columna de una versión más nueva del contrato que esta fuente aún no envía
            raw = F.lit(None).cast("string")
        cols.append(raw.alias(f"_raw_{c.name}"))
    # Tipo explícito: Spark infiere las particiones como fecha; silver lo guarda siempre como texto.
    base = df.select(*cols, F.col("ingest_date").cast("string").alias("_ingest_date"), "_ingested_at", "_source_file")
    typed = []
    for c in contract.columns:
        value = _typed(Column(f"_raw_{c.name}", c.type))
        if c.default is not None:
            value = F.coalesce(value, F.lit(c.default).cast(CAST[c.type]))
        typed.append(value.alias(c.name))
    return base.select("*", *typed)


def reasons(contract: Contract) -> SparkColumn:
    """Arreglo con los motivos por los que un registro no cumple el contrato (vacío = válido)."""
    checks = []
    for c in contract.columns:
        raw, val = F.col(f"_raw_{c.name}"), F.col(c.name)
        if c.required:
            checks.append(F.when(raw.isNull() | (F.trim(raw) == ""), F.lit(f"{c.name}:missing")))
        checks.append(F.when(raw.isNotNull() & (F.trim(raw) != "") & val.isNull(), F.lit(f"{c.name}:invalid_type")))
        if c.allowed:
            checks.append(F.when(val.isNotNull() & ~val.isin(*c.allowed), F.lit(f"{c.name}:not_allowed")))
        if c.min is not None:
            checks.append(F.when(val.isNotNull() & (val < c.min), F.lit(f"{c.name}:below_min")))
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def latest_per_key(df: DataFrame, contract: Contract) -> DataFrame:
    """Deduplicación por llave de negocio: se conserva el registro más reciente."""
    w = Window.partitionBy(*contract.business_key).orderBy(
        F.col(contract.order_by).desc(), F.col("_ingested_at").desc(), F.col("_ingest_date").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def run(spark: SparkSession, s: Settings, entity: str, day: date) -> JobResult:
    contract = CONTRACTS[entity]
    iso = day.isoformat()
    src = s.path("bronze", entity, f"ingest_date={iso}")
    out, quarantine = s.path("silver", entity), s.path("quarantine", entity)
    ensure_table(spark, s, entity)
    if not path_exists(spark, src):  # p. ej. ningún comercio cambió hoy
        return JobResult(details={"note": "sin datos en bronze para la fecha"})

    bronze = spark.read.option("basePath", s.path("bronze", entity)).parquet(src)
    rows_read = bronze.count()

    corrupt = bronze.where(F.col(CORRUPT).isNotNull()) if CORRUPT in bronze.columns else None
    clean = bronze.where(F.col(CORRUPT).isNull()) if CORRUPT in bronze.columns else bronze
    df = align(clean, contract).withColumn("_reasons", reasons(contract))
    df = _cross_entity_checks(spark, s, entity, df, day)

    # Datos demasiado tardíos: fuera de la ventana aceptada se apartan para backfill manual.
    if contract.event_date:
        df = df.withColumn("dt", F.to_date(F.col(contract.event_date)))
        too_old = F.col("dt") < F.lit(day - timedelta(days=s.late_data_days))
        df = df.withColumn("_reasons", F.when(too_old, F.array_union("_reasons", F.array(F.lit("late:out_of_window"))))
                           .otherwise(F.col("_reasons")))
    df = df.localCheckpoint(eager=True)  # se reutiliza para válidos y cuarentena: se materializa una sola vez

    invalid = df.where(F.size("_reasons") > 0)
    valid = df.where(F.size("_reasons") == 0)
    raw_cols = [f"_raw_{c.name}" for c in contract.columns]
    q = invalid.select(F.to_json(F.struct(*[F.col(c).alias(c[5:]) for c in raw_cols])).alias("_raw"),
                       "_reasons", "_source_file", F.current_timestamp().alias("_quarantined_at"))
    if corrupt is not None:
        q = q.unionByName(corrupt.select(F.col(CORRUPT).alias("_raw"), F.array(F.lit("record:malformed_json"))
                                         .alias("_reasons"), "_source_file",
                                         F.current_timestamp().alias("_quarantined_at")))
    q = q.withColumn("ingest_date", F.lit(iso)).localCheckpoint(eager=True)
    n_quarantined = q.count()
    if n_quarantined:
        q.coalesce(1).write.mode("overwrite").partitionBy("ingest_date").parquet(quarantine)

    keep = [*contract.names, *LINEAGE_OUT] + (["payment_found"] if entity in REFERENCE_WINDOW else [])
    batch = valid.select(*keep, *(["dt"] if contract.event_date else []))
    n_valid = batch.count()
    batch = latest_per_key(batch, contract)
    n_batch = batch.count()
    details = {"duplicates_removed": n_valid - n_batch,
               "quarantine_reasons": _reason_counts(invalid) | ({"record:malformed_json": corrupt.count()}
                                                                 if corrupt is not None else {})}

    if contract.event_date:
        affected = sorted(r["dt"].isoformat() for r in batch.select("dt").distinct().collect())
        details["late_rows"] = batch.where(F.col("dt") < F.lit(day)).count()
        merged = batch
        if affected and path_exists(spark, out):
            existing = (spark.read.parquet(out)
                        .where(F.col("dt").isin([date.fromisoformat(a) for a in affected])))  # partition pruning
            merged = existing.unionByName(batch, allowMissingColumns=True)
        merged = latest_per_key(merged, contract).localCheckpoint(eager=True)
        if affected:
            # Un archivo por partición: evita miles de archivos pequeños en el lake.
            merged.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(out)
        partitions = [f"dt={a}" for a in affected]
        details["rows_in_rewritten_partitions"] = merged.count()  # costo real del upsert por partición
        written = n_batch
    else:
        merged = batch
        if path_exists(spark, out):
            merged = spark.read.parquet(out).unionByName(batch, allowMissingColumns=True)
        merged = latest_per_key(merged, contract).localCheckpoint(eager=True)
        merged.coalesce(1).write.mode("overwrite").parquet(out)
        partitions = ["current"]
        written = n_batch

    return JobResult(rows_read=rows_read, rows_written=written, rows_quarantined=n_quarantined,
                     partitions=partitions, details=details)


EMPTY_PARTITION = "dt=1900-01-01"


def silver_schema(entity: str) -> StructType:
    contract = CONTRACTS[entity]
    fields = [StructField(c.name, _spark_type(c.type)) for c in contract.columns]
    fields += [StructField("_ingest_date", StringType()), StructField("_ingested_at", TimestampType()),
               StructField("_source_file", StringType())]
    if entity in REFERENCE_WINDOW:
        fields.append(StructField("payment_found", BooleanType()))
    return StructType(fields)


def _spark_type(t: str):
    return {"string": StringType(), "long": LongType(), "double": DoubleType(), "timestamp": TimestampType(),
            "date": DateType()}[t]


def ensure_table(spark: SparkSession, s: Settings, entity: str) -> None:
    """Crea la tabla vacía (con esquema) si aún no existe.

    En AWS las tablas existen desde el inicio en el Glue Catalog (Terraform). En local, una
    entidad sin datos todavía (p. ej. contracargos en la primera semana) no tendría archivos y
    los consumidores (dbt) fallarían al leerla. Para hechos se usa una partición centinela vacía.
    """
    out = s.path("silver", entity)
    if table_readable(spark, out):
        return
    empty = spark.createDataFrame([], silver_schema(entity))
    target = f"{out}/{EMPTY_PARTITION}" if CONTRACTS[entity].event_date else out
    empty.coalesce(1).write.mode("overwrite").parquet(target)


def _cross_entity_checks(spark: SparkSession, s: Settings, entity: str, df: DataFrame, day: date) -> DataFrame:
    """Reglas que necesitan otra entidad: comercio existente, devolución <= pago, etc."""
    if entity == "payments":
        merchants_path = s.path("silver", "merchants")
        if path_exists(spark, merchants_path):
            merchants = spark.read.parquet(merchants_path).select(F.col("merchant_id").alias("_m_id"))
            # Broadcast join: la dimensión es pequeña y se copia a cada ejecutor en vez de hacer shuffle.
            df = df.join(F.broadcast(merchants), df.merchant_id == F.col("_m_id"), "left")
            df = df.withColumn("_reasons", F.when(F.col("merchant_id").isNotNull() & F.col("_m_id").isNull(),
                                                  F.array_union("_reasons", F.array(F.lit("merchant_id:unknown"))))
                               .otherwise(F.col("_reasons"))).drop("_m_id")
        return df
    if entity in REFERENCE_WINDOW:
        payments_path = s.path("silver", "payments")
        if not path_exists(spark, payments_path):
            return df.withColumn("payment_found", F.lit(False))
        since = day - timedelta(days=REFERENCE_WINDOW[entity])
        payments = (spark.read.parquet(payments_path)
                    .where(F.col("dt") >= F.lit(since))  # partition pruning: solo la ventana relevante
                    .select(F.col("payment_id").alias("_p_id"), F.col("amount").alias("_p_amount")))
        df = df.join(payments, df.payment_id == F.col("_p_id"), "left")
        exceeds = F.col("_p_amount").isNotNull() & (F.col("amount") > F.col("_p_amount"))
        df = df.withColumn("_reasons", F.when(exceeds, F.array_union("_reasons", F.array(F.lit("amount:exceeds_payment"))))
                           .otherwise(F.col("_reasons")))
        # Pago no encontrado: puede ser un padre que aún no llega (dato tardío). Se conserva marcado.
        return df.withColumn("payment_found", F.col("_p_id").isNotNull()).drop("_p_id", "_p_amount")
    return df


def _reason_counts(invalid: DataFrame) -> dict[str, int]:
    rows = invalid.select(F.explode("_reasons").alias("r")).groupBy("r").count().collect()
    return {r["r"]: r["count"] for r in rows}
