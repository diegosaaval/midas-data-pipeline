"""Silver -> features de riesgo y KPIs por comercio (las optimizaciones de PySpark viven aquí).

* payment_features: por cada pago del día, comportamiento del cliente en los últimos 7 días
  (window functions con rango de tiempo), enriquecido con comercio (broadcast join) y
  segmento de riesgo del cliente (join normal; AQE decide la estrategia).
* merchant_daily: KPIs por comercio y día. Un marketplace concentra ~25% del volumen:
  se usa *salting* para repartir esa llave caliente entre varias tareas.
* Los planes físicos se guardan como evidencia (data/meta/plans).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from ..config import Settings
from ..metadata import JobResult
from ..spark import explain_string, path_exists

LOOKBACK_DAYS = 7
SALT_BUCKETS = 8
SECONDS_7D = 7 * 24 * 3600


def run(spark: SparkSession, s: Settings, day: date) -> JobResult:
    payments_path = s.path("silver", "payments")
    if not path_exists(spark, payments_path):
        return JobResult(details={"note": "sin pagos en silver"})

    since = day - timedelta(days=LOOKBACK_DAYS)
    payments = (spark.read.parquet(payments_path)
                .where((F.col("dt") >= F.lit(since)) & (F.col("dt") <= F.lit(day)))  # partition pruning
                .where(F.col("status") == "approved"))
    merchants = spark.read.parquet(s.path("silver", "merchants")).select("merchant_id", "category")
    customers = spark.read.parquet(s.path("silver", "customers")).select("customer_id", "risk_segment")

    enriched = (payments
                .join(F.broadcast(merchants), "merchant_id", "left")  # dimensión pequeña: sin shuffle
                .join(customers, "customer_id", "left"))

    ts = F.col("transaction_date").cast("long")
    last_7d = Window.partitionBy("customer_id").orderBy(ts).rangeBetween(-SECONDS_7D, 0)
    seq = Window.partitionBy("customer_id").orderBy(ts)
    features = (enriched
                .withColumn("txn_count_7d", F.count("*").over(last_7d))
                .withColumn("amount_7d", F.sum("amount").over(last_7d))
                .withColumn("avg_amount_7d", F.avg("amount").over(last_7d))
                .withColumn("seconds_since_prev", ts - F.lag(ts).over(seq))
                .withColumn("high_velocity", F.col("txn_count_7d") > 12)
                .withColumn("amount_spike", F.col("amount") > 4 * F.col("avg_amount_7d"))
                .where(F.col("dt") == F.lit(day))
                .select("payment_id", "customer_id", "merchant_id", "category", "risk_segment", "amount",
                        "transaction_date", "txn_count_7d", "amount_7d", "avg_amount_7d", "seconds_since_prev",
                        "high_velocity", "amount_spike", "dt"))

    # Salting: la llave caliente (marketplace) se reparte en SALT_BUCKETS sub-llaves para la
    # agregación parcial y luego se combina. Evita que una sola tarea procese el 25% de los datos.
    todays = enriched.where(F.col("dt") == F.lit(day))
    salted = todays.withColumn("_salt", (F.rand(seed=day.toordinal()) * SALT_BUCKETS).cast("int"))
    partial = salted.groupBy("dt", "merchant_id", "category", "_salt").agg(
        F.count("*").alias("n"), F.sum("amount").alias("tpv"), F.max("amount").alias("max_ticket"))
    merchant_daily = partial.groupBy("dt", "merchant_id", "category").agg(
        F.sum("n").alias("approved_payments"), F.sum("tpv").alias("tpv"), F.max("max_ticket").alias("max_ticket"))

    features.write.mode("overwrite").partitionBy("dt").parquet(s.path("silver", "payment_features"))
    merchant_daily.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(s.path("silver", "merchant_daily"))

    plans = Path(s.meta_db).parent / "plans"
    plans.mkdir(parents=True, exist_ok=True)
    (plans / f"payment_features_{day}.txt").write_text(explain_string(features), encoding="utf-8")
    (plans / f"merchant_daily_{day}.txt").write_text(explain_string(merchant_daily), encoding="utf-8")

    n_features = spark.read.parquet(s.path("silver", "payment_features")).where(F.col("dt") == F.lit(day)).count()
    n_merchants = spark.read.parquet(s.path("silver", "merchant_daily")).where(F.col("dt") == F.lit(day)).count()
    return JobResult(rows_read=todays.count(), rows_written=n_features + n_merchants,
                     partitions=[f"dt={day}"], details={"payment_features": n_features, "merchant_daily": n_merchants})
