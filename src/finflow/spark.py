"""SparkSession con la configuración que el pipeline necesita (local o cluster)."""

from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession

from .config import Settings, get_settings


def get_spark(settings: Settings | None = None, app: str = "finflow") -> SparkSession:
    s = settings or get_settings()
    # Los workers de Python deben usar el mismo intérprete que el driver (mismo venv).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # Idempotencia: al escribir con mode=overwrite solo se reemplazan las particiones presentes en el DataFrame.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Adaptive Query Execution: ajusta particiones de shuffle y divide particiones sesgadas en joins.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(s.shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", os.getenv("SPARK_UI", "false"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def explain_string(df: DataFrame, mode: str = "formatted") -> str:
    """Plan físico como texto (para guardarlo como evidencia de optimizaciones)."""
    try:
        jvm = df.sparkSession.sparkContext._jvm
        return jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)
    except Exception as exc:  # p. ej. Spark Connect: no hay acceso a la JVM
        return f"(plan no disponible: {exc})"


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs.exists(hpath)


def table_readable(spark: SparkSession, path: str) -> bool:
    """True si hay archivos Parquet legibles (una carpeta vacía o a medio escribir no cuenta)."""
    if not path_exists(spark, path):
        return False
    try:
        spark.read.parquet(path).schema  # noqa: B018 - fuerza la inferencia de esquema
        return True
    except Exception:
        return False
