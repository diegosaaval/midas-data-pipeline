"""DAG diario de MIDAS.

Airflow solo orquesta: cada tarea llama al CLI `midas task ...`, que contiene toda la
lógica (probada con pytest). Así la misma tarea corre igual en Airflow, en un notebook o
en un job de Glue, y el DAG se mantiene delgado.

Flujo por día de ingesta ({{ ds }}):
  [simular fuentes] -> esperar archivos -> bronze (5 entidades en paralelo)
  -> silver (dimensiones -> hechos) -> features -> dbt build -> publicar

* catchup=True + max_active_runs=1: los backfills se procesan en orden cronológico.
* retries con backoff exponencial en cada tarea (fallas transitorias).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, TaskGroup

ENTITIES = ("customers", "merchants", "payments", "refunds", "chargebacks")
DIMENSIONS = ("customers", "merchants")
FACTS = ("payments", "refunds", "chargebacks")
LANDING = os.getenv("MIDAS_LANDING", "/opt/midas/data/landing")
SIMULATE_SOURCES = os.getenv("MIDAS_SIMULATE_SOURCES", "true").lower() == "true"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="midas_daily",
    description="Landing -> Bronze -> Silver (PySpark) -> Gold (dbt) para la fecha de ingesta",
    schedule="0 6 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["midas", "finance", "pyspark", "dbt"],
) as dag:
    if SIMULATE_SOURCES:
        sources = BashOperator(task_id="simulate_sources",
                               bash_command="midas generate --start {{ ds }} --end {{ ds }}")
    wait = FileSensor(task_id="wait_for_payments", filepath=f"{LANDING}/payments/ingest_date={{{{ ds }}}}",
                      poke_interval=60, timeout=6 * 3600, mode="reschedule")
    if SIMULATE_SOURCES:
        sources >> wait

    with TaskGroup("bronze") as bronze:
        for entity in ENTITIES:
            BashOperator(task_id=entity, bash_command=f"midas task bronze.{entity} --date {{{{ ds }}}}")

    with TaskGroup("silver") as silver:
        dims = [BashOperator(task_id=e, bash_command=f"midas task silver.{e} --date {{{{ ds }}}}") for e in DIMENSIONS]
        payments = BashOperator(task_id="payments", bash_command="midas task silver.payments --date {{ ds }}")
        dependents = [BashOperator(task_id=e, bash_command=f"midas task silver.{e} --date {{{{ ds }}}}")
                      for e in ("refunds", "chargebacks")]
        dims >> payments >> dependents  # pagos validan contra comercios; devoluciones contra pagos

    features = BashOperator(task_id="features", bash_command="midas task features --date {{ ds }}")
    dbt_build = BashOperator(task_id="dbt_build", bash_command="midas task dbt.build")
    publish = BashOperator(task_id="publish", bash_command="midas task publish --date {{ ds }}")

    wait >> bronze >> silver >> features >> dbt_build >> publish
