"""El DAG debe importar sin errores y respetar el orden de dependencias (se omite si Airflow no está instalado)."""

import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAG_FILE = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "finflow_daily.py"


def load_dag():
    spec = importlib.util.spec_from_file_location("finflow_daily", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # falla aquí si el DAG tiene errores de importación
    return module.dag


def test_dag_structure():
    dag = load_dag()
    assert dag.dag_id == "finflow_daily"
    assert dag.catchup and dag.max_active_runs == 1
    assert {"bronze.payments", "silver.payments", "features", "dbt_build", "publish"} <= set(dag.task_ids)


def test_dependencies_follow_data_contracts():
    dag = load_dag()
    assert "silver.payments" in dag.get_task("silver.customers").downstream_task_ids
    assert "silver.refunds" in dag.get_task("silver.payments").downstream_task_ids
    assert dag.get_task("publish").upstream_task_ids == {"dbt_build"}
    assert dag.get_task("bronze.payments").retries == 2
