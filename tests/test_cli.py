"""El CLI es la interfaz que usa Airflow: se prueba como lo invocaría un operador."""

import pytest

from finflow.cli import main


@pytest.fixture
def env(tmp_path, monkeypatch, spark):
    for key, value in {"FINFLOW_LAKE": tmp_path / "lake", "FINFLOW_LANDING": tmp_path / "landing",
                       "FINFLOW_META_DB": tmp_path / "meta" / "m.db", "FINFLOW_DUCKDB": tmp_path / "wh" / "f.duckdb",
                       "FINFLOW_SCALE": "0.03", "FINFLOW_BACKOFF_SECONDS": "0"}.items():
        monkeypatch.setenv(key, str(value))
    return tmp_path


def test_generate_run_status(env, capsys):
    assert main(["generate", "--start", "2026-09-01", "--end", "2026-09-02"]) == 0
    assert "payments=" in capsys.readouterr().out
    assert main(["run"]) == 0
    assert main(["run"]) == 0  # nada nuevo
    assert "(nada nuevo)" in capsys.readouterr().out
    assert main(["status", "--limit", "2"]) == 0
    out = capsys.readouterr().out
    assert "Watermark (última fecha de ingesta procesada): 2026-09-02" in out
    assert "silver.payments" in out


def test_airflow_style_single_tasks(env, capsys):
    main(["generate", "--start", "2026-09-01", "--end", "2026-09-01"])
    for task in ("bronze.merchants", "silver.merchants", "bronze.customers", "silver.customers",
                 "bronze.payments", "silver.payments", "features"):
        assert main(["task", task, "--date", "2026-09-01"]) == 0, task
    assert main(["task", "dbt.build"]) == 0
    assert main(["task", "publish", "--date", "2026-09-01"]) == 0
    assert main(["task", "nope", "--date", "2026-09-01"]) == 1
    assert main(["task", "silver.payments"]) == 1  # falta --date
    assert "Tarea desconocida" in capsys.readouterr().err


def test_backfill_and_errors(env, capsys):
    main(["generate", "--start", "2026-09-01", "--end", "2026-09-03"])
    assert main(["run"]) == 0
    assert main(["backfill", "--start", "2026-09-02", "--end", "2026-09-03"]) == 0
    assert main(["backfill", "--start", "2027-01-01", "--end", "2027-01-02"]) == 1
    assert "No hay datos en landing" in capsys.readouterr().err
