"""API de la pantalla de etapas: estados de cada etapa a partir de los metadatos, sin escribir nada."""

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from conftest import land, make_settings
from fastapi.testclient import TestClient

from midas.contracts import ENTITIES
from midas.metadata import JobResult, Metadata
from midas.ui.api import create_app

DAY = "2026-09-10"


@pytest.fixture
def env(tmp_path):
    s = make_settings(tmp_path)
    meta = Metadata(s.meta_db)
    yield s, meta, TestClient(create_app(s))
    meta.close()


def ok(meta, run_id, task, day=DAY, attempt=1, **kw):
    row = meta.start_task(run_id, task, day, attempt, datetime.now(UTC).isoformat())
    meta.record_task(run_id, task, day, attempt, "success", datetime.now(UTC).isoformat(), 0.5, JobResult(**kw), row=row)


def fail(meta, run_id, task, day=DAY, attempt=1, error="RuntimeError: boom"):
    row = meta.start_task(run_id, task, day, attempt, datetime.now(UTC).isoformat())
    meta.record_task(run_id, task, day, attempt, "failed", datetime.now(UTC).isoformat(), 0.2, error=error, row=row)


def full_day(meta, run_id, day=DAY):
    for e in ENTITIES:
        ok(meta, run_id, f"bronze.{e}", day, rows_read=10, rows_written=10, partitions=[f"ingest_date={day}"],
           details={"corrupt_lines": 1 if e == "payments" else 0, "unknown_columns": ["promo_code"] if e == "payments" else []})
    for e in ENTITIES:
        q = 2 if e == "payments" else 0
        ok(meta, run_id, f"silver.{e}", day, rows_read=10, rows_written=8 - q, rows_quarantined=q, partitions=[f"dt={day}"],
           details={"duplicates_removed": 1, "late_rows": 3, "quarantine_reasons": {"amount:below_min": q, "x:y": 0}})
    ok(meta, run_id, "features", day, rows_read=8, rows_written=9, partitions=[f"dt={day}"])


def stages(client, run_id="latest"):
    body = client.get(f"/api/runs/{run_id}").json()
    return body, {s["key"]: s for s in body["stages"]}


def test_without_runs_the_api_is_empty_and_serves_the_page(tmp_path):
    client = TestClient(create_app(make_settings(tmp_path)))  # ni siquiera existe la base
    assert client.get("/healthz").json() == {"ok": True}
    assert client.get("/api/runs").json() == []
    assert client.get("/api/runs/latest").status_code == 404
    assert client.get("/api/meta").json()["watermark"] is None
    page = client.get("/")
    assert page.status_code == 200 and "MIDAS" in page.text
    assert client.get("/app.js").status_code == 200


def test_completed_run_with_a_retry(env):
    s, meta, client = env
    for e in ENTITIES:
        land(s, e, datetime.fromisoformat(DAY).date(), [{"id": 1}, {"id": 2}])
    run_id = meta.start_run("date", [DAY])
    fail(meta, run_id, "bronze.payments", error="InjectedFailure: falla transitoria")
    full_day(meta, run_id)
    ok(meta, run_id, "dbt.build", None, rows_written=15, details={"dbt": {"success": 15, "pass": 26}})
    ok(meta, run_id, "publish", None, rows_written=100, details={"pagos_gold": 90, "clientes_gold": 10})
    meta.finish_run(run_id, "success")

    body, st = stages(client)
    assert body["status"] == "success" and body["kind_label"] == "Re-proceso" and body["retries"] == 1
    assert st["bronze"]["status"] == "retried" and st["bronze"]["retries"] == 1
    assert st["bronze"]["errors"][0]["error"].startswith("InjectedFailure")
    assert st["silver"]["status"] == "ok" and st["silver"]["done"] == st["silver"]["expected"] == 5
    assert st["silver"]["rows_quarantined"] == 2 and st["silver"]["partitions"] == [f"dt={DAY}"]
    assert st["landing"]["status"] == "ok" and st["landing"]["rows_written"] == 10  # 5 archivos x 2 líneas
    assert all(st[k]["status"] == "ok" for k in ("features", "dbt", "publish")) and body["publish_ok"]
    assert body["quality"] == {"quarantine_total": 2, "quarantine_by_reason": {"amount:below_min": 2},
                               "quarantine_by_entity": {"payments": 2}, "duplicates_removed": 5, "late_rows": 15,
                               "corrupt_lines": 1, "unknown_columns": ["payments.promo_code"]}
    assert body["flows"] == [50, 50, 8, 38, 100]


def test_running_run_shows_the_current_task_and_what_is_waiting(env):
    _, meta, client = env
    run_id = meta.start_run("incremental", [DAY, "2026-09-11"])
    full_day(meta, run_id)
    meta.start_task(run_id, "bronze.customers", "2026-09-11", 1, datetime.now(UTC).isoformat())

    body, st = stages(client)
    assert body["status"] == "running"
    assert st["bronze"]["status"] == "running" and st["bronze"]["current"]["date"] == "2026-09-11"
    assert st["bronze"]["done"] == 5 and st["bronze"]["expected"] == 10
    assert st["silver"]["status"] == "running"  # el día 1 ya pasó por silver, falta el día 2
    assert st["dbt"]["status"] == st["publish"]["status"] == "waiting" and not body["publish_ok"]
    assert st["landing"]["status"] == "failed" and st["landing"]["missing"] == 10  # no hay archivos en landing


def test_failed_attempt_while_running_means_waiting_for_retry(env):
    _, meta, client = env
    run_id = meta.start_run("date", [DAY])
    full_day(meta, run_id)
    fail(meta, run_id, "dbt.build", None)
    _, st = stages(client, run_id)
    assert st["dbt"]["status"] == "retrying" and st["publish"]["status"] == "waiting"


def test_failed_run_marks_the_failing_stage_and_skips_the_rest(env):
    _, meta, client = env
    run_id = meta.start_run("date", [DAY])
    full_day(meta, run_id)
    for attempt in (1, 2, 3):
        fail(meta, run_id, "dbt.build", None, attempt, error="PipelineError: dbt build falló")
    meta.finish_run(run_id, "failed", "PipelineError: dbt build falló")
    body, st = stages(client, run_id)
    assert body["status"] == "failed" and body["error"].startswith("PipelineError")
    assert st["dbt"]["status"] == "failed" and len(st["dbt"]["errors"]) == 3
    assert st["publish"]["status"] == "skipped" and not body["publish_ok"]


def test_running_run_without_activity_is_reported_as_interrupted(env):
    _, meta, client = env
    run_id = meta.start_run("incremental", [DAY])
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    meta.conn.execute("UPDATE runs SET started_at = ? WHERE run_id = ?", (old, run_id))
    meta.conn.commit()
    assert client.get("/api/runs").json()[0]["status"] == "interrupted"


def test_single_airflow_task_run_only_expects_its_stage(env):
    _, meta, client = env
    run_id = meta.start_run("task:silver.payments", [DAY])
    ok(meta, run_id, "silver.payments", rows_read=10, rows_written=10)
    meta.finish_run(run_id, "success")
    body, st = stages(client, run_id)
    assert body["kind_label"] == "Tarea silver.payments"
    assert st["silver"]["status"] == "ok" and st["silver"]["expected"] == 1
    assert {st[k]["status"] for k in ("landing", "bronze", "features", "dbt", "publish")} == {"skipped"}


def test_history_lists_runs_with_seconds_per_stage(env):
    _, meta, client = env
    first = meta.start_run("incremental", [DAY])
    full_day(meta, first)
    meta.finish_run(first, "success")
    second = meta.start_run("backfill", [DAY])
    meta.finish_run(second, "success")
    runs = client.get("/api/runs").json()
    assert {r["run_id"] for r in runs} == {first, second}
    summary = next(r for r in runs if r["run_id"] == first)
    assert summary["stage_seconds"]["bronze"] == pytest.approx(2.5) and summary["stage_seconds"]["dbt"] == 0
    assert client.get("/api/runs/nope").status_code == 404


def test_plans_are_served_but_only_by_safe_name(env):
    s, _, client = env
    plans = Path(s.meta_db).parent / "plans"
    plans.mkdir(parents=True)
    (plans / f"payment_features_{DAY}.txt").write_text("== Physical Plan ==\nBroadcastHashJoin")
    (Path(s.meta_db).parent / "secret.txt").write_text("no")
    assert "BroadcastHashJoin" in client.get(f"/api/plans/payment_features_{DAY}.txt").text
    assert client.get("/api/plans/..%2Fsecret.txt").status_code == 404
    assert client.get("/api/plans/secret.txt").status_code == 404


def test_the_screen_never_writes_to_the_metadata(env):
    s, meta, client = env
    run_id = meta.start_run("date", [DAY])
    full_day(meta, run_id)
    meta.finish_run(run_id, "success")
    before = hashlib.sha256(Path(s.meta_db).read_bytes()).hexdigest()
    for path in ("/api/meta", "/api/runs", "/api/runs/latest", f"/api/runs/{run_id}"):
        assert client.get(path).status_code == 200
    assert hashlib.sha256(Path(s.meta_db).read_bytes()).hexdigest() == before


def test_real_pipeline_run_is_all_green(lake):
    s, _, run_id = lake
    body, st = stages(TestClient(create_app(s)), run_id)
    assert body["status"] == "success" and body["publish_ok"]
    assert all(x["status"] == "ok" for x in body["stages"]), {k: v["status"] for k, v in st.items()}
    assert st["landing"]["done"] == st["landing"]["expected"] == 25
    assert st["silver"]["rows_read"] > 0 and body["plans"]
    assert body["quality"]["duplicates_removed"] > 0
