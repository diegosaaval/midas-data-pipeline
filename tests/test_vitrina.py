"""Vitrina: la demo web repite una corrida real en bucle y publica gold como si MIDAS estuviera corriendo."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from conftest import land, make_settings
from fastapi.testclient import TestClient

from midas.contracts import ENTITIES
from midas.metadata import JobResult, Metadata
from midas.ui.api import create_app
from midas.ui.vitrina import Replay, ReplayStore, build_bundle

MONTH = ["2026-09-29", "2026-09-30"]
DAY = "2026-10-01"


def recorded_lake(tmp_path):
    """Dos corridas reales en miniatura: el 'mes' (con un reintento) y el día del incidente."""
    s = make_settings(tmp_path)
    meta = Metadata(s.meta_db)
    t0 = datetime(2026, 10, 6, 16, 0, tzinfo=UTC)

    def run(dates, start, retry=False):
        run_id = meta.start_run("incremental", dates)
        meta.conn.execute("UPDATE runs SET started_at = ? WHERE run_id = ?", (start.isoformat(), run_id))
        at = start
        for d in dates:
            for task in [f"bronze.{e}" for e in ENTITIES] + [f"silver.{e}" for e in ENTITIES] + ["features"]:
                if retry and task == "silver.payments":
                    meta.record_task(run_id, task, d, 1, "failed", at.isoformat(), 2.0, error="InjectedFailure")
                    at += timedelta(seconds=5)  # 2 s del intento + 3 s de espera antes de reintentar
                    retry = False
                meta.record_task(run_id, task, d, 1, "success", at.isoformat(), 2.0,
                                 JobResult(10, 9, 1 if task == "silver.payments" else 0, [f"dt={d}"],
                                           {"duplicates_removed": 1, "quarantine_reasons": {"amount:below_min": 1}}))
                at += timedelta(seconds=2)
        for task in ("dbt.build", "publish"):
            meta.record_task(run_id, task, None, 1, "success", at.isoformat(), 2.0, JobResult(rows_written=5))
            at += timedelta(seconds=2)
        meta.conn.execute("UPDATE runs SET status = 'success', finished_at = ? WHERE run_id = ?", (at.isoformat(), run_id))
        meta.conn.commit()
        return run_id

    month_id = run(MONTH, t0, retry=True)
    day_id = run([DAY], t0 + timedelta(minutes=10))
    for d in [*MONTH, DAY]:
        for e in ENTITIES:
            land(s, e, date.fromisoformat(d), [{"x": 1}, {"x": 2}])
    plans = Path(s.meta_db).parent / "plans"
    plans.mkdir()
    (plans / f"payment_features_{DAY}.txt").write_text("== Physical Plan ==")
    gold = Path(s.path("gold"))
    gold.mkdir(parents=True)
    days = "(VALUES (DATE '2026-09-29'), (DATE '2026-09-30'), (DATE '2026-10-01')) t(fecha)"
    for name in ("pagos_gold", "contracargos_gold", "indicadores_financieros"):
        duckdb.sql(f"COPY (SELECT fecha, 1 AS n FROM {days}) TO '{gold / (name + '.parquet')}' (FORMAT parquet)")
    duckdb.sql(f"COPY (SELECT 'C1' AS id_cliente) TO '{gold / 'clientes_gold.parquet'}' (FORMAT parquet)")
    (gold / "_manifest.json").write_text(json.dumps({"run_id": day_id, "dates": [DAY], "published_at": "x",
                                                      "datasets": {"pagos_gold": 3}}))
    meta.close()
    return s, month_id, day_id


@pytest.fixture
def bundle(tmp_path):
    s, month_id, day_id = recorded_lake(tmp_path)
    out = tmp_path / "vitrina"
    info = build_bundle(s, out)
    assert info["runs"] == [month_id, day_id] and info["plans"] == 1
    return s, out, month_id, day_id


def at(replay, offset):
    """Fija el reloj en `offset` segundos del ciclo actual."""
    base = (1_800_000_000 // replay.loop) * replay.loop
    replay.clock = lambda: base + offset


def test_bundle_has_both_publications(bundle):
    _, out, month_id, day_id = bundle
    month = json.loads((out / "gold" / "mes" / "_manifest.json").read_text())
    assert month["run_id"] == month_id and month["dates"] == MONTH
    assert month["quality"]["quarantined"] == 2 and month["datasets"]["pagos_gold"] == 2  # sin el 1 de octubre
    assert json.loads((out / "landing.json").read_text())[f"payments/{DAY}"] == 2


def test_replay_shows_the_story_in_order(bundle):
    s, out, month_id, day_id = bundle
    replay = Replay(out)
    store = ReplayStore(s, replay)
    month_end = replay._pos(month_id, replay.month["finished_at"])
    day_start, day_end = replay.start[day_id], replay._pos(day_id, replay.day["finished_at"])

    at(replay, 0.5)
    assert store.runs() == [] and replay.publication()[1]["run_id"] == day_id  # sigue la del ciclo anterior

    failed = next(t for t in replay.tasks if t["status"] == "failed")
    at(replay, replay._pos(month_id, failed["started_at"]) + 3)  # falló y espera para reintentar
    run = store.runs()[0]
    assert run["run_id"] == month_id and run["status"] == "running"
    silver = next(st for st in store.run_detail(month_id)["stages"] if st["key"] == "silver")
    assert silver["status"] == "retrying"  # justo después del intento fallido

    at(replay, month_end + 1)
    assert store.run_detail(month_id)["publish_ok"] and replay.publication()[1]["run_id"] == month_id

    at(replay, day_start + 1)
    assert store.runs()[0]["run_id"] == day_id and store.runs()[0]["status"] == "running"

    at(replay, day_end + 1)
    folder, manifest = replay.publication()
    assert folder.name == "final" and manifest["run_id"] == day_id and store.watermark() == DAY
    assert store.run_detail(day_id)["stages"][0]["rows_written"] == 10  # landing desde el paquete


def test_published_at_is_stable_so_atlas_sees_one_publication(bundle):
    _, out, month_id, _ = bundle
    replay = Replay(out)
    end = replay._pos(month_id, replay.month["finished_at"])
    at(replay, end + 1)
    first = replay.publication()[1]["published_at"]
    at(replay, end + 4.3)
    assert replay.publication()[1]["published_at"] == first
    at(replay, end + 1 + replay.loop)  # el ciclo siguiente es una publicación nueva
    assert replay.publication()[1]["published_at"] != first


def test_app_in_vitrina_mode_serves_gold_for_atlas(bundle):
    s, out, _, _ = bundle
    client = TestClient(create_app(s, vitrina=str(out)))
    assert client.get("/api/meta").json()["vitrina"] is True
    manifest = client.get("/vitrina/gold/_manifest.json").json()
    assert {"run_id", "dates", "published_at", "datasets"} <= set(manifest)
    parquet = client.get("/vitrina/gold/pagos_gold.parquet")
    assert parquet.status_code == 200 and parquet.content[:4] == b"PAR1"
    assert client.get("/vitrina/gold/..%2Fmeta.db").status_code == 404
    assert client.get("/vitrina/gold/secreto.parquet").status_code == 404
    assert client.get("/api/runs").status_code == 200


def test_local_mode_has_no_vitrina_routes(tmp_path):
    client = TestClient(create_app(make_settings(tmp_path), vitrina=""))
    assert client.get("/api/meta").json()["vitrina"] is False
    assert client.get("/vitrina/gold/_manifest.json").status_code == 404


def test_bundle_needs_the_two_runs(tmp_path):
    s = make_settings(tmp_path)
    Metadata(s.meta_db).close()
    with pytest.raises(ValueError, match="dos corridas"):
        build_bundle(s, tmp_path / "v")
