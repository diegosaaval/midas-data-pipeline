import json
from datetime import date
from pathlib import Path

import duckdb
import pytest
from conftest import DAYS, make_settings

from midas.generator import write_landing
from midas.pipeline import GOLD_DATASETS, Pipeline, PipelineError


def gold_counts(s):
    return {n: duckdb.sql(f"select count(*) from read_parquet('{s.path('gold', n + '.parquet')}')").fetchone()[0]
            for n in GOLD_DATASETS}


def test_end_to_end_publishes_gold(lake):
    s, pipe, run_id = lake
    counts = gold_counts(s)
    assert counts["pagos_gold"] > 500 and counts["indicadores_financieros"] == len(DAYS)
    manifest = json.loads(Path(s.path("gold", "_manifest.json")).read_text())
    assert manifest["run_id"] == run_id and manifest["datasets"] == counts
    assert pipe.meta.get_watermark("ingest_date") == "2026-09-05"


def test_incremental_run_with_nothing_new_is_a_no_op(lake):
    _, pipe, _ = lake
    assert pipe.run_incremental() is None


def test_rerunning_a_day_end_to_end_does_not_change_gold(lake):
    s, pipe, _ = lake
    before = gold_counts(s)
    pipe.run_date(date(2026, 9, 3))
    assert gold_counts(s) == before


def test_kpis_are_consistent(lake):
    s, _, _ = lake
    rows = duckdb.sql(f"select * from read_parquet('{s.path('gold', 'indicadores_financieros.parquet')}')").fetchall()
    assert rows and all(0 < r[3] <= 1 for r in rows)  # tasa_aprobacion


def test_metrics_are_recorded_per_task(lake):
    _, pipe, run_id = lake
    tasks = pipe.meta.query("SELECT task, rows_read, rows_written, duration_s FROM task_runs WHERE run_id = ?",
                            (run_id,))
    names = {t["task"] for t in tasks}
    assert {"bronze.payments", "silver.payments", "features", "dbt.build", "publish"} <= names
    silver = next(t for t in tasks if t["task"] == "silver.payments")
    assert silver["rows_read"] > 0 and silver["duration_s"] > 0


def test_explain_plans_show_broadcast_and_pruning(lake):
    s, _, _ = lake
    plan = (Path(s.meta_db).parent / "plans" / "payment_features_2026-09-05.txt").read_text()
    assert "Broadcast" in plan
    assert "PartitionFilters" in plan


def test_transient_failure_is_retried(lake, monkeypatch):
    s, _, _ = lake
    monkeypatch.setenv("MIDAS_FAIL", "silver.customers:1")
    pipe = Pipeline(s, lake[1].spark)
    pipe.backoff_seconds = 0
    run_id = pipe.run_date(date(2026, 9, 4))
    attempts = pipe.meta.query("SELECT attempt, status FROM task_runs WHERE run_id = ? AND task = 'silver.customers'",
                               (run_id,))
    assert [a["status"] for a in attempts] == ["failed", "success"]
    pipe.close()


def test_persistent_failure_fails_the_run_after_retries(lake, monkeypatch):
    s, _, _ = lake
    monkeypatch.setenv("MIDAS_FAIL", "features:99")
    pipe = Pipeline(s, lake[1].spark)
    pipe.backoff_seconds = 0
    with pytest.raises(PipelineError):
        pipe.run_date(date(2026, 9, 4))
    run = pipe.meta.query("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")[0]
    failed = pipe.meta.query("SELECT COUNT(*) AS n FROM task_runs WHERE task = 'features' AND status = 'failed'")
    assert run["status"] == "failed" and failed[0]["n"] >= s.max_retries + 1
    pipe.close()


def test_backfill_does_not_move_watermark_backwards(lake):
    s, pipe, _ = lake
    pipe.backfill(date(2026, 9, 2), date(2026, 9, 3))
    assert pipe.meta.get_watermark("ingest_date") == "2026-09-05"


def test_missing_source_is_not_retried(spark, tmp_path):
    s = make_settings(tmp_path)
    write_landing(date(2026, 9, 1), s)
    (Path(s.landing) / "refunds" / "ingest_date=2026-09-01" / "part-00000.jsonl").unlink()
    (Path(s.landing) / "refunds" / "ingest_date=2026-09-01").rmdir()
    pipe = Pipeline(s, spark)
    pipe.backoff_seconds = 0
    with pytest.raises(PipelineError, match="No hay archivo de refunds"):
        pipe.run_incremental()
    attempts = pipe.meta.query("SELECT COUNT(*) AS n FROM task_runs WHERE task = 'bronze.refunds'")
    assert attempts[0]["n"] == 1
    pipe.close()


def test_reset_keeps_the_last_gold_publication(tmp_path):
    from midas.pipeline import reset_workspace

    s = make_settings(tmp_path)
    for folder in ("landing/payments", "lake/bronze/payments", "lake/silver/payments", "lake/gold", "meta/plans", "warehouse"):
        (tmp_path / folder).mkdir(parents=True)
    (tmp_path / "lake" / "gold" / "_manifest.json").write_text("{}")
    reset_workspace(s)
    assert (tmp_path / "lake" / "gold" / "_manifest.json").exists()  # ATLAS sigue viendo la publicación anterior
    assert not any((tmp_path / p).exists() for p in ("landing", "lake/bronze", "lake/silver", "meta", "warehouse"))
