"""Pantalla de etapas: API de solo lectura sobre los metadatos del pipeline.

Lee `data/meta/midas.db` (runs, task_runs, schema_events, watermarks), los archivos de landing y los
planes de ejecución guardados en `data/meta/plans/`. Nunca escribe: la base se abre en modo solo lectura.

    midas ui            # http://localhost:8100
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from ..config import Settings, get_settings
from ..contracts import ENTITIES

WEB = Path(__file__).parent / "web"
STAGES = (  # (clave, nombre, prefijo de tarea en task_runs)
    ("landing", "Fuentes", None),
    ("bronze", "Bronze", "bronze."),
    ("silver", "Silver", "silver."),
    ("features", "Features", "features"),
    ("dbt", "dbt build", "dbt.build"),
    ("publish", "Publicación", "publish"),
)
KIND_LABELS = {"incremental": "Incremental", "backfill": "Backfill", "date": "Re-proceso"}
STALE_SECONDS = 30 * 60  # una corrida "running" sin actividad en 30 min murió sin cerrar su registro
PLAN_NAME = re.compile(r"^[a-z_]+_\d{4}-\d{2}-\d{2}\.txt$")


def _ts(value: str | None) -> float | None:
    return datetime.fromisoformat(value).timestamp() if value else None


def stage_of(task: str) -> str | None:
    for key, _, prefix in STAGES[1:]:
        if task == prefix or task.startswith(prefix):
            return key
    return None


class Store:
    """Consultas de solo lectura. Cada llamada abre su conexión: el pipeline puede estar escribiendo."""

    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self._lines: dict[str, tuple[float, int]] = {}

    def rows(self, sql: str, params: tuple = ()) -> list[dict]:
        if not Path(self.s.meta_db).exists():
            return []
        conn = sqlite3.connect(f"file:{self.s.meta_db}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        except sqlite3.OperationalError:  # base recién creada, sin tablas todavía
            return []
        finally:
            conn.close()

    # ------------------------------------------------------------------ corridas
    def runs(self, limit: int = 50) -> list[dict]:
        runs = self.rows("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,))
        if not runs:
            return []
        ids = tuple(r["run_id"] for r in runs)
        marks = ",".join("?" * len(ids))
        tasks = self.rows(f"SELECT run_id, task, status, started_at, duration_s FROM task_runs WHERE run_id IN ({marks})", ids)
        by_run: dict[str, list[dict]] = {}
        for t in tasks:
            by_run.setdefault(t["run_id"], []).append(t)
        return [self._run_summary(r, by_run.get(r["run_id"], [])) for r in runs]

    def _run_summary(self, run: dict, tasks: list[dict]) -> dict:
        now = time.time()
        stage_secs = {key: 0.0 for key, _, _ in STAGES[1:]}
        last = _ts(run["started_at"]) or now
        retries = 0
        for t in tasks:
            started = _ts(t["started_at"]) or now
            secs = t["duration_s"] if t["duration_s"] is not None else max(0.0, now - started)
            key = stage_of(t["task"])
            if key:
                stage_secs[key] += secs
            last = max(last, started + secs)
            retries += t["status"] == "failed"
        status = run["status"]
        if status == "running" and now - last > STALE_SECONDS:
            status = "interrupted"
        end = _ts(run["finished_at"]) or (now if status == "running" else last)
        dates = json.loads(run["dates"] or "[]")
        kind = run["kind"]
        return {
            "run_id": run["run_id"], "kind": kind,
            "kind_label": KIND_LABELS.get(kind, "Tarea " + kind.split(":", 1)[-1] if kind.startswith("task:") else kind),
            "status": status, "started_at": run["started_at"], "finished_at": run["finished_at"],
            "duration_s": round(end - (_ts(run["started_at"]) or end), 1), "dates": dates,
            "error": run["error"], "retries": retries,
            "stage_seconds": {k: round(v, 2) for k, v in stage_secs.items()},
        }

    def run_detail(self, run_id: str) -> dict | None:
        run = self.rows("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        if not run:
            return None
        tasks = self.rows("SELECT rowid AS id, * FROM task_runs WHERE run_id = ? ORDER BY rowid", (run_id,))
        summary = self._run_summary(run[0], tasks)
        running = summary["status"] == "running"
        dates = summary["dates"]
        kind = summary["kind"]
        only = stage_of(kind.split(":", 1)[1]) if kind.startswith("task:") else None
        expected = {"bronze": 5 * len(dates), "silver": 5 * len(dates), "features": len(dates), "dbt": 1, "publish": 1}
        if only:
            expected = {k: (1 if k == only else 0) for k in expected}

        stages = [self._landing(dates, only)]
        for key, name, _ in STAGES[1:]:
            stage_tasks = [t for t in tasks if stage_of(t["task"]) == key]
            stages.append(self._stage(key, name, stage_tasks, expected[key], running))
        if summary["status"] in ("failed", "interrupted"):  # lo que no alcanzó a correr
            for st in stages:
                if st["status"] == "waiting":
                    st["status"] = "skipped"

        flows = [stages[1]["rows_read"], stages[2]["rows_read"], stages[3]["rows_read"],
                 stages[2]["rows_written"], stages[5]["rows_written"]]
        started, finished = summary["started_at"], summary["finished_at"] or datetime.now(UTC).isoformat()
        events = self.rows("SELECT * FROM schema_events WHERE detected_at >= ? AND detected_at <= ? ORDER BY detected_at",
                           (started, finished))
        for e in events:
            e["columns"] = json.loads(e["columns"] or "[]")
        return {**summary, "stages": stages, "flows": flows, "quality": self._quality(tasks),
                "schema_events": events, "plans": self._plans(dates) if expected["features"] else [],
                "publish_ok": stages[5]["status"] in ("ok", "retried")}

    def _stage(self, key: str, name: str, tasks: list[dict], expected: int, running: bool) -> dict:
        now = time.time()
        units: dict[tuple, list[dict]] = {}
        for t in tasks:
            units.setdefault((t["task"], t["ingest_date"]), []).append(t)
        done = failed = retries = 0
        current = None
        secs = rows_read = rows_written = rows_q = 0
        partitions: list[str] = []
        errors, items = [], []
        for (task, day), attempts in units.items():
            final = attempts[-1]
            retries += sum(a["status"] == "failed" for a in attempts)
            for a in attempts:
                if a["status"] == "running":
                    a["duration_s"] = round(max(0.0, now - (_ts(a["started_at"]) or now)), 1)
                    current = {"task": task, "date": day, "attempt": a["attempt"]}
                secs += a["duration_s"] or 0
                if a["status"] == "failed":
                    errors.append({"task": task, "date": day, "attempt": a["attempt"], "error": a["error"]})
            if final["status"] == "success":
                done += 1
                rows_read += final["rows_read"] or 0
                rows_written += final["rows_written"] or 0
                rows_q += final["rows_quarantined"] or 0
                partitions += json.loads(final["partitions"] or "[]")
            elif final["status"] == "failed" and not running:
                failed += 1
            items.append({"task": task, "date": day, "status": final["status"], "attempts": len(attempts),
                          "duration_s": round(sum(a["duration_s"] or 0 for a in attempts), 2),
                          "rows_read": final["rows_read"], "rows_written": final["rows_written"],
                          "rows_quarantined": final["rows_quarantined"],
                          "details": json.loads(final["details"]) if final["details"] else None})
        waiting_retry = running and any(a[-1]["status"] == "failed" for a in units.values())
        if failed:
            status = "failed"
        elif current:
            status = "running"
        elif waiting_retry:
            status = "retrying"  # falló y espera el backoff antes del siguiente intento
        elif expected == 0 and not units:
            status = "skipped"
        elif done >= expected and units:
            status = "retried" if retries else "ok"
        elif running and units:
            status = "running"  # entre una tarea y la siguiente (p. ej. otro día del mismo lote)
        else:
            status = "waiting"
        return {"key": key, "name": name, "status": status, "done": done, "expected": expected,
                "duration_s": round(secs, 1), "rows_read": rows_read, "rows_written": rows_written,
                "rows_quarantined": rows_q, "retries": retries, "current": current,
                "partitions": sorted(set(partitions)), "errors": errors, "tasks": items}

    def _landing(self, dates: list[str], only: str | None) -> dict:
        """Archivos que entregaron las fuentes para las fechas de la corrida (el pipeline no los escribe)."""
        present = missing = lines = 0
        files = []
        for day in dates if only in (None, "bronze") else []:
            for entity in ENTITIES:
                f = Path(self.s.landing) / entity / f"ingest_date={day}" / "part-00000.jsonl"
                if f.exists():
                    present += 1
                    n = self._count_lines(f)
                    lines += n
                    files.append({"entity": entity, "date": day, "rows": n})
                else:
                    missing += 1
                    files.append({"entity": entity, "date": day, "rows": None})
        status = "skipped" if not files else ("failed" if missing and not present else "ok")
        return {"key": "landing", "name": "Fuentes", "status": status, "done": present, "expected": present + missing,
                "duration_s": None, "rows_read": 0, "rows_written": lines, "rows_quarantined": 0, "retries": 0,
                "current": None, "partitions": [f"ingest_date={d}" for d in dates], "errors": [],
                "missing": missing, "files": files, "tasks": []}

    def _count_lines(self, path: Path) -> int:
        key, mtime = str(path), path.stat().st_mtime
        cached = self._lines.get(key)
        if cached and cached[0] == mtime:
            return cached[1]
        with open(path, "rb") as fh:
            n = sum(1 for line in fh if line.strip())
        self._lines[key] = (mtime, n)
        return n

    @staticmethod
    def _quality(tasks: list[dict]) -> dict:
        reasons: dict[str, int] = {}
        by_entity: dict[str, int] = {}
        dups = late = corrupt = 0
        unknown: set[str] = set()
        for t in tasks:
            if t["status"] != "success" or not t["details"]:
                continue
            d = json.loads(t["details"])
            entity = t["task"].split(".", 1)[-1]
            if t["task"].startswith("silver."):
                for reason, n in (d.get("quarantine_reasons") or {}).items():
                    if n:
                        reasons[reason] = reasons.get(reason, 0) + n
                if t["rows_quarantined"]:
                    by_entity[entity] = by_entity.get(entity, 0) + t["rows_quarantined"]
                dups += d.get("duplicates_removed") or 0
                late += d.get("late_rows") or 0
            elif t["task"].startswith("bronze."):
                corrupt += d.get("corrupt_lines") or 0
                unknown |= {f"{entity}.{c}" for c in d.get("unknown_columns") or []}
        return {"quarantine_total": sum(by_entity.values()),
                "quarantine_by_reason": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
                "quarantine_by_entity": by_entity, "duplicates_removed": dups, "late_rows": late,
                "corrupt_lines": corrupt, "unknown_columns": sorted(unknown)}

    def _plans(self, dates: list[str]) -> list[str]:
        folder = Path(self.s.meta_db).parent / "plans"
        return [p.name for d in dates for p in sorted(folder.glob(f"*_{d}.txt"))]

    def plan(self, name: str) -> str | None:
        if not PLAN_NAME.match(name):
            return None
        path = Path(self.s.meta_db).parent / "plans" / name
        return path.read_text(encoding="utf-8") if path.exists() else None

    def watermark(self) -> str | None:
        rows = self.rows("SELECT value FROM watermarks WHERE name = 'ingest_date'")
        return rows[0]["value"] if rows else None

    def manifest(self) -> dict | None:
        path = Path(self.s.path("gold", "_manifest.json"))
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None


def _atlas_up(url: str, cache: dict) -> bool:
    if time.time() - cache.get("at", 0) < 5:
        return cache["up"]
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/healthz", timeout=0.6):
            up = True
    except OSError:
        up = False
    cache.update(at=time.time(), up=up)
    return up


def create_app(settings: Settings | None = None) -> FastAPI:
    store = Store(settings or get_settings())
    atlas_url = os.getenv("MIDAS_ATLAS_URL", "http://localhost:8000")
    atlas_cache: dict = {}
    app = FastAPI(title="MIDAS · Etapas", version="0.1.0",
                  description="Solo lectura sobre los metadatos del pipeline MIDAS.")

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    @app.get("/api/meta")
    def meta() -> dict:
        return {"app": "midas", "watermark": store.watermark(), "manifest": store.manifest(),
                "atlas_url": atlas_url, "atlas_up": _atlas_up(atlas_url, atlas_cache),
                "stages": [{"key": k, "name": n} for k, n, _ in STAGES]}

    @app.get("/api/runs")
    def runs(limit: int = 40) -> list[dict]:
        return store.runs(max(1, min(limit, 200)))

    @app.get("/api/runs/{run_id}")
    def run(run_id: str) -> dict:
        if run_id == "latest":
            latest = store.runs(1)
            if not latest:
                raise HTTPException(404, "Todavía no hay corridas")
            run_id = latest[0]["run_id"]
        detail = store.run_detail(run_id)
        if detail is None:
            raise HTTPException(404, f"No existe la corrida {run_id}")
        return detail

    @app.get("/api/plans/{name}", response_class=PlainTextResponse)
    def plan(name: str) -> str:
        text = store.plan(name)
        if text is None:
            raise HTTPException(404, "Plan no encontrado")
        return text

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(WEB / "index.html")

    app.mount("/", StaticFiles(directory=WEB), name="web")
    return app


app = create_app()
