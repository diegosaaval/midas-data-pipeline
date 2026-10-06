"""Runner del pipeline: decide qué procesar y ejecuta cada tarea con reintentos y métricas.

* Incremental: procesa solo las fechas de ingesta posteriores al watermark.
* Por fecha: re-procesa un día puntual (idempotente).
* Backfill: re-procesa un rango histórico, en orden.
Airflow llama a este mismo código tarea por tarea; localmente se puede correr completo.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from .config import Settings, get_settings
from .contracts import ENTITIES
from .generator import landed_dates
from .jobs import bronze, features, silver
from .jobs.bronze import MissingSourceError
from .metadata import JobResult, Metadata, now

log = logging.getLogger("midas")
WATERMARK = "ingest_date"
GOLD_DATASETS = ("clientes_gold", "pagos_gold", "contracargos_gold", "indicadores_financieros")


class PipelineError(RuntimeError):
    pass


class InjectedFailure(RuntimeError):
    """Falla transitoria simulada (MIDAS_FAIL) para demostrar los reintentos."""


@dataclass
class Task:
    name: str
    day: date | None
    fn: Callable[[], JobResult]


class Pipeline:
    def __init__(self, settings: Settings | None = None, spark=None) -> None:
        self.s = settings or get_settings()
        self.meta = Metadata(self.s.meta_db)
        self._spark = spark
        self._faults = _parse_faults(os.getenv("MIDAS_FAIL", ""))
        self.backoff_seconds = float(os.getenv("MIDAS_BACKOFF_SECONDS", "2"))

    @property
    def spark(self):
        if self._spark is None:
            from .spark import get_spark

            self._spark = get_spark(self.s)
        return self._spark

    # ---------------------------------------------------------------- planning
    def pending_dates(self) -> list[date]:
        watermark = self.meta.get_watermark(WATERMARK)
        landed = landed_dates(self.s)
        return [d for d in landed if watermark is None or d > date.fromisoformat(watermark)]

    def tasks_for(self, day: date) -> list[Task]:
        s, sp, meta = self.s, self.spark, self.meta
        tasks = [Task(f"bronze.{e}", day, lambda e=e: bronze.run(sp, s, e, day, meta)) for e in ENTITIES]
        tasks += [Task(f"silver.{e}", day, lambda e=e: silver.run(sp, s, e, day)) for e in ENTITIES]
        tasks.append(Task("features", day, lambda: features.run(sp, s, day)))
        return tasks

    # --------------------------------------------------------------- execution
    def execute(self, run_id: str, task: Task) -> JobResult:
        attempts = self.s.max_retries + 1
        iso = task.day.isoformat() if task.day else None
        for attempt in range(1, attempts + 1):
            started, t0 = now(), time.perf_counter()
            row = self.meta.start_task(run_id, task.name, iso, attempt, started)
            try:
                self._maybe_fail(task.name)
                result = task.fn()
                self.meta.record_task(run_id, task.name, iso, attempt, "success", started,
                                      time.perf_counter() - t0, result, row=row)
                log.info("%s %s ok: leídas=%s escritas=%s cuarentena=%s", task.name, iso or "", result.rows_read,
                         result.rows_written, result.rows_quarantined)
                return result
            except MissingSourceError as exc:  # reintentar no ayuda: la fuente no ha entregado
                self.meta.record_task(run_id, task.name, iso, attempt, "failed", started,
                                      time.perf_counter() - t0, error=str(exc), row=row)
                raise
            except Exception as exc:
                self.meta.record_task(run_id, task.name, iso, attempt, "failed", started,
                                      time.perf_counter() - t0, error=f"{type(exc).__name__}: {exc}", row=row)
                if attempt == attempts:
                    raise
                wait = self.backoff_seconds * 2 ** (attempt - 1)
                log.warning("%s %s falló (intento %s/%s): %s. Reintento en %.0fs", task.name, iso or "", attempt,
                            attempts, exc, wait)
                time.sleep(wait)
            except BaseException:  # Ctrl+C: que la tarea no quede como 'running' para siempre
                self.meta.record_task(run_id, task.name, iso, attempt, "failed", started,
                                      time.perf_counter() - t0, error="interrumpida", row=row)
                raise
        raise AssertionError("unreachable")

    def _maybe_fail(self, name: str) -> None:
        if self._faults.get(name, 0) > 0:
            self._faults[name] -= 1
            raise InjectedFailure(f"falla transitoria simulada en {name}")

    def process(self, dates: list[date], kind: str, full_refresh: bool = False) -> str:
        run_id = self.meta.start_run(kind, [d.isoformat() for d in dates])
        try:
            for day in dates:
                for task in self.tasks_for(day):
                    self.execute(run_id, task)
            self.execute(run_id, Task("dbt.build", None, lambda: self.dbt_build(full_refresh)))
            self.execute(run_id, Task("publish", None, lambda: self.publish(run_id, dates)))
            if kind != "backfill" and dates:
                current = self.meta.get_watermark(WATERMARK)
                newest = max(dates).isoformat()
                if current is None or newest > current:
                    self.meta.set_watermark(WATERMARK, newest)
            self.meta.finish_run(run_id, "success")
            return run_id
        except Exception as exc:
            self.meta.finish_run(run_id, "failed", f"{type(exc).__name__}: {exc}")
            log.debug(traceback.format_exc())
            raise PipelineError(f"Corrida {run_id} falló: {exc}") from exc
        except BaseException:
            self.meta.finish_run(run_id, "failed", "interrumpida")
            raise

    def run_incremental(self) -> str | None:
        dates = self.pending_dates()
        if not dates:
            log.info("Nada nuevo que procesar (watermark=%s)", self.meta.get_watermark(WATERMARK))
            return None
        return self.process(dates, "incremental")

    def run_date(self, day: date) -> str:
        return self.process([day], "date")

    def backfill(self, start: date, end: date, full_refresh: bool = False) -> str:
        dates = [d for d in landed_dates(self.s) if start <= d <= end]
        if not dates:
            raise PipelineError(f"No hay datos en landing entre {start} y {end}")
        return self.process(dates, "backfill", full_refresh)

    # ------------------------------------------------------------------- gold
    def dbt_build(self, full_refresh: bool = False) -> JobResult:
        from dbt.cli.main import dbtRunner

        for entity in ENTITIES:  # dbt no debe fallar porque una fuente aún no tenga datos
            silver.ensure_table(self.spark, self.s, entity)
        Path(self.s.path("gold")).mkdir(parents=True, exist_ok=True)
        Path(self.s.duckdb).parent.mkdir(parents=True, exist_ok=True)
        os.environ["MIDAS_LAKE"] = self.s.lake
        os.environ["MIDAS_DUCKDB"] = self.s.duckdb
        args = ["build", "--project-dir", self.s.dbt_dir, "--profiles-dir", self.s.dbt_dir, "--quiet"]
        if full_refresh:
            args.append("--full-refresh")
        res = dbtRunner().invoke(args)
        statuses: dict[str, int] = {}
        failures = []
        for r in (res.result or []):
            status = str(r.status)
            statuses[status] = statuses.get(status, 0) + 1
            if status in ("error", "fail"):
                failures.append(f"{r.node.name}: {r.message}")
        if not res.success:
            raise PipelineError("dbt build falló: " + "; ".join(failures or [str(res.exception)]))
        return JobResult(rows_written=statuses.get("success", 0), details={"dbt": statuses})

    def publish(self, run_id: str, dates: list[date]) -> JobResult:
        """Manifiesto de la publicación: qué se procesó y cuántas filas tiene cada dataset gold."""
        import duckdb

        counts = {}
        for name in GOLD_DATASETS:
            path = Path(self.s.path("gold", f"{name}.parquet"))
            counts[name] = duckdb.sql(f"select count(*) from read_parquet('{path}')").fetchone()[0] if path.exists() else 0
        manifest = {"run_id": run_id, "dates": [d.isoformat() for d in dates], "published_at": now(),
                    "datasets": counts, "kind": self._run_kind(run_id), "quality": self._run_quality(run_id)}
        Path(self.s.path("gold", "_manifest.json")).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return JobResult(rows_written=sum(counts.values()), details=counts)

    def _run_kind(self, run_id: str) -> str:
        rows = self.meta.query("SELECT kind FROM runs WHERE run_id = ?", (run_id,))
        return rows[0]["kind"] if rows else "unknown"

    def _run_quality(self, run_id: str) -> dict:
        """Lo que silver apartó o corrigió en esta corrida: ATLAS lo muestra junto a sus propios controles."""
        q = {"quarantined": 0, "duplicates_removed": 0, "late_rows": 0, "quarantine_by_reason": {}}
        rows = self.meta.query("SELECT rows_quarantined, details FROM task_runs WHERE run_id = ? AND status = 'success' "
                               "AND task LIKE 'silver.%'", (run_id,))
        for r in rows:
            d = json.loads(r["details"] or "{}")
            q["quarantined"] += r["rows_quarantined"] or 0
            q["duplicates_removed"] += d.get("duplicates_removed") or 0
            q["late_rows"] += d.get("late_rows") or 0
            for reason, n in (d.get("quarantine_reasons") or {}).items():
                if n:
                    q["quarantine_by_reason"][reason] = q["quarantine_by_reason"].get(reason, 0) + n
        return q

    def close(self) -> None:
        self.meta.close()


def _parse_faults(raw: str) -> dict[str, int]:
    """MIDAS_FAIL="silver.payments:1,features:2" -> fallar ese número de intentos."""
    faults = {}
    for item in filter(None, (x.strip() for x in raw.split(","))):
        name, _, n = item.partition(":")
        faults[name] = int(n or 1)
    return faults


def reset_workspace(settings: Settings) -> None:
    """Deja el lago como nuevo pero conserva la última publicación gold y su manifiesto.

    Así quien lee gold (ATLAS) nunca ve un hueco: sigue viendo la publicación anterior hasta que la
    nueva la reemplace, y la reconoce por el run_id nuevo del manifiesto.
    """
    import shutil

    lake = Path(settings.lake)
    for path in [Path(settings.landing), Path(settings.meta_db).parent, Path(settings.duckdb).parent,
                 *(p for p in (lake.iterdir() if lake.exists() else []) if p.name != "gold")]:
        if path.is_dir():
            shutil.rmtree(path)


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]
