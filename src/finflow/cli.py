"""CLI de FINFLOW. Airflow ejecuta estos mismos comandos.

  finflow generate --start 2026-09-01 --end 2026-09-30   # simula las fuentes (landing)
  finflow run                                           # incremental: solo fechas nuevas
  finflow run --date 2026-09-10                         # re-procesa un día (idempotente)
  finflow backfill --start 2026-09-05 --end 2026-09-08  # re-procesa un rango
  finflow status                                        # últimas corridas y métricas
  finflow ui                                            # pantalla de etapas en http://localhost:8100
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta

from .config import get_settings
from .generator import write_landing
from .pipeline import Pipeline, PipelineError, date_range


def _date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="finflow", description="Pipeline de datos financieros FINFLOW")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="Generar archivos de las fuentes en landing")
    g.add_argument("--start", type=_date)
    g.add_argument("--end", type=_date)
    g.add_argument("--anomaly", action="append", default=[], choices=["unknown_column", "corrupt_lines", "approval_drop"])

    r = sub.add_parser("run", help="Procesar (incremental por defecto)")
    r.add_argument("--date", type=_date)

    b = sub.add_parser("backfill", help="Re-procesar un rango de fechas")
    b.add_argument("--start", type=_date, required=True)
    b.add_argument("--end", type=_date, required=True)
    b.add_argument("--full-refresh", action="store_true", help="Reconstruir los modelos incrementales de dbt")

    t = sub.add_parser("task", help="Ejecutar una sola tarea (lo usa Airflow)")
    t.add_argument("name", help="bronze.<entidad> | silver.<entidad> | features | dbt.build | publish")
    t.add_argument("--date", type=_date)

    st = sub.add_parser("status", help="Últimas corridas y métricas por tarea")
    st.add_argument("--limit", type=int, default=5)

    u = sub.add_parser("ui", help="Pantalla de etapas (solo lectura) en el navegador")
    u.add_argument("--port", type=int, default=8100)
    u.add_argument("--host", default="127.0.0.1")
    u.add_argument("--no-browser", action="store_true")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    s = get_settings()

    if args.cmd == "generate":
        start = args.start or s.epoch
        end = args.end or (date.today() - timedelta(days=1))
        for day in date_range(start, end):
            counts = write_landing(day, s, set(args.anomaly))
            print(f"{day}  " + "  ".join(f"{k}={v}" for k, v in counts.items()))
        return 0

    if args.cmd == "status":
        return _status(s, args.limit)

    if args.cmd == "ui":
        return _ui(args.host, args.port, not args.no_browser)

    pipe = Pipeline(s)
    try:
        if args.cmd == "run":
            run_id = pipe.run_date(args.date) if args.date else pipe.run_incremental()
            print(f"Corrida {run_id or '(nada nuevo)'} OK")
        elif args.cmd == "backfill":
            print(f"Backfill {pipe.backfill(args.start, args.end, args.full_refresh)} OK")
        elif args.cmd == "task":
            return _single_task(pipe, args.name, args.date)
        return 0
    except PipelineError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        pipe.close()


def _single_task(pipe: Pipeline, name: str, day: date | None) -> int:
    run_id = pipe.meta.start_run(f"task:{name}", [day.isoformat()] if day else [])
    try:
        if name == "dbt.build":
            from .pipeline import Task

            pipe.execute(run_id, Task(name, None, pipe.dbt_build))
        elif name == "publish":
            from .pipeline import Task

            pipe.execute(run_id, Task(name, None, lambda: pipe.publish(run_id, [day] if day else [])))
        else:
            if day is None:
                raise PipelineError("--date es obligatorio para tareas por fecha")
            task = next((t for t in pipe.tasks_for(day) if t.name == name), None)
            if task is None:
                raise PipelineError(f"Tarea desconocida: {name}")
            pipe.execute(run_id, task)
            if name == "features":  # última tarea del día: avanza el watermark
                current = pipe.meta.get_watermark("ingest_date")
                if current is None or day.isoformat() > current:
                    pipe.meta.set_watermark("ingest_date", day.isoformat())
        pipe.meta.finish_run(run_id, "success")
        return 0
    except Exception as exc:
        pipe.meta.finish_run(run_id, "failed", str(exc))
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def _status(s, limit: int) -> int:
    from .metadata import Metadata

    meta = Metadata(s.meta_db)
    print(f"Watermark (última fecha de ingesta procesada): {meta.get_watermark('ingest_date') or '—'}\n")
    for run in meta.query("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)):
        dates = json.loads(run["dates"])
        span = f"{dates[0]} … {dates[-1]}" if len(dates) > 1 else (dates[0] if dates else "")
        print(f"{run['run_id']}  {run['kind']:<12} {run['status']:<8} {span}")
        rows = meta.query(
            "SELECT task, COUNT(*) AS n, SUM(duration_s) AS secs, SUM(rows_read) AS r, SUM(rows_written) AS w, "
            "SUM(rows_quarantined) AS q, SUM(status = 'failed') AS fails FROM task_runs WHERE run_id = ? "
            "GROUP BY task ORDER BY MIN(started_at)", (run["run_id"],))
        for t in rows:
            retry = f"  ⟳ {t['fails']} fallo(s) reintentado(s)" if t["fails"] else ""
            print(f"    {t['task']:<20} {t['secs']:>7.1f}s  leídas={t['r']:<8} escritas={t['w']:<8} "
                  f"cuarentena={t['q']}{retry}")
        if run["error"]:
            print(f"    error: {run['error']}")
    events = meta.query("SELECT * FROM schema_events ORDER BY detected_at DESC LIMIT 5")
    if events:
        print("\nEventos de esquema recientes:")
        for e in events:
            print(f"    {e['ingest_date']} {e['entity']}: {e['kind']} {e['columns']}")
    meta.close()
    return 0


def _ui(host: str, port: int, open_browser: bool) -> int:
    try:
        import uvicorn
    except ImportError:
        print("Falta la pantalla: instala con  pip install -e '.[ui]'", file=sys.stderr)
        return 1
    url = f"http://localhost:{port}/"
    print(f"FINFLOW · Etapas en {url}  (Ctrl+C para cerrar)")
    if open_browser:
        import threading
        import webbrowser

        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    uvicorn.run("finflow.ui.api:app", host=host, port=port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
