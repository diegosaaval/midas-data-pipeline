"""Vitrina: la pantalla de etapas en internet, sin Spark, repitiendo en bucle una corrida real.

Spark no cabe en un servidor gratuito, así que la demo web reproduce la historia grabada de verdad:
el mes de septiembre (con su reintento) y el 1 de octubre, cuando se cae la pasarela de tarjetas. El
tiempo de la grabación se proyecta al presente, así la pantalla se ve en vivo, y las tablas gold con su
manifiesto se publican en `/vitrina/gold/` según el momento del bucle: ATLAS las valida como si MIDAS
estuviera corriendo.

    midas vitrina --out vitrina      # arma el paquete con las dos últimas corridas (y sus datos)
    MIDAS_VITRINA=vitrina midas ui   # sirve la pantalla en modo vitrina
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

from ..config import Settings
from ..contracts import ENTITIES
from ..metadata import SCHEMA, Metadata
from .api import Store

GOLD_TABLES = ("pagos_gold", "clientes_gold", "contracargos_gold", "indicadores_financieros")
INCREMENTAL = {"pagos_gold": "fecha", "contracargos_gold": "fecha", "indicadores_financieros": "fecha"}


def _ts(value: str | None) -> float | None:
    return datetime.fromisoformat(value).timestamp() if value else None


def _iso(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------- armado del paquete
def build_bundle(settings: Settings, out: Path) -> dict:
    """Empaqueta las dos últimas corridas: la del mes y la del día del incidente, con todo lo que muestran."""
    from ..pipeline import run_quality

    meta = Metadata(settings.meta_db)
    runs = meta.query("SELECT * FROM runs ORDER BY started_at DESC LIMIT 2")[::-1]
    if len(runs) != 2 or any(r["status"] != "success" for r in runs):
        raise ValueError("Hacen falta dos corridas exitosas: el mes y el día del incidente (make show).")
    if out.exists():
        shutil.rmtree(out)
    (out / "plans").mkdir(parents=True)
    ids = tuple(r["run_id"] for r in runs)

    db = sqlite3.connect(out / "meta.db")
    db.executescript(SCHEMA)
    for r in runs:
        db.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)", tuple(r.values()))
    for t in meta.query("SELECT * FROM task_runs WHERE run_id IN (?, ?) ORDER BY rowid", ids):
        db.execute("INSERT INTO task_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tuple(t.values()))
    for e in meta.query("SELECT * FROM schema_events WHERE detected_at >= ? AND detected_at <= ?",
                        (runs[0]["started_at"], runs[1]["finished_at"])):
        db.execute("INSERT INTO schema_events VALUES (?, ?, ?, ?, ?)", tuple(e.values()))
    db.commit()
    db.close()

    dates = [d for r in runs for d in json.loads(r["dates"])]
    plans = Path(settings.meta_db).parent / "plans"
    for d in dates:
        for f in plans.glob(f"*_{d}.txt"):
            shutil.copy(f, out / "plans" / f.name)
    store = Store(settings)
    landing = {f"{e}/{d}": store.landing_rows(e, d) for d in dates for e in ENTITIES}
    (out / "landing.json").write_text(json.dumps(landing))

    # gold: la publicación final (con el 1 de octubre) y la de septiembre (sin él)
    import duckdb

    gold = Path(settings.path("gold"))
    final, month = out / "gold" / "final", out / "gold" / "mes"
    final.mkdir(parents=True)
    month.mkdir(parents=True)
    last_month_day = max(json.loads(runs[0]["dates"]))
    counts = {}
    for name in GOLD_TABLES:
        shutil.copy(gold / f"{name}.parquet", final / f"{name}.parquet")
        src, dst = gold / f"{name}.parquet", month / f"{name}.parquet"
        where = f"WHERE {INCREMENTAL[name]} <= DATE '{last_month_day}'" if name in INCREMENTAL else ""
        duckdb.sql(f"COPY (SELECT * FROM read_parquet('{src}') {where}) TO '{dst}' (FORMAT parquet)")
        counts[name] = duckdb.sql(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0]
    shutil.copy(gold / "_manifest.json", final / "_manifest.json")
    month_manifest = {"run_id": runs[0]["run_id"], "dates": json.loads(runs[0]["dates"]),
                      "published_at": runs[0]["finished_at"], "datasets": counts, "kind": runs[0]["kind"],
                      "quality": run_quality(meta, runs[0]["run_id"])}
    (month / "_manifest.json").write_text(json.dumps(month_manifest, indent=2))
    meta.close()
    return {"runs": list(ids), "dates": len(dates), "plans": len(list((out / "plans").iterdir()))}


# ---------------------------------------------------------------------------------- repetición
class Replay:
    """Proyecta la grabación al presente, en bucle: [pausa][mes][pausa][día del incidente][resultado]."""

    def __init__(self, bundle: Path, speed: float = 1.0, lead: float = 2, gap: float = 6, hold: float = 45,
                 clock=time.time) -> None:
        self.bundle, self.speed, self.clock = bundle, speed, clock
        conn = sqlite3.connect(bundle / "meta.db")
        conn.row_factory = sqlite3.Row
        self.runs = [dict(r) for r in conn.execute("SELECT * FROM runs ORDER BY started_at")]
        self.tasks = [dict(r) for r in conn.execute("SELECT * FROM task_runs ORDER BY rowid")]
        self.events = [dict(r) for r in conn.execute("SELECT * FROM schema_events")]
        conn.close()
        self.landing = json.loads((bundle / "landing.json").read_text())
        month, day = self.runs
        # posición de cada corrida en el bucle, en segundos de la grabación
        self.start = {month["run_id"]: lead * speed}
        self.start[day["run_id"]] = self.start[month["run_id"]] + self._len(month) + gap * speed
        self.origin = {r["run_id"]: _ts(r["started_at"]) for r in self.runs}
        self.loop = self.start[day["run_id"]] + self._len(day) + hold * speed
        self.month, self.day = month, day

    @staticmethod
    def _len(run: dict) -> float:
        return _ts(run["finished_at"]) - _ts(run["started_at"])

    def _pos(self, run_id: str, ts: str) -> float:
        return self.start[run_id] + (_ts(ts) - self.origin[run_id])

    def _state(self) -> tuple[float, float]:
        """(inicio del ciclo actual, posición en el bucle), en segundos de grabación."""
        t = self.clock() * self.speed
        base = (t // self.loop) * self.loop
        return base, t - base

    def _real(self, base: float, pos: float) -> float:
        """Hora real (epoch) en la que la posición `pos` del ciclo que empezó en `base` ocurre o ocurrió."""
        return (base + pos) / self.speed

    def snapshot(self) -> sqlite3.Connection:
        """La base tal como estaría en este momento del bucle: lo que ya pasó, lo que está corriendo."""
        base, tau = self._state()
        wall = lambda pos: _iso(self._real(base, pos))  # noqa: E731
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        for r in self.runs:
            start, end = self.start[r["run_id"]], self._pos(r["run_id"], r["finished_at"])
            if start > tau:
                continue
            done = end <= tau
            conn.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (r["run_id"], r["kind"], r["dates"], r["status"] if done else "running", wall(start),
                          wall(end) if done else None, r["error"] if done else None))
        for t in self.tasks:
            start = self._pos(t["run_id"], t["started_at"])
            if start > tau:
                continue
            row = dict(t, started_at=wall(start))
            if start + (t["duration_s"] or 0) > tau:  # todavía corriendo en este momento del bucle
                row.update(status="running", duration_s=None, rows_read=None, rows_written=None,
                           rows_quarantined=None, partitions=None, details=None, error=None)
            conn.execute("INSERT INTO task_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tuple(row.values()))
        for e in self.events:
            pos = self.start[self.month["run_id"]] + (_ts(e["detected_at"]) - self.origin[self.month["run_id"]])
            if pos <= tau:
                conn.execute("INSERT INTO schema_events VALUES (?, ?, ?, ?, ?)", (*list(e.values())[:4], wall(pos)))
        latest = self.day if self._done(self.day, tau) or not self._done(self.month, tau) else self.month
        conn.execute("INSERT INTO watermarks VALUES ('ingest_date', ?, ?)",
                     (max(json.loads(latest["dates"])), wall(tau)))
        conn.commit()
        return conn

    def _done(self, run: dict, tau: float) -> bool:
        return self._pos(run["run_id"], run["finished_at"]) <= tau

    def publication(self) -> tuple[Path, dict]:
        """Carpeta gold vigente y su manifiesto, con `published_at` en el presente (ATLAS lo ve como nuevo)."""
        base, tau = self._state()
        day_end = self._pos(self.day["run_id"], self.day["finished_at"])
        month_end = self._pos(self.month["run_id"], self.month["finished_at"])
        if month_end <= tau < day_end:
            folder, published = self.bundle / "gold" / "mes", month_end
        else:  # después del incidente, o al inicio del bucle siguiente (sigue vigente la del bucle anterior)
            folder, published = self.bundle / "gold" / "final", day_end if tau >= day_end else day_end - self.loop
        manifest = json.loads((folder / "_manifest.json").read_text())
        manifest["published_at"] = _iso(self._real(base, published))  # misma hora en cada consulta
        return folder, manifest


class ReplayStore(Store):
    """El mismo Store de la pantalla, leyendo la repetición en vez de la base del pipeline."""

    def __init__(self, settings: Settings, replay: Replay) -> None:
        super().__init__(settings)
        self.replay = replay
        self.plans_dir = replay.bundle / "plans"

    def now(self) -> float:
        return self.replay.clock()

    def connect(self) -> sqlite3.Connection:
        return self.replay.snapshot()

    def landing_rows(self, entity: str, day: str) -> int | None:
        return self.replay.landing.get(f"{entity}/{day}")

    def manifest(self) -> dict | None:
        return self.replay.publication()[1]
