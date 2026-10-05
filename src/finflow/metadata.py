"""Registro operativo del pipeline (observabilidad).

Guarda cada corrida y cada tarea: duración, filas leídas/escritas/en cuarentena,
particiones tocadas, intentos y errores; además los eventos de esquema y el
watermark que hace el procesamiento incremental.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY, kind TEXT, dates TEXT, status TEXT, started_at TEXT, finished_at TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT, task TEXT, ingest_date TEXT, attempt INTEGER, status TEXT, started_at TEXT, duration_s REAL,
    rows_read INTEGER, rows_written INTEGER, rows_quarantined INTEGER, partitions TEXT, details TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS schema_events (
    ingest_date TEXT, entity TEXT, kind TEXT, columns TEXT, detected_at TEXT
);
CREATE TABLE IF NOT EXISTS watermarks (name TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
"""


def now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@dataclass
class JobResult:
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    partitions: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


class Metadata:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    def start_run(self, kind: str, dates: list[str]) -> str:
        run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        self.conn.execute("INSERT INTO runs VALUES (?, ?, ?, 'running', ?, NULL, NULL)",
                          (run_id, kind, json.dumps(dates), now()))
        self.conn.commit()
        return run_id

    def finish_run(self, run_id: str, status: str, error: str | None = None) -> None:
        self.conn.execute("UPDATE runs SET status = ?, finished_at = ?, error = ? WHERE run_id = ?",
                          (status, now(), error, run_id))
        self.conn.commit()

    def start_task(self, run_id: str, task: str, ingest_date: str | None, attempt: int, started_at: str) -> int:
        """Marca la tarea como 'running' para que la pantalla de etapas la vea en vivo; devuelve la fila."""
        cur = self.conn.execute(
            "INSERT INTO task_runs (run_id, task, ingest_date, attempt, status, started_at) VALUES (?, ?, ?, ?, 'running', ?)",
            (run_id, task, ingest_date, attempt, started_at))
        self.conn.commit()
        return cur.lastrowid

    def record_task(self, run_id: str, task: str, ingest_date: str | None, attempt: int, status: str,
                    started_at: str, duration: float, result: JobResult | None = None, error: str | None = None,
                    row: int | None = None) -> None:
        r = result or JobResult()
        values = (run_id, task, ingest_date, attempt, status, started_at, round(duration, 3), r.rows_read,
                  r.rows_written, r.rows_quarantined, json.dumps(r.partitions), json.dumps(r.details, default=str), error)
        if row is None:
            self.conn.execute("INSERT INTO task_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
        else:
            self.conn.execute(
                "UPDATE task_runs SET run_id = ?, task = ?, ingest_date = ?, attempt = ?, status = ?, started_at = ?, "
                "duration_s = ?, rows_read = ?, rows_written = ?, rows_quarantined = ?, partitions = ?, details = ?, "
                "error = ? WHERE rowid = ?", (*values, row))
        self.conn.commit()

    def schema_event(self, ingest_date: str, entity: str, kind: str, columns: list[str]) -> None:
        self.conn.execute("INSERT INTO schema_events VALUES (?, ?, ?, ?, ?)",
                          (ingest_date, entity, kind, json.dumps(columns), now()))
        self.conn.commit()

    def get_watermark(self, name: str) -> str | None:
        row = self.conn.execute("SELECT value FROM watermarks WHERE name = ?", (name,)).fetchone()
        return row[0] if row else None

    def set_watermark(self, name: str, value: str) -> None:
        self.conn.execute("INSERT INTO watermarks VALUES (?, ?, ?) ON CONFLICT(name) DO UPDATE SET "
                          "value = excluded.value, updated_at = excluded.updated_at", (name, value, now()))
        self.conn.commit()

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        return [dict(r) for r in self.conn.execute(sql, params)]
