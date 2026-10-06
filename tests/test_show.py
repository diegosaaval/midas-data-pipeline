"""`midas show` cuenta la historia completa: se prueba contra un ATLAS simulado que abre el incidente."""

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import duckdb
import pytest
from conftest import make_settings

from midas.show import Show

DAYS = (date(2026, 9, 1), date(2026, 9, 3))


@contextmanager
def fake_atlas(settings):
    """Responde como ATLAS (y como la pantalla de MIDAS): abre un incidente por cada corrida nueva de MIDAS."""
    state = {"source": "demo"}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, body):
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path in ("/api/meta", "/healthz"):
                return self.reply({"app": "midas", "source": state["source"]})
            if self.path == "/api/incidents":
                if not Path(settings.meta_db).exists():
                    return self.reply([])
                with sqlite3.connect(settings.meta_db) as conn:
                    row = conn.execute("SELECT run_id FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1").fetchone()
                return self.reply([{"id": f"INC-{row[0]}", "run_id": row[0], "title": "tasa", "owner": "Equipo",
                                    "table_title": "Indicadores financieros (MIDAS)"}] if row else [])
            return self.reply({"checks": [{"status": "falla", "message": "Promedio de tasa_aprobacion = 0,59"}]})

        def do_POST(self):
            state["source"] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["name"]
            self.reply({"active": state["source"]})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()


@pytest.fixture(autouse=True)
def keep_log_level():
    level = logging.getLogger().level
    yield
    logging.getLogger().setLevel(level)


def test_show_tells_the_whole_story(spark, tmp_path, capsys):
    s = make_settings(tmp_path)
    opened = []
    with fake_atlas(s) as (url, state):
        show = Show(True, int(url.rsplit(":", 1)[1]), url, s, days=DAYS, spark=spark, sleep=lambda _: None,
                    open_url=opened.append)
        show.ui = url
        show.run()
    out = capsys.readouterr().out
    assert state["source"] == "midas"  # conectó ATLAS a MIDAS
    assert "4 de septiembre: se cae la pasarela" in out and "MIDAS: todo OK" in out
    assert "INC-" in out and "tasa_aprobacion = 0,59" in out
    assert opened[-1].startswith(f"{url}/#run=")  # termina en la corrida que trajo el incidente
    rates = duckdb.sql(f"select tasa_aprobacion from read_parquet('{s.path('gold', 'indicadores_financieros.parquet')}') "
                       "order by fecha").fetchall()
    assert rates[-1][0] < rates[0][0] - 0.2  # el día del incidente la tasa cae


def test_show_without_atlas_still_runs_midas(spark, tmp_path, capsys):
    s = make_settings(tmp_path)
    with fake_atlas(s) as (url, _):
        show = Show(True, int(url.rsplit(":", 1)[1]), "http://127.0.0.1:9", s, days=DAYS[:1] * 2, spark=spark,
                    sleep=lambda _: None, open_url=lambda _: None)
        show.ui = url
        show.run()
    out = capsys.readouterr().out
    assert "ATLAS no responde" in out and "MIDAS: todo OK" in out
