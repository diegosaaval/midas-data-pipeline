"""`finflow show`: la demo de FINFLOW + ATLAS para presentar en vivo, sin teclear nada.

1. Abre las dos pantallas (etapas de FINFLOW y ATLAS con FINFLOW como fuente).
2. Procesa 30 días desde cero: FINFLOW publica y ATLAS lo valida todo en verde.
3. Llega el 1 de octubre con la pasarela de tarjetas caída: FINFLOW lo publica (cada registro es válido),
   ATLAS lo detecta y abre un incidente con el enlace a la corrida que lo trajo.

    finflow show            # pausa en cada paso hasta que presiones Enter (para presentar)
    finflow show --auto     # pausas fijas (para grabar un video)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
import webbrowser
from datetime import date
from pathlib import Path

from .config import PROJECT_ROOT, Settings, get_settings
from .generator import write_landing
from .pipeline import Pipeline, date_range, reset_workspace

DAYS = (date(2026, 9, 1), date(2026, 9, 30))
INCIDENT_DAY = date(2026, 10, 1)
BOLD, DIM, GREEN, RED, RESET = ("\033[1m", "\033[2m", "\033[32m", "\033[31m", "\033[0m") if sys.stdout.isatty() else ("",) * 5


def _n(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def _get(url: str, timeout: float = 1.5) -> dict | list | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except (OSError, ValueError):
        return None


def _post(url: str, body: dict) -> bool:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60):
            return True
    except OSError:
        return False


class Show:
    def __init__(self, auto: bool, ui_port: int, atlas_url: str, settings: Settings | None = None) -> None:
        self.auto = auto
        self.ui = f"http://localhost:{ui_port}"
        self.ui_port = ui_port
        self.atlas = atlas_url.rstrip("/")
        self.s = settings or get_settings()
        self.server: subprocess.Popen | None = None

    # ------------------------------------------------------------------ narración
    def say(self, text: str = "") -> None:
        print(f"  {text}", flush=True)

    def title(self, n: int, text: str) -> None:
        print(f"\n  {BOLD}{n}. {text}{RESET}\n", flush=True)

    def pause(self, seconds: float = 4) -> None:
        if self.auto:
            time.sleep(seconds)
        else:
            input(f"  {DIM}Enter para continuar…{RESET}")

    # ----------------------------------------------------------------- pantallas
    def ensure_ui(self) -> None:
        if (_get(f"{self.ui}/api/meta") or {}).get("app") == "finflow":
            return
        self.server = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "finflow.ui.api:app", "--host", "127.0.0.1", "--port", str(self.ui_port),
             "--log-level", "warning"], cwd=PROJECT_ROOT)
        for _ in range(60):
            if _get(f"{self.ui}/healthz"):
                return
            time.sleep(0.5)
        raise SystemExit("No arrancó la pantalla de etapas de FINFLOW.")

    def ensure_atlas(self) -> bool:
        meta = _get(f"{self.atlas}/api/meta")
        if meta is None:
            self.say(f"{RED}ATLAS no responde en {self.atlas}.{RESET} Ábrelo en otra terminal y vuelve a correr el show:")
            self.say("    cd ../atlas-one && ./start.sh --fuente finflow")
            return False
        if meta.get("source") != "finflow":
            self.say("Conectando ATLAS a FINFLOW (botón Fuente de datos)…")
            if not _post(f"{self.atlas}/api/source", {"name": "finflow"}):
                self.say(f"{RED}ATLAS no pudo conectarse a FINFLOW.{RESET}")
                return False
        return True

    def atlas_incidents(self) -> list[dict]:
        return _get(f"{self.atlas}/api/incidents") or []

    # ----------------------------------------------------------------- guion
    def run(self) -> None:
        logging.getLogger().setLevel(logging.WARNING)  # en la terminal solo la narración; el detalle está en la pantalla
        print(f"\n  {BOLD}FINFLOW + ATLAS{RESET} · construir los datos y poder confiar en ellos\n")
        self.ensure_ui()
        atlas_ok = self.ensure_atlas()
        webbrowser.open(f"{self.ui}/")
        if atlas_ok:
            time.sleep(0.8)
            webbrowser.open(f"{self.atlas}/")
        self.say("Abrí las dos pantallas: FINFLOW (etapas del pipeline) y ATLAS (monitor de calidad).")
        self.pause(3)

        self.title(1, "FINFLOW procesa un mes de una fintech")
        self.say("Cada día llegan archivos de 5 sistemas: clientes, comercios, pagos, devoluciones y contracargos.")
        self.say("Vienen con problemas reales: pagos repetidos, montos inválidos, datos que llegan tarde.")
        reset_workspace(self.s)  # conserva la última publicación gold: ATLAS nunca ve un hueco
        for day in date_range(*DAYS):
            write_landing(day, self.s)
        self.say("Fuentes listas: del 1 al 30 de septiembre. Mira la pantalla de FINFLOW: cada etapa se enciende.")
        t0 = time.time()
        pipe = Pipeline(self.s)
        try:
            run_id = pipe.run_incremental()
            q = json.loads(Path(self.s.path("gold", "_manifest.json")).read_text())["quality"]
        finally:
            pipe.close()
        self.say(f"{GREEN}Publicado en {time.time() - t0:.0f} s{RESET}: {_n(q['quarantined'])} registros en cuarentena "
                 f"con su motivo, {_n(q['duplicates_removed'])} duplicados eliminados, {_n(q['late_rows'])} filas tardías "
                 "integradas a su fecha.")
        self.pause()

        if atlas_ok:
            self.title(2, "ATLAS valida lo que FINFLOW publicó")
            time.sleep(3)
            self.say("ATLAS vio el manifiesto nuevo y validó las 4 tablas gold con sus 23 reglas.")
            self.say(f"Pantalla: {self.atlas}/#tablas")
            self.pause()

        self.title(3, "1 de octubre: se cae la pasarela de tarjetas")
        self.say("2 de cada 3 pagos con tarjeta salen rechazados. Cada registro es válido: estado 'declined',")
        self.say("monto correcto, cliente existente. Ninguna regla por registro tiene nada que objetar.")
        known = {i["id"] for i in self.atlas_incidents()} if atlas_ok else set()
        write_landing(INCIDENT_DAY, self.s, {"approval_drop"})
        pipe = Pipeline(self.s)
        try:
            run_id = pipe.run_incremental()
        finally:
            pipe.close()
        q = json.loads(Path(self.s.path("gold", "_manifest.json")).read_text())["quality"]
        self.say(f"{GREEN}FINFLOW: todo OK{RESET}. Apartó {_n(q['quarantined'])} registros inválidos, como cualquier día;")
        self.say("los pagos rechazados son válidos, así que pasan y se publican.")
        if not atlas_ok:
            return self.finish(run_id)
        self.pause(2)

        self.title(4, "ATLAS detecta lo que ninguna regla podía ver")
        incident = None
        for _ in range(60):
            incident = next((i for i in self.atlas_incidents() if i["id"] not in known and i.get("run_id") == run_id), None)
            if incident:
                break
            time.sleep(1)
        if incident:
            detail = _get(f"{self.atlas}/api/incidents/{incident['id']}") or {}
            msg = next((c["message"] for c in detail.get("checks", []) if c.get("status") != "ok"), incident["title"])
            self.say(f"{RED}{incident['id']} · {incident['table_title']}{RESET}")
            self.say(msg)
            self.say(f"Responsable avisado: {incident.get('owner', '')}")
            webbrowser.open(f"{self.atlas}/#incidentes")
            self.pause()
            self.title(5, "Del incidente a la corrida que lo trajo")
            self.say("En ATLAS, «Ver la corrida que la trajo» abre FINFLOW justo en esa corrida:")
            self.say(f"{self.ui}/#run={run_id}")
            webbrowser.open(f"{self.ui}/#run={run_id}")
        else:
            self.say("ATLAS todavía no abrió el incidente; revisa su pestaña Incidentes.")
        self.finish(run_id)

    def finish(self, run_id: str | None) -> None:
        moral = "FINFLOW detiene lo que incumple las reglas que conoce; ATLAS detecta lo que ninguna regla puede ver."
        print(f"\n  {BOLD}{moral}{RESET}\n")
        if self.server:
            self.say("La pantalla de FINFLOW sigue abierta. Ctrl+C para cerrarla.")
            try:
                self.server.wait()
            except KeyboardInterrupt:
                self.server.terminate()


def main(auto: bool = False, ui_port: int = 8100) -> int:
    Show(auto, ui_port, os.getenv("FINFLOW_ATLAS_URL", "http://localhost:8000")).run()
    return 0
