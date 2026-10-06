"""Lanzador de MIDAS: prepara todo y abre la pantalla de etapas. Lo usan «Iniciar MIDAS.command/.bat».

    python run.py                 # instala si hace falta, abre la pantalla (y corre la demo si no hay datos)
    python run.py --demo          # corre desde cero la demo de 30 días mientras la ves en vivo
    python run.py --show          # la demo de MIDAS + ATLAS: enciende ATLAS y cuenta la historia paso a paso
    python run.py --test          # corre las pruebas
    python run.py --reinstall     # rehace el entorno
    python run.py --sin-acceso    # no crear el acceso directo en el escritorio

Solo usa la librería estándar, así funciona con un Python recién instalado.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import venv
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV = ROOT / ".venv"
MARKER = VENV / ".midas-installed"
SHORTCUT_MARKER = VENV / ".midas-acceso"
WINDOWS = os.name == "nt"
MAC = sys.platform == "darwin"
VENV_PY = VENV / ("Scripts/python.exe" if WINDOWS else "bin/python")
WEB = ROOT / "src" / "midas" / "ui" / "web"
DEMO = ("2026-09-01", "2026-09-30")


def say(msg: str) -> None:
    print(f"  {msg}", flush=True)


def fail(msg: str) -> None:
    print(f"\n  Algo falló: {msg}\n", flush=True)
    sys.exit(1)


# ------------------------------------------------------------------ entorno
def deps_fingerprint() -> str:
    return hashlib.sha256((ROOT / "pyproject.toml").read_bytes()).hexdigest()


SUPPORTED = ((3, 11), (3, 14))  # Spark 4 y dbt: Python 3.11 a 3.13


def venv_python_ok() -> bool:
    """El intérprete del entorno corre y su versión sirve para Spark."""
    if not VENV_PY.exists():
        return False
    check = f"import sys; sys.exit(0 if {SUPPORTED[0]} <= sys.version_info[:2] < {SUPPORTED[1]} else 1)"
    probe = subprocess.run([str(VENV_PY), "-c", check], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return probe.returncode == 0


def find_uv() -> str | None:
    """uv instala en segundos y trae su propio Python 3.12 si el del equipo es muy nuevo (3.14)."""
    for cand in (shutil.which("uv"), Path.home() / ".local" / "bin" / "uv", Path.home() / ".cargo" / "bin" / "uv",
                 ROOT / ".tools" / ("Scripts/uv.exe" if WINDOWS else "bin/uv")):
        if cand and Path(cand).exists():
            return str(cand)
    return None


def bootstrap_uv() -> str:
    """Instala uv en .tools/ (dentro del proyecto, sin tocar el Python del equipo)."""
    say("Tu Python es muy nuevo para Spark: descargo un Python 3.12 solo para MIDAS…")
    tools = ROOT / ".tools"
    venv.create(tools, with_pip=True)
    tools_py = tools / ("Scripts/python.exe" if WINDOWS else "bin/python")
    subprocess.run([str(tools_py), "-m", "pip", "install", "-q", "--disable-pip-version-check", "uv"], cwd=ROOT, check=False)
    uv = find_uv()
    if not uv:
        fail("no pude preparar Python 3.12. Instala Python 3.12 desde https://www.python.org/downloads/ y vuelve a abrir.")
    return uv


def ensure_env(reinstall: bool) -> None:
    if sys.version_info < SUPPORTED[0]:  # noqa: UP036 - corre con el Python que tenga la persona
        fail(f"MIDAS necesita Python 3.11 o más reciente (tienes {sys.version.split()[0]}). "
             "Descárgalo en https://www.python.org/downloads/")

    if reinstall and VENV.exists():
        say("Borrando el entorno anterior…")
        shutil.rmtree(VENV)

    if VENV.exists() and not venv_python_ok():
        say("El entorno no sirve (incompleto o con otra versión de Python), lo creo de nuevo…")
        shutil.rmtree(VENV, ignore_errors=True)

    uv = find_uv()
    if not VENV_PY.exists():
        if SUPPORTED[0] <= sys.version_info[:2] < SUPPORTED[1] and not uv:
            say(f"Preparando MIDAS por primera vez (Python {sys.version.split()[0]})…")
            venv.create(VENV, with_pip=True)
        else:
            uv = uv or bootstrap_uv()
            say("Preparando MIDAS por primera vez (Python 3.12)…")
            subprocess.run([uv, "venv", "--quiet", "--python", "3.12", str(VENV)], cwd=ROOT, check=False)
        if not venv_python_ok():
            fail("no se pudo crear el entorno. Instala Python 3.12 desde https://www.python.org/downloads/ y vuelve a abrir.")

    if MARKER.exists() and MARKER.read_text() == deps_fingerprint():
        return

    say("Instalando componentes, solo la primera vez (necesita internet, unos minutos: Spark pesa ~300 MB)…")
    if uv:
        cmd = [uv, "pip", "install", "--quiet", "--python", str(VENV_PY), "-e", ".[ui,dev]"]
    else:
        subprocess.run([str(VENV_PY), "-m", "ensurepip", "--upgrade"], cwd=ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        cmd = [str(VENV_PY), "-m", "pip", "install", "-q", "--disable-pip-version-check", "-e", ".[ui,dev]"]
    if subprocess.run(cmd, cwd=ROOT).returncode != 0:
        fail("no se pudieron instalar los componentes. Revisa tu conexión y vuelve a abrir "
             "(o usa --reinstall).")
    MARKER.write_text(deps_fingerprint())


# --------------------------------------------------------- acceso directo
def create_shortcut() -> str | None:
    """Acceso directo «MIDAS» con ícono en el escritorio (Windows .lnk, Mac .app, Linux .desktop)."""
    desktop = Path.home() / "Desktop"
    if not desktop.is_dir():
        return None
    try:
        if WINDOWS:
            bat, ico = ROOT / "Iniciar MIDAS.bat", WEB / "icon.ico"
            q = lambda p: str(p).replace("'", "''")  # noqa: E731 - comillas de PowerShell
            ps = ("$w = New-Object -ComObject WScript.Shell; "
                  "$s = $w.CreateShortcut([IO.Path]::Combine([Environment]::GetFolderPath('Desktop'), 'MIDAS.lnk')); "
                  f"$s.TargetPath = '{q(bat)}'; $s.WorkingDirectory = '{q(ROOT)}'; $s.IconLocation = '{q(ico)}'; "
                  "$s.Description = 'MIDAS · Etapas del pipeline'; $s.Save()")
            subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return str(desktop / "MIDAS.lnk")
        if MAC:
            app = desktop / "MIDAS.app"
            (app / "Contents" / "MacOS").mkdir(parents=True, exist_ok=True)
            (app / "Contents" / "Resources").mkdir(parents=True, exist_ok=True)
            shutil.copy(WEB / "icon.icns", app / "Contents" / "Resources" / "icon.icns")
            script = app / "Contents" / "MacOS" / "MIDAS"
            script.write_text(f"#!/bin/bash\nopen -a Terminal {shlex.quote(str(ROOT / 'Iniciar MIDAS.command'))}\n")
            script.chmod(0o755)
            with open(app / "Contents" / "Info.plist", "wb") as fh:
                plistlib.dump({
                    "CFBundleName": "MIDAS", "CFBundleDisplayName": "MIDAS",
                    "CFBundleIdentifier": "io.github.diegosaaval.midas", "CFBundleExecutable": "MIDAS",
                    "CFBundleIconFile": "icon", "CFBundlePackageType": "APPL", "CFBundleVersion": "1",
                    "LSMinimumSystemVersion": "10.13",
                }, fh)
            os.utime(app)  # que el Finder refresque el ícono
            return str(app)
        entry = desktop / "midas.desktop"
        entry.write_text(
            "[Desktop Entry]\nType=Application\nName=MIDAS\nComment=Etapas del pipeline de datos\n"
            f"Exec=bash -c 'cd {shlex.quote(str(ROOT))} && python3 run.py'\n"
            f"Icon={WEB / 'icon-512.png'}\nTerminal=true\n")
        entry.chmod(0o755)
        return str(entry)
    except Exception as exc:  # nunca impedir que MIDAS arranque por un acceso directo
        say(f"(No pude crear el acceso directo: {exc})")
        return None


def maybe_shortcut(skip: bool) -> None:
    mac_app = Path.home() / "Desktop" / "MIDAS.app"
    if skip:
        return
    if not SHORTCUT_MARKER.exists():
        made = create_shortcut()
        SHORTCUT_MARKER.write_text("ok")
        if made:
            say("Creé un acceso directo «MIDAS» en tu escritorio, con su ícono.")
    elif MAC and mac_app.exists():
        create_shortcut()  # si moviste la carpeta del proyecto, el acceso se actualiza solo


# ------------------------------------------------------------------- red
def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def midas_running(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/meta", timeout=1) as r:
            return json.loads(r.read()).get("app") == "midas"
    except (OSError, ValueError):
        return False


def free_port(preferred: int) -> int:
    for port in range(preferred, preferred + 50):
        if not port_in_use(port):
            return port
    fail(f"no hay puertos libres entre {preferred} y {preferred + 49}.")
    return preferred


def wait_until_ready(url: str, proc: subprocess.Popen, timeout: float = 90) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with urllib.request.urlopen(url + "/healthz", timeout=1):
                return True
        except OSError:
            time.sleep(0.4)
    return False


# ------------------------------------------------------------------ demo
def has_java() -> bool:
    try:
        return subprocess.run(["java", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    except OSError:
        return False


def run_demo(fresh: bool) -> None:
    """Genera 30 días de fuentes y corre el pipeline: la pantalla lo muestra en vivo."""
    if not has_java():
        say("Para correr el pipeline hace falta Java 17 o más reciente (Spark lo necesita).")
        say("Mac: brew install openjdk@17 · Windows: winget install EclipseAdoptium.Temurin.17.JRE")
        return
    midas = [str(VENV_PY), "-m", "midas.cli"]
    if fresh:  # conserva la última publicación gold: ATLAS no ve un hueco mientras se reconstruye
        subprocess.run([*midas, "reset"], cwd=ROOT, stdout=subprocess.DEVNULL)
    say(f"Generando las fuentes ({DEMO[0]} a {DEMO[1]})…")
    subprocess.run([*midas, "generate", "--start", DEMO[0], "--end", DEMO[1]], cwd=ROOT, stdout=subprocess.DEVNULL)
    say("Corriendo el pipeline (unos 3 minutos). Míralo avanzar en el navegador.")
    subprocess.run([*midas, "run"], cwd=ROOT)
    say("Listo. Las tablas gold quedaron en data/lake/gold (ATLAS las puede validar).")


# ------------------------------------------------------------- MIDAS + ATLAS
ATLAS_URL = os.getenv("MIDAS_ATLAS_URL", "http://localhost:8000")
ATLAS_DIRS = ("atlas-data-quality", "atlas-one", "atlas")  # el repo de ATLAS clonado al lado de este


def find_atlas() -> Path | None:
    candidates = [Path(os.environ["ATLAS_DIR"])] if os.getenv("ATLAS_DIR") else [ROOT.parent / d for d in ATLAS_DIRS]
    return next((d for d in candidates if (d / "run.py").exists() and (d / "conectores").is_dir()), None)


def atlas_running() -> bool:
    try:
        with urllib.request.urlopen(f"{ATLAS_URL}/healthz", timeout=1):
            return True
    except OSError:
        return False


def run_show(auto: bool) -> None:
    """Enciende ATLAS si hace falta (con MIDAS como fuente) y corre `midas show`."""
    if not has_java():
        run_demo(fresh=False)  # explica cómo instalar Java
        return
    atlas = None
    if not atlas_running():
        folder = find_atlas()
        if folder is None:
            say("No encuentro ATLAS al lado de este proyecto. Clónalo en la misma carpeta:")
            say("    git clone https://github.com/diegosaaval/atlas-data-quality.git")
            say("La demo sigue solo con MIDAS.")
        else:
            say("Encendiendo ATLAS (la primera vez se instala, ~1 minuto)…")
            source = os.getenv("MIDAS_ATLAS_SOURCE", "midas")
            atlas = subprocess.Popen([sys.executable, "run.py", "--fuente", source, "--no-browser", "--sin-acceso"],
                                     cwd=folder, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deadline = time.time() + 300
            while time.time() < deadline and atlas.poll() is None and not atlas_running():
                time.sleep(1)
            if not atlas_running():
                say(f"ATLAS no arrancó. Ábrelo con doble clic en «Iniciar ATLAS» ({folder}) y vuelve a intentar.")
    try:
        subprocess.run([str(VENV_PY), "-m", "midas.cli", "show", *(["--auto"] if auto else [])], cwd=ROOT)
    except KeyboardInterrupt:
        pass
    finally:
        if atlas and atlas.poll() is None:
            say("Apagando ATLAS…")
            atlas.terminate()


# --------------------------------------------------------------- servidor
def serve(port: int, open_browser: bool, demo: bool | None) -> None:
    """demo: True = borrar y correr la demo; None = correrla solo si no hay datos; False = no correrla."""
    first_time = not (ROOT / "data" / "meta" / "midas.db").exists()
    if midas_running(port):
        say("MIDAS ya está abierto. Abriendo el navegador…")
        webbrowser.open(f"http://localhost:{port}/")
        if demo or (demo is None and first_time):
            run_demo(fresh=bool(demo))
        return
    port = free_port(port)
    local = f"http://localhost:{port}"
    say("Encendiendo la pantalla de etapas…")
    proc = subprocess.Popen(
        [str(VENV_PY), "-m", "uvicorn", "midas.ui.api:app", "--host", "127.0.0.1", "--port", str(port),
         "--log-level", "warning"],
        cwd=ROOT,
    )
    try:
        if not wait_until_ready(local, proc):
            fail("el servidor no arrancó. Revisa los mensajes de arriba.")
        print()
        print("  ================================================")
        print("    MIDAS · Etapas del pipeline")
        print("  ================================================")
        print()
        print(f"    En este equipo:        {local}")
        print(f"    Documentación de API:  {local}/docs")
        print()
        print("    NO cierres esta ventana: si la cierras, la pantalla se apaga.")
        print("    (Puedes minimizarla.)")
        print()
        if open_browser:
            threading.Timer(0.3, webbrowser.open, args=(local + "/",)).start()
        if demo or (demo is None and first_time):
            time.sleep(1.5)  # que el navegador alcance a abrir antes de que empiece la corrida
            run_demo(fresh=bool(demo))
        proc.wait()
    except KeyboardInterrupt:
        say("Apagando MIDAS…")
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def main() -> None:
    parser = argparse.ArgumentParser(description="Lanzador de MIDAS")
    parser.add_argument("--port", type=int, default=int(os.getenv("MIDAS_UI_PORT", "8100")))
    parser.add_argument("--no-browser", action="store_true", help="no abrir el navegador")
    parser.add_argument("--demo", action="store_true", default=None,
                        help="borrar data/ y correr la demo de 30 días mientras la ves en vivo")
    parser.add_argument("--sin-demo", dest="demo", action="store_false", help="nunca correr la demo")
    parser.add_argument("--show", action="store_true", help="la demo de MIDAS + ATLAS, paso a paso")
    parser.add_argument("--auto", action="store_true", help="con --show: pausas fijas en vez de Enter (para grabar)")
    parser.add_argument("--test", action="store_true", help="correr las pruebas en vez del servidor")
    parser.add_argument("--reinstall", action="store_true", help="rehacer el entorno")
    parser.add_argument("--sin-acceso", action="store_true", help="no crear el acceso directo en el escritorio")
    args = parser.parse_args()

    sys.stdout.reconfigure(line_buffering=True)  # los mensajes salen en el acto, también en Windows
    os.chdir(ROOT)
    ensure_env(args.reinstall)
    if args.test:
        sys.exit(subprocess.run([str(VENV_PY), "-m", "pytest"], cwd=ROOT).returncode)
    if args.show:
        run_show(args.auto)
        return
    maybe_shortcut(args.sin_acceso)
    serve(args.port, not args.no_browser, args.demo)


if __name__ == "__main__":
    main()
