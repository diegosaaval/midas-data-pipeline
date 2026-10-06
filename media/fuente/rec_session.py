"""Graba MIDAS y ATLAS a la vez, en vivo, mientras ocurre la historia real. Deja out/midas.mp4, out/atlas.mp4
y out/marks.json (segundo de cada momento, relativo al inicio de la grabación)."""

import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

from recorder import OUT, OVERLAY_JS, Recorder

MIDAS_DIR = Path(__file__).resolve().parents[2]  # raíz del repo
MIDAS_BIN = str(MIDAS_DIR / ".venv/bin/midas")
M, A = "http://localhost:8100", "http://localhost:8000"


def get(url):
    with urllib.request.urlopen(url, timeout=3) as r:
        return json.loads(r.read())


def midas(*args, env=None, wait=True):
    e = {**os.environ, **(env or {})}
    p = subprocess.Popen([MIDAS_BIN, *args], cwd=MIDAS_DIR, env=e, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return p.wait() if wait else p


class Cam:
    def __init__(self, page):
        self.page = page

    def point(self, selector, ms=800, nth=0):
        loc = self.page.locator(selector).nth(nth)
        loc.scroll_into_view_if_needed()
        box = loc.bounding_box()
        x, y = box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
        self.page.evaluate("([x, y, ms]) => window.__rec.move(x, y, ms)", [x, y, ms])
        idle(ms + 150)
        return loc, x, y

    def click(self, selector, nth=0, ms=800, pause=600):
        loc, x, y = self.point(selector, ms, nth)
        self.page.evaluate("([x, y]) => window.__rec.click(x, y)", [x, y])
        loc.click()
        idle(pause)

    def scroll(self, y, wait=1200):
        self.page.evaluate(f"() => scrollTo({{top: {y}, behavior: 'smooth'}})")
        idle(wait)


PAGES = []


def idle(ms):
    """Espera manteniendo vivos los dos screencast."""
    end = time.time() + ms / 1000
    while time.time() < end:
        for p in PAGES:
            p.wait_for_timeout(40)


with sync_playwright() as pw:
    def open_page(url):
        b = pw.chromium.launch(channel="chrome", headless=True)
        ctx = b.new_context(viewport={"width": 1440, "height": 810}, device_scale_factor=4 / 3, color_scheme="dark")
        page = ctx.new_page()
        page.goto(url)
        page.wait_for_timeout(1500)
        page.add_style_tag(content="html{scroll-behavior:smooth}")
        page.evaluate(OVERLAY_JS)
        return page

    midas("reset")
    pm = open_page(M + "/")
    pa = open_page(A + "/#resumen")
    PAGES[:] = [pm, pa]
    cm, ca = Cam(pm), Cam(pa)
    rm, ra = Recorder(pm, "midas"), Recorder(pa, "atlas")
    rm.start()
    ra.start()
    t0 = time.time()
    marks = {}

    def mark(name):
        marks[name] = round(time.time() - t0, 2)
        print(f"  {marks[name]:7.2f}s  {name}", flush=True)

    mark("start")
    idle(3000)
    mark("gen")
    midas("generate", "--start", "2026-09-01", "--end", "2026-09-30")
    mark("run_start")
    run = midas("run", env={"MIDAS_FAIL": "silver.payments:1", "MIDAS_BACKOFF_SECONDS": "4"}, wait=False)
    retry_seen = False
    pa_moved = False
    while run.poll() is None:
        idle(400)
        if not retry_seen:
            try:
                st = {s["key"]: s["status"] for s in get(M + "/api/runs/latest")["stages"]}
                if st.get("silver") == "retrying":
                    mark("retry")
                    retry_seen = True
            except Exception:
                pass
        if not pa_moved and time.time() - t0 > 40:
            ca.point("[data-view=tablas]", ms=1200)
            pa_moved = True
    mark("run_end")
    idle(3500)

    # exploración en MIDAS
    mark("explore")
    cm.click(".stage[data-stage=silver]", pause=1600)
    cm.click("[data-tab=errors]", pause=3000)
    cm.point("#quality", ms=900)
    idle(2500)
    cm.click(".stage[data-stage=features]", pause=1200)
    cm.click("[data-tab=plans]", pause=3500)
    cm.click(".tabs [data-view=historial]", pause=4000)
    cm.click(".tabs [data-view=corrida]", pause=1500)
    mark("explore_end")

    # ATLAS validó la publicación nueva: todo en verde
    mark("atlas_green")
    ca.click("[data-view=tablas]", pause=3500)
    ca.click("[data-view=resumen]", pause=3000)
    mark("atlas_green_end")

    # 1 de octubre: la pasarela de tarjetas se cae
    cm.point(".stage[data-stage=publish]", ms=900)
    mark("inc_gen")
    midas("generate", "--start", "2026-10-01", "--end", "2026-10-01", "--anomaly", "approval_drop")
    mark("inc_run")
    run = midas("run", wait=False)
    while run.poll() is None:
        idle(300)
    mark("inc_done")
    while not any(i["status"] == "abierto" for i in get(A + "/api/incidents")):
        idle(300)
    mark("atlas_inc")
    idle(3500)
    ca.click("[data-view=incidentes]", pause=1500)
    ca.click("[data-inc]", pause=4500)
    mark("inc_detail")
    link = pa.get_by_text("Ver la corrida que la trajo").first
    href = link.get_attribute("href")
    loc, x, y = ca.point("text=Ver la corrida que la trajo", ms=1000)
    pa.evaluate("([x, y]) => window.__rec.click(x, y)", [x, y])
    idle(700)
    mark("link_click")
    pm.evaluate("h => { location.hash = h }", href.split("#", 1)[1])  # misma página: la pantalla abre esa corrida
    idle(5500)
    mark("end")
    rm.stop()
    ra.stop()
    (OUT / "marks.json").write_text(json.dumps(marks, indent=1))
