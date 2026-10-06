import sys
from pathlib import Path
from playwright.sync_api import sync_playwright
from recorder import Recorder

SLIDES = {"m_intro": 6, "m_dolor": 13, "m_solucion": 10.5, "m_tecnica": 9, "m_teaser": 6, "m_outro": 7.5,
          "d_intro": 6.5, "d_dolor": 12.5, "d_idea": 9.5, "d_cierre": 9, "d_outro": 8}
only = sys.argv[1:] or list(SLIDES)
html = Path("slides.html").resolve()
with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 810}, device_scale_factor=4 / 3, color_scheme="dark")
    for name in only:
        page = ctx.new_page()
        page.goto("about:blank")
        rec = Recorder(page, f"slide_{name}")
        rec.start()
        page.goto(f"file://{html}?s={name}")
        page.wait_for_timeout(SLIDES[name] * 1000)
        rec.stop()
        page.close()
    browser.close()
