import base64
from pathlib import Path
from playwright.sync_api import sync_playwright

def uri(p): return "data:image/png;base64," + base64.b64encode(Path(p).read_bytes()).decode()

BASE = """<html><head><style>
body{margin:0;width:1280px;height:720px;overflow:hidden;background:#000;font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display",sans-serif;-webkit-font-smoothing:antialiased;position:relative;color:#fff}
.glow{position:absolute;width:820px;height:820px;border-radius:50%;left:-260px;top:-240px;background:radial-gradient(circle,rgba(247,200,67,.32),transparent 62%)}
.glow2{position:absolute;width:820px;height:820px;border-radius:50%;right:-300px;bottom:-320px;background:radial-gradient(circle,rgba(41,151,255,.28),transparent 62%)}
.l{position:absolute;left:66px;top:96px;width:600px}
.icon{width:108px;height:108px;border-radius:25px;box-shadow:0 16px 44px rgba(247,200,67,.35)}
h1{font-size:122px;margin:20px 0 0;letter-spacing:-.05em;line-height:1}
h2{font-size:46px;margin:12px 0 0;letter-spacing:-.03em;line-height:1.08;font-weight:700}
.gold{background:linear-gradient(100deg,#f7c843,#c47a3a);-webkit-background-clip:text;color:transparent}
.blue{background:linear-gradient(100deg,#64b5ff,#a58bff);-webkit-background-clip:text;color:transparent}
.badges{margin-top:28px;display:flex;gap:12px}
.badges span{font-size:23px;font-weight:600;padding:9px 18px;border-radius:999px;background:rgba(255,255,255,.12)}
.badges .k{background:#f7c843;color:#111}
.shot{position:absolute;left:650px;top:110px;width:780px;border-radius:18px;overflow:hidden;box-shadow:0 40px 90px rgba(0,0,0,.7),0 0 0 1px rgba(255,255,255,.1);transform:perspective(1600px) rotateY(-14deg) rotateX(3deg);transform-origin:left center}
.shot img{width:780px;display:block}
.play{position:absolute;left:900px;top:300px;width:116px;height:116px;border-radius:50%;background:rgba(255,255,255,.94);box-shadow:0 20px 50px rgba(0,0,0,.5);display:grid;place-items:center}
</style></head><body>%BODY%</body></html>"""
PLAY = '<div class="play"><svg width="44" height="50" viewBox="0 0 46 52"><path d="M6 3 L43 26 L6 49 Z" fill="#c47a3a" stroke="#c47a3a" stroke-width="6" stroke-linejoin="round"/></svg></div>'

midas = f"""<div class="glow"></div><div class="l"><img class="icon" src="{uri('midas.png')}"><h1 class="gold">MIDAS</h1>
<h2>Convierte datos crudos<br><span class="gold">en oro</span></h2>
<div class="badges"><span class="k">Demo · 2 min</span><span>PySpark · dbt · Airflow</span></div></div>
<div class="shot"><img src="{uri('chk/shot_midas.png')}"></div>{PLAY}"""
duo = f"""<div class="glow"></div><div class="glow2"></div><div class="l" style="top:84px"><div style="display:flex;gap:16px">
<img class="icon" src="{uri('midas.png')}"><img class="icon" style="box-shadow:0 16px 44px rgba(41,151,255,.4)" src="{uri('atlas.png')}"></div>
<h1 style="font-size:96px"><span class="gold">MIDAS</span><br><span class="blue">+ ATLAS</span></h1>
<h2 style="font-size:40px">Construir los datos.<br>Y poder confiar en ellos.</h2></div>
<div class="shot" style="top:150px;width:820px"><img style="width:820px" src="{uri('chk/shot_duo.png')}"></div>{PLAY.replace('left:900px', 'left:950px')}"""
with sync_playwright() as p:
    b = p.chromium.launch(channel="chrome", headless=True)
    pg = b.new_page(viewport={"width": 1280, "height": 720})
    for name, body in (("midas", midas), ("duo", duo)):
        pg.set_content(BASE.replace("%BODY%", body))
        pg.wait_for_timeout(400)
        pg.screenshot(path=f"out/miniatura-{name}.png")
    b.close()
