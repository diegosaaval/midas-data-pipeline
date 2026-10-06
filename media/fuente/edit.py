"""Monta los dos videos: diapositivas + tomas reales (con subtítulos, velocidad y pantalla dividida) y fundidos.

    python edit.py midas        -> out/midas_mute.mp4 + out/midas_timeline.json
    python edit.py duo          -> out/duo_mute.mp4   + out/duo_timeline.json
"""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import imageio_ffmpeg
from playwright.sync_api import sync_playwright

F = imageio_ffmpeg.get_ffmpeg_exe()
HERE = Path(__file__).parent
OUT = HERE / "out"
CAPS = OUT / "caps"
CAPS.mkdir(exist_ok=True)
FADE = 0.6

CAP_CSS = """
html,body{margin:0;width:1920px;height:1080px;background:transparent;font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display",sans-serif;-webkit-font-smoothing:antialiased}
.cap{position:absolute;left:50%;bottom:54px;transform:translateX(-50%);max-width:1500px;width:max-content;background:rgba(18,18,20,.86);
 border:1px solid rgba(255,255,255,.12);color:#f5f5f7;border-radius:22px;padding:20px 34px;font:600 31px/1.35 -apple-system,BlinkMacSystemFont,"SF Pro Display",sans-serif;letter-spacing:-.015em;
 text-align:center;box-shadow:0 24px 60px rgba(0,0,0,.45)}
.cap b{color:#f7c843;font-weight:700} .cap b.b{color:#64b5ff} .cap b.r{color:#ff6961} .cap b.g{color:#30d158}
.badge{position:absolute;right:46px;top:42px;background:rgba(247,200,67,.95);color:#111;border-radius:999px;padding:10px 22px;
 font:700 26px ui-rounded,"SF Pro Rounded",-apple-system,sans-serif;letter-spacing:-.01em;box-shadow:0 10px 30px rgba(0,0,0,.4)}
"""

SPLIT_HTML = """<html><head><style>
html,body{margin:0;width:1920px;height:1080px;background:#000;font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display",sans-serif;-webkit-font-smoothing:antialiased;color:#f5f5f7;overflow:hidden}
.g1,.g2{position:absolute;width:1100px;height:1100px;border-radius:50%;top:-300px}
.g1{left:-350px;background:radial-gradient(circle,rgba(247,200,67,.16),transparent 62%)}
.g2{right:-350px;background:radial-gradient(circle,rgba(41,151,255,.16),transparent 62%)}
.lab{position:absolute;top:150px;display:flex;align-items:center;gap:16px;font-size:38px;font-weight:700;letter-spacing:-.03em}
.lab img{width:54px;height:54px;border-radius:13px} .lab span{color:#98989d;font-weight:600}
.l{left:13px} .r{left:966px}
.gold{background:linear-gradient(100deg,#f7c843,#c47a3a);-webkit-background-clip:text;color:transparent}
.blue{background:linear-gradient(100deg,#64b5ff,#a58bff);-webkit-background-clip:text;color:transparent}
.frame{position:absolute;top:228px;width:940px;height:529px;border-radius:18px;box-shadow:0 0 0 1px rgba(255,255,255,.12),0 30px 80px rgba(0,0,0,.6)}
</style></head><body><div class="g1"></div><div class="g2"></div>
<div class="lab l"><img src="%MIDAS%"><b class="gold">MIDAS</b><span>· produce</span></div>
<div class="lab r"><img src="%ATLAS%"><b class="blue">ATLAS</b><span>· verifica</span></div>
<div class="frame" style="left:13px"></div><div class="frame" style="left:966px"></div></body></html>"""


def render_png(html: str, name: str, transparent=True) -> Path:
    path = CAPS / f"{name}.png"
    if path.exists():
        return path
    with sync_playwright() as p:
        b = p.chromium.launch(channel="chrome", headless=True)
        pg = b.new_page(viewport={"width": 1920, "height": 1080})
        pg.set_content(html)
        pg.wait_for_timeout(300)
        pg.screenshot(path=str(path), omit_background=transparent)
        b.close()
    return path


def caption_png(text: str) -> Path:
    key = hashlib.md5(text.encode()).hexdigest()[:10]
    return render_png(f"<html><head><style>{CAP_CSS}</style></head><body><div class='cap'>{text}</div></body></html>", f"cap_{key}")


def badge_png(text: str) -> Path:
    key = hashlib.md5(text.encode()).hexdigest()[:10]
    return render_png(f"<html><head><style>{CAP_CSS}</style></head><body><div class='badge'>{text}</div></body></html>", f"badge_{key}")


def run(args):
    subprocess.run([F, "-y", "-loglevel", "error", *args], check=True)


def overlays(base_label: str, n_inputs: int, dur: float, caps, badge):
    """Encadena subtítulos (y la marca de velocidad) sobre base_label. Devuelve (inputs, filtros, etiqueta final)."""
    inputs, filters, label = [], [], base_label
    items = [(caption_png(t), a, b) for a, b, t in caps]
    if badge:
        items.append((badge_png(badge), 0.2, dur - 0.2))
    for k, (png, a, b) in enumerate(items):
        idx = n_inputs + k
        inputs += ["-loop", "1", "-t", f"{dur:.3f}", "-i", str(png)]
        filters.append(f"[{idx}:v]format=rgba,fade=t=in:st={a}:d=0.35:alpha=1,fade=t=out:st={max(a, b - 0.35)}:d=0.35:alpha=1[c{k}]")
        out = f"o{k}"
        filters.append(f"[{label}][c{k}]overlay=0:0:enable='between(t,{a},{b})'[{out}]")
        label = out
    return inputs, filters, label


def clip(name, src, a, b, speed=1.0, caps=(), badge=None):
    dur = (b - a) / speed
    inputs = ["-ss", f"{a:.3f}", "-t", f"{b - a:.3f}", "-i", str(OUT / f"{src}.mp4")]
    filters = [f"[0:v]setpts=(PTS-STARTPTS)/{speed},fps=30,scale=1920:1080,format=yuv420p[v0]"]
    ci, cf, last = overlays("v0", 1, dur, caps, badge)
    run([*inputs, *ci, "-filter_complex", ";".join(filters + cf), "-map", f"[{last}]", "-t", f"{dur:.3f}",
         "-c:v", "libx264", "-crf", "17", "-preset", "medium", "-r", "30", "-pix_fmt", "yuv420p", str(OUT / f"seg_{name}.mp4")])
    return OUT / f"seg_{name}.mp4"


def split(name, a, b, caps=()):
    dur = b - a
    import base64

    uri = {k: "data:image/png;base64," + base64.b64encode((HERE / f"{k}.png").read_bytes()).decode() for k in ("midas", "atlas")}
    bg = render_png(SPLIT_HTML.replace("%MIDAS%", uri["midas"]).replace("%ATLAS%", uri["atlas"]), "split_bg2", transparent=False)
    inputs = ["-loop", "1", "-t", f"{dur:.3f}", "-i", str(bg),
              "-ss", f"{a:.3f}", "-t", f"{dur:.3f}", "-i", str(OUT / "midas.mp4"),
              "-ss", f"{a:.3f}", "-t", f"{dur:.3f}", "-i", str(OUT / "atlas.mp4")]
    filters = ["[0:v]fps=30,format=yuv420p[bg]",
               "[1:v]setpts=PTS-STARTPTS,fps=30,scale=940:529[m]",
               "[2:v]setpts=PTS-STARTPTS,fps=30,scale=940:529[at]",
               "[bg][m]overlay=13:228[t1]", "[t1][at]overlay=966:228[v0]"]
    ci, cf, last = overlays("v0", 3, dur, caps, None)
    run([*inputs, *ci, "-filter_complex", ";".join(filters + cf), "-map", f"[{last}]", "-t", f"{dur:.3f}",
         "-c:v", "libx264", "-crf", "17", "-preset", "medium", "-r", "30", "-pix_fmt", "yuv420p", str(OUT / f"seg_{name}.mp4")])
    return OUT / f"seg_{name}.mp4"


def dur(path):
    out = subprocess.run([F, "-i", str(path)], capture_output=True, text=True).stderr
    h, m, s = out.split("Duration: ")[1].split(",")[0].split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)


def assemble(video, parts):
    durs = [dur(p) for _, p in parts]
    inputs = []
    for _, p in parts:
        inputs += ["-i", str(p)]
    chain, offset, starts, filters = "[0:v]", 0.0, [0.0], []
    for i in range(1, len(parts)):
        offset += durs[i - 1] - FADE
        starts.append(round(offset, 3))
        label = f"[x{i}]" if i < len(parts) - 1 else "[v]"
        filters.append(f"{chain}[{i}:v]xfade=transition=fade:duration={FADE}:offset={offset:.3f}{label}")
        chain = label
    total = offset + durs[-1]
    run([*inputs, "-filter_complex", ";".join(filters), "-map", "[v]", "-c:v", "libx264", "-crf", "18",
         "-preset", "medium", "-pix_fmt", "yuv420p", "-r", "30", str(OUT / f"{video}_mute.mp4")])
    tl = {"parts": [n for n, _ in parts], "durations": durs, "starts": starts, "total": total}
    (OUT / f"{video}_timeline.json").write_text(json.dumps(tl, indent=1))
    print(video, {n: s for (n, _), s in zip(parts, starts)}, "total", round(total, 1))


def slide(n):
    return (n, OUT / f"slide_{n}.mp4")


def midas_video():
    parts = [slide("m_intro"), slide("m_dolor"), slide("m_solucion")]
    parts.append(("llegan", clip("m_llegan", "midas", 2.5, 7.8, caps=[
        (0.3, 5.1, "Llegan los archivos del mes: <b>30 días × 5 sistemas</b>")])))
    parts.append(("arranca", clip("m_arranca", "midas", 7.8, 26, caps=[
        (0.3, 9.6, "<b>Bronze</b> guarda una copia fiel · <b>Silver</b> aplica las reglas"),
        (9.9, 18.0, "⟳ Falla transitoria en silver: <b>MIDAS reintenta solo</b>")])))
    parts.append(("mes", clip("m_mes", "midas", 26, 181, speed=6, badge="×6 · acelerado", caps=[
        (0.3, 8.6, "Día por día: <b>cuarentena</b> con su motivo, <b>duplicados</b> fuera, <b>tardíos</b> a su fecha"),
        (8.9, 17.2, "Un dato tardío solo reescribe <b>su</b> día: partition pruning"),
        (17.5, 25.6, "Al final, <b>dbt</b> construye las tablas gold y MIDAS las publica")])))
    parts.append(("explora", clip("m_explora", "midas", 181, 210, caps=[
        (0.3, 3.6, "Mes publicado: <b>todas las etapas en verde</b>"),
        (4.0, 9.8, "Cada reintento queda registrado con su causa"),
        (10.1, 14.2, "Calidad en el camino: <b>cada rechazo con su motivo</b>"),
        (14.5, 20.6, "<b>Explain plans</b> de Spark: broadcast join, pruning, ventanas"),
        (20.9, 28.8, "Historial: duración por etapa de cada corrida")])))
    parts += [slide("m_tecnica"), slide("m_teaser"), slide("m_outro")]
    assemble("midas", parts)


def duo_video():
    parts = [slide("d_intro"), slide("d_dolor"), slide("d_idea")]
    parts.append(("mes", clip("d_mes", "midas", 7.8, 181, speed=8, badge="×8 · acelerado", caps=[
        (0.3, 10.6, "<b>MIDAS</b> procesa septiembre: cuarentena, duplicados, datos tardíos"),
        (10.9, 21.4, "Publica las tablas gold y su manifiesto")])))
    parts.append(("verde", clip("d_verde", "atlas", 209.7, 219, caps=[
        (0.3, 9.1, "<b class='b'>ATLAS</b> valida la publicación: 4 tablas, 23 reglas. <b class='g'>Todo en verde.</b>")])))
    parts.append(("caida", split("d_caida", 219.6, 244, caps=[
        (0.3, 7.2, "<b>1 de octubre</b>: la pasarela de tarjetas falla. 2 de cada 3 pagos con tarjeta, rechazados."),
        (7.5, 18.7, "Cada registro es válido. <b class='g'>MIDAS: todo OK</b>, publicado."),
        (19.0, 24.3, "<b class='b'>ATLAS</b>: tasa de aprobación 0,59, 7σ por debajo de lo normal. <b class='r'>Incidente.</b>")])))
    parts.append(("incidente", clip("d_incidente", "atlas", 244, 252.4, caps=[
        (0.3, 8.2, "Evidencia, responsable e impacto en el negocio: <b class='b'>listo para escalar</b>")])))
    parts.append(("corrida", clip("d_corrida", "midas", 252.3, 257.8, caps=[
        (0.2, 5.4, "Un clic: <b>la corrida de MIDAS que lo trajo</b>")])))
    parts += [slide("d_cierre"), slide("d_outro")]
    assemble("duo", parts)


if __name__ == "__main__":
    {"midas": midas_video, "duo": duo_video}[sys.argv[1]]()
