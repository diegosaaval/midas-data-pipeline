# Cómo se hicieron los videos

Todo es reproducible: grabación real de las dos aplicaciones, diapositivas en HTML y música original sintetizada con numpy (sin derechos de autor).

```bash
uv venv --python 3.12 .venv && uv pip install --python .venv playwright imageio-ffmpeg numpy pillow
cp ../../src/midas/ui/web/icon-512.png midas.png
cp ../../../atlas-data-quality/web/icon-512.png atlas.png
# con MIDAS (midas ui, puerto 8100) y ATLAS (fuente midas, puerto 8000) abiertos y septiembre ya procesado:
.venv/bin/python rec_session.py      # graba las dos pantallas mientras ocurre la historia real
.venv/bin/python rec_slides.py       # graba las diapositivas animadas (slides.html)
.venv/bin/python edit.py midas && .venv/bin/python music.py midas
.venv/bin/python edit.py duo && .venv/bin/python music.py duo
```

| Archivo | Qué hace |
|---|---|
| `rec_session.py` | Una sola sesión en vivo: el mes con un reintento real, la exploración de MIDAS, ATLAS en verde y el incidente del 1 de octubre. |
| `recorder.py` | Graba una página con el screencast de Chrome a 1080p y 30 fps. |
| `slides.html` / `rec_slides.py` | Diapositivas de los dos videos. |
| `edit.py` | Montaje: subtítulos, cámara rápida, pantalla dividida y fundidos. |
| `music.py` | Música original: re mayor para MIDAS; de menor a mayor para MIDAS + ATLAS, con un golpe cuando ATLAS detecta el incidente. |
| `thumbs.py` | Miniaturas para YouTube. |
