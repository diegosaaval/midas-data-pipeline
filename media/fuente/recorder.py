"""Graba páginas con el protocolo DevTools (screencast) y las convierte en MP4 1080p a 30 fps."""

from __future__ import annotations

import base64
import shutil
import subprocess
import time
from pathlib import Path

import imageio_ffmpeg

FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
OUT = Path(__file__).parent / "out"
OUT.mkdir(exist_ok=True)

OVERLAY_JS = r"""
(() => {
  if (window.__rec) return;
  const css = document.createElement('style');
  css.textContent = `
    #rec-cursor { position: fixed; left: 0; top: 0; width: 26px; height: 26px; z-index: 99999; pointer-events: none;
      transform: translate(720px, 900px); transition: transform .8s cubic-bezier(.45,.05,.2,1); filter: drop-shadow(0 2px 3px rgba(0,0,0,.35)); }
    #rec-ripple { position: fixed; width: 44px; height: 44px; margin: -22px 0 0 -22px; border-radius: 50%; z-index: 99998; pointer-events: none;
      background: rgba(0,113,227,.35); transform: scale(.2); opacity: 0; }
    #rec-ripple.go { animation: rec-r .55s ease-out; }
    @keyframes rec-r { 0% { transform: scale(.2); opacity: .9; } 100% { transform: scale(1.6); opacity: 0; } }
    #rec-cap { position: fixed; left: 50%; bottom: 34px; transform: translate(-50%, 12px); z-index: 99997; max-width: 1080px;
      background: rgba(29,29,31,.86); backdrop-filter: saturate(180%) blur(20px); color: #f5f5f7; border-radius: 18px;
      padding: 16px 26px; font: 600 21px/1.4 -apple-system, BlinkMacSystemFont, "SF Pro Display", sans-serif; letter-spacing: -.015em;
      text-align: center; opacity: 0; transition: opacity .45s, transform .45s cubic-bezier(.2,.8,.2,1); box-shadow: 0 20px 50px rgba(0,0,0,.25); }
    #rec-cap.on { opacity: 1; transform: translate(-50%, 0); }
    #rec-cap b { color: #64b5ff; font-weight: 700; }
    .tour { display: none !important; }
  `;
  document.head.appendChild(css);
  const cur = document.createElement('div');
  cur.id = 'rec-cursor';
  cur.innerHTML = '<svg viewBox="0 0 26 26" width="26" height="26"><path d="M4 2 L4 21 L9 16.5 L12.5 24 L15.6 22.6 L12.2 15.3 L19 15.3 Z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>';
  document.body.appendChild(cur);
  const rip = document.createElement('div'); rip.id = 'rec-ripple'; document.body.appendChild(rip);
  const cap = document.createElement('div'); cap.id = 'rec-cap'; document.body.appendChild(cap);
  window.__rec = {
    move(x, y, ms = 800) { cur.style.transition = `transform ${ms}ms cubic-bezier(.45,.05,.2,1)`; cur.style.transform = `translate(${x - 4}px, ${y - 2}px)`; },
    click(x, y) { rip.style.left = x + 'px'; rip.style.top = y + 'px'; rip.classList.remove('go'); void rip.offsetWidth; rip.classList.add('go'); },
    caption(html) {
      if (!html) { cap.classList.remove('on'); return; }
      if (cap.classList.contains('on')) { cap.classList.remove('on'); setTimeout(() => { cap.innerHTML = html; cap.classList.add('on'); }, 350); }
      else { cap.innerHTML = html; cap.classList.add('on'); }
    },
  };
})();
"""


class Recorder:
    """Screencast de una página: guarda cada fotograma con su tiempo."""

    def __init__(self, page, name: str, max_size=(1920, 1080), vf="scale=1920:1080:flags=lanczos"):
        self.max_size, self.vf = max_size, vf
        self.page = page
        self.name = name
        self.frames: list[tuple[float, bytes]] = []
        self.client = page.context.new_cdp_session(page)
        self.client.on("Page.screencastFrame", self._on_frame)

    def _on_frame(self, params):
        # Tiempo real del fotograma según Chrome (no cuando Python lo procesa).
        ts = params.get("metadata", {}).get("timestamp") or time.time()
        self.frames.append((ts, base64.b64decode(params["data"])))
        try:
            self.client.send("Page.screencastFrameAck", {"sessionId": params["sessionId"]})
        except Exception:
            pass

    def start(self):
        self.t0 = time.time()
        self.client.send("Page.startScreencast", {"format": "jpeg", "quality": 92, "maxWidth": self.max_size[0], "maxHeight": self.max_size[1],
                                                  "everyNthFrame": 1})

    def stop(self) -> Path:
        self.t1 = time.time()
        self.client.send("Page.stopScreencast")
        return self.encode()

    def encode(self) -> Path:
        tmp = OUT / f"_{self.name}"
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir()
        frames = [f for f in self.frames if f[0] >= self.t0 - 1]
        lines = []
        for i, (t, data) in enumerate(frames):
            path = tmp / f"{i:06d}.jpg"
            path.write_bytes(data)
            start = max(t, self.t0)
            end = frames[i + 1][0] if i + 1 < len(frames) else self.t1
            dur = max(end - start, 0.001)
            lines.append(f"file '{path}'\nduration {dur:.4f}")
        lines.append(f"file '{tmp / f'{len(frames) - 1:06d}.jpg'}'")
        (tmp / "list.txt").write_text("\n".join(lines))
        out = OUT / f"{self.name}.mp4"
        subprocess.run([FFMPEG, "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(tmp / "list.txt"),
                        "-vf", f"{self.vf},fps=30,format=yuv420p", "-c:v", "libx264", "-crf", "17",
                        "-preset", "medium", "-r", "30", str(out)], check=True)
        shutil.rmtree(tmp, ignore_errors=True)
        print(f"  {self.name}: {len(frames)} fotogramas, {self.t1 - self.t0:.1f}s -> {out.name}")
        return out
