"""Música original para los videos (síntesis aditiva con numpy, el mismo motor del video de ATLAS).
Libre de derechos: la generamos nosotros.

    python music.py midas   -> out/midas_music.wav
    python music.py duo     -> out/duo_music.wav
"""

import json
import sys
import wave

import numpy as np

SR = 44100
VIDEO = sys.argv[1]
tl = json.load(open(f"out/{VIDEO}_timeline.json"))
TOTAL = tl["total"] + 0.3
S = dict(zip(tl["parts"], tl["starts"]))
N = int(TOTAL * SR)
rng = np.random.default_rng(11)


def hz(m):
    return 440.0 * 2 ** ((m - 69) / 12)


def env(n, a, d, s, r):
    e = np.ones(n) * s
    na, nd, nr = int(a * SR), int(d * SR), int(r * SR)
    e[:na] = np.linspace(0, 1, na)
    e[na:na + nd] = np.linspace(1, s, len(e[na:na + nd]))
    if nr:
        e[-nr:] *= np.linspace(1, 0, nr)
    return e


def add(buf, t, sig, gain=1.0):
    i = int(t * SR)
    if i >= len(buf) or gain <= 0:
        return
    j = min(len(buf), i + len(sig))
    buf[i:j] += sig[: j - i] * gain


def level(t, pts):
    xs, ys = zip(*pts)
    return float(np.interp(t, xs, ys))


# --------------------------------------------------------------------- estilo de cada video
# progresión = (acordes del colchón, raíces del bajo, notas del arpegio), una barra por acorde
D_MAJOR = ([[62, 66, 69, 73], [59, 62, 66, 69], [55, 59, 62, 66], [57, 61, 64, 69]],
           [38, 47, 43, 45],
           [[74, 78, 81, 85], [71, 74, 78, 81], [67, 71, 74, 78], [69, 73, 76, 81]])
D_MINOR = ([[62, 65, 69, 72], [58, 62, 65, 69], [55, 58, 62, 65], [57, 61, 64, 67]],
           [38, 46, 43, 45],
           [[74, 77, 81, 84], [70, 74, 77, 81], [67, 70, 74, 77], [69, 73, 76, 79]])
F_MAJOR = ([[65, 69, 72, 76], [62, 65, 69, 72], [58, 62, 65, 69], [60, 64, 67, 72]],
           [41, 50, 46, 48],
           [[77, 81, 84, 88], [74, 77, 81, 84], [70, 74, 77, 81], [72, 76, 79, 84]])
END = TOTAL

if VIDEO == "midas":
    BPM = 92
    demo, tech, teaser, outro = S["llegan"], S["m_tecnica"], S["m_teaser"], S["m_outro"]
    sol = S["m_solucion"]
    progression = lambda t: D_MAJOR
    pad_l = lambda t: level(t, [(0, 1.0), (S["m_dolor"], .8), (sol, 1.1), (demo, 1.0), (teaser, 1.2), (end_, 1.0)])
    arp_l = lambda t: level(t, [(0, .35), (S["m_dolor"] - .5, .35), (S["m_dolor"] + 1, 0), (sol - .5, 0), (sol + 1.5, .7),
                                (demo, .65), (tech, .8), (teaser, .25), (outro, .55), (END - 2, .2), (END, 0)])
    bass_l = lambda t: level(t, [(0, 0), (S["m_dolor"] + 2, 0), (S["m_dolor"] + 4, .35), (sol, .5), (demo, .7),
                                 (teaser - .3, .6), (teaser + .3, 0), (outro + 1, .4), (END - 3, 0), (END, 0)])
    drum_l = lambda t: level(t, [(0, 0), (demo - .2, 0), (demo + .2, 1), (teaser - .4, 1), (teaser, 0), (END, 0)])
    clap_from = S["mes"]
    impacts = [(sol, .8), (demo, .6)]
else:
    BPM = 96
    idea, demo, caida, cierre, outro = S["d_idea"], S["mes"], S["caida"], S["d_cierre"], S["d_outro"]
    detect = caida + 19.0  # ATLAS abre el incidente
    progression = lambda t: D_MINOR if t < idea - .3 or caida <= t < detect else F_MAJOR
    pad_l = lambda t: level(t, [(0, 1.0), (S["d_dolor"], .9), (idea, 1.1), (demo, 1.0), (caida, 1.2), (detect, 1.25),
                                (cierre, 1.1), (END, 1.0)])
    arp_l = lambda t: level(t, [(0, .3), (S["d_dolor"], .15), (idea - .5, .15), (idea + 1.5, .7), (demo, .65),
                                (caida - .2, .65), (caida + .5, .2), (detect - .3, .2), (detect + 1, .8), (cierre, .7),
                                (outro, .55), (END - 2, .2), (END, 0)])
    bass_l = lambda t: level(t, [(0, 0), (S["d_dolor"] + 1, .3), (idea, .5), (demo, .7), (caida, .55), (detect, .75),
                                 (outro + 1, .4), (END - 3, 0), (END, 0)])
    drum_l = lambda t: level(t, [(0, 0), (demo - .2, 0), (demo + .2, 1), (caida - .2, 1), (caida + .3, .25),
                                 (detect - .2, .25), (detect + .2, 1), (outro - .5, 1), (outro + 1.5, 0), (END, 0)])
    clap_from = S["verde"]
    impacts = [(idea, .7), (demo, .6), (detect, 1.3)]

end_ = END
BEAT = 60 / BPM
BAR = 4 * BEAT

pad = np.zeros((N, 2))
arp = np.zeros((N, 2))
bass = np.zeros(N)
drums = np.zeros(N)
kicks = []

for b in range(int(np.ceil(TOTAL / BAR)) + 1):
    t0 = b * BAR
    chords, roots, arps = progression(t0)
    ci = b % 4
    n = int((BAR + 1.2) * SR)
    tt = np.arange(n) / SR
    e = env(n, 0.5, 0.6, 0.85, 1.2)
    for note in chords[ci]:
        for det, ch in ((-0.06, 0), (0.06, 1)):
            f = hz(note + det)
            sig = sum(np.sin(2 * np.pi * f * h * tt + h) / h ** 1.15 for h in range(1, 10))
            sig += 0.3 * np.sin(2 * np.pi * f * 2 * tt + 0.5)
            add(pad[:, ch], t0, sig * e * 0.045, pad_l(t0))
    for q in range(4):
        tq = t0 + q * BEAT
        n = int(BEAT * SR)
        tt = np.arange(n) / SR
        f = hz(roots[ci])
        sig = (np.sin(2 * np.pi * f * tt) + 0.25 * np.sin(4 * np.pi * f * tt)) * np.exp(-tt * 3.2) * env(n, .01, .05, 1, .05)
        add(bass, tq, sig * 0.17, bass_l(tq))
    for s8 in range(8):
        ts = t0 + s8 * BEAT / 2
        note = arps[ci][[0, 1, 2, 3, 2, 1, 2, 3][s8]] + (12 if s8 == 4 and b % 2 else 0)
        n = int(1.1 * SR)
        tt = np.arange(n) / SR
        f = hz(note)
        sig = (np.sin(2 * np.pi * f * tt) + 0.35 * np.sin(4 * np.pi * f * tt) + 0.15 * np.sin(6 * np.pi * f * tt)
               + 0.08 * np.sin(8 * np.pi * f * tt)) * np.exp(-tt * 5.5) * env(n, .004, .0, 1, .02)
        g = arp_l(ts) * 0.11
        pan = 0.35 if s8 % 2 else -0.35
        add(arp[:, 0], ts, sig, g * (1 - pan))
        add(arp[:, 1], ts, sig, g * (1 + pan))
        for k, (dl, fb) in enumerate(((BEAT * .75, .35), (BEAT * 1.5, .15))):
            add(arp[:, 1 - k % 2], ts + dl, sig, g * fb)
    for q in range(4):
        tq = t0 + q * BEAT
        dl = drum_l(tq)
        if dl <= 0:
            continue
        n = int(.4 * SR)
        tt = np.arange(n) / SR
        f = 45 + 75 * np.exp(-tt * 28)
        add(drums, tq, np.sin(2 * np.pi * np.cumsum(f) / SR) * np.exp(-tt * 9) * 0.27, dl)
        kicks.append((tq, dl))
        n = int(.08 * SR)
        noise = rng.standard_normal(n)
        hat = np.diff(np.concatenate([[0], noise])) * np.exp(-np.arange(n) / SR * 60)
        add(drums, tq + BEAT / 2, hat * 0.16, dl)
        if q in (1, 3) and tq > clap_from:
            n = int(.18 * SR)
            clap = np.convolve(rng.standard_normal(n), np.ones(6) / 6, "same") * np.exp(-np.arange(n) / SR * 22)
            add(drums, tq, clap * 0.07, dl)

# golpes de transición: barrido de ruido que sube + golpe grave
for t, g in impacts:
    n = int(1.6 * SR)
    sweep = np.convolve(rng.standard_normal(n), np.ones(24) / 24, "same") * np.linspace(0, 1, n) ** 2.2
    add(drums, t - 1.6, sweep * 0.05 * g)
    n = int(2.2 * SR)
    tt = np.arange(n) / SR
    f = 32 + 50 * np.exp(-tt * 9)
    boom = np.sin(2 * np.pi * np.cumsum(f) / SR) * np.exp(-tt * 2.2)
    add(drums, t, boom * 0.32 * g)

duck = np.ones(N)
for tq, dl in kicks:
    i = int(tq * SR)
    n = min(int(.35 * SR), N - i)
    duck[i:i + n] -= 0.32 * dl * np.exp(-np.arange(n) / SR * 9)
pad *= duck[:, None]

ir_len = int(2.2 * SR)
ir = rng.standard_normal((ir_len, 2)) * np.exp(-np.arange(ir_len) / SR * 2.6)[:, None]
ir /= np.abs(ir).sum(axis=0) / 6
wet_in = pad + arp
L = 1 << int(np.ceil(np.log2(N + ir_len)))
wet = np.stack([np.fft.irfft(np.fft.rfft(wet_in[:, c], L) * np.fft.rfft(ir[:, c], L), L)[:N] for c in (0, 1)], axis=1)

mix = pad * 0.9 + arp * 0.9 + wet * 0.22 + (bass + drums)[:, None]
mix = np.tanh(mix * 1.1)
fi, fo = int(1.5 * SR), int(4.0 * SR)
mix[:fi] *= np.linspace(0, 1, fi)[:, None]
mix[-fo:] *= np.linspace(1, 0, fo)[:, None]
mix /= np.abs(mix).max() / 0.89

with wave.open(f"out/{VIDEO}_music.wav", "wb") as w:
    w.setnchannels(2)
    w.setsampwidth(2)
    w.setframerate(SR)
    w.writeframes((mix * 32767).astype("<i2").tobytes())
print(f"{VIDEO}: música {TOTAL:.1f}s")
