/* FINFLOW · pantalla de etapas. Solo lee la API (/api/...) y dibuja; nunca cambia el pipeline. */
"use strict";

const $ = (s, el = document) => el.querySelector(s);
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const nf = new Intl.NumberFormat("es-CO");
const num = (n) => (n == null ? "—" : nf.format(n));
const compact = (n) => (n == null ? "—" : n >= 1e6 ? `${(n / 1e6).toFixed(1).replace(".", ",")} M` : n >= 1e4 ? `${Math.round(n / 1e3)} mil` : nf.format(n));
const store = {
  get(k, d) { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* sin almacenamiento */ } },
};

function dur(s) {
  if (s == null) return "—";
  if (s < 10) return `${s.toFixed(1).replace(".", ",")} s`;
  if (s < 60) return `${Math.round(s)} s`;
  const m = Math.floor(s / 60), r = Math.round(s % 60);
  return m < 60 ? `${m} min ${r} s` : `${Math.floor(m / 60)} h ${m % 60} min`;
}
const MONTHS = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];
function day(iso) { const [y, m, d] = iso.split("-").map(Number); return { y, m, d }; }
function span(dates) {
  if (!dates.length) return "sin fechas";
  const a = day(dates[0]), b = day(dates[dates.length - 1]);
  if (dates.length === 1) return `${a.d} ${MONTHS[a.m - 1]} ${a.y}`;
  if (a.m === b.m && a.y === b.y) return `${a.d}–${b.d} ${MONTHS[a.m - 1]} ${a.y}`;
  return `${a.d} ${MONTHS[a.m - 1]} – ${b.d} ${MONTHS[b.m - 1]} ${b.y}`;
}
function when(iso) {
  return iso ? new Date(iso).toLocaleString("es-CO", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }) : "—";
}

const STATUS = {
  waiting: "En espera", running: "Corriendo", ok: "OK", retried: "OK tras reintento", retrying: "Reintentando",
  failed: "Falla", skipped: "No aplica", success: "Completada", interrupted: "Interrumpida",
};
const RUN_STATUS = { running: "En curso", success: "Completada", failed: "Falló", interrupted: "Interrumpida" };
const pill = (st, label) => `<span class="pill ${esc(st)}">${esc(label ?? STATUS[st] ?? st)}</span>`;

const GLYPHS = {
  landing: '<path d="M3 4.5h10M3 8h10M3 11.5h6" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/>',
  bronze: '<rect x="2.5" y="3" width="11" height="10" rx="2" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M2.5 6.5h11" stroke="currentColor" stroke-width="1.5"/>',
  silver: '<path d="M2.5 3.5h11l-4 5v4l-3 1v-5z" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/>',
  features: '<path d="M2.5 12.5l3.5-4 3 2 4-6" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>',
  dbt: '<path d="M8 2l5.5 3v6L8 14l-5.5-3V5z M2.5 5L8 8l5.5-3M8 8v6" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/>',
  publish: '<path d="M3.5 8.5l3 3 6-7" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>',
};
const STAGE_COLORS = { bronze: "#c47a3a", silver: "#8e8e93", features: "#0071e3", dbt: "#ff694b", publish: "#34c759" };
const FLOW_LABELS = ["filas", "filas", "pagos", "filas silver", "filas gold"];

const state = {
  runs: [], run: null, meta: null, selectedRun: null, follow: true,
  stage: store.get("finflow.stage", "silver"), tab: "tasks", plan: null, view: "corrida",
};

const planCache = {};
function setHTML(el, html) { if (el.dataset.sig !== html) { el.innerHTML = html; el.dataset.sig = html; } }  // no repintar si nada cambió

async function api(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(`${r.status}`);
  return r.headers.get("content-type")?.includes("json") ? r.json() : r.text();
}

// ------------------------------------------------------------------------------ ciclo
let timer;
async function tick() {
  clearTimeout(timer);
  let live = false;
  try {
    const [runs, meta] = await Promise.all([api("/api/runs?limit=60"), api("/api/meta")]);
    state.runs = runs; state.meta = meta;
    if (state.follow || !runs.some((r) => r.run_id === state.selectedRun)) state.selectedRun = runs[0]?.run_id ?? null;
    state.run = state.selectedRun ? await api(`/api/runs/${encodeURIComponent(state.selectedRun)}`) : null;
    live = runs.some((r) => r.status === "running");
    setLive(live ? "on" : "idle");
    render();
  } catch {
    setLive("off");
  }
  timer = setTimeout(tick, live ? 1000 : 4000);
}

function setLive(mode) {
  const el = $("#live");
  el.className = "clock" + (mode === "on" ? " on" : mode === "off" ? " paused" : "");
  el.lastElementChild.textContent = mode === "on" ? "Corrida en curso" : mode === "off" ? "Sin conexión" : "En espera de corridas";
}

// ----------------------------------------------------------------------------- render
function render() {
  renderHeader();
  renderAtlas();
  if (state.view === "corrida") renderRun(); else renderHistory();
}

function renderHeader() {
  const sel = $("#run-select");
  const opts = state.runs.map((r) => `<option value="${esc(r.run_id)}">${esc(r.kind_label)} · ${esc(span(r.dates))} · ${esc(when(r.started_at))}</option>`).join("");
  if (sel.dataset.sig !== opts) { sel.innerHTML = opts; sel.dataset.sig = opts; }
  sel.value = state.selectedRun ?? "";
  sel.hidden = !state.runs.length;
  $("#btn-follow").hidden = state.follow || !state.runs.length;
}

function renderAtlas() {
  const btn = $("#btn-atlas"), m = state.meta, run = state.run;
  if (!m) return;
  btn.href = m.atlas_url;
  const ready = run ? run.publish_ok : !!m.manifest;
  btn.setAttribute("aria-disabled", ready ? "false" : "true");
  btn.title = !ready ? "Se habilita cuando la publicación termina" : m.atlas_up ? "Abrir ATLAS, que valida las tablas gold" : `ATLAS no responde en ${m.atlas_url}: ábrelo con «Iniciar ATLAS» y elige FINFLOW como fuente`;
}

function renderRun() {
  const run = state.run;
  $("#empty").hidden = !!run;
  $("#pipeline-card").hidden = $("#lower").hidden = !run;
  if (!run) {
    $("#run-title").textContent = "Etapas del pipeline";
    $("#run-sub").textContent = state.meta?.watermark ? `Watermark: ${state.meta.watermark}` : "Esperando la primera corrida.";
    return;
  }
  $("#run-title").textContent = `${run.kind_label} · ${span(run.dates)}`;
  const n = run.dates.length;
  $("#run-sub").innerHTML = [
    pill(run.status, RUN_STATUS[run.status]),
    `${n} ${n === 1 ? "fecha" : "fechas"}`, `inició ${esc(when(run.started_at))}`, `duración ${dur(run.duration_s)}`,
    run.retries ? `<span style="color:var(--warning-ink)">⟳ ${run.retries} reintento${run.retries > 1 ? "s" : ""}</span>` : "",
    `<span class="mono muted">${esc(run.run_id)}</span>`,
  ].filter(Boolean).join(" · ");
  renderPipeline(run);
  const err = $("#run-error");
  err.hidden = !run.error;
  err.textContent = run.error ? `Error de la corrida: ${run.error}` : "";
  renderDetail(run);
  renderQuality(run);
  renderPublication(run);
}

function renderPipeline(run) {
  const html = [];
  run.stages.forEach((s, i) => {
    if (i > 0) {
      const prev = run.stages[i - 1];
      const active = s.status === "running" || s.status === "retrying";
      const done = ["ok", "retried"].includes(s.status) || (s.done > 0 && !active);
      const rows = run.flows[i - 1];
      html.push(`<div class="flow ${active ? "active" : done ? "done" : ""}" aria-hidden="true">
        ${rows && (active || done) ? `<span class="lbl" title="${num(rows)} ${FLOW_LABELS[i - 1]}">${compact(rows)}</span>` : ""}
        <svg viewBox="0 0 40 14" preserveAspectRatio="none"><line class="rail" x1="0" y1="7" x2="34" y2="7"/><line class="dots" x1="0" y1="7" x2="34" y2="7"/><path class="head" d="M33 2.5L40 7l-7 4.5z"/></svg>
      </div>`);
    }
    const pct = s.expected ? Math.min(100, (100 * s.done) / s.expected) : s.status === "skipped" ? 0 : 100;
    const now = s.current
      ? `▸ ${s.current.task}${s.current.date ? " · " + s.current.date.slice(5) : ""}${s.current.attempt > 1 ? ` · intento ${s.current.attempt}` : ""}`
      : s.status === "retrying" ? "esperando para reintentar…" : s.key === "landing" && s.missing ? `faltan ${s.missing} archivo(s)` : "";
    const metrics = s.key === "landing"
      ? `<dt>archivos</dt><dd>${s.done}/${s.expected}</dd><dt>líneas</dt><dd>${compact(s.rows_written)}</dd>`
      : s.key === "dbt"
        ? `<dt>nodos ok</dt><dd>${num(s.rows_written)}</dd>`
        : s.key === "publish"
          ? `<dt>filas gold</dt><dd>${compact(s.rows_written)}</dd><dt>tablas</dt><dd>${s.tasks[0]?.details ? Object.keys(s.tasks[0].details).length : "—"}</dd>`
        : `<dt>leídas</dt><dd>${compact(s.rows_read)}</dd><dt>escritas</dt><dd>${compact(s.rows_written)}</dd>${s.rows_quarantined ? `<dt>cuarentena</dt><dd class="q">${compact(s.rows_quarantined)}</dd>` : ""}`;
    html.push(`<button class="stage ${esc(s.status)} ${state.stage === s.key ? "selected" : ""}" data-stage="${esc(s.key)}" title="${esc(s.name)}: ${esc(STATUS[s.status])}">
      ${s.retries ? `<span class="retry-badge" title="intentos fallidos que se reintentaron">⟳ ${s.retries}</span>` : ""}
      <div class="top"><span class="glyph"><svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">${GLYPHS[s.key]}</svg></span>
        <span class="name">${esc(s.name)}</span></div>
      ${pill(s.status)}
      <div class="time">${s.key === "landing" ? `${s.done}<small> / ${s.expected} archivos</small>` : dur(s.duration_s)}</div>
      <dl>${metrics}</dl>
      <div class="bar"><i style="width:${pct}%"></i></div>
      <div class="now">${esc(now)}</div>
    </button>`);
  });
  setHTML($("#pipeline"), html.join(""));
}

// ---------------------------------------------------------------------- detalle
function renderDetail(run) {
  const s = run.stages.find((x) => x.key === state.stage) ?? run.stages[2];
  const tabs = [
    ["tasks", "Tareas", s.key === "landing" ? s.files.length : s.tasks.length],
    ["partitions", "Particiones", s.partitions.length],
    ["errors", "Reintentos y errores", s.errors.length],
  ];
  if (["bronze", "silver"].includes(s.key)) tabs.push(["schema", "Esquema", run.schema_events.length]);
  if (s.key === "features") tabs.push(["plans", "Explain plans", run.plans.length]);
  if (!tabs.some((t) => t[0] === state.tab)) state.tab = "tasks";

  let body = "";
  if (state.tab === "tasks") body = s.key === "landing" ? landingTable(s) : tasksTable(s);
  else if (state.tab === "partitions") {
    body = s.partitions.length
      ? `<p class="muted small">${s.key === "landing" || s.key === "bronze" ? "Particiones por fecha de ingesta" : "Particiones reescritas (dynamic partition overwrite): solo estas cambiaron en el lake."}</p><div class="chips">${s.partitions.map((p) => `<span class="chip">${esc(p)}</span>`).join("")}</div>`
      : `<p class="empty-note">Esta etapa no escribe particiones${s.key === "dbt" ? " (dbt materializa tablas)" : ""}.</p>`;
  } else if (state.tab === "errors") {
    body = s.errors.length
      ? `<ul class="errors">${s.errors.map((e) => `<li class="${s.status === "failed" ? "final" : ""}"><b>${esc(e.task)}</b>${e.date ? " · " + esc(e.date) : ""} · intento ${e.attempt}<br><code>${esc(e.error)}</code></li>`).join("")}</ul>
        <p class="muted small">${s.status === "failed" ? "Se agotaron los reintentos: la corrida se detuvo aquí." : "Cada falla se reintentó con espera exponencial y la tarea terminó bien."}</p>`
      : `<p class="empty-note">Sin fallas en esta etapa.</p>`;
  } else if (state.tab === "schema") {
    body = run.schema_events.length
      ? `<table><tr><th>Fecha</th><th>Entidad</th><th>Evento</th><th>Columnas</th></tr>${run.schema_events.map((e) => `<tr><td>${esc(e.ingest_date)}</td><td>${esc(e.entity)}</td><td>${esc(e.kind === "unknown_columns" ? "columnas no acordadas" : e.kind === "missing_required_columns" ? "faltan columnas obligatorias" : e.kind)}</td><td><code>${esc(e.columns.join(", "))}</code></td></tr>`).join("")}</table>
        <p class="muted small">Las columnas no acordadas quedan en bronze y no pasan a silver hasta agregarlas al contrato.</p>`
      : `<p class="empty-note">La estructura de las fuentes coincidió con el contrato. Sin eventos de esquema.</p>`;
  } else if (state.tab === "plans") {
    if (!run.plans.includes(state.plan)) state.plan = run.plans[run.plans.length - 1] ?? null;
    body = run.plans.length
      ? `<select id="plan-select">${run.plans.map((p) => `<option ${p === state.plan ? "selected" : ""}>${esc(p)}</option>`).join("")}</select>
         <pre class="plan" id="plan-text">${esc(planCache[state.plan] ?? "Cargando…")}</pre>`
      : `<p class="empty-note">Sin planes guardados para estas fechas.</p>`;
  }

  const kv = s.key === "landing"
    ? [["archivos", `${s.done}/${s.expected}`], ["líneas", num(s.rows_written)], ["faltantes", num(s.missing)], ["fechas", num(run.dates.length)]]
    : [["duración", dur(s.duration_s)], ["leídas", num(s.rows_read)], ["escritas", num(s.rows_written)], ["en cuarentena", num(s.rows_quarantined)]];
  setHTML($("#detail"), `
    <div class="card-head"><h3>${esc(s.name)} ${pill(s.status)}</h3>
      <div class="seg">${tabs.map(([k, l, n]) => `<button data-tab="${k}" class="${state.tab === k ? "active" : ""}">${l}<span class="n">${n}</span></button>`).join("")}</div></div>
    <div class="kv">${kv.map(([l, v]) => `<div><b>${v}</b><span>${l}</span></div>`).join("")}</div>
    ${body}`);
  if (state.tab === "plans" && state.plan && !(state.plan in planCache)) loadPlan();
}

function tasksTable(s) {
  if (!s.tasks.length) return `<p class="empty-note">${s.status === "skipped" ? "Esta corrida no incluye esta etapa." : "Todavía no empieza."}</p>`;
  const rows = s.tasks.map((t) => {
    const cur = s.current && s.current.task === t.task && s.current.date === t.date;
    const extra = t.details?.duplicates_removed ? `dup ${num(t.details.duplicates_removed)}` : t.details?.late_rows ? `tardías ${num(t.details.late_rows)}` : t.details?.dbt ? Object.entries(t.details.dbt).map(([k, v]) => `${k} ${v}`).join(" · ") : "";
    return `<tr class="${cur ? "current" : ""}"><td>${esc(t.task)}</td><td>${esc(t.date ?? "—")}</td><td>${pill(t.status === "success" ? (t.attempts > 1 ? "retried" : "ok") : t.status)}</td>
      <td class="num">${t.attempts}</td><td class="num">${dur(t.duration_s)}</td><td class="num">${num(t.rows_read)}</td><td class="num">${num(t.rows_written)}</td>
      <td class="num">${t.rows_quarantined ? num(t.rows_quarantined) : "—"}</td><td class="muted">${esc(extra)}</td></tr>`;
  }).join("");
  return `<div class="table-wrap"><table><tr><th>Tarea</th><th>Fecha</th><th>Estado</th><th class="num">Intentos</th><th class="num">Duración</th><th class="num">Leídas</th><th class="num">Escritas</th><th class="num">Cuarentena</th><th></th></tr>${rows}</table></div>`;
}

function landingTable(s) {
  if (!s.files.length) return `<p class="empty-note">Esta corrida no lee archivos de landing.</p>`;
  return `<p class="muted small">Archivos JSONL que dejaron las fuentes. Los escribe el generador (<code>finflow generate</code>), fuera del pipeline.</p>
    <div class="table-wrap"><table><tr><th>Fecha</th><th>Entidad</th><th class="num">Líneas</th></tr>${s.files.map((f) => `<tr><td>${esc(f.date)}</td><td>${esc(f.entity)}</td><td class="num">${f.rows == null ? '<span class="chip miss">no llegó</span>' : num(f.rows)}</td></tr>`).join("")}</table></div>`;
}

async function loadPlan() {
  const name = state.plan;
  try {
    const text = await api(`/api/plans/${encodeURIComponent(name)}`);
    planCache[name] = text;
    if (state.plan === name && $("#plan-text")) $("#plan-text").textContent = text;
  } catch {
    if ($("#plan-text")) $("#plan-text").textContent = "No se pudo leer el plan.";
  }
}

// ----------------------------------------------------------------- calidad y publicación
function renderQuality(run) {
  const q = run.quality;
  const reasons = Object.entries(q.quarantine_by_reason);
  const max = Math.max(1, ...reasons.map(([, n]) => n));
  setHTML($("#quality"), `
    <div class="card-head"><h3>Calidad en el camino</h3><span class="muted small">bronze → silver</span></div>
    <div class="qkpis">
      <div class="${q.quarantine_total ? "warn" : ""}"><b>${num(q.quarantine_total)}</b><span>en cuarentena</span></div>
      <div><b>${num(q.duplicates_removed)}</b><span>duplicados eliminados</span></div>
      <div><b>${num(q.late_rows)}</b><span>filas tardías integradas</span></div>
    </div>
    ${reasons.length ? `<h4>Cuarentena por motivo</h4><div class="bars">${reasons.map(([r, n]) => `<div class="row"><code title="${esc(r)}">${esc(r)}</code><span class="track"><i style="width:${(100 * n) / max}%"></i></span><b class="num">${num(n)}</b></div>`).join("")}</div>` : `<p class="empty-note">${run.status === "running" ? "Aún no hay registros en cuarentena." : "Ningún registro fue a cuarentena."}</p>`}
    ${q.corrupt_lines || q.unknown_columns.length ? `<p class="muted small" style="margin-top:12px">${q.corrupt_lines ? `${num(q.corrupt_lines)} líneas con JSON corrupto. ` : ""}${q.unknown_columns.length ? `Columnas no acordadas: <code>${esc(q.unknown_columns.join(", "))}</code>` : ""}</p>` : ""}`);
}

function renderPublication(run) {
  const pub = run.stages.find((s) => s.key === "publish");
  const counts = pub.tasks[0]?.details ?? null;
  const m = state.meta ?? {};
  const atlas = run.publish_ok
    ? `<a class="btn primary" href="${esc(m.atlas_url)}" target="_blank" rel="noopener">Ver en ATLAS</a>
       <p class="atlas-note" style="margin-top:10px">${m.atlas_up ? "ATLAS está abierto y valida estas tablas apenas cambia el manifiesto." : `ATLAS no responde en ${esc(m.atlas_url)}. Ábrelo con «Iniciar ATLAS» y elige FINFLOW en Fuente de datos.`}</p>`
    : `<p class="atlas-note">${run.status === "running" ? "Las tablas gold se publican al final de la corrida." : "Esta corrida no publicó tablas gold."}</p>`;
  setHTML($("#publication"), `
    <div class="card-head"><h3>Publicación gold</h3>${pill(pub.status)}</div>
    ${counts ? `<ul class="datasets">${Object.entries(counts).map(([k, v]) => `<li><code>${esc(k)}</code><b>${num(v)}</b></li>`).join("")}</ul>` : ""}
    ${atlas}`);
}

// --------------------------------------------------------------------------- historial
function renderHistory() {
  const runs = state.runs;
  $("#runs").innerHTML = `<tr><th>Tipo</th><th>Fechas</th><th>Estado</th><th>Inicio</th><th class="num">Duración</th><th class="num">Reintentos</th></tr>` +
    (runs.length ? runs.map((r) => `<tr class="clickable" data-run="${esc(r.run_id)}"><td>${esc(r.kind_label)}</td><td>${esc(span(r.dates))}</td><td>${pill(r.status, RUN_STATUS[r.status])}</td><td>${esc(when(r.started_at))}</td><td class="num">${dur(r.duration_s)}</td><td class="num">${r.retries || "—"}</td></tr>`).join("")
      : `<tr><td colspan="6" class="empty-note">Sin corridas todavía.</td></tr>`);
  renderChart(runs.filter((r) => !r.kind.startsWith("task:")).slice(0, 30).reverse());
}

function renderChart(runs) {
  const keys = Object.keys(STAGE_COLORS);
  const names = { bronze: "Bronze", silver: "Silver", features: "Features", dbt: "dbt build", publish: "Publicación" };
  $("#legend").innerHTML = keys.map((k) => `<span><i style="background:${STAGE_COLORS[k]}"></i>${names[k]}</span>`).join("");
  $("#chart-note").textContent = runs.length ? `últimas ${runs.length} corridas` : "";
  const el = $("#chart");
  if (!runs.length) { el.innerHTML = `<p class="empty-note">Sin corridas del pipeline todavía.</p>`; return; }
  const W = el.clientWidth || 800, H = el.clientHeight || 240, L = 44, B = 22, T = 8;
  const totals = runs.map((r) => keys.reduce((a, k) => a + (r.stage_seconds[k] || 0), 0));
  const top = niceMax(Math.max(...totals, 1));
  const y = (v) => T + (H - T - B) * (1 - v / top);
  const step = (W - L) / runs.length, bw = Math.min(38, step * 0.62);
  let svg = `<g class="grid">${[0, 0.25, 0.5, 0.75, 1].map((f) => `<line x1="${L}" x2="${W}" y1="${y(top * f)}" y2="${y(top * f)}"/>`).join("")}</g>`;
  svg += `<g class="axis">${[0, 0.5, 1].map((f) => `<text x="${L - 8}" y="${y(top * f) + 3}" text-anchor="end">${dur(top * f).replace(" min ", "m").replace(" s", "s")}</text>`).join("")}</g>`;
  runs.forEach((r, i) => {
    const x = L + step * i + (step - bw) / 2;
    let acc = 0, rects = "";
    keys.forEach((k) => {
      const v = r.stage_seconds[k] || 0;
      if (v <= 0) return;
      rects += `<rect class="seg-bar" x="${x}" y="${y(acc + v)}" width="${bw}" height="${Math.max(0.5, y(acc) - y(acc + v))}" fill="${STAGE_COLORS[k]}" rx="2"/>`;
      acc += v;
    });
    const label = runs.length <= 12 || i % Math.ceil(runs.length / 12) === 0 ? `<text x="${x + bw / 2}" y="${H - 6}" text-anchor="middle">${esc(when(r.started_at).split(",")[0])}</text>` : "";
    svg += `<g class="col" data-i="${i}">${rects}<rect x="${L + step * i}" y="${T}" width="${step}" height="${H - T - B}" fill="transparent"/></g><g class="axis">${label}</g>`;
  });
  el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Duración por etapa de cada corrida">${svg}</svg>`;
  el.querySelectorAll("g.col").forEach((g) => {
    const r = runs[+g.dataset.i];
    g.addEventListener("mousemove", (ev) => showTip(ev, `<b>${esc(r.kind_label)} · ${esc(span(r.dates))}</b>` +
      keys.map((k) => `<div class="tt-row"><span><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${STAGE_COLORS[k]};margin-right:5px"></i>${names[k]}</span><b>${dur(r.stage_seconds[k])}</b></div>`).join("") +
      `<div class="tt-row muted"><span>total</span><b>${dur(r.duration_s)}</b></div>`));
    g.addEventListener("mouseleave", hideTip);
    g.addEventListener("click", () => openRun(r.run_id));
  });
}
function niceMax(v) { const p = 10 ** Math.floor(Math.log10(v)); return Math.ceil(v / p) * p; }
function showTip(ev, html) {
  const t = $("#tooltip"); t.innerHTML = html; t.hidden = false;
  t.style.left = `${Math.min(ev.clientX + 14, innerWidth - t.offsetWidth - 8)}px`; t.style.top = `${ev.clientY + 14}px`;
}
function hideTip() { $("#tooltip").hidden = true; }

// --------------------------------------------------------------------------- eventos
function openRun(id) {
  state.selectedRun = id; state.follow = id === state.runs[0]?.run_id;
  history.replaceState(null, "", state.follow ? location.pathname : `#run=${encodeURIComponent(id)}`);
  showView("corrida"); tick();
}
// Enlace directo a una corrida (p. ej. desde ATLAS): http://localhost:8100/#run=<run_id>
function fromHash() {
  const m = location.hash.match(/^#run=(.+)$/);
  if (m) { state.selectedRun = decodeURIComponent(m[1]); state.follow = false; }
}
addEventListener("hashchange", () => { fromHash(); showView("corrida"); tick(); });
fromHash();
function showView(v) {
  state.view = v;
  document.querySelectorAll(".tabs button").forEach((b) => b.classList.toggle("active", b.dataset.view === v));
  document.querySelectorAll(".view").forEach((s) => s.classList.toggle("active", s.dataset.view === v));
  render();
}
document.querySelectorAll(".tabs button").forEach((b) => b.addEventListener("click", () => showView(b.dataset.view)));
$("#run-select").addEventListener("change", (e) => openRun(e.target.value));
$("#btn-follow").addEventListener("click", () => { state.follow = true; history.replaceState(null, "", location.pathname); tick(); });
$("#pipeline").addEventListener("click", (e) => {
  const b = e.target.closest(".stage"); if (!b) return;
  state.stage = b.dataset.stage; store.set("finflow.stage", state.stage); render();
  if (innerWidth < 1100) $("#detail").scrollIntoView({ behavior: "smooth", block: "start" });
});
$("#detail").addEventListener("click", (e) => { const b = e.target.closest("[data-tab]"); if (b) { state.tab = b.dataset.tab; render(); } });
$("#detail").addEventListener("change", (e) => { if (e.target.id === "plan-select") { state.plan = e.target.value; render(); } });
$("#runs").addEventListener("click", (e) => { const tr = e.target.closest("tr[data-run]"); if (tr) openRun(tr.dataset.run); });
addEventListener("resize", () => { if (state.view === "historial") renderHistory(); });

function applyTheme(t) { t ? (document.documentElement.dataset.theme = t) : delete document.documentElement.dataset.theme; }
applyTheme(store.get("finflow.theme", null));
$("#btn-theme").addEventListener("click", () => {
  const dark = document.documentElement.dataset.theme ? document.documentElement.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(dark ? "light" : "dark"); store.set("finflow.theme", dark ? "light" : "dark");
});

tick();
