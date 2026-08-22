/* Front end for the Synthetic Data Designer.
 *
 * Two rules shape the whole file.
 *
 * The browser never decides whether something is valid. It holds the
 * configuration as a plain object and posts it back on every edit; the loader
 * that answers is the same one the CLI uses, so this page cannot accept a
 * configuration the engine would reject.
 *
 * The browser never aggregates. Charts, search and sorting all run server-side
 * against the generated file, so a twelve-million-row panel behaves exactly like
 * a twelve-thousand-row one.
 *
 * No build step, no dependencies: the tool is usually run on a laptop with no
 * internet, and a CDN link would make the page fail there.
 */

const VIEWS = ["upload", "review", "configure", "generate", "results", "download"];

// Where the code lives. Referenced from the shared-instance notice, which tells
// a visitor how to run this privately and therefore has to say where to get it.
const SOURCE_URL =
  "https://github.com/Algoritmica-ai/deeploans/tree/main/synthetic-data-designer";

const state = {
  view: "upload",
  source: null,          // token tying the uploaded schema + sample together
  spec: null,            // the configuration being edited
  profile: null,         // the analysis, when a sample was uploaded
  capabilities: null,    // which controls this configuration can honour
  schema: null,          // the review table
  edits: new Map(),      // column name -> pending edit
  valid: false,
  problems: [],
  uploads: { schema: null, sample: null },
  job: null,
  jobStages: [],
  result: null,
  charts: null,
  table: { offset: 0, limit: 25, sort: null, descending: false, search: "" },
  settings: {
    records: 10000,
    seed: 42,
    scenario: "",
    method: null,
    noise: 0,
    correlation: 1,
    outliers: 0,
    missing: 0,
    periods: null,
    freq: null,
    default_rate: null,
    prepayment_rate: null,
    recovery_rate: null,
    origination_rate: 0,
    touched: new Set(),
  },
  reachable: { upload: true },
};

/* --------------------------------------------------------------- utilities */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const el = (tag, attrs = {}, kids = []) => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k === "html") node.innerHTML = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v === true ? "" : v);
  }
  for (const kid of [].concat(kids)) {
    if (kid !== null && kid !== undefined && kid !== false) {
      node.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
    }
  }
  return node;
};

const fmt = {
  int: (n) => (n === null || n === undefined ? "—" : Math.round(Number(n)).toLocaleString()),
  pct: (n, dp = 1) => (n === null || n === undefined ? "—" : `${(n * 100).toFixed(dp)}%`),
  num: (n, dp = 4) => (n === null || n === undefined ? "—" : Number(n).toFixed(dp)),
  money: (n) => {
    const v = Number(n);
    if (!Number.isFinite(v)) return "—";
    if (Math.abs(v) >= 1e9) return `${(v / 1e9).toFixed(2)}bn`;
    if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}m`;
    if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(0)}k`;
    return v.toFixed(0);
  },
  bytes: (n) => {
    if (n === null || n === undefined) return "—";
    if (n < 1024) return `${n} B`;
    if (n < 1024 ** 2) return `${(n / 1024).toFixed(0)} KB`;
    if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
    return `${(n / 1024 ** 3).toFixed(2)} GB`;
  },
  seconds: (s) => {
    if (s === null || s === undefined) return "";
    if (s < 60) return `${Math.max(1, Math.round(s))}s`;
    const m = Math.floor(s / 60);
    return `${m}m ${Math.round(s - m * 60)}s`;
  },
};

async function call(path, options = {}) {
  const response = await fetch(path, {
    headers: options.body instanceof FormData ? {} : { "Content-Type": "application/json" },
    ...options,
  });
  const text = await response.text();
  let payload;
  try { payload = text ? JSON.parse(text) : {}; } catch { payload = { detail: text }; }
  if (!response.ok) {
    const problems = payload.problems ? `: ${payload.problems[0]}` : "";
    throw new Error((payload.detail || payload.error || `HTTP ${response.status}`) + problems);
  }
  return payload;
}

function debounce(fn, wait = 250) {
  let handle;
  return (...args) => {
    clearTimeout(handle);
    handle = setTimeout(() => fn(...args), wait);
  };
}

function status(message, kind = "") {
  const node = $("#footer-status");
  node.textContent = message;
  node.style.color = kind === "bad" ? "var(--bad)" : kind === "good" ? "var(--good)" : "";
}

function stat(key, value, extra = "") {
  return el("div", { class: "stat" }, [
    el("div", { class: "k", text: key }),
    el("div", { class: `v ${extra}`, text: value }),
  ]);
}

function note(text, kind = "", html = false) {
  return el("div", { class: `note ${kind}`, [html ? "html" : "text"]: text });
}

/* ------------------------------------------------------------- navigation */

const NEXT_LABEL = {
  upload: "Analyze data",
  review: "Generate configuration",
  configure: "Generate data",
  generate: "Generating…",
  results: "Go to downloads",
  download: "Start over",
};

function reach(view) { state.reachable[view] = true; }

/* What each screen is for, said on arrival.
 *
 * The status bar held whatever the last action set, so three screens into a
 * run it still read "Review its schema, or go straight to configuring" — advice
 * about a screen already behind you. A caller with something more specific to
 * say still overrides it by calling status() afterwards. */
const VIEW_STATUS = {
  upload: "Pick a calibrated pack, or upload a schema of your own.",
  review: "Check the detected types and key. Change anything that looks wrong.",
  configure: "Two sections are open. The rest have working defaults — press Generate when ready.",
  generate: "Running. This takes seconds for a small pool and minutes for a large one.",
  results: "Charts and a queryable table. Download comes next.",
  download: "Five formats, plus the configuration that produced them.",
};

function show(view) {
  state.view = view;
  VIEWS.forEach((name) => { $(`#view-${name}`).hidden = name !== view; });
  if (VIEW_STATUS[view]) status(VIEW_STATUS[view]);

  const at = VIEWS.indexOf(view);
  $$(".rail .step").forEach((button) => {
    const target = button.dataset.view;
    button.setAttribute("aria-current", String(target === view));
    button.disabled = !state.reachable[target];
    // Boolean(): classList.toggle treats an undefined second argument as "no
    // force given" and flips the class instead of clearing it.
    button.classList.toggle(
      "complete",
      Boolean(state.reachable[target]) && VIEWS.indexOf(target) < at
    );
  });

  $("#btn-back").hidden = at === 0;
  const next = $("#btn-next");
  next.textContent = NEXT_LABEL[view];
  next.disabled =
    (view === "upload" && !state.uploads.schema && !state.uploads.sample) ||
    (view === "review" && !state.spec) ||
    (view === "configure" && !state.valid) ||
    (view === "generate" && state.job !== null && !state.result) ||
    (view === "results" && !state.result);
  if (view === "generate" && state.result) next.textContent = "View results";

  window.scrollTo({ top: 0, behavior: "instant" });
}

function goNext() {
  switch (state.view) {
    case "upload":    return analyse();
    case "review":    return applyEditsThenConfigure();
    case "configure": return startRun();
    case "generate":  return state.result ? show("results") : null;
    case "results":   return show("download");
    case "download":  return window.location.reload();
  }
}

/* ---------------------------------------------------------- spec lifecycle */

async function adoptSpec(spec, { origin = null } = {}) {
  state.spec = spec;
  if (origin) {
    $("#spec-name").textContent = origin;
    $("#spec-name").hidden = false;
  }
  await validate();
  renderConfigure();
  return state.valid;
}

async function validate() {
  try {
    const result = await call("/api/check", {
      method: "POST",
      body: JSON.stringify(state.spec),
    });
    state.valid = result.valid;
    state.problems = result.problems || [];
    state.summary = result.spec;
  } catch (error) {
    state.valid = false;
    state.problems = [error.message];
  }

  const tag = $("#validity");
  tag.hidden = false;
  tag.className = `tag ${state.valid ? "ok" : "err"}`;
  tag.textContent = state.valid
    ? `valid · ${state.summary.columns} cols · ${state.summary.periods}p`
    : `${state.problems.length} problem(s)`;

  // The count went in the header, which every screen shows, while the reason
  // went into a panel only the configure screen has. So a spec that failed
  // while you were still on upload reported "1 problem(s)" and no way to find
  // out which — a number you can neither act on nor dismiss.
  const detail = !state.valid
    ? note(`<strong>This configuration will not run.</strong><ul>${
        state.problems.slice(0, 8).map((p) => `<li>${escapeHtml(p)}</li>`).join("")
      }</ul>`, "bad", true)
    : null;

  for (const id of ["#config-problems", "#upload-error"]) {
    const host = $(id);
    if (!host) continue;
    if (detail) host.replaceChildren(detail.cloneNode(true));
    else if (id === "#config-problems") host.replaceChildren();
  }
  tag.title = state.valid ? "" : state.problems.join("\n");
  if (state.view === "configure") $("#btn-next").disabled = !state.valid;
  return state.valid;
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
}

/* ------------------------------------------------------------- 1. upload */

function wireDrop(dropId, inputId, kind) {
  const drop = $(dropId);
  const input = $(inputId);
  drop.addEventListener("click", () => input.click());
  drop.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); input.click(); }
  });
  drop.addEventListener("dragover", (e) => { e.preventDefault(); drop.classList.add("over"); });
  drop.addEventListener("dragleave", () => drop.classList.remove("over"));
  drop.addEventListener("drop", (e) => {
    e.preventDefault();
    drop.classList.remove("over");
    if (e.dataTransfer.files[0]) upload(e.dataTransfer.files[0], kind);
  });
  input.addEventListener("change", () => {
    if (input.files[0]) upload(input.files[0], kind);
  });
}

async function upload(file, kind) {
  const target = $(`#${kind}-status`);
  target.replaceChildren(note(`Uploading ${file.name} (${fmt.bytes(file.size)})…`, ""));
  target.firstChild.prepend(el("span", { class: "spin" }));

  try {
    const form = new FormData();
    form.append("file", file);
    form.append("kind", kind);
    const stored = await call("/api/upload", { method: "POST", body: form });
    state.uploads[kind] = stored;

    const detail = kind === "schema"
      ? `${stored.columns.length} column(s), ${stored.typed} with a declared type.`
      : `${stored.columns.length} column(s) found.`;
    target.replaceChildren(note(
      `<strong>${escapeHtml(stored.original_name)}</strong> — ${detail} ` +
      `<code class="inline">${fmt.bytes(stored.size_bytes)}</code>`, "good", true));

    $("#schema-req").textContent = state.uploads.schema ? "Loaded" : "Required";
    status(state.uploads.sample
      ? "Ready to analyse. The sample supplies the distributions."
      : "Ready to analyse. Add sample data to measure distributions rather than assume them.");
    show("upload");
  } catch (error) {
    target.replaceChildren(note(error.message, "bad"));
  }
}

async function analyse() {
  const button = $("#btn-next");
  button.disabled = true;
  button.textContent = "Analyzing…";
  $("#upload-error").replaceChildren();

  try {
    const result = await call("/api/analyse", {
      method: "POST",
      body: JSON.stringify({
        schema_file: state.uploads.schema?.file || null,
        sample_file: state.uploads.sample?.file || null,
        name: (state.uploads.sample || state.uploads.schema)?.original_name,
      }),
    });

    state.source = result.source;
    state.profile = result.profile;
    state.schema = result.schema;
    state.capabilities = result.capabilities;
    seedSettings(result.spec, result.capabilities);
    await adoptSpec(result.spec, {
      origin: (state.uploads.sample || state.uploads.schema).original_name,
    });

    reach("review");
    reach("configure");
    renderReview();
    show("review");
    status(result.profile
      ? `Analysed ${fmt.int(result.profile.rows)} rows across ${result.profile.columns.length} columns.`
      : "Schema read. Distributions are yours to choose — there was no sample to measure.");
  } catch (error) {
    $("#upload-error").replaceChildren(note(error.message, "bad"));
    status(error.message, "bad");
  } finally {
    button.textContent = NEXT_LABEL[state.view];
    show(state.view);
  }
}

function describeInstance(meta) {
  // Run locally the page says nothing leaves this machine, and that is true.
  // Hosted, it is false: uploads land on someone else's disk in a workspace
  // every visitor shares. Saying it anyway would be lying to the person
  // deciding whether to upload a real tape.
  if (!meta.shared) return;

  $("#upload-lede").textContent =
    "Give it a schema and, if you have one, data to learn from. This is a shared demo, " +
    "so read the notice below before uploading anything real.";

  const limits = meta.limits || {};
  // "entities", not "rows". A row is one entity at one cut-off, so the two
  // differ by the number of periods, and this notice used to quote the entity
  // cap as though it were a row cap — which read as a promise the run could not
  // keep, by a factor of forty.
  const bounds = [
    limits.records && `${fmt.int(limits.records)} entities per run`,
    limits.periods && `${limits.periods} periods`,
    limits.rows && `${fmt.int(limits.rows)} rows of output`,
    limits.upload_mb && `${limits.upload_mb} MB uploads`,
  ].filter(Boolean);

  $("#shared-notice").replaceChildren(note(
    "<strong>This is a hosted, shared instance.</strong> Anything you upload is written to " +
    "the server's disk, into a workspace shared with every other visitor, and the operator " +
    "of this instance can read it. Storage is temporary and is wiped when the instance " +
    "restarts, so download what you generate before you leave." +
    (bounds.length ? ` Limited to ${bounds.join(", ")}.` : "") +
    "<br><br>Do not upload confidential data. To use your own tapes privately, run it on " +
    "your own machine — clone the repository, then " +
    "<code class=\"inline\">pip install -e '.[web]'</code> and " +
    "<code class=\"inline\">sdd ui</code>. Nothing leaves it. " +
    `<a href="${SOURCE_URL}" target="_blank" rel="noopener noreferrer">Source</a>.`,
    "warn", true));
}

async function loadPacks() {
  const meta = await call("/api/meta");
  state.meta = meta;
  $("#version").textContent = `v${meta.version}`;
  describeInstance(meta);

  const list = $("#packs");
  list.replaceChildren();
  if (!meta.packs.length) {
    list.append(el("p", { class: "hint", text: "No packs are bundled with this build." }));
    return;
  }
  const broken = meta.pack_problems || {};
  for (const name of meta.packs) {
    // A pack the server cannot load is shown as unavailable, with the reason.
    // Listing it and letting the click fail is how a stale process reads as a
    // dead button.
    if (broken[name]) {
      list.append(el("div", { class: "pack broken" }, [
        el("span", { class: "pack-text" }, [
          el("div", { class: "name" }, [
            el("span", { text: name }),
            el("span", { class: "star warn", text: "unavailable" }),
          ]),
          ...broken[name].map((reason) => el("div", { class: "m", text: reason })),
        ]),
      ]));
      continue;
    }
    const info = await call(`/api/packs/${name}`);
    const summary = info.summary;
    list.append(el("button", {
      class: summary.featured ? "pack featured" : "pack",
      onclick: () => loadPack(name, info),
    }, [
      el("span", { class: "pack-text" }, [
        el("div", { class: "name" }, [
          el("span", { text: summary.title }),
          summary.featured ? el("span", { class: "star", text: "Start here" }) : null,
        ].filter(Boolean)),
        summary.regulatory_template
          ? el("div", { class: "m", text: summary.regulatory_template })
          : null,
        el("div", {
          class: "m",
          text: `${summary.asset_class} · ${summary.columns} columns · ` +
                `${summary.periods} periods · ` +
                `${summary.scenarios.join(", ") || "no scenarios"}`,
        }),
      ]),
      el("span", { class: "spacer" }),
      // The identifier `sdd run` takes. Kept visible, but away from the title —
      // beside it, the two read as one run-together word.
      el("code", { class: "pack-id", text: name }),
      el("span", { class: "badge req", text: "Load" }),
    ]));
  }
}

/* Loading a pack is asynchronous and used to be fired and forgotten.
 *
 * `onclick: () => choosePack(...)` returns a promise nobody holds, so anything
 * that threw inside became an unhandled rejection: a line in a console the user
 * is not looking at, and an interface that simply did not respond. "The button
 * does not work" is what that looks like from the outside, and it is the one
 * failure mode with no way to report itself. */
async function loadPack(name, info) {
  try {
    await choosePack(name, info);
  } catch (error) {
    $("#upload-error").replaceChildren(note(
      `<strong>${escapeHtml(info?.summary?.title || name)} could not be loaded.</strong>` +
      `<br>${escapeHtml(error.message || String(error))}`, "bad", true));
    status(`${name} could not be loaded.`, "bad");
    show("upload");
  }
}

async function choosePack(name, info) {
  state.source = null;
  state.profile = null;
  state.capabilities = info.capabilities;
  seedSettings(info.spec, info.capabilities);
  await adoptSpec(structuredClone(info.spec), { origin: info.summary.title });

  state.schema = await call("/api/schema", {
    method: "POST",
    body: JSON.stringify({ spec: state.spec }),
  });
  reach("review");
  reach("configure");
  renderReview();
  show("review");
  status(`Loaded ${info.summary.title}. Check the detected types, or go straight to configuring.`);
}

/* -------------------------------------------------------------- 2. review */

function seedSettings(spec, capabilities) {
  const s = state.settings;
  s.periods = spec.entity.calendar.periods;
  s.freq = spec.entity.calendar.freq;
  s.method = spec.generation?.method || "distribution";
  s.noise = spec.generation?.noise ?? 0;
  s.correlation = spec.generation?.correlation ?? 1;
  s.outliers = spec.generation?.outliers ?? 0;
  s.missing = spec.generation?.missing ?? 0;
  const rates = capabilities?.rates || {};
  s.default_rate = rates.default_rate;
  s.prepayment_rate = rates.prepayment_rate;
  s.recovery_rate = rates.recovery_rate;
  s.origination_rate = spec.originations?.rate ?? 0;

  // A pack that states the size it was calibrated for opens at that size.
  // The CLO pack aims at EUR 500m across 500 facilities and the box defaulted
  // to 10,000, so loading it and pressing Generate produced a EUR 10bn
  // portfolio — arithmetically correct and not the thing the pack describes.
  const declared = (spec.entity?.targets || []).find((t) => t.entities)?.entities;
  if (declared) s.records = declared;

  s.touched = new Set();
}

function renderReview() {
  const schema = state.schema;
  if (!schema) return;
  const profile = state.profile;

  $("#review-lede").textContent = profile
    ? (profile.is_panel
        ? "The file holds the same entities at several cut-offs, so behaviour over time was measured as well as the shape of each column."
        : "The file is a single snapshot. Column shapes were measured, but with nothing observed twice there is no way to tell a fixed field from a moving one.")
    : "Read from the schema alone. Types come from what the schema declares; distributions are yours to choose in the next step.";

  $("#review-stats").replaceChildren(
    stat("Columns", fmt.int(schema.counts.columns)),
    stat("Static", fmt.int(schema.counts.static)),
    stat("Dynamic", fmt.int(schema.counts.dynamic)),
    stat("Derived", fmt.int(schema.counts.derived)),
    stat("Date columns", fmt.int(schema.date_columns.length)),
    stat("Nullable", fmt.int(schema.nullable.length)),
    stat("Need review", fmt.int(schema.needs_review.length),
      schema.needs_review.length ? "bad" : "good"),
  );

  const detection = $("#review-detection");
  detection.replaceChildren();
  const notes = profile?.detection_notes || {};
  detection.append(note(
    `<strong>Primary key</strong> <code class="inline">${escapeHtml(schema.primary_key)}</code>` +
    (notes.id_column ? ` — ${escapeHtml(notes.id_column)}` : ""), "", true));
  detection.append(note(
    `<strong>Date column</strong> <code class="inline">${escapeHtml(schema.time_column)}</code>` +
    (notes.time_column ? ` — ${escapeHtml(notes.time_column)}` : ""), "", true));
  if (schema.needs_review.length) {
    detection.append(note(
      `${schema.needs_review.length} column(s) were inferred with low confidence. Filter to ` +
      `“needs review” below and check those first.`, "warn"));
  }

  renderReviewTable();
  $("#review-filter").oninput = renderReviewTable;
  $("#review-review-only").onchange = renderReviewTable;
}

const DTYPES = ["int", "float", "str", "category", "bool", "date"];

function renderReviewTable() {
  const needle = $("#review-filter").value.trim().toLowerCase();
  const reviewOnly = $("#review-review-only").checked;
  const rows = state.schema.columns.filter((c) =>
    (!needle || c.name.toLowerCase().includes(needle)) && (!reviewOnly || c.review));

  const table = $("#review-table");
  table.replaceChildren(
    el("thead", {}, el("tr", {}, [
      el("th", { text: "Column" }),
      el("th", { text: "Type" }),
      el("th", { text: "Role" }),
      el("th", { text: "Key" }),
      el("th", { text: "Required" }),
      el("th", { class: "num", text: "Distinct" }),
      el("th", { class: "num", text: "Null in sample" }),
      el("th", { class: "num", text: "Confidence" }),
      el("th", { text: "Example" }),
    ])),
    el("tbody", {}, rows.map(reviewRow)),
  );
  if (!rows.length) {
    table.append(el("tbody", {}, el("tr", {}, el("td", { colspan: 9 }, "No columns match."))));
  }
}

function reviewRow(column) {
  const pending = state.edits.get(column.name) || {};

  const name = el("input", {
    type: "text",
    value: pending.rename ?? column.name,
    oninput: (e) => stageEdit(column.name, { rename: e.target.value.trim() }),
  });

  const type = el("select", {
    onchange: (e) => stageEdit(column.name, { dtype: e.target.value }),
  }, DTYPES.map((d) => el("option", {
    value: d, text: d, selected: d === (pending.dtype ?? column.dtype),
  })));

  const required = el("input", {
    type: "checkbox",
    checked: pending.required ?? column.required,
    onchange: (e) => stageEdit(column.name, { required: e.target.checked }),
  });

  const confidence = column.confidence;
  const badge = confidence === null || confidence === undefined ? ""
    : confidence >= 0.75 ? "good" : confidence >= 0.5 ? "warn" : "bad";

  return el("tr", {}, [
    el("td", {}, name),
    el("td", {}, type),
    el("td", {}, el("span", { class: "badge role", text: column.role })),
    el("td", {}, column.primary_key
      ? el("span", { class: "badge req", text: "key" })
      : column.date_column ? el("span", { class: "badge", text: "date" }) : ""),
    el("td", {}, el("label", { class: "check" }, [required, el("span", { text: "required" })])),
    el("td", { class: "num", text: fmt.int(column.distinct) }),
    el("td", { class: "num", text: column.observed_nulls === null || column.observed_nulls === undefined
      ? "—" : fmt.pct(column.observed_nulls, 1) }),
    el("td", { class: "num" }, confidence === null || confidence === undefined
      ? "—" : el("span", { class: `badge ${badge}`, text: fmt.num(confidence, 2) })),
    el("td", {}, el("code", {
      text: (column.examples || []).slice(0, 2).join(", ").slice(0, 42) || "—",
    })),
  ]);
}

function stageEdit(original, patch) {
  const current = state.edits.get(original) || { original, name: original };
  state.edits.set(original, { ...current, ...patch });
  status(`${state.edits.size} pending edit(s). They apply when you generate the configuration.`);
}

async function applyEditsThenConfigure() {
  if (state.edits.size) {
    try {
      const result = await call("/api/schema/edit", {
        method: "POST",
        body: JSON.stringify({
          spec: state.spec,
          source: state.source,
          edits: Array.from(state.edits.values()),
        }),
      });
      state.spec = result.spec;
      state.edits.clear();
      await validate();
      if (result.applied.length) {
        status(`Applied: ${result.applied.slice(0, 3).join("; ")}` +
          (result.applied.length > 3 ? ` (+${result.applied.length - 3} more)` : ""), "good");
      }
      state.schema = await call("/api/schema", {
        method: "POST",
        body: JSON.stringify({ spec: state.spec, source: state.source }),
      });
      renderReview();
    } catch (error) {
      status(error.message, "bad");
      return;
    }
  }
  await pushConfigure({});
  reach("configure");
  show("configure");
}

/* ----------------------------------------------------------- 3. configure */

const METHODS = [
  ["statistical", "Statistical",
   "Every numeric column becomes a normal with the same mean and spread. Fast and obvious; wrong in the tails on purpose."],
  ["distribution", "Distribution based",
   "The best-fitting named distribution per column — lognormal, gamma, beta — chosen by the profiler. The most faithful closed-form option."],
  ["rule_based", "Rule based",
   "No fitted shape at all: numbers uniform inside their bounds, categories equally likely inside their domain. The schema-only path."],
  ["sampling", "Sampling",
   "Resamples the observed values, spikes and all. The only method that reproduces a zero-inflated column exactly.", "needs sample data"],
  ["ctgan", "CTGAN",
   "A deep tabular model trained on your tape, learning how columns move together rather than one at a time.", "needs sample data and the deep extra"],
  ["hybrid", "Hybrid",
   "Fitted distributions across the schema, then a deep polish over the columns it can improve.", "needs sample data and the deep extra"],
];

function renderConfigure() {
  if (!state.spec) return;
  renderScale();
  renderMethods();
  renderRandomness();
  renderAging();
  renderColumns();
  refreshYaml();
  renderGroupStates();
}

/* What each collapsed group currently holds.
 *
 * A tab hides its contents behind a click and says nothing about them, so the
 * only way to know whether a setting mattered was to open all six. A line of
 * state on the summary turns an unopened group into an answered question. */
function renderGroupStates() {
  const s = state.settings;
  const caps = state.capabilities || {};
  const spec = state.spec;

  const set = (id, text, changed) => {
    const host = $(id);
    if (!host) return;
    host.textContent = text;
    host.classList.toggle("changed", Boolean(changed));
  };

  const noun = entityNoun();
  set("#state-essentials",
      `${fmt.int(s.records)} ${noun} · ${s.periods} × ${freqLabel(s.freq)}` +
      (s.scenario ? ` · ${s.scenario}` : ""),
      s.scenario);

  const rates = ["default_rate", "prepayment_rate", "recovery_rate"].filter((k) => s.touched.has(k));
  set("#state-behaviour",
      caps.ageing
        ? (rates.length ? `${rates.length} rate${rates.length > 1 ? "s" : ""} changed`
                        : "calibrated defaults")
        : "no lifecycle — single snapshot",
      rates.length);

  const knobs = Object.entries(s.randomness || {}).filter(([, v]) => Number(v) > 0);
  const method = (spec.generation || {}).method || "distribution";
  set("#state-realism",
      `${method.replace("_", " ")}` +
      (knobs.length ? ` · ${knobs.length} knob${knobs.length > 1 ? "s" : ""} on` : " · no noise"),
      knobs.length);

  set("#state-columns", `${outputColumnCount()} columns`, false);
  set("#state-document", caps.ageing ? `${(caps.states || []).length} states` : "no lifecycle",
      false);
}

/* The word for one row of the opening book. "Rows" was wrong and actively
 * misleading: the field has always held entities, and the note underneath said
 * so while the label above it said otherwise. */
/* What this pack calls one row of its opening book.
 *
 * The spec says so — `meta.entity_noun` — because the pack knows and the
 * browser does not. This used to match substrings of `asset_class` here, which
 * meant a pack could not name itself: anything the list did not recognise was a
 * loan, so a buy-now-pay-later plan and a trade receivable were both "loans"
 * and no pack author could do anything about it.
 *
 * The old guesses survive as a fallback, for a spec profiled from a tape rather
 * than written by hand — which has an asset class and no vocabulary. */
function entityNoun() {
  const declared = state.summary?.entity_noun_plural || state.spec?.meta?.entity_noun_plural;
  if (declared) return declared;

  const asset = (state.spec?.meta?.asset_class || "").toLowerCase();
  if (asset.includes("clo") || asset.includes("leveraged")) return "facilities";
  if (asset.includes("card")) return "accounts";
  if (asset.includes("lease")) return "leases";
  return "loans";
}

function freqLabel(freq) {
  return {
    day: "daily",
    week_end: "weekly",
    fortnight_end: "fortnightly",
    month_end: "monthly",
    month_start: "monthly",
    quarter_end: "quarterly",
    year_end: "annual",
  }[freq] || "monthly";
}

/* Columns as they reach the file, which is what every other screen reports.
 * The review step counted declared columns and the header counted output ones,
 * so a grouped pack disagreed with itself by the number of group attributes. */
function outputColumnCount() {
  const spec = state.spec;
  if (!spec) return 0;
  if (spec.emit?.column_order?.length) return spec.emit.column_order.length;
  const own = (spec.columns || []).filter((c) => c.role !== "helper").map((c) => c.name);
  const grouped = [];
  for (const group of spec.groups || []) {
    grouped.push(group.key);
    for (const column of group.columns || []) {
      if (column.role !== "helper") grouped.push(column.name);
    }
  }
  return new Set([...own, ...grouped]).size;
}

function renderScale() {
  const spec = state.spec;
  const s = state.settings;
  $("#cfg-records").value = s.records;
  $("#cfg-seed").value = s.seed;

  const scenarios = Object.keys(spec.scenarios || {});
  const select = $("#cfg-scenario");
  select.replaceChildren(el("option", {
    value: "",
    text: scenarios.length ? "None (base calibration)" : "No scenarios in this configuration",
  }));
  for (const name of scenarios) {
    select.append(el("option", { value: name, text: name, selected: name === s.scenario }));
  }
  select.disabled = !scenarios.length;
  select.onchange = () => {
    s.scenario = select.value;
    const chosen = spec.scenarios?.[select.value];
    $("#cfg-scenario-sub").textContent = chosen?.description
      || "A stress overlay shifting defaults, prepayment and collateral together.";
  };

  updateSizeNote();
}

/* One sentence saying what pressing Generate produces.
 *
 * Sat at the bottom of a tab before, under the heading "How much data", where
 * the label said "Rows to generate" and the field held entities. Three
 * different claims on one screen, and the arithmetic between them is the whole
 * point: entities times cut-offs is the number that fills a disk. */
function updateSizeNote() {
  const s = state.settings;
  const periods = s.periods || 1;
  const columns = outputColumnCount();
  const noun = entityNoun();
  const rows = s.records * periods;
  const host = $("#cfg-outcome");
  if (!host) return;

  const limits = state.meta?.limits || {};
  let warning = "";
  if (limits.rows && rows > limits.rows) {
    warning = `<br><span class="bad">This asks for more than the ` +
      `${fmt.int(limits.rows)} rows this instance allows. Reduce either number.</span>`;
  }

  host.innerHTML =
    `<strong>${fmt.int(s.records)}</strong> ${noun} over ` +
    `<strong>${periods}</strong> ${freqLabel(s.freq)} cut-offs ` +
    `→ about <strong>${fmt.int(rows)}</strong> rows across ` +
    `<strong>${columns}</strong> columns.` +
    `<span class="muted"> ${cap(noun)} leave the pool as they redeem, mature or write off, ` +
    `so the real count comes in a little under.</span>${warning}`;

  const label = $("#cfg-records-label");
  if (label) label.textContent = `How many ${noun}`;
  const sub = $("#cfg-records-sub");
  if (sub) {
    sub.textContent = `Each one appears once per cut-off, so the row count is this ` +
      `times ${periods}.`;
  }
  renderGroupStates();
}

function cap(word) { return word.charAt(0).toUpperCase() + word.slice(1); }

function renderMethods() {
  const host = $("#cfg-methods");
  const hasSample = Boolean(state.source && state.uploads.sample);
  const hasDeep = Boolean(state.meta?.deep_models);
  host.replaceChildren();

  for (const [key, label, description, requirement] of METHODS) {
    const needsSample = key === "sampling" ? !state.profile : ["ctgan", "hybrid"].includes(key) && !hasSample;
    const needsDeep = ["ctgan", "hybrid"].includes(key) && !hasDeep;
    const blocked = needsSample || needsDeep;

    host.append(el("button", {
      class: "method",
      "aria-pressed": String(state.settings.method === key),
      disabled: blocked,
      title: blocked ? requirement : "",
      onclick: () => chooseMethod(key),
    }, [
      el("div", { class: "m-name" }, [
        label,
        state.settings.method === key ? el("span", { class: "badge good", text: "in use" }) : null,
      ]),
      el("div", { class: "m-desc", text: description }),
      blocked ? el("div", { class: "m-need", text:
        needsDeep && needsSample ? `Unavailable — ${requirement}.`
        : needsDeep ? "Unavailable — install the deep extra: pip install -e '.[deep]'."
        : "Unavailable — upload sample data in step 1." }) : null,
    ]));
  }
}

async function chooseMethod(method) {
  state.settings.method = method;
  state.settings.touched.add("method");
  renderMethods();
  await pushConfigure({ method, from_base: Boolean(state.source) });
}

function renderRandomness() {
  const host = $("#cfg-randomness");
  const s = state.settings;
  const canCorrelate = Boolean(state.capabilities?.correlation);
  const optional = state.capabilities?.optional_columns || [];
  host.replaceChildren();

  host.append(sliderField(
    "Noise", s.noise, 0, 0.5, 0.01,
    "Gaussian jitter added to every numeric column, as a share of that column's own standard deviation. 5% is a realistic amount of measurement error.",
    (v) => commitRandomness("noise", v), (v) => fmt.pct(v, 0)));

  host.append(sliderField(
    "Correlation", s.correlation, 0, 1, 0.05,
    canCorrelate
      ? "How much of the sample's measured correlation to reimpose. Columns are reordered to match, which changes how they move together without changing any column's own distribution."
      : "No sample was analysed, so no relationship between columns was ever measured. This control has nothing to reimpose.",
    (v) => commitRandomness("correlation", v), (v) => fmt.pct(v, 0), !canCorrelate));

  host.append(sliderField(
    "Outliers", s.outliers, 0, 0.1, 0.005,
    "Share of rows pushed four standard deviations into the tail — the data-quality artefacts a downstream system should survive. Declared bounds are still respected.",
    (v) => commitRandomness("outliers", v), (v) => fmt.pct(v, 1)));

  host.append(sliderField(
    "Missing values", s.missing, 0, 0.5, 0.01,
    optional.length
      ? `Share of values blanked across the ${optional.length} optional column(s). Identifiers, dates and anything the engine reads are never blanked.`
      : "Every column is currently marked required, so there is nothing to blank. Mark columns optional in the review step.",
    (v) => commitRandomness("missing", v), (v) => fmt.pct(v, 0), !optional.length));
}

function sliderField(label, value, min, max, step, sub, onCommit, format, disabled = false) {
  const output = el("output", { text: format(value) });
  const input = el("input", {
    type: "range", min, max, step, value, disabled,
    oninput: (e) => { output.textContent = format(parseFloat(e.target.value)); },
    onchange: (e) => onCommit(parseFloat(e.target.value)),
  });
  return el("label", { class: "field" }, [
    el("span", { class: "lbl", text: label }),
    el("div", { class: "slider" }, [input, output]),
    el("span", { class: "sub", text: sub }),
  ]);
}

const commitRandomness = (key, value) => {
  state.settings[key] = value;
  state.settings.touched.add(key);
  pushConfigure({});
};

function renderAging() {
  const spec = state.spec;
  const caps = state.capabilities || {};
  const s = state.settings;

  const basics = $("#cfg-aging-basics");
  basics.replaceChildren(
    el("label", { class: "field" }, [
      el("span", { class: "lbl", text: "Number of periods" }),
      el("input", {
        type: "number", min: 1, step: 1, value: s.periods,
        onchange: (e) => {
          s.periods = Math.max(1, parseInt(e.target.value, 10) || 1);
          s.touched.add("periods");
          updateSizeNote();
          pushConfigure({});
        },
      }),
      el("span", { class: "sub", text: `Cut-offs to emit, starting ${spec.entity.calendar.start}.` }),
    ]),
    el("label", { class: "field" }, [
      el("span", { class: "lbl", text: "Frequency" }),
      el("select", {
        onchange: (e) => {
          s.freq = e.target.value;
          s.touched.add("freq");
          updateSizeNote();
          pushConfigure({});
        },
      }, [
        el("option", { value: "week_end", text: "Weekly", selected: s.freq === "week_end" }),
        el("option", { value: "fortnight_end", text: "Fortnightly", selected: s.freq === "fortnight_end" }),
        el("option", { value: "month_end", text: "Monthly", selected: s.freq === "month_end" }),
        el("option", { value: "quarter_end", text: "Quarterly", selected: s.freq === "quarter_end" }),
        el("option", { value: "year_end", text: "Annually", selected: s.freq === "year_end" }),
      ]),
      el("span", { class: "sub", text: "How far apart the cut-offs are. Rates below are annual either way." }),
    ]),
    el("div", { class: "field" }, [
      el("span", { class: "lbl", text: "Lifecycle" }),
      el("div", {}, caps.ageing
        ? el("span", { class: "badge good", text: `${(caps.states || []).length} states` })
        : el("span", { class: "badge warn", text: "none — single snapshot" })),
      el("span", { class: "sub", text: caps.ageing
        ? (caps.states || []).join(" → ")
        : "Without states there is nothing to age: every period would be identical." }),
    ]),
  );

  const host = $("#cfg-aging-rates");
  host.replaceChildren();
  host.append(rateField(
    "Default rate", s.default_rate, caps.default_rate,
    "Share of performing loans that default within a year. Setting it rescales the transition matrix and renormalises every row, so the matrix stays a matrix.",
    "this configuration declares no state a loan cannot recover from, so nothing counts as a default",
    (v) => commitRate("default_rate", v)));
  host.append(rateField(
    "Prepayment rate", s.prepayment_rate, caps.prepayment_rate,
    "Annual chance a healthy loan redeems early. Applied as the hazard rate the engine already uses.",
    "no hazard in this configuration takes a healthy loan out of the pool",
    (v) => commitRate("prepayment_rate", v)));
  host.append(rateField(
    "Recovery rate", s.recovery_rate, caps.recovery_rate,
    "Share of the balance recovered when a loan writes off, booked in the period it happens. A recovery column is added if there is not one.",
    "this configuration has no write-off state, so there is no point at which a recovery would be booked",
    (v) => commitRate("recovery_rate", v)));

  renderOriginations();
  renderMatrix();
}

function renderOriginations() {
  const host = $("#cfg-originations");
  const caps = state.capabilities || {};
  const s = state.settings;
  host.replaceChildren();

  if (!caps.originations) {
    host.append(note(
      "New loans need a lifecycle: without states there is no ageing for them to join, " +
      "and every period would be identical.", "warn"));
    return;
  }

  host.append(sliderField(
    "New loans per period", s.origination_rate, 0, 0.1, 0.0025,
    "Share of the opening pool written at every cut-off after the first. At 0 the pool is " +
    "closed — it only shrinks, and every loan exists at the first cut-off. Above 0 it stays " +
    "open, so the panel holds loans written during the window as well as before it.",
    (v) => { s.origination_rate = v; s.touched.add("origination_rate");
             pushConfigure({ origination_rate: v }); },
    (v) => (v ? `${(v * 100).toFixed(2)}%` : "closed pool")));

  // The arithmetic people actually want: how big does the pool get?
  const periods = s.periods || 1;
  const perPeriod = Math.round(state.settings.records * s.origination_rate);
  host.append(el("div", { class: "field" }, [
    el("span", { class: "lbl", text: "Over the whole run" }),
    el("div", { class: "note", html: s.origination_rate
      ? `About <strong>${fmt.int(perPeriod)}</strong> new loans each period — ` +
        `<strong>${fmt.int(perPeriod * (periods - 1))}</strong> across ${periods - 1} cut-off(s), ` +
        `on top of the ${fmt.int(state.settings.records)} the pool opens with. Attrition still ` +
        `removes loans as they redeem and write off, so the pool grows by less than that.`
      : `The pool opens with ${fmt.int(state.settings.records)} loans and only shrinks from there.` }),
  ]));
}

function rateField(label, value, available, sub, why, onCommit) {
  if (!available) {
    return el("div", { class: "field" }, [
      el("span", { class: "lbl", text: label }),
      el("input", { type: "text", value: "not applicable", disabled: true }),
      el("span", { class: "sub", text: `Unavailable — ${why}.` }),
    ]);
  }
  const current = value === null || value === undefined ? 0 : value;
  const output = el("output", { text: fmt.pct(current, 2) });
  const input = el("input", {
    type: "range", min: 0, max: 0.5, step: 0.0025, value: current,
    oninput: (e) => { output.textContent = fmt.pct(parseFloat(e.target.value), 2); },
    onchange: (e) => onCommit(parseFloat(e.target.value)),
  });
  return el("label", { class: "field" }, [
    el("span", { class: "lbl", text: `${label} (annual)` }),
    el("div", { class: "slider" }, [input, output]),
    el("span", { class: "sub", text: sub }),
  ]);
}

const commitRate = (key, value) => {
  state.settings[key] = value;
  state.settings.touched.add(key);
  pushConfigure({});
};

function renderMatrix() {
  const lc = state.spec.lifecycle;
  $("#card-lifecycle").hidden = !lc || !lc.transitions;
  if (!lc || !lc.transitions) return;

  const states = lc.transition_states
    || lc.states.filter((s) => !(lc.terminal || []).includes(s));

  const head = el("tr", {}, [el("th", { class: "rowhdr", text: "from ╲ to" })]);
  for (const name of states) head.append(el("th", { class: "num", text: name }));
  head.append(el("th", { class: "num", text: "Σ" }));

  const body = el("tbody");
  lc.transitions.forEach((row, i) => {
    const tr = el("tr", {}, [el("th", { class: "rowhdr", text: states[i] })]);
    row.forEach((value, j) => {
      const input = el("input", {
        type: "number", step: "0.0001", min: "0", max: "1", value: Number(value).toFixed(4),
        oninput: (e) => {
          const parsed = parseFloat(e.target.value);
          lc.transitions[i][j] = Number.isFinite(parsed) ? parsed : 0;
          updateRowSum(i);
        },
        onchange: () => { validate(); refreshYaml(); },
      });
      tr.append(el("td", { class: i === j ? "diag" : "" }, input));
    });
    tr.append(el("td", { class: "sum", "data-sum": String(i) }));
    body.append(tr);
  });

  $("#cfg-matrix").replaceChildren(el("table", {}, [el("thead", {}, head), body]));
  lc.transitions.forEach((_, i) => updateRowSum(i));
}

function updateRowSum(i) {
  const total = (state.spec.lifecycle.transitions[i] || [])
    .reduce((a, b) => a + (Number(b) || 0), 0);
  const cell = $(`#cfg-matrix td.sum[data-sum="${i}"]`);
  if (!cell) return;
  cell.textContent = total.toFixed(4);
  cell.className = `sum ${Math.abs(total - 1) < 1e-6 ? "ok" : "off"}`;
}

function renderColumns() {
  const host = $("#cfg-columns");
  const needle = $("#cfg-filter").value.trim().toLowerCase();
  host.replaceChildren();

  const editable = state.spec.columns.filter((c) =>
    c.generator && (!needle || c.name.toLowerCase().includes(needle)));
  if (!editable.length) {
    host.append(el("p", { class: "hint", text: "No sampled columns match." }));
    return;
  }

  for (const column of editable.slice(0, 100)) {
    const gen = column.generator;
    const row = el("div", { class: "col-row" }, [
      el("div", { class: "col-head" }, [
        el("code", { class: "cname", text: column.name }),
        el("span", { class: "badge role", text: column.role }),
        el("span", { class: "badge", text: gen.kind }),
        column.required === false ? el("span", { class: "badge warn", text: "optional" }) : null,
        el("span", { class: "spacer" }),
        column.confidence === null || column.confidence === undefined ? null
          : el("span", {
              class: `badge ${column.confidence >= 0.75 ? "good" : column.confidence >= 0.5 ? "warn" : "bad"}`,
              text: `conf ${fmt.num(column.confidence, 2)}`,
            }),
      ]),
    ]);

    const params = el("div", { class: "col-params" });
    if (gen.kind === "categorical" && gen.weights) {
      gen.values.slice(0, 24).forEach((value, i) => {
        params.append(labelled(String(value), numberInput(gen.weights, i, 0.001)));
      });
    } else if (gen.kind === "scipy") {
      for (const key of Object.keys(gen.params || {})) {
        params.append(labelled(key, numberInput(gen.params, key)));
      }
    } else if (gen.kind === "gaussian") {
      params.append(labelled("mean", numberInput(gen, "mean")));
      params.append(labelled("stddev", numberInput(gen, "stddev")));
    } else if (gen.kind === "uniform") {
      params.append(labelled("low", numberInput(gen, "low")));
      params.append(labelled("high", numberInput(gen, "high")));
    } else if (gen.kind === "bernoulli") {
      params.append(labelled("p", numberInput(gen, "p", 0.005)));
    } else if (gen.kind === "constant") {
      params.append(labelled("value", el("input", {
        type: "text", value: gen.value ?? "",
        onchange: (e) => { gen.value = e.target.value; validate(); refreshYaml(); },
      })));
    } else if (gen.kind === "empirical") {
      params.append(el("p", { class: "hint", style: "margin:0", text:
        `${gen.values.length} observed value(s) resampled with their measured weights. ` +
        `Edit these in the Advanced tab.` }));
    } else {
      params.append(el("p", { class: "hint", style: "margin:0", text:
        `${gen.kind} — edit this one in the Advanced tab.` }));
    }
    row.append(params);

    if (column.review) row.append(el("div", { class: "col-review", text: `Review: ${column.review}` }));
    host.append(row);
  }

  if (editable.length > 100) {
    host.append(el("p", { class: "hint", text:
      `Showing 100 of ${editable.length} columns — filter to narrow it down.` }));
  }
}

function labelled(title, control) {
  return el("label", { class: "field" }, [
    el("span", { class: "lbl", text: title }),
    control,
  ]);
}

function numberInput(target, key, step = "any") {
  return el("input", {
    type: "number", step, value: target[key],
    onchange: (e) => {
      const parsed = parseFloat(e.target.value);
      target[key] = Number.isFinite(parsed) ? parsed : 0;
      validate();
      refreshYaml();
    },
  });
}

/* the configure round trip -------------------------------------------------- */

async function pushConfigure(extra) {
  const s = state.settings;
  const touched = s.touched;
  const body = {
    source: state.source,
    spec: state.spec,
    noise: s.noise,
    correlation: s.correlation,
    outliers: s.outliers,
    missing: s.missing,
    periods: s.periods,
    freq: s.freq,
    // Only sent once deliberately set: re-sending a rate would overwrite a
    // matrix the user had just edited by hand.
    default_rate: touched.has("default_rate") ? s.default_rate : null,
    prepayment_rate: touched.has("prepayment_rate") ? s.prepayment_rate : null,
    recovery_rate: touched.has("recovery_rate") ? s.recovery_rate : null,
    origination_rate: touched.has("origination_rate") ? s.origination_rate : null,
    ...extra,
  };

  try {
    const result = await call("/api/configure", { method: "POST", body: JSON.stringify(body) });
    state.spec = result.spec;
    state.valid = result.valid;
    state.problems = result.problems;
    if (result.capabilities) {
      state.capabilities = { ...result.capabilities, optional_columns: result.optional_columns };
    }
    if (result.rates) {
      for (const key of ["default_rate", "prepayment_rate", "recovery_rate"]) {
        if (!touched.has(key)) s[key] = result.rates[key];
      }
    }
    // What the method did describes the spec as it now stands, so it survives
    // later edits; every other note describes the call that produced it.
    const notes = result.notes || [];
    if (extra.method) state.methodNotes = notes.filter((n) => !/rate set|not applied/i.test(n));
    renderNotes(extra.method ? notes : [...(state.methodNotes || []), ...notes]);
    await validate();
    renderConfigure();
  } catch (error) {
    status(error.message, "bad");
  }
}

// A note belongs beside the control it is about, so it is routed by what it
// mentions. Anything unrecognised goes to the method tab, which is where a note
// about the configuration as a whole makes most sense.
const NOTE_TABS = [
  [/correlation|missing values|blank|noise|outlier/i, "#randomness-notes"],
  [/new loans|origination|dated to the period|rate|matrix|recovery|prepay/i, "#aging-notes"],
];

function renderNotes(notes) {
  const hosts = ["#method-notes", "#aging-notes", "#randomness-notes"];
  hosts.forEach((id) => $(id).replaceChildren());
  for (const text of notes) {
    const match = NOTE_TABS.find(([pattern]) => pattern.test(text));
    $(match ? match[1] : "#method-notes").append(
      note(text, /not applied|nowhere|no sample|nothing to/i.test(text) ? "warn" : "")
    );
  }
}

/* raw YAML ------------------------------------------------------------------ */

async function refreshYaml() {
  try {
    const { yaml } = await call("/api/spec/yaml", {
      method: "POST", body: JSON.stringify(state.spec),
    });
    $("#cfg-yaml").value = yaml;
  } catch { /* the forms remain the source of truth */ }
}

function wireYaml() {
  $("#btn-yaml-apply").onclick = async () => {
    const target = $("#yaml-status");
    try {
      const result = await call("/api/spec/parse", {
        method: "POST", body: JSON.stringify({ yaml: $("#cfg-yaml").value }),
      });
      if (!result.valid) {
        target.replaceChildren(note(
          `<strong>Not applied.</strong><ul>${
            result.problems.map((p) => `<li>${escapeHtml(p)}</li>`).join("")
          }</ul>`, "bad", true));
        return;
      }
      state.spec = result.parsed;
      seedSettings(state.spec, state.capabilities);
      target.replaceChildren(note("Applied. The forms now reflect this document.", "good"));
      await validate();
      renderConfigure();
    } catch (error) {
      target.replaceChildren(note(error.message, "bad"));
    }
  };
  $("#btn-yaml-revert").onclick = () => {
    $("#yaml-status").replaceChildren();
    refreshYaml();
  };
}

/* ------------------------------------------------------------ 4. generate */

async function startRun() {
  state.job = null;
  state.result = null;
  $("#run-error").replaceChildren();
  reach("generate");
  show("generate");

  state.settings.records = parseInt($("#cfg-records").value, 10) || 10000;
  state.settings.seed = parseInt($("#cfg-seed").value, 10) || 42;

  try {
    const started = await call("/api/run", {
      method: "POST",
      body: JSON.stringify({
        spec: state.spec,
        source: state.source,
        num_records: state.settings.records,
        seed: state.settings.seed,
        periods: state.settings.periods,
        scenario: state.settings.scenario || null,
      }),
    });
    state.job = started.job;
    state.jobStages = started.stages;
    renderStages("generating", 0);
    poll();
  } catch (error) {
    $("#run-error").replaceChildren(note(error.message, "bad"));
    status(error.message, "bad");
  }
}

function renderStages(current, progress) {
  const list = $("#run-stages");
  const order = state.jobStages.map((s) => s.key);
  const at = order.indexOf(current);
  list.replaceChildren(...state.jobStages.map((stage, i) => {
    const done = i < at || (progress >= 1 && i <= at);
    const active = i === at && progress < 1;
    return el("li", { class: done ? "done" : active ? "active" : "" }, [
      el("span", { class: "dot" }),
      stage.label,
    ]);
  }));
}

async function poll() {
  if (!state.job) return;
  let job;
  try {
    job = await call(`/api/run/${state.job}`);
  } catch (error) {
    $("#run-error").replaceChildren(note(error.message, "bad"));
    return;
  }

  $("#run-stage").textContent = job.stage || job.status;
  $("#run-pct").textContent = fmt.pct(job.progress, 0);
  $("#run-bar").style.width = `${(job.progress * 100).toFixed(1)}%`;
  $("#run-eta").textContent = job.eta_seconds
    ? `about ${fmt.seconds(job.eta_seconds)} left` : "";
  renderStages(job.step || "generating", job.progress);

  if (job.status === "error") {
    $("#run-error").replaceChildren(note(job.error, "bad"));
    status("The run failed.", "bad");
    show("generate");
    return;
  }
  if (job.status === "done") {
    state.result = job.result;
    reach("results");
    reach("download");
    status(`Done in ${job.result.timings.total_seconds.toFixed(1)}s.`, "good");
    await renderResults(job.result);
    renderDownloads(job.result);
    show("results");
    return;
  }
  setTimeout(poll, 400);
}

/* ------------------------------------------------------------- 5. results */

async function renderResults(result) {
  const rows = result.mix.reduce((sum, m) => sum + m.rows, 0);
  const validation = result.validation;

  $("#results-lede").textContent =
    `${fmt.int(rows)} rows across ${result.periods} cut-offs, generated with the ` +
    `${(result.method || "distribution").replace("_", " ")} method in ` +
    `${result.timings.total_seconds.toFixed(1)} seconds.` +
    (result.originated
      ? ` The pool opened with ${fmt.int(result.entities)} ${entityNoun()} and took on ` +
        `${fmt.int(result.originated)} more as it aged.`
      : "");

  const originated = result.originated || 0;
  const total = result.total_entities || result.entities;

  // Filtered, not spread raw: `replaceChildren` turns anything that is not a
  // Node into a text node, so a conditional tile that evaluates to null renders
  // the literal word "null" between the tiles.
  $("#run-stats").replaceChildren(...[
    stat("Rows generated", fmt.int(rows)),
    stat("Columns", fmt.int(state.summary?.columns ?? state.spec.columns.length)),
    stat(cap(entityNoun()), fmt.int(total)),
    originated ? stat("Written later", fmt.int(originated)) : null,
    stat("Periods", fmt.int(result.periods)),
    // Survival is measured against everything that ever entered the pool, not
    // against the opening book, or an open pool reports over 100%.
    stat("Surviving", fmt.pct(total ? result.surviving_entities / total : 0, 1)),
    stat("Time taken", `${result.timings.total_seconds.toFixed(1)}s`),
    stat("Validation",
      validation ? (validation.passed ? "Passed" : `${validation.failed} failed`) : "—",
      validation ? (validation.passed ? "good" : "bad") : ""),
  ].filter(Boolean));

  const target = $("#run-validation");
  target.replaceChildren();
  if (validation) {
    const failed = validation.checks.filter((c) => !c.passed);
    target.append(note(
      `<strong>${validation.total - validation.failed} of ${validation.total} invariants passed</strong> — ` +
      (validation.passed
        ? "the generated panel does everything the configuration says it does."
        : `<ul>${failed.map((c) =>
            `<li><code class="inline">${escapeHtml(c.name)}</code> — ${
              c.error ? escapeHtml(c.error) : `${fmt.int(c.violations)} violating row(s)`}</li>`
          ).join("")}</ul>`),
      validation.passed ? "good" : "bad", true));
  }
  const randomness = result.generation_notes?.randomness;
  if (randomness) {
    const applied = [
      [randomness.correlated, "reordered to match the sample's correlation"],
      [randomness.outliers, "given outliers"],
      [randomness.noised, "jittered"],
      [randomness.blanked, "blanked"],
    ].filter(([n]) => n).map(([n, label]) => `${n} column(s) ${label}`);
    if (applied.length) target.append(note(`Randomness applied: ${applied.join(", ")}.`));
  }
  if (result.scenario) {
    target.append(note(`Scenario overlay applied: <strong>${escapeHtml(result.scenario)}</strong>.`, "", true));
  }

  await loadCharts();
  state.table = { offset: 0, limit: 25, sort: null, descending: false, search: "" };
  await loadTable();
}

async function loadCharts(column = null) {
  const hosts = ["#chart-distribution", "#chart-delinquency", "#chart-ltv", "#chart-balance"];
  hosts.forEach((h) => $(h).replaceChildren(el("div", { class: "empty" }, [
    el("span", { class: "spin" }), "Aggregating…",
  ])));

  try {
    const charts = await call(`/api/charts/${state.job}${column ? `?columns=${encodeURIComponent(column)}` : ""}`);
    state.charts = charts;

    const picker = $("#chart-column");
    if (!column) {
      picker.replaceChildren(...charts.numeric_columns.map((name) =>
        el("option", { value: name, text: name })));
      picker.onchange = () => loadCharts(picker.value);
    }

    const distribution = charts.distribution.find((d) => !column || d.column === column)
      || charts.distribution[0];
    $("#dist-hint").textContent = charts.has_reference
      ? "Generated data against the sample it was built from, on shared bins."
      : "Generated data. Upload sample data in step 1 to compare it against the real thing.";
    renderDistribution($("#chart-distribution"), distribution);
    renderPackCharts(charts.configured || []);
    renderDelinquency($("#chart-delinquency"), charts.delinquency, charts.unavailable.delinquency);
    renderLtv($("#chart-ltv"), charts.ltv, charts.unavailable.ltv);
    renderBalance($("#chart-balance"), charts.pool_balance, charts.unavailable.pool_balance);
  } catch (error) {
    hosts.forEach((h) => $(h).replaceChildren(el("div", { class: "empty", text: error.message })));
  }
}

/* the data table ------------------------------------------------------------ */

async function loadTable() {
  const t = state.table;
  const params = new URLSearchParams({
    offset: t.offset, limit: t.limit, descending: String(t.descending),
  });
  if (t.search) params.set("search", t.search);
  if (t.sort) params.set("sort", t.sort);

  try {
    const page = await call(`/api/table/${state.job}?${params}`);
    renderTable(page);
  } catch (error) {
    $("#data-table").replaceChildren(el("tbody", {}, el("tr", {},
      el("td", { text: error.message }))));
  }
}

function renderTable(page) {
  const t = state.table;
  const table = $("#data-table");
  table.replaceChildren(
    el("thead", {}, el("tr", {}, page.columns.map((name) =>
      el("th", {
        class: "sortable",
        onclick: () => {
          t.descending = t.sort === name ? !t.descending : false;
          t.sort = name;
          t.offset = 0;
          loadTable();
        },
      }, [name, t.sort === name ? el("span", { class: "dir", text: t.descending ? "▾" : "▴" }) : null])
    ))),
    el("tbody", {}, page.rows.map((row) =>
      el("tr", {}, row.map((cell) =>
        el("td", {
          class: cell === null ? "nul" : typeof cell === "number" ? "num" : "",
          text: cell === null ? "null" : typeof cell === "number"
            ? Number(cell.toFixed(4)).toLocaleString() : String(cell),
        })))))
  );

  const last = Math.min(page.offset + page.limit, page.total);
  $("#table-pager").replaceChildren(
    el("button", {
      class: "btn ghost sm", disabled: page.offset === 0,
      onclick: () => { t.offset = Math.max(0, t.offset - t.limit); loadTable(); },
    }, "Previous"),
    el("button", {
      class: "btn ghost sm", disabled: last >= page.total,
      onclick: () => { t.offset += t.limit; loadTable(); },
    }, "Next"),
    el("span", { text:
      page.total
        ? `Showing ${fmt.int(page.offset + 1)}–${fmt.int(last)} of ${fmt.int(page.total)} rows`
        : "No rows match this search." }),
  );
}

/* ------------------------------------------------------------ 6. download */

const DOWNLOADS = [
  ["csv", "CSV", "csv", "The whole panel as one comma-separated file. Opens anywhere."],
  ["parquet", "Parquet", "parquet", "Columnar and typed. The right choice for anything that will be read by code."],
  ["xlsx", "Excel", "xlsx", "A workbook with the data on one sheet and its provenance on another. Capped at a million rows."],
  ["yaml", "Configuration", "yaml", "The exact configuration that produced this data. Re-run it and get the same file back."],
  ["report", "Validation report", "html", "A standalone page listing every invariant checked and its result. Nothing external to load."],
];

function renderDownloads(result) {
  const grid = $("#download-grid");
  grid.replaceChildren(...DOWNLOADS.map(([format, label, ext, description]) =>
    el("a", {
      class: "dl",
      href: `/api/export/${state.job}?format=${format}`,
      download: "",
      onclick: (e) => { e.currentTarget.classList.add("busy");
        setTimeout(() => e.currentTarget.classList.remove("busy"), 2500); },
    }, [
      el("div", { class: "dl-name" }, [
        `Download ${label}`,
        el("span", { class: "dl-ext", text: ext }),
      ]),
      el("div", { class: "dl-desc", text: description }),
    ])
  ));

  const files = [];
  if (result.panel) files.push({ path: result.panel, label: "Consolidated panel (every period)" });
  for (const path of result.files) files.push({ path, label: "One cut-off" });

  $("#run-files").replaceChildren(
    el("thead", {}, el("tr", {}, [
      el("th", { text: "File" }), el("th", { text: "What it is" }), el("th", { text: "" }),
    ])),
    el("tbody", {}, files.slice(0, 60).map((file) =>
      el("tr", {}, [
        el("td", {}, el("code", { text: file.path.split("/").pop() })),
        el("td", { text: file.label }),
        el("td", {}, el("a", {
          href: `/api/download?path=${encodeURIComponent(file.path)}`, text: "Download",
        })),
      ])))
  );
  if (files.length > 60) {
    $("#run-files").append(el("tfoot", {}, el("tr", {}, el("td", { colspan: 3 },
      el("span", { class: "hint", text: `…and ${files.length - 60} more in .sdd-workspace.` })))));
  }
}

/* --------------------------------------------------------------- charting */

const NS = "http://www.w3.org/2000/svg";
const PALETTE = ["#3f8f7f", "#d8b34a", "#dd9040", "#c9603c", "#a63a4a", "#6d4a7c", "#7fae56", "#8d8b84"];

function svgNode(tag, attrs = {}, kids = []) {
  const node = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined) continue;
    node.setAttribute(k, String(v));
  }
  for (const kid of [].concat(kids)) if (kid) node.append(kid);
  return node;
}

function frame(host, { width = 860, height = 280, pad = { t: 14, r: 16, b: 34, l: 54 } } = {}) {
  const svg = svgNode("svg", {
    viewBox: `0 0 ${width} ${height}`,
    preserveAspectRatio: "xMidYMid meet",
    role: "img",
  });
  host.replaceChildren(svg);
  return {
    svg,
    width, height, pad,
    innerW: width - pad.l - pad.r,
    innerH: height - pad.t - pad.b,
  };
}

function gridlines(f, max, format) {
  for (let step = 0; step <= 4; step++) {
    const value = (max / 4) * step;
    const y = f.pad.t + f.innerH - (max ? (value / max) * f.innerH : 0);
    f.svg.append(svgNode("line", {
      x1: f.pad.l, x2: f.width - f.pad.r, y1: y, y2: y, class: "axis",
    }));
    const label = svgNode("text", { x: f.pad.l - 8, y: y + 3.5, "text-anchor": "end" });
    label.textContent = format(value);
    f.svg.append(label);
  }
}

function xLabels(f, labels, count = 8) {
  const every = Math.max(1, Math.ceil(labels.length / count));
  labels.forEach((text, i) => {
    if (i % every && i !== labels.length - 1) return;
    const x = f.pad.l + (labels.length === 1 ? f.innerW / 2 : (i / (labels.length - 1)) * f.innerW);
    const node = svgNode("text", { x, y: f.height - 11, "text-anchor": "middle" });
    node.textContent = text;
    f.svg.append(node);
  });
}

function legend(host, entries) {
  host.append(el("div", { class: "legend" }, entries.map(([label, colour]) =>
    el("span", {}, [el("i", { style: `background:${colour}` }), label]))));
}

function empty(host, message) {
  host.replaceChildren(el("div", { class: "empty", text: message }));
}

function renderDistribution(host, chart) {
  if (!chart) return empty(host, "Nothing numeric to compare.");
  const f = frame(host);
  const series = [["Generated", chart.synthetic, PALETTE[0]]];
  if (chart.reference) series.push(["Sample", chart.reference, PALETTE[3]]);

  const peak = Math.max(...series.flatMap(([, values]) => values), 0.0001);
  gridlines(f, peak, (v) => `${(v * 100).toFixed(0)}%`);

  const n = chart.synthetic.length;
  const x = (i) => f.pad.l + (i / n) * f.innerW;
  const y = (v) => f.pad.t + f.innerH - (v / peak) * f.innerH;

  for (const [, values, colour] of series) {
    // Stepped outline: a histogram is bins, and smoothing between bin centres
    // draws a curve the data never had.
    const points = [`${f.pad.l},${y(0)}`];
    values.forEach((v, i) => { points.push(`${x(i)},${y(v)}`, `${x(i + 1)},${y(v)}`); });
    points.push(`${x(n)},${y(0)}`);
    f.svg.append(svgNode("polygon", {
      points: points.join(" "), fill: colour, "fill-opacity": 0.28,
      stroke: colour, "stroke-width": 1.4, "stroke-linejoin": "round",
    }));
  }

  const edges = chart.edges;
  xLabels(f, edges.slice(0, -1).map((e) => fmt.money(e)), 7);
  legend(host, series.map(([label, , colour]) => [label, colour]));

  const stats = chart.stats;
  const line = (label, s) => s
    ? `${label} mean ${fmt.money(s.mean)} · median ${fmt.money(s.median)} · sd ${fmt.money(s.std)}`
    : null;
  host.append(el("div", { class: "chart-stats" }, [
    el("span", { text: `${chart.column}` }),
    el("span", { text: line("generated", stats.synthetic) }),
    stats.reference ? el("span", { text: line("sample", stats.reference) }) : null,
  ]));
}

function renderDelinquency(host, chart, why) {
  if (!chart) return empty(host, why ? `No curve — ${why}.` : "No lifecycle to chart.");
  const f = frame(host, { height: 250 });
  const peak = Math.max(...chart.total_delinquent, 0.001) * 1.15;
  gridlines(f, peak, (v) => `${(v * 100).toFixed(1)}%`);

  const n = chart.periods.length;
  const x = (i) => f.pad.l + (n === 1 ? f.innerW / 2 : (i / (n - 1)) * f.innerW);
  const y = (v) => f.pad.t + f.innerH - (v / peak) * f.innerH;

  chart.series.forEach((series, index) => {
    const colour = PALETTE[(index + 1) % PALETTE.length];
    f.svg.append(svgNode("polyline", {
      points: series.values.map((v, i) => `${x(i)},${y(v)}`).join(" "),
      fill: "none", stroke: colour, "stroke-width": 1.8,
      "stroke-linejoin": "round", "stroke-linecap": "round",
    }));
  });
  f.svg.append(svgNode("polyline", {
    points: chart.total_delinquent.map((v, i) => `${x(i)},${y(v)}`).join(" "),
    fill: "none", stroke: PALETTE[0], "stroke-width": 2.4, "stroke-dasharray": "5 3",
  }));

  xLabels(f, chart.periods.map((p) => String(p).slice(0, 7)), 7);
  legend(host, [
    ...chart.series.map((s, i) => [s.state, PALETTE[(i + 1) % PALETTE.length]]),
    ["all distressed", PALETTE[0]],
  ]);
}

function renderLtv(host, chart, why) {
  if (!chart) return empty(host, why ? `No chart — ${why}.` : "No LTV column found.");
  const f = frame(host, { height: 250 });
  const series = [
    [`First cut-off`, chart.first.values, PALETTE[0]],
    [`Last cut-off`, chart.last.values, PALETTE[2]],
  ];
  const peak = Math.max(...series.flatMap(([, v]) => v), 0.0001);
  gridlines(f, peak, (v) => `${(v * 100).toFixed(0)}%`);

  const n = chart.first.values.length;
  const x = (i) => f.pad.l + (i / n) * f.innerW;
  const y = (v) => f.pad.t + f.innerH - (v / peak) * f.innerH;

  for (const [, values, colour] of series) {
    const points = [`${f.pad.l},${y(0)}`];
    values.forEach((v, i) => { points.push(`${x(i)},${y(v)}`, `${x(i + 1)},${y(v)}`); });
    points.push(`${x(n)},${y(0)}`);
    f.svg.append(svgNode("polygon", {
      points: points.join(" "), fill: colour, "fill-opacity": 0.26,
      stroke: colour, "stroke-width": 1.4,
    }));
  }

  xLabels(f, chart.edges.slice(0, -1).map((e) => e.toFixed(0)), 7);
  legend(host, series.map(([label, , colour]) => [label, colour]));
  host.append(el("div", { class: "chart-stats" }, [
    el("span", { text: chart.column }),
    el("span", { text: `first mean ${chart.first.mean.toFixed(1)}` }),
    el("span", { text: `last mean ${chart.last.mean.toFixed(1)}` }),
  ]));
}

function renderBalance(host, chart, why) {
  if (!chart) return empty(host, why ? `No chart — ${why}.` : "No balance column found.");
  const f = frame(host);
  const peak = Math.max(...chart.balance, 1);
  gridlines(f, peak, (v) => fmt.money(v));

  const n = chart.periods.length;
  const x = (i) => f.pad.l + (n === 1 ? f.innerW / 2 : (i / (n - 1)) * f.innerW);
  const y = (v) => f.pad.t + f.innerH - (v / peak) * f.innerH;

  const line = chart.balance.map((v, i) => `${x(i)},${y(v)}`).join(" ");
  f.svg.append(svgNode("polygon", {
    points: `${f.pad.l},${y(0)} ${line} ${x(n - 1)},${y(0)}`,
    fill: PALETTE[0], "fill-opacity": 0.22,
  }));
  f.svg.append(svgNode("polyline", {
    points: line, fill: "none", stroke: PALETTE[0], "stroke-width": 2,
    "stroke-linejoin": "round",
  }));

  xLabels(f, chart.periods.map((p) => String(p).slice(0, 7)), 8);
  legend(host, [[`total ${chart.column}`, PALETTE[0]]]);
  const lastFactor = chart.factor[chart.factor.length - 1];
  host.append(el("div", { class: "chart-stats" }, [
    el("span", { text: `opening ${fmt.money(chart.balance[0])}` }),
    el("span", { text: `closing ${fmt.money(chart.balance[chart.balance.length - 1])}` }),
    el("span", { text: `pool factor ${lastFactor.toFixed(3)}` }),
    el("span", { text: `${fmt.int(chart.loans[chart.loans.length - 1])} loans left` }),
  ]));
}

/* ------------------------------------------------------------------- wire */

function wireTabs() {
  /* Which configure groups are open, remembered for the session.
   *
   * Opening a group is a statement that you care about it, and re-collapsing it
   * on every re-render — which happens on every keystroke that touches the
   * spec — would throw that away and shut the panel a user was reading. */
  const opened = new Set();
  $$("details.group").forEach((group) => {
    if (group.open) opened.add(group.id);
    group.addEventListener("toggle", () => {
      if (group.open) opened.add(group.id);
      else opened.delete(group.id);
    });
  });
  state.restoreGroups = () => {
    $$("details.group").forEach((group) => { group.open = opened.has(group.id); });
  };
}

function wireNav() {
  $$(".rail .step").forEach((button) => {
    button.onclick = () => {
      if (state.reachable[button.dataset.view]) show(button.dataset.view);
    };
  });
  $("#btn-back").onclick = () => {
    const order = VIEWS.filter((v) => state.reachable[v]);
    const at = order.indexOf(state.view);
    show(order[Math.max(0, at - 1)]);
  };
  $("#btn-next").onclick = goNext;

  $("#cfg-filter").oninput = renderColumns;
  $("#cfg-records").onchange = (e) => {
    state.settings.records = Math.max(1, parseInt(e.target.value, 10) || 1);
    updateSizeNote();
  };
  $("#cfg-seed").onchange = (e) => {
    state.settings.seed = parseInt(e.target.value, 10) || 42;
  };
  $("#table-search").oninput = debounce((e) => {
    state.table.search = e.target.value.trim();
    state.table.offset = 0;
    loadTable();
  }, 320);
}

async function boot() {
  wireNav();
  wireTabs();
  wireYaml();
  wireDrop("#drop-schema", "#file-schema", "schema");
  wireDrop("#drop-sample", "#file-sample", "sample");
  try {
    await loadPacks();
  } catch (error) {
    status(`Could not reach the server: ${error.message}`, "bad");
  }
  show("upload");
}

boot();


/* Hovering a chart.
 *
 * A line with an axis tells you the shape and refuses to tell you the number.
 * Every chart here gets an invisible column per data point, so the pointer does
 * not have to find a 2px line, and a small panel follows it with the figure.
 *
 * Keyboard reaches it too: the hit zones are focusable, so tabbing across a
 * chart reads out the same values a mouse would.
 */
function hoverLayer(host, f) {
  const tip = el("div", { class: "chart-tip", hidden: true });
  host.append(tip);

  const marker = svgNode("circle", { r: 3.5, class: "hover-dot", visibility: "hidden" });
  f.svg.append(marker);

  const show = (clientLeft, clientTop, html) => {
    tip.innerHTML = html;
    tip.hidden = false;
    const box = host.getBoundingClientRect();
    const width = tip.offsetWidth;
    // Flip to the left near the right edge, so the panel never leaves the card.
    let x = clientLeft - box.left + 14;
    if (x + width > box.width - 6) x = clientLeft - box.left - width - 14;
    tip.style.left = `${Math.max(4, x)}px`;
    tip.style.top = `${Math.min(clientTop - box.top + 12, box.height - 12)}px`;
  };

  return {
    hide() {
      tip.hidden = true;
      marker.setAttribute("visibility", "hidden");
    },
    /** One hit zone. `dot` is where to park the marker, if anywhere. */
    zone({ x, y, width, height, html, dot, label }) {
      const hit = svgNode("rect", {
        x, y, width: Math.max(width, 1), height: Math.max(height, 1),
        fill: "transparent", class: "hover-zone", tabindex: "0", role: "img",
        "aria-label": label,
      });
      const enter = (event) => {
        const point = event.touches?.[0] || event;
        const box = f.svg.getBoundingClientRect();
        show(point.clientX ?? box.left + x + width / 2, point.clientY ?? box.top + y, html);
        if (dot) {
          marker.setAttribute("cx", dot[0]);
          marker.setAttribute("cy", dot[1]);
          marker.setAttribute("visibility", "visible");
        }
      };
      hit.addEventListener("mousemove", enter);
      hit.addEventListener("focus", enter);
      hit.addEventListener("mouseleave", () => tip.hidden = true);
      hit.addEventListener("blur", () => tip.hidden = true);
      f.svg.append(hit);
    },
  };
}

/* Charts a pack asked for.
 *
 * The four drawn before this were fixed here and named for a mortgage — a
 * delinquency curve and a loan-to-value distribution. A CLO run drew both, and
 * neither means anything for a corporate loan: there is no LTV, and the ladder
 * is watchlist and distress rather than days past due. A pack now says what it
 * wants, and when it does the mortgage-shaped pair steps aside rather than
 * sitting underneath drawing nothing.
 *
 * The distribution comparison stays either way. It works off any numeric column
 * and is the one chart that is genuinely about the data rather than the asset. */
function renderPackCharts(configured) {
  const host = $("#pack-charts");
  const pair = $("#legacy-charts-pair");
  const balance = $("#legacy-chart-balance");
  host.replaceChildren();

  const declared = configured.length > 0;
  if (pair) pair.hidden = declared;
  if (balance) balance.hidden = declared;
  if (!declared) return;

  // Two to a row, so a portfolio and the thing that explains it sit side by side.
  let row = null;
  configured.forEach((chart, index) => {
    if (index % 2 === 0) {
      row = el("div", { class: "grid two" });
      host.append(row);
    }
    const body = el("div", { class: "chart-host" });
    row.append(el("div", { class: "card" }, [
      el("div", { class: "card-head" }, [
        el("h2", { text: chart.title }),
        chart.explain ? helpButton(chart.title, chart.explain) : null,
      ].filter(Boolean)),
      chart.description ? el("p", { class: "hint", text: chart.description }) : null,
      body,
    ].filter(Boolean)));

    if (chart.unavailable) {
      empty(body, chart.unavailable);
      return;
    }
    try {
      if (chart.kind === "series") renderSeries(body, chart);
      else if (chart.kind === "stacked_series") renderStacked(body, chart);
      else if (chart.kind === "category_bar") renderCategoryBar(body, chart);
      else if (chart.kind === "histogram") renderHistogram(body, chart);
      else empty(body, `no renderer for a ${chart.kind} chart`);
    } catch (error) {
      empty(body, error.message);
    }
  });
}


/* An explanation, for a reader who does not have the vocabulary.
 *
 * "CCC share of par" is four words of jargon and a preposition. The one-line
 * description under a title has no room to unpack it, and a chart nobody can
 * read is a chart nobody uses.
 */
function helpButton(title, text) {
  const panel = el("div", { class: "chart-help", hidden: true }, [
    el("strong", { text: title }),
    el("p", { text }),
  ]);
  const button = el("button", {
    class: "help-btn", type: "button", "aria-label": `What ${title} means`,
    "aria-expanded": "false", text: "i",
    onclick: (event) => {
      event.stopPropagation();
      const opening = panel.hidden;
      // One at a time: two open panels overlap and neither is readable.
      $$(".chart-help").forEach((other) => { other.hidden = true; });
      $$(".help-btn").forEach((other) => other.setAttribute("aria-expanded", "false"));
      panel.hidden = !opening;
      button.setAttribute("aria-expanded", String(opening));
    },
  });
  return el("span", { class: "help-wrap" }, [button, panel]);
}

function axisFormat(unit) {
  if (unit === "money") return (v) => fmt.money(v);
  if (unit === "percent") return (v) => `${(v * 100).toFixed(1)}%`;
  if (unit === "count") return (v) => fmt.int(v);
  return (v) => (Math.abs(v) >= 1000 ? fmt.int(v) : v.toFixed(2));
}

function renderSeries(host, chart) {
  const values = chart.values.map((v) => (v == null ? 0 : v));
  if (!values.length) return empty(host, "no periods to draw");
  const format = axisFormat(chart.unit);
  const f = frame(host);
  const peak = Math.max(...values, 0) * 1.05 || 1;
  gridlines(f, peak, format);

  const n = values.length;
  const x = (i) => f.pad.l + (n === 1 ? f.innerW / 2 : (i / (n - 1)) * f.innerW);
  const y = (v) => f.pad.t + f.innerH - (v / peak) * f.innerH;

  f.svg.append(svgNode("polygon", {
    points: [`${x(0)},${y(0)}`, ...values.map((v, i) => `${x(i)},${y(v)}`), `${x(n - 1)},${y(0)}`]
      .join(" "),
    fill: PALETTE[0], "fill-opacity": 0.16,
  }));
  f.svg.append(svgNode("polyline", {
    points: values.map((v, i) => `${x(i)},${y(v)}`).join(" "),
    fill: "none", stroke: PALETTE[0], "stroke-width": 1.8,
  }));

  xLabels(f, chart.periods.map((d) => String(d).slice(0, 7)), 6);

  const hover = hoverLayer(host, f);
  const slice = f.innerW / n;
  values.forEach((v, i) => {
    hover.zone({
      x: x(i) - slice / 2, y: f.pad.t, width: slice, height: f.innerH,
      dot: [x(i), y(v)],
      label: `${chart.periods[i]}: ${format(v)}`,
      html: `<b>${escapeHtml(String(chart.periods[i]).slice(0, 10))}</b>` +
            `<span>${escapeHtml(format(v))}</span>`,
    });
  });
  host.addEventListener("mouseleave", hover.hide);

  host.append(el("div", { class: "chart-stats" }, [
    el("span", { text: `first ${format(values[0])}` }),
    el("span", { text: `last ${format(values[n - 1])}` }),
    el("span", { text: `peak ${format(Math.max(...values))}` }),
  ]));
}

function renderStacked(host, chart) {
  if (!chart.series?.length) return empty(host, "nothing to stack");
  const f = frame(host);
  const n = chart.periods.length;
  gridlines(f, 1, (v) => `${(v * 100).toFixed(0)}%`);

  const x = (i) => f.pad.l + (n === 1 ? f.innerW / 2 : (i / (n - 1)) * f.innerW);
  const y = (v) => f.pad.t + f.innerH - v * f.innerH;

  // Bottom up, so the largest band sits on the axis and the eye reads the small
  // ones against a flat edge rather than a moving one.
  const floors = new Array(n).fill(0);
  chart.series.forEach((series, index) => {
    const tops = series.values.map((v, i) => floors[i] + v);
    const points = [
      ...tops.map((v, i) => `${x(i)},${y(v)}`),
      ...floors.map((v, i) => `${x(n - 1 - i)},${y(floors[n - 1 - i])}`),
    ];
    f.svg.append(svgNode("polygon", {
      points: points.join(" "),
      fill: PALETTE[index % PALETTE.length], "fill-opacity": 0.75,
    }));
    tops.forEach((v, i) => { floors[i] = v; });
  });

  xLabels(f, chart.periods.map((d) => String(d).slice(0, 7)), 6);
  legend(host, chart.series.map((s, i) => [s.label, PALETTE[i % PALETTE.length]]));

  // One zone per period listing every band, rather than one per band: at these
  // heights the thin ones are a pixel or two and impossible to point at.
  const hover = hoverLayer(host, f);
  const slice = f.innerW / n;
  chart.periods.forEach((period, i) => {
    const rows = chart.series
      .map((series) => `<span>${escapeHtml(series.label)}` +
                       `<b>${(series.values[i] * 100).toFixed(1)}%</b></span>`)
      .join("");
    hover.zone({
      x: x(i) - slice / 2, y: f.pad.t, width: slice, height: f.innerH,
      label: `${period}: ${chart.series.map((sr) =>
        `${sr.label} ${(sr.values[i] * 100).toFixed(1)}%`).join(", ")}`,
      html: `<b>${escapeHtml(String(period).slice(0, 10))}</b>${rows}`,
    });
  });
  host.addEventListener("mouseleave", hover.hide);
}

function renderCategoryBar(host, chart) {
  const shown = chart.categories.slice(0, 12);
  if (!shown.length) return empty(host, "no categories");
  const format = axisFormat(chart.unit);
  // Wide left margin for the category names, and room on the right for the
  // share printed at the end of each bar.
  const f = frame(host, { pad: { t: 14, r: 62, b: 24, l: 132 } });
  const peak = Math.max(...chart.values) * 1.05 || 1;

  const rowHeight = f.innerH / shown.length;
  shown.forEach((label, i) => {
    const width = (chart.values[i] / peak) * f.innerW;
    const top = f.pad.t + i * rowHeight + rowHeight * 0.18;
    const height = rowHeight * 0.64;
    f.svg.append(svgNode("rect", {
      x: f.pad.l, y: top, width: Math.max(width, 1), height,
      fill: PALETTE[0], "fill-opacity": 0.8, rx: 2,
    }));
    // `svgNode` sets everything it is given as an *attribute*, so a `text:` key
    // became text="Software" — which SVG ignores, and every category name was
    // silently absent. Text content has to be assigned, not passed.
    const name = svgNode("text", {
      x: f.pad.l - 8, y: top + height / 2 + 4, "text-anchor": "end", class: "tick",
    });
    name.textContent = label.length > 20 ? `${label.slice(0, 19)}…` : label;
    f.svg.append(name);

    const share = svgNode("text", {
      x: f.pad.l + Math.max(width, 1) + 6, y: top + height / 2 + 4, class: "tick",
    });
    share.textContent = `${(chart.shares[i] * 100).toFixed(1)}%`;
    f.svg.append(share);
  });
  const hover = hoverLayer(host, f);
  shown.forEach((label, i) => {
    hover.zone({
      x: f.pad.l, y: f.pad.t + i * rowHeight, width: f.innerW, height: rowHeight,
      label: `${label}: ${format(chart.values[i])}, ${(chart.shares[i] * 100).toFixed(1)}%`,
      html: `<b>${escapeHtml(label)}</b>` +
            `<span>${escapeHtml(format(chart.values[i]))}</span>` +
            `<span>${(chart.shares[i] * 100).toFixed(1)}% of the book</span>`,
    });
  });
  host.addEventListener("mouseleave", hover.hide);

  host.append(el("div", { class: "chart-stats" }, [
    el("span", { text: `${chart.categories.length} categories` }),
    el("span", { text: `largest ${(chart.shares[0] * 100).toFixed(1)}%` }),
    el("span", { text: `as of ${String(chart.as_of).slice(0, 10)}` }),
  ]));
}

function renderHistogram(host, chart) {
  if (!chart.counts?.length) return empty(host, "no values to spread");
  const f = frame(host);
  const peak = Math.max(...chart.counts) * 1.05 || 1;
  gridlines(f, peak, (v) => fmt.int(v));
  const n = chart.counts.length;
  const x = (i) => f.pad.l + (i / n) * f.innerW;
  const y = (v) => f.pad.t + f.innerH - (v / peak) * f.innerH;
  chart.counts.forEach((count, i) => {
    f.svg.append(svgNode("rect", {
      x: x(i) + 1, y: y(count), width: Math.max(f.innerW / n - 2, 1),
      height: f.pad.t + f.innerH - y(count), fill: PALETTE[0], "fill-opacity": 0.75,
    }));
  });
  xLabels(f, chart.edges.slice(0, -1).map((e) => Number(e).toFixed(0)), 7);

  const hover = hoverLayer(host, f);
  const total = chart.counts.reduce((a, b) => a + b, 0) || 1;
  chart.counts.forEach((count, i) => {
    const from = Number(chart.edges[i]).toFixed(0);
    const to = Number(chart.edges[i + 1]).toFixed(0);
    hover.zone({
      x: x(i), y: f.pad.t, width: f.innerW / n, height: f.innerH,
      label: `${from} to ${to}: ${count}`,
      html: `<b>${from} – ${to}</b><span>${fmt.int(count)} rows</span>` +
            `<span>${((count / total) * 100).toFixed(1)}%</span>`,
    });
  });
  host.addEventListener("mouseleave", hover.hide);
}

