const sampleTape = [
  { loan_id: 'AUT-0001', current_balance: 24000, rate: 0.053, remaining_term_months: 48, ltv: 0.78 },
  { loan_id: 'AUT-0002', current_balance: 18000, rate: 0.049, remaining_term_months: 40, ltv: 0.72 },
  { loan_id: 'AUT-0003', current_balance: 31500, rate: 0.061, remaining_term_months: 56, ltv: 0.85 },
  { loan_id: 'AUT-0004', current_balance: 12750, rate: 0.046, remaining_term_months: 27, ltv: 0.68 },
  { loan_id: 'AUT-0005', current_balance: 22500, rate: 0.058, remaining_term_months: 51, ltv: 0.81 },
];

const el = (id) => document.getElementById(id);
let activeTape = [...sampleTape];

function money(v) {
  return new Intl.NumberFormat('en-GB', { maximumFractionDigits: 0 }).format(v);
}

function pct(v, digits = 2) {
  return `${(v * 100).toFixed(digits)}%`;
}

function pickNumber(row, candidates, fallback = 0) {
  for (const key of candidates) {
    if (row[key] !== undefined && row[key] !== null && row[key] !== '') {
      const n = Number(row[key]);
      if (!Number.isNaN(n)) return n;
    }
  }
  return fallback;
}

function deriveAssumptionsFromTape(rows) {
  if (!rows.length) return null;

  const balances = rows.map((r) => pickNumber(r, ['current_balance', 'balance_eur', 'balance', 'outstanding_balance'], 0));
  const totalBalance = balances.reduce((sum, b) => sum + b, 0);
  if (!totalBalance) return null;

  const weightedCoupon = rows.reduce((sum, r, i) => {
    const raw = pickNumber(r, ['rate', 'coupon', 'interest_rate'], 0.05);
    const coupon = raw > 1 ? raw / 100 : raw;
    return sum + (balances[i] * coupon);
  }, 0) / totalBalance;

  const weightedTerm = rows.reduce((sum, r, i) => {
    const term = pickNumber(r, ['remaining_term_months', 'remaining_term', 'term_months', 'term'], 48);
    return sum + (balances[i] * term);
  }, 0) / totalBalance;

  const avgLtv = rows.reduce((sum, r) => {
    const raw = pickNumber(r, ['ltv', 'current_ltv', 'loan_to_value'], 0.75);
    return sum + (raw > 1 ? raw / 100 : raw);
  }, 0) / rows.length;

  const cdr = Math.min(0.05, Math.max(0.01, 0.01 + Math.max(0, avgLtv - 0.7) * 0.06));
  const cpr = Math.min(0.16, Math.max(0.03, 0.05 + (weightedCoupon - 0.03) * 0.5));
  const severity = Math.min(0.6, Math.max(0.2, 0.25 + Math.max(0, avgLtv - 0.65) * 0.55));

  return {
    poolBalance: Math.round(totalBalance),
    coupon: weightedCoupon,
    termMonths: Math.max(6, Math.round(weightedTerm)),
    cdr,
    cpr,
    severity,
    rows: rows.length,
    avgLtv,
  };
}

function applyAssumptionsToInputs(a) {
  if (!a) return;
  el('poolBalance').value = String(a.poolBalance);
  el('coupon').value = (a.coupon * 100).toFixed(2);
  el('termMonths').value = String(a.termMonths);
  el('cpr').value = (a.cpr * 100).toFixed(2);
  el('cdr').value = (a.cdr * 100).toFixed(2);
  el('severity').value = (a.severity * 100).toFixed(2);

  el('tapeSummary').innerHTML = `
    <div class="kpi"><strong>Tape Rows</strong><br/>${a.rows}</div>
    <div class="kpi"><strong>Pool Balance</strong><br/>€ ${money(a.poolBalance)}</div>
    <div class="kpi"><strong>WAC</strong><br/>${pct(a.coupon)}</div>
    <div class="kpi"><strong>WARM</strong><br/>${a.termMonths}m</div>
    <div class="kpi"><strong>Avg LTV</strong><br/>${pct(a.avgLtv)}</div>
  `;
}

function readInputs() {
  return {
    dealName: el('dealName').value.trim(),
    poolBalance: Number(el('poolBalance').value),
    coupon: Number(el('coupon').value) / 100,
    termMonths: Number(el('termMonths').value),
    cpr: Number(el('cpr').value) / 100,
    cdr: Number(el('cdr').value) / 100,
    severity: Number(el('severity').value) / 100,
    seniorPct: Number(el('seniorPct').value) / 100,
  };
}

function buildPayload(i) {
  const seniorBalance = Math.round(i.poolBalance * i.seniorPct);
  const juniorBalance = i.poolBalance - seniorBalance;

  return {
    metadata: {
      source: 'deeploans',
      tape_rows: activeTape.length,
      created_at: new Date().toISOString(),
    },
    deal: {
      name: i.dealName,
      currency: 'EUR',
      pool: {
        balance: i.poolBalance,
        assumptions: {
          cpr: i.cpr,
          cdr: i.cdr,
          severity: i.severity,
          coupon: i.coupon,
          termMonths: i.termMonths,
        },
      },
      liabilities: [
        { class: 'A', type: 'senior', balance: seniorBalance, coupon: Math.max(0, i.coupon - 0.01) },
        { class: 'B', type: 'junior', balance: juniorBalance, coupon: i.coupon + 0.02 },
      ],
      waterfall: ['fees', 'interest_A', 'principal_A', 'interest_B', 'principal_B', 'residual'],
    },
  };
}

function projectCashflows(i, months = 12) {
  let bal = i.poolBalance;
  const rows = [];
  const schedPrin = i.poolBalance / i.termMonths;

  for (let m = 1; m <= months; m += 1) {
    const prepay = bal * i.cpr / 12;
    const defaults = bal * i.cdr / 12;
    const loss = defaults * i.severity;
    const recoveries = defaults - loss;
    const principal = Math.min(schedPrin + prepay + recoveries, bal);
    const interest = bal * i.coupon / 12;
    const startBalance = bal;
    bal = Math.max(0, bal - principal - loss);

    rows.push({ month: `M${m}`, start_balance: startBalance, interest, principal, loss, end_balance: bal });
  }
  return rows;
}

function renderKpis(rows) {
  const totInt = rows.reduce((s, r) => s + r.interest, 0);
  const totPrin = rows.reduce((s, r) => s + r.principal, 0);
  const totLoss = rows.reduce((s, r) => s + r.loss, 0);
  el('kpis').innerHTML = `
    <div class="kpi"><strong>12m Interest</strong><br/>€ ${money(totInt)}</div>
    <div class="kpi"><strong>12m Principal</strong><br/>€ ${money(totPrin)}</div>
    <div class="kpi"><strong>12m Loss</strong><br/>€ ${money(totLoss)}</div>
    <div class="kpi"><strong>12m End Balance</strong><br/>€ ${money(rows[rows.length - 1]?.end_balance || 0)}</div>
  `;
}

function renderTable(rows) {
  const table = el('cashflowTable');
  const cols = Object.keys(rows[0] || {});
  table.innerHTML = `<thead><tr>${cols.map((c) => `<th>${c}</th>`).join('')}</tr></thead>`;
  const body = document.createElement('tbody');
  rows.forEach((r) => {
    const tr = document.createElement('tr');
    tr.innerHTML = cols.map((c) => `<td>${typeof r[c] === 'number' ? money(r[c]) : r[c]}</td>`).join('');
    body.appendChild(tr);
  });
  table.appendChild(body);
}

function buildAndRender() {
  const inputs = readInputs();
  const payload = buildPayload(inputs);
  const rows = projectCashflows(inputs);
  renderKpis(rows);
  renderTable(rows);
  el('payload').textContent = JSON.stringify(payload, null, 2);
  return payload;
}

async function fetchTape() {
  const mode = el('sourceMode').value;
  if (mode === 'sample') {
    el('sourceStatus').textContent = 'Using sample tape.';
    return [...sampleTape];
  }

  const baseUrl = el('baseUrl').value.trim().replace(/\/$/, '');
  const creditType = el('creditType').value;
  const tableName = el('tableName').value.trim();
  const limit = el('limit').value;
  const offset = el('offset').value;
  const filter = el('filterQuery').value.trim();
  const apiKey = el('apiKey').value.trim();

  const params = new URLSearchParams({ limit, offset });
  if (filter) params.set('filter', filter);

  const res = await fetch(`${baseUrl}/api/v1/${creditType}/${tableName}?${params.toString()}`, {
    headers: apiKey ? { 'x-algoritmica-api-key': apiKey } : {},
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  const payload = await res.json();
  const rows = Array.isArray(payload) ? payload : (payload.data || [payload]);
  return Array.isArray(rows) ? rows : [];
}

async function loadTapeAndDerive() {
  try {
    const tape = await fetchTape();
    if (!tape.length) throw new Error('No rows returned');
    activeTape = tape;
    const assumptions = deriveAssumptionsFromTape(tape);
    if (!assumptions) throw new Error('Could not derive assumptions from returned columns');
    applyAssumptionsToInputs(assumptions);
    el('sourceStatus').textContent = `Loaded ${tape.length} rows from ${el('sourceMode').value === 'api' ? 'Deeploans API' : 'sample tape'}.`;
    buildAndRender();
  } catch (err) {
    activeTape = [...sampleTape];
    const fallback = deriveAssumptionsFromTape(activeTape);
    applyAssumptionsToInputs(fallback);
    el('sourceStatus').textContent = `Data load failed (${err.message}). Fallback to sample tape.`;
    buildAndRender();
  }
}

async function runHastructure() {
  const payload = buildAndRender();
  const url = el('serviceUrl').value.trim();
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const out = await res.json();
    el('response').textContent = JSON.stringify(out, null, 2);
  } catch (err) {
    el('response').textContent = `Could not reach Hastructure service (${err.message}).\n\nTip: start Hastructure locally and expose a POST endpoint, then update Service URL.`;
  }
}

el('loadTapeBtn').addEventListener('click', loadTapeAndDerive);
el('buildBtn').addEventListener('click', buildAndRender);
el('runBtn').addEventListener('click', runHastructure);
loadTapeAndDerive();
