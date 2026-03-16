const sampleData = [
  { deal_id: 'DL-AUT-001', asset_class: 'aut', country: 'DE', originator: 'NorthBank', report_date: '2025-01-31', loan_count: 1200, balance_eur: 18200000 },
  { deal_id: 'DL-SME-201', asset_class: 'sme', country: 'IT', originator: 'Atlas Credit', report_date: '2025-01-31', loan_count: 870, balance_eur: 25650000 },
  { deal_id: 'DL-CMR-045', asset_class: 'cmr', country: 'FR', originator: 'RetailOne', report_date: '2025-01-31', loan_count: 2100, balance_eur: 14120000 },
];

const el = (id) => document.getElementById(id);

function duplicateRate(rows) {
  if (!rows.length) return 0;
  const seen = new Set();
  let dup = 0;
  for (const row of rows) {
    const key = JSON.stringify(row);
    if (seen.has(key)) dup += 1;
    seen.add(key);
  }
  return dup / rows.length;
}

function computeQuality(rows) {
  if (!rows.length) return [];
  const columns = Object.keys(rows[0]);
  return columns.map((column) => {
    const vals = rows.map((r) => r[column]);
    const nulls = vals.filter((v) => v === null || v === undefined || v === '').length;
    const distinct = new Set(vals.filter((v) => v !== null && v !== undefined && v !== '')).size;
    return { column, null_rate: (nulls / rows.length).toFixed(3), distinct_count: distinct };
  }).sort((a, b) => Number(b.null_rate) - Number(a.null_rate));
}

function renderTable(tableId, rows) {
  const table = el(tableId);
  table.innerHTML = '';
  if (!rows.length) return;
  const columns = Object.keys(rows[0]);
  const thead = document.createElement('thead');
  thead.innerHTML = `<tr>${columns.map((c) => `<th>${c}</th>`).join('')}</tr>`;
  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = columns.map((c) => `<td>${row[c] ?? ''}</td>`).join('');
    tbody.appendChild(tr);
  });
  table.appendChild(thead);
  table.appendChild(tbody);
}

function renderKpis(rows, source) {
  el('kpis').innerHTML = `
    <div class="kpi"><strong>Rows</strong><br/>${rows.length}</div>
    <div class="kpi"><strong>Columns</strong><br/>${rows.length ? Object.keys(rows[0]).length : 0}</div>
    <div class="kpi"><strong>Source</strong><br/>${source}</div>
  `;
}

async function fetchData() {
  const mode = el('sourceMode').value;
  if (mode === 'sample') return { rows: sampleData, source: 'sample' };

  const baseUrl = el('baseUrl').value.trim().replace(/\/$/, '');
  const creditType = el('creditType').value;
  const tableName = el('tableName').value.trim();
  const limit = el('limit').value;
  const offset = el('offset').value;
  const filter = el('filterQuery').value.trim();
  const apiKey = el('apiKey').value.trim();

  const params = new URLSearchParams({ limit, offset });
  if (filter) params.set('filter', filter);

  const response = await fetch(`${baseUrl}/api/v1/${creditType}/${tableName}?${params.toString()}`, {
    headers: apiKey ? { 'x-algoritmica-api-key': apiKey } : {},
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);

  const payload = await response.json();
  const rows = Array.isArray(payload) ? payload : (payload.data || [payload]);
  return { rows: Array.isArray(rows) ? rows : [], source: 'api' };
}

async function loadAndRender() {
  try {
    const { rows, source } = await fetchData();
    renderKpis(rows, source);
    renderTable('loanTable', rows);

    const quality = computeQuality(rows);
    renderTable('qualityTable', quality);
    el('qualitySummary').textContent = `Duplicate rate: ${(duplicateRate(rows) * 100).toFixed(2)}%`;
  } catch (err) {
    renderKpis(sampleData, 'sample-fallback');
    renderTable('loanTable', sampleData);
    const quality = computeQuality(sampleData);
    renderTable('qualityTable', quality);
    el('qualitySummary').textContent = `API unavailable (${err.message}). Showing sample data.`;
  }
}

el('loadBtn').addEventListener('click', loadAndRender);
loadAndRender();
