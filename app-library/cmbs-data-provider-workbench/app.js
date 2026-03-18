const el = (id) => document.getElementById(id);

const sampleRows = [
  {
    deal_id: 'BMARK_2026-CMBS1',
    loan_id: 'LN-10001A',
    property_type: 'Office',
    country: 'US',
    current_balance: 149800000,
    appraised_value: 255000000,
    dscr: 1.12,
    occupancy_rate: 68.5,
    special_servicing: 'Yes',
  },
  {
    deal_id: 'BMARK_2026-CMBS1',
    loan_id: 'LN-10002A',
    property_type: 'Industrial',
    country: 'US',
    current_balance: 152600000,
    appraised_value: 178400000,
    dscr: 1.43,
    occupancy_rate: 91.0,
    special_servicing: 'No',
  },
];

function renderTable(rows) {
  const table = el('dealTable');
  table.innerHTML = '';
  if (!rows.length) return;
  const columns = ['deal_id', 'loan_id', 'property_type', 'country', 'current_balance', 'appraised_value', 'dscr', 'occupancy_rate', 'special_servicing'];

  const thead = document.createElement('thead');
  thead.innerHTML = `<tr>${columns.map((column) => `<th>${column}</th>`).join('')}</tr>`;

  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = columns.map((column) => `<td>${row[column] ?? ''}</td>`).join('');
    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
}

function renderKpis(rows, source) {
  const special = rows.filter((row) => String(row.special_servicing || '').toLowerCase() === 'yes').length;
  const avgDscr = rows.length
    ? (rows.reduce((acc, row) => acc + (Number(row.dscr) || 0), 0) / rows.length).toFixed(2)
    : '0.00';

  el('kpis').innerHTML = `
    <div class="kpi"><strong>Rows</strong><br/>${rows.length}</div>
    <div class="kpi"><strong>Special servicing</strong><br/>${special}</div>
    <div class="kpi"><strong>Avg DSCR</strong><br/>${avgDscr}</div>
    <div class="kpi"><strong>Source</strong><br/>${source}</div>
  `;
}

function extractSignals(text) {
  const lowered = text.toLowerCase();
  const signals = [];
  const dscrMatch = text.match(/dscr[^\d]*(\d+(?:\.\d+)?)/i);
  const occMatch = text.match(/occupanc[^\d]*(\d+(?:\.\d+)?)\s*%?/i);
  const appraisedMatch = text.match(/apprais(?:ed|al)[^\d$]*\$?([\d,.]+)\s*(million|m)?/i);

  if (lowered.includes('special servicing')) signals.push('Transferred to special servicing.');
  if (lowered.includes('forbearance')) signals.push('Forbearance terms detected.');
  if (lowered.includes('bankruptcy')) signals.push('Bankruptcy event detected.');
  if (dscrMatch) signals.push(`DSCR mentioned: ${dscrMatch[1]}.`);
  if (occMatch) signals.push(`Occupancy mentioned: ${occMatch[1]}%.`);
  if (appraisedMatch) signals.push(`Appraisal update mentioned: ${appraisedMatch[1]} ${appraisedMatch[2] || ''}`.trim());

  return signals.length ? signals : ['No explicit risk signal was extracted from the note.'];
}

function renderSignals() {
  const items = extractSignals(el('commentary').value);
  el('signals').innerHTML = items.map((item) => `<li>${item}</li>`).join('');
}

async function fetchFromApi() {
  const baseUrl = el('baseUrl').value.trim().replace(/\/$/, '');
  const apiKey = el('apiKey').value.trim();
  const response = await fetch(`${baseUrl}/api/v1/cre/deals?limit=200`, {
    headers: apiKey ? { 'x-algoritmica-api-key': apiKey } : {},
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const data = await response.json();
  const rows = Array.isArray(data) ? data : (data.data || [data]);
  return rows.map((row) => ({
    deal_id: row.deal_id || row.id || row.CREL1,
    loan_id: row.loan_id || row.CREL4,
    property_type: row.property_type || row.CREL26,
    country: row.country || row.CREL19,
    current_balance: row.current_balance || row.CREL41,
    appraised_value: row.appraised_value || row.CREL32,
    dscr: row.dscr || row.CREL44,
    occupancy_rate: row.occupancy_rate || row.CREL59,
    special_servicing: row.special_servicing || row.CREL71,
  }));
}

async function fetchFromEtlJson() {
  const response = await fetch(el('etlPath').value.trim());
  if (!response.ok) throw new Error(`ETL JSON unavailable (${response.status})`);
  const payload = await response.json();
  return Array.isArray(payload.records) ? payload.records : [];
}

async function loadDataset() {
  const source = el('sourceMode').value;
  try {
    const rows = source === 'api'
      ? await fetchFromApi()
      : source === 'etl'
        ? await fetchFromEtlJson()
        : sampleRows;

    renderKpis(rows, source);
    renderTable(rows);
  } catch (error) {
    renderKpis(sampleRows, `${source}-fallback`);
    renderTable(sampleRows);
  }
}

el('loadBtn').addEventListener('click', loadDataset);
el('analyzeBtn').addEventListener('click', renderSignals);
loadDataset();
