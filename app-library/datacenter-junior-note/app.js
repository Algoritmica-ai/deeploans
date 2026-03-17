const facilities = [
  {
    id: 'DC-AMS-01',
    sponsor: 'NorthGrid Partners',
    country: 'NL',
    occupancy: 92,
    pue: 1.29,
    dscrBase: 1.62,
    ltvBase: 67,
    juniorAttachment: 73,
    juniorBalanceM: 41,
    maturity: '2028-06-30',
  },
  {
    id: 'DC-FRA-02',
    sponsor: 'Vector Infra',
    country: 'DE',
    occupancy: 88,
    pue: 1.34,
    dscrBase: 1.41,
    ltvBase: 71,
    juniorAttachment: 76,
    juniorBalanceM: 36,
    maturity: '2029-03-31',
  },
  {
    id: 'DC-MAD-04',
    sponsor: 'Helio Compute',
    country: 'ES',
    occupancy: 95,
    pue: 1.24,
    dscrBase: 1.77,
    ltvBase: 63,
    juniorAttachment: 70,
    juniorBalanceM: 44,
    maturity: '2028-12-31',
  },
];

const covenantThresholds = {
  dscrWatch: 1.35,
  dscrBreach: 1.2,
  ltvWatch: 74,
  ltvBreach: 78,
};

const el = (id) => document.getElementById(id);

function statusFromMetric(dscr, ltv) {
  if (dscr < covenantThresholds.dscrBreach || ltv > covenantThresholds.ltvBreach) return 'breach';
  if (dscr < covenantThresholds.dscrWatch || ltv > covenantThresholds.ltvWatch) return 'watch';
  return 'ok';
}

function runScenario() {
  const noiShock = Number(el('noiShock').value) / 100;
  const energyInflation = Number(el('energyInflation').value) / 100;
  const spreadShift = Number(el('spreadShift').value);

  const modeled = facilities.map((f) => {
    const dscrStress = f.dscrBase * (1 + noiShock - 0.45 * energyInflation - spreadShift / 2500);
    const ltvStress = f.ltvBase * (1 - noiShock * 0.35 + energyInflation * 0.2);
    const status = statusFromMetric(dscrStress, ltvStress);
    const refinanceRisk = spreadShift > 250 && f.maturity.startsWith('2028') ? 'elevated' : 'normal';

    return {
      facility: f.id,
      sponsor: f.sponsor,
      occupancy: `${f.occupancy}%`,
      pue: f.pue.toFixed(2),
      'junior balance (€m)': f.juniorBalanceM.toFixed(1),
      'dscr (stressed)': dscrStress.toFixed(2),
      'ltv (stressed %)': ltvStress.toFixed(1),
      status,
      'refi risk': refinanceRisk,
    };
  });

  renderKpis(modeled);
  renderTable('facilityTable', modeled);
  renderTriggers(modeled);
  renderActions(modeled);
}

function renderKpis(rows) {
  const totalJunior = rows.reduce((sum, row) => sum + Number(row['junior balance (€m)']), 0);
  const breaches = rows.filter((row) => row.status === 'breach').length;
  const watchlist = rows.filter((row) => row.status === 'watch').length;
  const avgDscr = rows.reduce((sum, row) => sum + Number(row['dscr (stressed)']), 0) / rows.length;

  el('kpis').innerHTML = `
    <div class="kpi"><strong>Junior note exposure</strong><br/>€${totalJunior.toFixed(1)}m</div>
    <div class="kpi"><strong>Avg stressed DSCR</strong><br/>${avgDscr.toFixed(2)}x</div>
    <div class="kpi"><strong>Watchlist facilities</strong><br/>${watchlist}</div>
    <div class="kpi"><strong>Covenant breaches</strong><br/>${breaches}</div>
  `;
}

function renderTable(id, rows) {
  const table = el(id);
  const columns = Object.keys(rows[0]);
  const thead = `<thead><tr>${columns.map((c) => `<th>${c}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${rows
    .map((row) => `<tr>${columns
      .map((c) => {
        if (c === 'status') {
          const cls = row[c] === 'breach' ? 'status-breach' : row[c] === 'watch' ? 'status-watch' : '';
          return `<td class="${cls}">${row[c]}</td>`;
        }
        return `<td>${row[c]}</td>`;
      })
      .join('')}</tr>`)
    .join('')}</tbody>`;
  table.innerHTML = thead + tbody;
}

function renderTriggers(rows) {
  const triggers = rows.map((row) => {
    const dscr = Number(row['dscr (stressed)']);
    const ltv = Number(row['ltv (stressed %)']);
    const reserveTrap = dscr < 1.3 ? 'yes' : 'no';
    const equityCure = dscr < 1.2 || ltv > 78 ? 'required' : 'not required';
    return {
      facility: row.facility,
      'cash trap': reserveTrap,
      'equity cure': equityCure,
      'distribution lockup': row.status === 'ok' ? 'no' : 'yes',
    };
  });

  const tripped = triggers.filter((t) => t['cash trap'] === 'yes' || t['equity cure'] === 'required').length;
  el('triggerSummary').textContent = `${tripped} / ${triggers.length} facilities are triggering defensive junior note protections.`;
  renderTable('triggerTable', triggers);
}

function renderActions(rows) {
  const actions = [];
  const breaches = rows.filter((r) => r.status === 'breach');
  const watch = rows.filter((r) => r.status === 'watch');
  const elevatedRefi = rows.filter((r) => r['refi risk'] === 'elevated');

  if (breaches.length) actions.push(`Launch consent request: require sponsor equity cure for ${breaches.map((b) => b.facility).join(', ')}.`);
  if (watch.length) actions.push(`Move ${watch.map((w) => w.facility).join(', ')} to monthly IC call cadence and enhanced covenant reporting.`);
  if (elevatedRefi.length) actions.push(`Start refinancing workstream for ${elevatedRefi.map((e) => e.facility).join(', ')} and price hold-to-maturity downside.`);
  actions.push('Export scenario pack to IC memo with covenant heatmap and recommended voting stance.');

  el('actions').innerHTML = actions.map((action) => `<li>${action}</li>`).join('');
}

function wireSliders() {
  const sliderMap = [
    ['noiShock', 'noiShockValue', '%'],
    ['energyInflation', 'energyInflationValue', '%'],
    ['spreadShift', 'spreadShiftValue', ' bps'],
  ];

  sliderMap.forEach(([sliderId, valueId, suffix]) => {
    const slider = el(sliderId);
    const output = el(valueId);
    slider.addEventListener('input', () => {
      output.textContent = `${slider.value}${suffix}`;
    });
  });
}

wireSliders();
el('runScenario').addEventListener('click', runScenario);
runScenario();
