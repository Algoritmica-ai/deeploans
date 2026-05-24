# Deeploans app library

This folder contains lightweight MVP applications built on top of Deeploans components.

## Available apps

### 1) Data Quality App

- Path: `app-library/data-quality/`
- Purpose: dataset ingestion status, loan tape exploration, data-quality dashboard, API usage starter view.
- Run locally:

```bash
cd app-library/data-quality
python -m http.server 4173
```

Open <http://localhost:4173>.

---

### 2) Data Center Junior Note MVP

- Path: `app-library/datacenter-junior-note/`
- Purpose: monitor junior-note exposure, facility metrics, covenant triggers, and scenario actions.
- Run locally:

```bash
cd app-library/datacenter-junior-note
python -m http.server 4174
```

Open <http://localhost:4174>.

---

### 3) Hastructure Studio bridge

- Path: `app-library/hastructure-studio/`
- Purpose: map Deeploans loan tape data into Hastructure-style payloads.
- Run locally:

```bash
cd app-library/hastructure-studio
python -m http.server 4180
```

Open <http://localhost:4180>.

---

### 4) CMBS Data Provider Workbench

- Path: `app-library/cmbs-data-provider-workbench/`
- Purpose: CMBS monitoring UI that consumes Deeploans API data with local fallback.
- Run locally:

```bash
cd app-library/cmbs-data-provider-workbench
python -m http.server 4174
```

Open <http://localhost:4174>.

## Related docs

- Root overview: `readme.md`
- Contribution guide: `CONTRIBUTING.md`
