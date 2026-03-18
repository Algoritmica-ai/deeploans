# CMBS Data Provider Workbench

A lightweight app for **CMBS deal monitoring** with a CMBS data provider-style workflow, using
**Deeploans as the integration layer**.

## Features

1. **Deeploans integration mode** (`/api/v1/cre/deals`) for live CRE data.
2. **CRE ETL integration mode** loading normalized output from:
   - `etl-pipelines/ESMA-Loan-level-data-templates/commercial-real-estate/output/cre_normalized.json`
3. **Newsflash-style risk extraction** from servicer commentary (forbearance, special servicing, bankruptcy, DSCR/occupancy mentions).
4. **Deal and loan monitor** with key metrics (balance, appraisal, DSCR, occupancy).

## Run locally

```bash
cd app-library/cmbs-data-provider-workbench
python -m http.server 4174
```

Open <http://localhost:4174>.

## Screenshot

Screenshot generation was attempted, but this environment does not provide the required browser screenshot tool.
After running locally, add a screenshot file (for example `./assets/cmbs-workbench.png`) and reference it here:

```md
![CMBS Data Provider Workbench UI](./assets/cmbs-workbench.png)
```
