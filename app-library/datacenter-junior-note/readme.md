# Deeploans Data Center Junior Note MVP

This MVP demonstrates how a **private debt investor** could use Deeploans to monitor and manage a **junior note backed by data centers**.

## What it shows

1. **Portfolio snapshot** with junior note exposure and stressed DSCR metrics.
2. **Facility monitor** tracking occupancy, efficiency (PUE), and covenant-sensitive ratios.
3. **Trigger engine** for cash trap, equity cure, and lockup conditions.
4. **Action center** that turns scenario output into investor decisions.

## Run locally

```bash
cd app-library/datacenter-junior-note
python -m http.server 4174
```

Then open <http://localhost:4174>.
