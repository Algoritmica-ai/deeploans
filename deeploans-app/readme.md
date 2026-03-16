# Deeploans App MVP

<img width="1440" height="1214" alt="image" src="https://github.com/user-attachments/assets/89c6beac-a1f5-4447-bc63-5d5428fa50b4" />


A lightweight front-end MVP for:

1. **Dataset Ingestion Status**
2. **Loan Tape Explorer**
3. **Data Quality Dashboard**
4. **API Usage Console (starter)**

## Is this runnable from GitHub?

Yes — this app is now configured for **GitHub Pages** deployment via GitHub Actions.

### First-time repository setup

1. Push this branch to GitHub.
2. In repository settings, go to **Pages**.
3. Set source to **GitHub Actions**.
4. Merge to `main` (or run the workflow manually).

After deployment, the app will be available at:

- `https://<org-or-user>.github.io/<repo>/`

## Run locally

```bash
cd deeploans-app
python -m http.server 4173
```

Open <http://localhost:4173>.

## API mode

Switch **Data source** to `API` and set:
- Base API URL (default `http://localhost:8000`)
- Optional `x-algoritmica-api-key`
- Credit type + table + filters

If API loading fails, the app automatically falls back to sample data.
