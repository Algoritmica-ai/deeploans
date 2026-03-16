# Deeploans × Hastructure Studio

<img width="1440" height="2160" alt="image" src="https://github.com/user-attachments/assets/4b4ea52a-2eba-468f-ac4c-d4290cace5c3" />


This app is the bridge between **Deeploans** and **[Hastructure](https://github.com/absbox/Hastructure)**:

1. Pull loan tape rows from Deeploans (sample or Deeploans API).
2. Derive deal assumptions from tape-level fields (pool balance, WAC, WARM, proxy CPR/CDR/severity).
3. Generate a Hastructure-style JSON payload.
4. Optionally POST the payload to a running Hastructure service endpoint.

## Run locally

```bash
cd app-library/hastructure-studio
python -m http.server 4180
```

Open http://localhost:4180.

## Deeploans connection

- API mode calls: `GET /api/v1/{creditType}/{table}`
- Returned loan-level rows are mapped into pool-level assumptions used in the payload builder.
- If API is unavailable, the app falls back to sample Deeploans-style tape rows.

## Notes

- This utility does **not** bundle the Hastructure engine.
- It acts as a Deeploans-informed payload builder and connector for users running Hastructure separately.
