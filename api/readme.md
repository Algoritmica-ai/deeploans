# Deeploans API quickstart

This folder contains the Deeploans API backend project.

## Project location

- Backend code: `api/api-backend-main/backend/`
- Backend docs and setup details: `api/api-backend-main/readme.md`
- OpenAPI schema snapshot: `api/api-backend-main/openapi.json`

## Local run (Docker Compose)

From the repository root:

```bash
cd api/api-backend-main
docker compose up --build
```

The backend service runs on:

- `http://localhost:3000`
- Swagger docs: `http://localhost:3000/docs`

## Notes

- Create `backend/.env` before startup (use the variables documented in `api/api-backend-main/readme.md`).
- A `backend/bigquery.json` credentials file is required to run the API locally.
- `mongo-express` is intended for local development only.
- For full endpoint details (including admin endpoints), see `api/api-backend-main/readme.md`.
