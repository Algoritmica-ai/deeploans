# Security Policy

This project handles credit data and trained model artifacts. Treat all
contents of `data/` and `checkpoints`/`models/*/checkpoints/` as sensitive.

## Reporting a vulnerability
Report suspected vulnerabilities or data-handling concerns privately to the
project owner rather than opening a public issue.

## Data handling
- Never commit raw credit data, PII, or trained weights (see `.gitignore`).
- Enforce the observation-date / leakage policy in `docs/credit_event_schema.md`.
