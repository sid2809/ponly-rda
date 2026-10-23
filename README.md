# Ponly RDA

Creates paused Google Display campaigns with responsive display ads for ponly.com articles,
from a Google Sheet. Build spec: "Ponly RDA Pipeline – Build Spec" (Claude Docs).

## How it runs
One Python worker (`python -m rda.worker`) on Railway. It loops every minute, reads the
Sheet's Settings tab, moves each article one step forward and writes progress to the
Output, Campaign map and Summary tabs. All state is in SQLite on the Railway volume
(`/data`), so it can be stopped and restarted at any time without repeating paid work.

Order: preflight → URL check → alcohol tag → themes → waves assigned → per wave: copy
(Haiku batch) → images (Replicate) → image check (Haiku batch) → campaign split → upload
(Google Ads batch job, paused) → enable when you tick the wave box → daily sync.
Wave 2 starts only after Wave 1 is enabled, passes the rules, and you set "Image model".
Wave 3 starts after Wave 2 passes.

## Railway variables (secrets live only here)
| Variable | What |
|---|---|
| `SHEET_ID` | ID from the Sheet URL (between `/d/` and `/edit`) |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | full JSON key text of the service account |
| `GOOGLE_ADS_CUSTOMER_ID` | your Google Ads account ID, no dashes |
| `GOOGLE_ADS_LOGIN_CUSTOMER_ID` | manager (MCC) ID, only if access is through a manager |
| `GOOGLE_ADS_DEVELOPER_TOKEN` | if you have one (Google's docs disagree on whether it is still needed) |
| `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET`, `GOOGLE_ADS_REFRESH_TOKEN` | OAuth login (same as n8n) |
| `GOOGLE_ADS_USE_SERVICE_ACCOUNT` | `1` to log in to Google Ads with the service account instead of OAuth |
| `ANTHROPIC_API_KEY` | Claude API key |
| `REPLICATE_API_TOKEN` | Replicate token |
| `DATA_DIR` | `/data` (volume mount) |

## Commands
- `python -m scripts.check_connections` — one harmless call to each service (Phase 0)
- `python -m scripts.load_input data_csv` — fills the Input tab from the category CSVs
- `python -m scripts.validate_only 1` — Google Ads validate-only for wave 1 (creates nothing)
- `python -m pytest -q tests` — rule tests; `python -m tests.dryrun` — full offline run with fakes

## Settings tab
W1/W2/W3 enable · Image model (nano-banana / minimax) · Daily API quota cap · Access level ·
Auto-apply recommendations off · Pilot categories · Pause
