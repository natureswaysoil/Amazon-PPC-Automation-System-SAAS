# AI Coding Agent Instructions
Concise, codebase-specific guidance to make AI agents productive here.

## Architecture & Jobs
- Production modules live under [automation/](../automation); root files are local prototypes (e.g., [bid_optimizer.py](../bid_optimizer.py), [budget_monitor.py](../budget_monitor.py)).
- Container images copy only [automation/](../automation) per [Dockerfile](../Dockerfile). Add new production code under `automation/` and re-export via [automation/shared/](../automation/shared).
- Cloud Run jobs (see [cloudbuild.yaml](../cloudbuild.yaml)):
  - Bid Optimizer: `python -m automation.bid_optimizer` (job `bid-optimizer`)
  - Budget Monitor: `python -m automation.budget_monitor` (job `budget-monitor`)
  - Verify Live Data: `python -m automation.verify_live_data` (job `verify-live-data`)
  - Minimal Winning Bid: `python -m automation.min_winning_bid` (ad-hoc tool; see [README.md](../README.md))

## Config & Secrets
- Central config via `settings` in [shared/config.py](../shared/config.py) re-exported by [automation/shared/config.py](../automation/shared/config.py).
- Required env: `GCP_PROJECT`, `BQ_DATASET`, `DRY_RUN`; optional: `GCP_REGION`, `DEFAULT_AOV`.
- Google Secret Manager keys: `amazon_client_id`, `amazon_client_secret`, `amazon_refresh_token`, `amazon_profile_id`.
- Respect `settings.dry_run` across Amazon API calls and BigQuery writes.

## Data & Tables
- Dataset defaults to `amazon_ppc` in `settings.project_id`.
- Core tables:
  - `sp_advertised_product_metrics` → AOV + freshness (see [shared/bigquery_client.py](../shared/bigquery_client.py), [automation/verify_live_data.py](../automation/verify_live_data.py)).
  - `sp_targeting_metrics`, `sp_keywords` → keyword performance (see `get_keywords_for_optimization()` in [shared/bigquery_client.py](../shared/bigquery_client.py)).
  - `sp_campaigns`, `sp_campaign_metrics` → budget pacing (see `get_campaign_budget_status()` in [shared/bigquery_client.py](../shared/bigquery_client.py)).
  - `bid_change_log` → audit trail; auto-created by `BigQueryClient._ensure_bid_log_table_exists()`.

## Patterns & Conventions
- Bids: Use `BidCalculator` from [automation/shared/rules_engine.py](../automation/shared/rules_engine.py) (re-export of [shared/rules_engine.py](../shared/rules_engine.py)). It applies AOV tiers, performance tier, match-type, and time-of-day modifiers; obeys `settings.min_bid`/`max_bid`.
- Amazon Ads API: Use [automation/shared/amazon_client.py](../automation/shared/amazon_client.py). Handles token refresh via `TokenManager`, retries (`tenacity`) and respects `DRY_RUN`.
- Tokens: Use [automation/shared/token_manager.py](../automation/shared/token_manager.py); supports refresh + Secret rotation. Prefer `get_token_manager()`.
- BigQuery: Use [shared/bigquery_client.py](../shared/bigquery_client.py); parameterize queries via `QueryJobConfig` and avoid hardcoded project/dataset.
- Logging: Use [automation/shared/logger.py](../automation/shared/logger.py) for structured logs; include concise reasons/components when logging bid actions.
- Bid updates: Optional batch mode via env `USE_BATCH_UPDATE=true` when supported by the client.

## Developer Workflows
- Install deps: `pip install -r requirements.txt`.
- Local prototypes: `python ../bid_optimizer.py`, `python ../budget_monitor.py` (ensure GCP auth + env).
- Min Winning Bid (quick experiment): set `KEYWORD_ID`, `CAMPAIGN_ID`, `ADGROUP_ID`, optional `OBSERVE_MIN`, `MIN_BID`, `MAX_BID`, then run `python -m automation.min_winning_bid` (see [README.md](../README.md)).
- Min Winning Bid notes: adjust `REPORT_COLUMNS` if Reporting v3 schema differs; `REPORT_TYPE_ID` defaults to `spSearchTerm`.
- Cloud Run dry-run: use [test_run.sh](../test_run.sh) or `gcloud run jobs execute <job> --update-env-vars=DRY_RUN=true --wait`.
- Scheduler: see [setup_scheduler.sh](../setup_scheduler.sh) for job schedules.

## Testing
- Run with `PYTHONPATH=.` to resolve `automation.shared.*` imports.
- Focused tests exist in [tests/test_rules_engine.py](../tests/test_rules_engine.py) and [tests/test_token_manager.py](../tests/test_token_manager.py).
- If duplicate nested tests exist, you can ignore them: `pytest -q --ignore Amazon-PPC-Automation-System-SAAS/tests` (see [README.md](../README.md)).

## Implementation Tips
- Place production modules under `automation/`; keep root prototypes minimal.
- Use `BigQueryClient.log_bid_change(...)` for audit with clear `reason` and `components`.
- Verify environment health via [automation/verify_live_data.py](../automation/verify_live_data.py) (`--skip-bq`, `--skip-amazon`).
- Read config via `settings` and use `settings.timezone` for time-based logic.

Questions or missing details (e.g., batch update helpers, additional entry points)? Share what you’re building and we’ll refine these instructions.