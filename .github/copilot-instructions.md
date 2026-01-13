# AI Coding Agent Instructions
Concise, codebase-specific guidance to make AI agents productive here.

## Architecture & Jobs
- **Dual-layer structure**: Root files ([bid_optimizer.py](../bid_optimizer.py), [budget_monitor.py](../budget_monitor.py)) are runnable prototypes; production code lives under `automation/`. [Dockerfile](../Dockerfile) copies entire repo; [main.py](../main.py) provides health checks but jobs override entry via `--command`.
- **Module organization**: [shared/](../shared) contains core logic, [automation/shared/](../automation/shared) re-exports for clean imports (see [automation/shared/__init__.py](../automation/shared/__init__.py)).
- Cloud Run jobs deploy via [cloudbuild.yaml](../cloudbuild.yaml):
  - **Bid Optimizer** (`bid-optimizer`): Hourly keyword bid optimization using AOV tiers, performance classification, match types
  - **Budget Monitor** (`budget-monitor`): 15-min pacing checks; emergency bid cuts at 3 PM threshold violations

## Config & Secrets
- Central config via `settings` in [shared/config.py](../shared/config.py) using pydantic-settings; all fields have defaults. Re-exported by [automation/shared/config.py](../automation/shared/config.py).
- **Required env vars**: `GCP_PROJECT`, `BQ_DATASET`, `DRY_RUN` (boolean); optional: `TIMEZONE` (default: America/New_York), `DEFAULT_AOV`, `MIN_BID`, `MAX_BID`.
- **Secret Manager keys** (auto-fetched): `amazon_client_id`, `amazon_client_secret`, `amazon_refresh_token`, `amazon_profile_id`.
- **DRY_RUN enforcement**: Honors `settings.dry_run` across Amazon API calls and BigQuery writes. See [automation/shared/amazon_client.py](../automation/shared/amazon_client.py) for pattern.

## Data & Tables
- Dataset defaults to `amazon_ppc` in `settings.project_id`.
- Core tables:
  - `sp_advertised_product_metrics` → AOV + freshness (see [shared/bigquery_client.py](../shared/bigquery_client.py)).
  - `sp_targeting_metrics`, `sp_keywords` → keyword performance (see `get_keywords_for_optimization()` in [shared/bigquery_client.py](../shared/bigquery_client.py)).
  - `sp_campaigns`, `sp_campaign_metrics` → budget pacing (see `get_campaign_budget_status()` in [shared/bigquery_client.py](../shared/bigquery_client.py)).
  - `bid_change_log` → audit trail; auto-created by `BigQueryClient._ensure_bid_log_table_exists()`.

## Patterns & Conventions
- **Bid calculation**: Use `BidCalculator` from [shared/rules_engine.py](../shared/rules_engine.py) (re-exported via [automation/shared/rules_engine.py](../automation/shared/rules_engine.py)). Applies:
  - AOV-based base ceilings (L/M/H/X tiers from `_get_aov_base_ceiling()`)
  - Performance tier multipliers (A/B/C/D/E from `classify_performance_tier()`)
  - Match type modifiers (EXACT=1.0, PHRASE=0.8, BROAD=0.6)
  - Time-of-day multipliers (Prime 6-10pm: 1.2×, Overnight: 0.7×)
  - Respects `settings.min_bid`/`max_bid` hard limits; stability threshold ($0.05 min change)
- **Amazon Ads API**: Use [automation/shared/amazon_client.py](../automation/shared/amazon_client.py). Key features:
  - **CRITICAL**: Amazon API requires `keywordId` as STRING, not number. Always cast: `str(keyword_id)` before API calls (BigQuery returns integers)
  - Token refresh via `TokenManager` (auto-detects 401, calls `force_refresh()`)
  - Retry logic via `tenacity` decorators (5 attempts, exponential backoff for 429 rate limits)
  - Respects `DRY_RUN` at method level (returns mock success dicts)
  - Payload normalization: `keywordId`→string, `bid`→2 decimals, `state`→"ENABLED"
- **Token Management**: [shared/token_manager.py](../shared/token_manager.py) handles OAuth refresh + Secret Manager rotation. Singleton via `get_token_manager()`.
- **BigQuery patterns**: Use [shared/bigquery_client.py](../shared/bigquery_client.py):
  - Parameterize queries via `QueryJobConfig` (never hardcode project/dataset)
  - **Attribution lag handling**: All queries exclude last 3 days (e.g., `DATE_SUB(CURRENT_DATE(), INTERVAL @days + 3 DAY)` to `INTERVAL 3 DAY`)
  - Auto-creates audit tables if missing (`_ensure_bid_log_table_exists()`)
- **Logging**: [shared/logger.py](../shared/logger.py) emits JSON to stdout for Cloud Logging. Include `reason`/`components` in bid action logs.
- **AOV caching**: [aov_fetcher.py](../aov_fetcher.py) pre-loads 14d+30d AOV maps via `fetch_all()` (call once per job). Tiered fallback: 14d → 30d → default.

## Developer Workflows
- Install deps: `pip install -r requirements.txt`.
- Local runs: `python bid_optimizer.py`, `python budget_monitor.py` (ensure GCP auth + env).
- Cloud Run dry-run: use [test_run.sh](../test_run.sh) or `gcloud run jobs execute <job> --update-env-vars=DRY_RUN=true --wait`.
- Scheduler: see [setup_scheduler.sh](../setup_scheduler.sh) for job schedules.

## Testing
- Run with `PYTHONPATH=.` to resolve `automation.shared.*` imports.
- Focused tests exist in [tests/test_rules_engine.py](../tests/test_rules_engine.py) and [tests/test_token_manager.py](../tests/test_token_manager.py).
- If duplicate nested tests exist, you can ignore them: `pytest -q --ignore Amazon-PPC-Automation-System-SAAS/tests` (see [README.md](../README.md)).

## Implementation Tips
- Place production modules under `automation/`; keep root prototypes minimal.
- Use `BigQueryClient.log_bid_change(...)` for audit with clear `reason` and `components`.
- Preload AOV where needed via [aov_fetcher.py](../aov_fetcher.py).
- Read config via `settings` and use `settings.timezone` for time-based logic.

## Common Pitfalls
- **400 "NUMBER_VALUE can not be converted to a String"**: Amazon API received numeric `keywordId`. Always cast to string: `str(keyword["keywordId"])` when building payloads. BigQuery returns keywordId as INT64.
- **Attribution lag**: Never query BigQuery metrics for last 3 days (data incomplete). All metric queries must exclude: `WHERE segments_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)`.
- **Token expiry**: Don't cache `access_token` outside `TokenManager`. Always call `get_valid_access_token()` which handles refresh automatically.

Questions or missing details (e.g., batch update helpers, additional entry points)? Share what you’re building and we’ll refine these instructions.