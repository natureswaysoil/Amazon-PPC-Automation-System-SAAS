# Amazon PPC Automation System (SAAS)

This repository automates bid optimization and budget pacing for Amazon Sponsored Products using BigQuery and the Amazon Ads API. Production modules live under `automation/` and are deployed to Cloud Run.

## Minimal Winning Bid Tool

Find the minimum bid that yields impressions for a single keyword by bisection search, measured via Reporting v3.

- Location: `automation/min_winning_bid.py`
- Respects `DRY_RUN` for bid updates (reports are read-only)
- Uses a small 1-day report window with `TIME_UNIT=SUMMARY` for speed

### Quick Start

```bash
pip install -r requirements.txt
export KEYWORD_ID=123456789
export CAMPAIGN_ID=11111111
export ADGROUP_ID=22222222
export OBSERVE_MIN=15
export MIN_BID=0.40
export MAX_BID=3.00
export REPORT_TYPE_ID=spSearchTerm
# Optional: USE_BATCH_UPDATE=true to use client batch updates
python -m automation.min_winning_bid
```

### Sample Output

```
=== RESULT ===
best_bid: 0.85
   bid  impressions  clicks   won
 3.00            12       1  True
 1.92             7       0  True
 1.38             2       0  True
 1.12             0       0  False
 0.99             1       0  True
 0.92             0       0  False
 0.85             1       0  True
```

### Notes
- If Reporting v3 returns 400 due to missing columns, update `REPORT_COLUMNS` via env to match your tenant.
- If impressions are sparse, increase `OBSERVE_MIN` or allow a larger `MAX_BID`.
- To use batch bid updates when supported by your client, set `USE_BATCH_UPDATE=true`.

## Build & Run
- Cloud Run jobs and image build are configured in `cloudbuild.yaml`. `Dockerfile` only copies `automation/`.
- Test execution in DRY RUN mode: see `test_run.sh`.
- Schedules: `setup_scheduler.sh` (hourly optimizer, 15-minute budget monitor).

## Tests
- Run with `PYTHONPATH=.` to resolve `automation.shared.*` imports:

```bash
PYTHONPATH=. pytest -q --ignore Amazon-PPC-Automation-System-SAAS/tests
```

## Configuration & Secrets
- Env: `GCP_PROJECT`, `BQ_DATASET`, `DRY_RUN`; optional `GCP_REGION`, `DEFAULT_AOV`.
- Secret Manager: `amazon_client_id`, `amazon_client_secret`, `amazon_refresh_token`, `amazon_profile_id`.
