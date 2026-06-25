backend/
├── automation/
│   ├── __init__.py
│   ├── bid_optimizer.py          # Main optimization job
│   ├── budget_monitor.py         # Budget pacing job
│   ├── data_sync.py              # Amazon API sync job
│   ├── data_verification.py      # Live data checks (NEW)
│   │
│   ├── shared/
│   │   ├── __init__.py
│   │   ├── amazon_client.py      # Amazon Ads API wrapper
│   │   ├── bigquery_client.py    # BigQuery operations
│   │   ├── rules_engine.py       # Bid calculation logic
│   │   ├── config.py             # Configuration
│   │   └── logger.py             # Structured logging
│   │
│   └── tests/
│       ├── test_rules_engine.py
│       └── test_verification.py
│
├── requirements.txt
├── Dockerfile
└── cloudbuild.yaml
