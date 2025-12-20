#!/bin/bash
set -e

PROJECT_ID="amazon-ppc-474902"
REGION="us-central1"

echo "⏰ Setting up Cloud Scheduler"

# Bid Optimizer - Runs every hour
echo "Creating bid-optimizer schedule..."
gcloud scheduler jobs create http bid-optimizer-hourly \
  --location=$REGION \
  --schedule="0 * * * *" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/bid-optimizer:run" \
  --http-method=POST \
  --oauth-service-account-email=amazon-ppc-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --project=$PROJECT_ID \
  --time-zone="America/New_York" \
  || echo "Schedule already exists"

# Budget Monitor - Runs every 15 minutes
echo "Creating budget-monitor schedule..."
gcloud scheduler jobs create http budget-monitor-15min \
  --location=$REGION \
  --schedule="*/15 * * * *" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/budget-monitor:run" \
  --http-method=POST \
  --oauth-service-account-email=amazon-ppc-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --project=$PROJECT_ID \
  --time-zone="America/New_York" \
  || echo "Schedule already exists"

echo ""
echo "✅ Scheduler setup complete!"
echo ""
echo "View schedules:"
echo "https://console.cloud.google.com/cloudscheduler?project=$PROJECT_ID"
