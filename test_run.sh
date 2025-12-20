#!/bin/bash
set -e

PROJECT_ID="amazon-ppc-474902"
REGION="us-central1"

echo "🧪 Running test execution (DRY RUN mode)"
echo ""

# Test bid optimizer
echo "Testing Bid Optimizer..."
gcloud run jobs execute bid-optimizer \
  --region=$REGION \
  --project=$PROJECT_ID \
  --update-env-vars=DRY_RUN=true \
  --wait

echo ""
echo "Testing Budget Monitor..."
gcloud run jobs execute budget-monitor \
  --region=$REGION \
  --project=$PROJECT_ID \
  --update-env-vars=DRY_RUN=true \
  --wait

echo ""
echo "✅ Test execution complete!"
echo ""
echo "View logs:"
echo "https://console.cloud.google.com/run/jobs?project=$PROJECT_ID"
