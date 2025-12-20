#!/bin/bash
set -e

PROJECT_ID="amazon-ppc-474902"
REGION="us-central1"
SERVICE_ACCOUNT="amazon-ppc-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🚀 Deploying Amazon PPC Automation System"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo ""

# Build and push image
echo "📦 Building Docker image..."
gcloud builds submit --config cloudbuild.yaml --project=$PROJECT_ID

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📋 Next steps:"
echo "1. Set up Cloud Scheduler triggers"
echo "2. Verify secrets in Secret Manager"
echo "3. Run test execution"
