#!/bin/bash
set -e

# Configuration
PROJECT_ID="amazon-ppc-474902"
REGION="us-central1"
SERVICE_ACCOUNT="amazon-ppc-sa@${PROJECT_ID}.iam.gserviceaccount.com"
# Use Artifact Registry (recommended) instead of legacy GCR
IMAGE_BASE="us-central1-docker.pkg.dev/${PROJECT_ID}/ppc-automation/amazon-ppc-automation"
IMAGE_LATEST="${IMAGE_BASE}:latest"
IMAGE_TAG="${IMAGE_BASE}:$(date +%Y%m%d-%H%M%S)"

echo "🚀 Starting Deployment for Project: $PROJECT_ID"
echo "==================================================="

# 1. Enable Required APIs (Run once)
echo "🔌 Enabling Google Cloud APIs..."
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com \
    secretmanager.googleapis.com \
    --project=$PROJECT_ID

# 2. Build and Push Docker Image
echo "📦 Building and Pushing Docker Image to Artifact Registry..."
# Ensure Artifact Registry repository exists
gcloud services enable artifactregistry.googleapis.com --project "$PROJECT_ID"
gcloud artifacts repositories describe ppc-automation \
    --location=$REGION \
    --project="$PROJECT_ID" \
    || gcloud artifacts repositories create ppc-automation \
             --repository-format=docker \
             --location=$REGION \
             --description="PPC automation images" \
             --project="$PROJECT_ID"

# Build and push with both timestamp and latest tags
gcloud builds submit --tag "$IMAGE_TAG" --tag "$IMAGE_LATEST" --project="$PROJECT_ID" .

# 3. Deploy Cloud Run Jobs (Create or Update)
echo "☁️  Deploying Cloud Run Jobs..."

# --- Job: Bid Optimizer ---
echo "   > Deploying Bid Optimizer..."
# Try to update, if fail (doesn't exist), then create
if ! gcloud run jobs update bid-optimizer \
    --image="$IMAGE_LATEST" \
    --region=$REGION \
    --command=python \
    --args=bid_optimizer.py \
    --set-env-vars=GCP_PROJECT=$PROJECT_ID,BQ_DATASET=amazon_ppc,DRY_RUN=false \
    --service-account=$SERVICE_ACCOUNT \
    --memory=2Gi \
    --task-timeout=30m \
    --max-retries=2 \
    --project=$PROJECT_ID 2>/dev/null; then

    echo "     (Job not found, creating new...)"
    gcloud run jobs create bid-optimizer \
        --image="$IMAGE_LATEST" \
        --region=$REGION \
        --command=python \
        --args=bid_optimizer.py \
        --set-env-vars=GCP_PROJECT=$PROJECT_ID,BQ_DATASET=amazon_ppc,DRY_RUN=false \
        --service-account=$SERVICE_ACCOUNT \
        --memory=2Gi \
        --task-timeout=30m \
        --max-retries=2 \
        --project=$PROJECT_ID
fi

# --- Job: Budget Monitor ---
echo "   > Deploying Budget Monitor..."
if ! gcloud run jobs update budget-monitor \
    --image="$IMAGE_LATEST" \
    --region=$REGION \
    --command=python \
    --args=budget_monitor.py \
    --set-env-vars=GCP_PROJECT=$PROJECT_ID,BQ_DATASET=amazon_ppc,DRY_RUN=false \
    --service-account=$SERVICE_ACCOUNT \
    --memory=1Gi \
    --task-timeout=10m \
    --project=$PROJECT_ID 2>/dev/null; then

    echo "     (Job not found, creating new...)"
    gcloud run jobs create budget-monitor \
        --image="$IMAGE_LATEST" \
        --region=$REGION \
        --command=python \
        --args=budget_monitor.py \
        --set-env-vars=GCP_PROJECT=$PROJECT_ID,BQ_DATASET=amazon_ppc,DRY_RUN=false \
        --service-account=$SERVICE_ACCOUNT \
        --memory=1Gi \
        --task-timeout=10m \
        --project=$PROJECT_ID
fi

# --- Job: Keyword Harvester ---
echo "   > Deploying Keyword Harvester..."
if ! gcloud run jobs update keyword-harvester \
    --image="$IMAGE_LATEST" \
    --region=$REGION \
    --command=python \
    --args=automation/min_winning_bid.py \
    --set-env-vars=GCP_PROJECT=$PROJECT_ID,BQ_DATASET=amazon_ppc,DRY_RUN=false \
    --service-account=$SERVICE_ACCOUNT \
    --memory=1Gi \
    --task-timeout=15m \
    --project=$PROJECT_ID 2>/dev/null; then

    echo "     (Job not found, creating new...)"
    gcloud run jobs create keyword-harvester \
        --image="$IMAGE_LATEST" \
        --region=$REGION \
        --command=python \
        --args=automation/min_winning_bid.py \
        --set-env-vars=GCP_PROJECT=$PROJECT_ID,BQ_DATASET=amazon_ppc,DRY_RUN=false \
        --service-account=$SERVICE_ACCOUNT \
        --memory=1Gi \
        --task-timeout=15m \
        --project=$PROJECT_ID
fi

# 4. Set up Cloud Scheduler Triggers
echo "⏰ Setting up Cloud Schedulers..."

# Schedule: Bid Optimizer (Hourly)
if ! gcloud scheduler jobs describe amazon-bid-optimizer --location=$REGION --project=$PROJECT_ID > /dev/null 2>&1; then
    gcloud scheduler jobs create http amazon-bid-optimizer \
        --location=$REGION \
        --schedule="0 * * * *" \
        --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/bid-optimizer:run" \
        --http-method=POST \
        --oauth-service-account-email=$SERVICE_ACCOUNT \
        --project=$PROJECT_ID
    echo "   + Created Scheduler: amazon-bid-optimizer (Hourly)"
else
    echo "   = Scheduler amazon-bid-optimizer already exists"
fi

# Schedule: Budget Monitor (Every 15 mins)
if ! gcloud scheduler jobs describe amazon-budget-monitor --location=$REGION --project=$PROJECT_ID > /dev/null 2>&1; then
    gcloud scheduler jobs create http amazon-budget-monitor \
        --location=$REGION \
        --schedule="*/15 * * * *" \
        --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/budget-monitor:run" \
        --http-method=POST \
        --oauth-service-account-email=$SERVICE_ACCOUNT \
        --project=$PROJECT_ID
    echo "   + Created Scheduler: amazon-budget-monitor (Every 15m)"
else
    echo "   = Scheduler amazon-budget-monitor already exists"
fi

# Schedule: Keyword Harvester (Daily at 2 AM)
if ! gcloud scheduler jobs describe amazon-keyword-harvester --location=$REGION --project=$PROJECT_ID > /dev/null 2>&1; then
    gcloud scheduler jobs create http amazon-keyword-harvester \
        --location=$REGION \
        --schedule="0 2 * * *" \
        --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/keyword-harvester:run" \
        --http-method=POST \
        --oauth-service-account-email=$SERVICE_ACCOUNT \
        --project=$PROJECT_ID
    echo "   + Created Scheduler: amazon-keyword-harvester (Daily 2 AM)"
else
    echo "   = Scheduler amazon-keyword-harvester already exists"
fi

echo ""
echo "✅✅ Deployment Complete! System is LIVE."
echo "---------------------------------------------------"
echo "Monitor executions here: https://console.cloud.google.com/run/jobs?project=$PROJECT_ID"
