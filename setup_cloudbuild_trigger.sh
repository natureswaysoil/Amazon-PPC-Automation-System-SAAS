#!/bin/bash
set -euo pipefail

# Configure a Cloud Build trigger to run cloudbuild.yaml on pushes to main.
# Prereqs:
# - gcloud installed and authenticated
# - Cloud Build API enabled
# - GitHub repo connected via Cloud Build Github App (Console: Cloud Build > Triggers > Connect repository)

PROJECT_ID=${PROJECT_ID:-"amazon-ppc-474902"}
TRIGGER_NAME=${TRIGGER_NAME:-"ppc-automation-main"}
REPO_OWNER=${REPO_OWNER:-"natureswaysoil"}
REPO_NAME=${REPO_NAME:-"Amazon-PPC-Automation-System-SAAS"}
BRANCH_PATTERN=${BRANCH_PATTERN:-"^main$"}
BUILD_CONFIG="cloudbuild.yaml"

echo "Enabling Cloud Build API (if not already)..."
gcloud services enable cloudbuild.googleapis.com --project "$PROJECT_ID"

echo "Creating/updating Cloud Build trigger '$TRIGGER_NAME' on branch pattern '$BRANCH_PATTERN'..."
# Using GitHub App connection. Ensure repo is connected in the Console first.
if gcloud beta builds triggers create github \
	--name="$TRIGGER_NAME" \
	--repo-owner="$REPO_OWNER" \
	--repo-name="$REPO_NAME" \
	--branch-pattern="$BRANCH_PATTERN" \
	--build-config="$BUILD_CONFIG" \
	--project "$PROJECT_ID"; then
	echo "Trigger created."
else
	echo "Trigger may already exist. Updating build config and branch pattern..."
	TRIGGER_ID=$(gcloud beta builds triggers list --project "$PROJECT_ID" --filter "name=$TRIGGER_NAME" --format="value(id)")
	if [[ -n "$TRIGGER_ID" ]]; then
		gcloud beta builds triggers update "$TRIGGER_ID" \
			--build-config="$BUILD_CONFIG" \
			--branch-pattern="$BRANCH_PATTERN" \
			--project "$PROJECT_ID"
		echo "Trigger updated."
	else
		echo "Trigger not found and create failed. Please ensure GitHub App connection exists for $REPO_OWNER/$REPO_NAME."
		exit 1
	fi
fi

echo "Done. Trigger '$TRIGGER_NAME' is configured to build on pushes to main."