# Amazon PPC Automation Setup Checklist

## ✅ Pre-Deployment

### 1. Google Cloud Project Setup
- [ ] Project created: `amazon-ppc-474902`
- [ ] Billing enabled
- [ ] APIs enabled:
  - [ ] Cloud Run API
  - [ ] Cloud Scheduler API
  - [ ] Cloud Build API
  - [ ] BigQuery API
  - [ ] Secret Manager API

### 2. Service Account
- [ ] Created: `amazon-ppc-sa@amazon-ppc-474902.iam.gserviceaccount.com`
- [ ] Roles assigned:
  - [ ] `roles/bigquery.dataViewer`
  - [ ] `roles/bigquery.jobUser`
  - [ ] `roles/secretmanager.secretAccessor`
  - [ ] `roles/logging.logWriter`

### 3. Secrets in Secret Manager
Create these secrets with your Amazon credentials:

```bash
# Amazon Ads API credentials
gcloud secrets create amazon_client_id --data-file=- <<< "amzn1.application-oa2-client.5f71a2504cb34903be357c736c290a30"
gcloud secrets create amazon_client_secret --data-file=- <<< "amzn1.oa2-cs.v1.a1a0e3a3cf314be2eb5269334bd4401a18762fd702e2b100a4f61697a674f3af"
gcloud secrets create amazon_refresh_token --data-file=- <<< "Atzr|IwEBIFQ0aBsCM7kG9_n7zzCQ5EsG_XcN6u-I8KyNhMU2g5oS2rsfpaSshkCDarQnDqlLyssf8HR96AwcYBnMLR-zn1wGdkF8fZxraS7NhXZXmO4aOUCKEgIOpwzoYlNSHIGhs2e6hCv2r1vVTlOWmjK-WU1SXslUaUIi1WHu_yp6rHHx90nFPVGKcYR84DZEZDuU6CT19kPcCcvF3Yvph2Q1NNUXKzMe9CSxfTDcAFH43AOkOBNUK7KpkLiu_I8EOyCDOEDLsJME6rWlyRMtbxhHXIr9iw5hUO0nSPGosYEgvymEezryRveA6XoxUIKwwDcLS3p8L1dWNq_7T6h0PjlqSf9A"
gcloud secrets create amazon_profile_id --data-file=- <<< "1780498399290938"

# Grant service account access
for secret in amazon_client_id amazon_client_secret amazon_refresh_token amazon_profile_id; do
  gcloud secrets add-iam-policy-binding $secret \
    --member="serviceAccount:amazon-ppc-sa@amazon-ppc-474902.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
done
