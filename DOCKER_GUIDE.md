# PropVal Docker Build & Run Guide

## Building the Image

```bash
docker build -t propval-ingest:latest .
```

## Running Locally

### Test with FOR_SALE data (default):
```bash
docker run \
  -e API_KEY_ZILLOW="your_api_key" \
  -e GOOGLE_CLOUD_STORAGE_BUCKET_NAME="your_bucket" \
  -e GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json" \
  propval-ingest:latest
```

### Test with SOLD data:
```bash
docker run \
  -e API_KEY_ZILLOW="your_api_key" \
  -e GOOGLE_CLOUD_STORAGE_BUCKET_NAME="your_bucket" \
  -e GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json" \
  propval-ingest:latest sold
```

### Test with FOR_SALE data (explicit):
```bash
docker run \
  -e API_KEY_ZILLOW="your_api_key" \
  -e GOOGLE_CLOUD_STORAGE_BUCKET_NAME="your_bucket" \
  -e GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json" \
  propval-ingest:latest forSale
```

## Pushing to Google Artifact Registry

```bash
# 1. Configure Docker for GCP
gcloud auth configure-docker us-central1-docker.pkg.dev

# 2. Tag the image
docker tag propval-ingest:latest \
  us-central1-docker.pkg.dev/YOUR_PROJECT_ID/propval/ingest:latest

# 3. Push to Artifact Registry
docker push us-central1-docker.pkg.dev/YOUR_PROJECT_ID/propval/ingest:latest
```

## Cloud Run Deployment

### Deploy as TWO separate jobs (recommended):

#### Job 1: Collect SOLD data
```bash
gcloud run jobs create propval-ingest-sold \
  --image=us-central1-docker.pkg.dev/YOUR_PROJECT_ID/propval/ingest:latest \
  --args="sold" \
  --set-env-vars="API_KEY_ZILLOW=xxx,GOOGLE_CLOUD_STORAGE_BUCKET_NAME=xxx" \
  --service-account=ingestion-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --region=us-central1
```

#### Job 2: Collect FOR_SALE data
```bash
gcloud run jobs create propval-ingest-forsale \
  --image=us-central1-docker.pkg.dev/YOUR_PROJECT_ID/propval/ingest:latest \
  --args="forSale" \
  --set-env-vars="API_KEY_ZILLOW=xxx,GOOGLE_CLOUD_STORAGE_BUCKET_NAME=xxx" \
  --service-account=ingestion-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --region=us-central1
```

### Test the jobs:
```bash
# Test sold job
gcloud run jobs execute propval-ingest-sold --region=us-central1

# Test forSale job
gcloud run jobs execute propval-ingest-forsale --region=us-central1
```

## Notes

- The Dockerfile uses `ENTRYPOINT` + `CMD` pattern for flexibility
- Default mode is `forSale` if no argument provided
- Environment variables should be set in Cloud Run configuration (not in .env)
- Service account credentials are baked into the image (alternative: use Workload Identity)
