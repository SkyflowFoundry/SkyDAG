# SkyDAG

An Airflow pipeline for processing files from cloud storage through Skyflow's detection API.

## What it does

1. Discovers files in cloud storage buckets
2. Processes each file in parallel through Skyflow detection API
3. Writes processed results to destination storage 
4. Supports partial success - successful files continue even if others fail

## Quick Start

### Prerequisites

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment**:
   ```bash
   cp .env.local.example .env.local
   # Edit .env.local with your values
   ```

3. **Authenticate with your cloud provider**:
   ```bash
   # GCP
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   
   # AWS (coming soon)
   # aws configure
   
   # Azure (coming soon) 
   # az login
   ```

### End-to-End Workflow

#### Step 1: Setup Infrastructure
```bash
python deploy.py setup
```
- **Duration**: ~25 minutes (Composer environment creation)
- **Creates**: Buckets, Composer environment, service accounts, IAM permissions

#### Step 2: Deploy DAGs  
```bash
python deploy.py deploy
```
- **Duration**: ~2 minutes
- **Uploads**: DAG files, sets Airflow Variables, verifies deployment

#### Step 3: Upload Test Data (Manual)
```bash
gcloud storage cp test-data/test_records_10.csv gs://skydag-test-source/input-files/
```

#### Step 4: Trigger Pipeline
```bash
# Process single file
python deploy.py trigger test_records_10.csv

# Process all files in source bucket
python deploy.py trigger
```

#### Step 5: Monitor & Results
- **Airflow UI**: Monitor DAG execution progress
- **Results**: Check `gs://skydag-test-dest/output-files/` for processed files
- **Logs**: View task logs for detailed processing information

#### Step 6: Cleanup (Optional)
```bash
python deploy.py undeploy  # Remove DAGs only
python deploy.py destroy   # Remove all infrastructure
```

## Platform Support

- ✅ **Google Cloud Platform** - Full support with Cloud Composer
- 🚧 **Amazon Web Services** - Coming soon (MWAA)
- 🚧 **Microsoft Azure** - Coming soon (Data Factory)

## Key Features

### Parallel Processing
- Files processed simultaneously through Skyflow API
- Write tasks start as soon as each file completes processing
- No waiting for slowest file - true streaming behavior

### Fault Tolerance  
- Individual file failures don't block other files
- Successful files continue to completion
- Clear error reporting for failed files

### Smart File Handling
- Supports both single-file and bulk processing
- Dynamic task mapping for efficient resource usage
- Intelligent file discovery and filtering

## Configuration

### Required Settings (.env.local)
```bash
# Platform
SKYDAG_PLATFORM=gcp
SKYDAG_SOURCE=your-source-bucket/input-files
SKYDAG_DEST=your-dest-bucket/output-files

# Skyflow API
SKYFLOW_START_URL=https://your-vault.vault.skyflowapis.com/v1/detect/deidentify/file  
SKYFLOW_POLL_URL_TEMPLATE=https://your-vault.vault.skyflowapis.com/v1/detect/runs/{run_id}?vault_id=YOUR_VAULT_ID
SKYFLOW_AUTH_HEADER=Bearer YOUR_JWT_TOKEN

# Deployment (GCP)
DEPLOY_PLATFORM=gcp
DEPLOY_GCP_PROJECT=your-project-id
DEPLOY_GCP_REGION=us-central1
DEPLOY_COMPOSER_ENVIRONMENT=your-composer-env
DEPLOY_DAG_BUCKET=your-dags-bucket
```

See `.env.local.example` for complete configuration options.

## Advanced Usage

```bash
# Infrastructure management
python deploy.py setup      # Create all infrastructure 
python deploy.py destroy    # Destroy all infrastructure
python deploy.py status     # Show deployment status

# DAG management  
python deploy.py upload     # Upload DAG files only
python deploy.py variables  # Set Airflow Variables only
python deploy.py undeploy   # Remove DAGs with cleanup

# Development
python deploy.py dependencies  # Check/install dependencies
```

## Important Notes

- **Processing**: Each file processed independently - failures don't block others
- **State Management**: Uses `.skydag_state.json` for deployment tracking
- **Cost Awareness**: Cloud resources incur costs - use `destroy` when done
- **File Size**: Base64 encoding inflates file size by ~33%
- **Security**: Never commit .env.local - contains sensitive credentials

## Monitoring

- **Airflow UI**: `https://console.cloud.google.com/composer/environments`
- **Storage**: `https://console.cloud.google.com/storage/browser`
- **Logs**: Available in both Airflow UI and Cloud Console
- **Variables**: Airflow UI → Admin → Variables