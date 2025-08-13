# SkyDAG

**A packaged accelerator for automated data protection at scale using Skyflow Detect.**

## About Skyflow Detect

Skyflow Detect is a data protection service that automatically identifies, classifies, and de-identifies sensitive data across your files and data pipelines. Built on Skyflow's Data Privacy Vault, it provides:

- **Intelligent PII Detection**: Automatically discovers 50+ types of sensitive data including names, SSNs, credit cards, medical records, and custom patterns
- **Advanced De-identification**: Multiple tokenization strategies including format-preserving tokens, vault tokens, and entity counters
- **Enterprise-Grade Security**: Zero-trust architecture with end-to-end encryption and compliance with SOC 2, PCI DSS, HIPAA, and GDPR
- **API-First Design**: RESTful APIs for seamless integration into existing data workflows and pipelines

## What SkyDAG Does

SkyDAG is a complete implementation accelerator that packages Skyflow Detect capabilities within a fully deployable Apache Airflow DAG, enabling:

1. **Automated File Processing**: Monitors cloud storage buckets and automatically processes new files
2. **Parallel Data Protection**: Processes multiple files simultaneously through Skyflow Detect API with intelligent tokenization
3. **Smart Token Selection**: Automatically selects appropriate token types based on file format (vault tokens for text, entity counters for binary)
4. **Production Infrastructure**: Complete cloud deployment with managed Airflow (Composer/MWAA), storage, and IAM
5. **Fault Tolerance**: Individual file failures don't block processing of other files
6. **Enterprise Monitoring**: Full observability through Airflow UI and cloud monitoring services

### Core Workflow

```
Cloud Storage → SkyDAG Pipeline → Skyflow Detect API → Protected Data Storage
```

1. **Ingestion**: Files uploaded to source bucket trigger processing
2. **Detection**: Skyflow Detect identifies and classifies sensitive data
3. **Protection**: Advanced tokenization de-identifies sensitive elements
4. **Output**: Protected files written to destination storage with full audit trail 

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

### Required GCP Permissions

Your GCP account needs the following IAM roles for SkyDAG operations:

**Core Roles:**
- `Composer Administrator` - Manage Composer environments
- `Storage Admin` - Create and manage buckets
- `Service Account Admin` - Create service accounts
- `Project IAM Admin` - Assign IAM permissions

**API Requirements:**
These APIs must be enabled in your GCP project:
- Cloud Composer API (`composer.googleapis.com`)
- Cloud Storage API (`storage.googleapis.com`)
- Identity and Access Management API (`iam.googleapis.com`)
- Service Usage API (`serviceusage.googleapis.com`)
- Resource Manager API (`cloudresourcemanager.googleapis.com`)

**Alternative:** Use `Owner` or `Editor` project-level role (includes all required permissions)

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
- **Bucket Names**: A unique 5-character suffix is automatically added to prevent naming conflicts
- **State Management**: Uses `.skydag_state.json` for deployment tracking
- **Cost Awareness**: Cloud resources incur costs - use `destroy` when done
- **File Size**: Base64 encoding inflates file size by ~33%
- **Security**: Never commit .env.local - contains sensitive credentials

## Monitoring

- **Airflow UI**: `https://console.cloud.google.com/composer/environments`
- **Storage**: `https://console.cloud.google.com/storage/browser`
- **Logs**: Available in both Airflow UI and Cloud Console
- **Variables**: Airflow UI → Admin → Variables