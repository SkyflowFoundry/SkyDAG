"""Deployment configuration management"""
import os
from pathlib import Path
from typing import Dict, Optional


def load_env_if_available():
    """Load .env.local from project root if available"""
    try:
        from dotenv import load_dotenv
        
        project_root = Path(__file__).parent.parent
        env_file = project_root / ".env.local"
        
        if env_file.exists():
            load_dotenv(env_file)
            return True
    except ImportError:
        pass
    
    return False


class DeploymentConfig:
    """Configuration for remote deployment"""
    
    def __init__(self):
        load_env_if_available()
        self.platform = self._get_required("DEPLOY_PLATFORM")
        self.environment = self._get_optional("DEPLOY_ENVIRONMENT", "dev")
        
        # Common settings
        self.dag_bucket_or_container = self._get_required("DEPLOY_DAG_BUCKET")
        self.dag_prefix = self._get_optional("DEPLOY_DAG_PREFIX", "dags")
        
        # Platform-specific configs
        if self.platform.lower() == "gcp":
            self._setup_gcp_config()
        elif self.platform.lower() == "aws":
            self._setup_aws_config()
        elif self.platform.lower() == "azure":
            self._setup_azure_config()
        else:
            raise ValueError(f"Unsupported deployment platform: {self.platform}")
    
    def _get_required(self, key: str) -> str:
        """Get required environment variable"""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable {key} is not set")
        return value
    
    def _get_optional(self, key: str, default: str = None) -> Optional[str]:
        """Get optional environment variable"""
        return os.getenv(key, default)
    
    def _setup_gcp_config(self):
        """Setup Google Cloud Composer configuration"""
        self.gcp_project = self._get_required("DEPLOY_GCP_PROJECT")
        self.gcp_region = self._get_required("DEPLOY_GCP_REGION")
        self.composer_environment = self._get_required("DEPLOY_COMPOSER_ENVIRONMENT")
        self.service_account_key = self._get_optional("GOOGLE_APPLICATION_CREDENTIALS")
    
    def _setup_aws_config(self):
        """Setup Amazon MWAA configuration"""
        self.aws_region = self._get_required("DEPLOY_AWS_REGION")
        self.mwaa_environment = self._get_required("DEPLOY_MWAA_ENVIRONMENT")
        self.aws_access_key_id = self._get_optional("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = self._get_optional("AWS_SECRET_ACCESS_KEY")
    
    def _setup_azure_config(self):
        """Setup Azure Data Factory configuration"""
        self.azure_subscription_id = self._get_required("DEPLOY_AZURE_SUBSCRIPTION_ID")
        self.azure_resource_group = self._get_required("DEPLOY_AZURE_RESOURCE_GROUP")
        self.azure_data_factory = self._get_required("DEPLOY_AZURE_DATA_FACTORY")
        self.azure_storage_account = self._get_required("DEPLOY_AZURE_STORAGE_ACCOUNT")
        self.azure_tenant_id = self._get_optional("AZURE_TENANT_ID")
        self.azure_client_id = self._get_optional("AZURE_CLIENT_ID")
        self.azure_client_secret = self._get_optional("AZURE_CLIENT_SECRET")
    
    def get_airflow_variables(self) -> Dict[str, str]:
        """Get Airflow Variables to set remotely"""
        variables = {}
        
        # Core SkyDAG variables
        skydag_vars = [
            "SKYDAG_PLATFORM",
            "SKYDAG_SOURCE", 
            "SKYDAG_DEST",
            "SKYDAG_POLL_MAX_WAIT",
            "SKYDAG_POLL_INITIAL",
            "SKYDAG_POLL_BACKOFF", 
            "SKYDAG_POLL_MAX_INTERVAL"
        ]
        
        # Skyflow API variables
        skyflow_vars = [
            "SKYFLOW_START_URL",
            "SKYFLOW_POLL_URL_TEMPLATE",
            "SKYFLOW_AUTH_HEADER"
        ]
        
        for var in skydag_vars + skyflow_vars:
            value = os.getenv(var)
            if value:
                variables[var] = value
        
        return variables