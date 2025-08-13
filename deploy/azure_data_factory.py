"""Azure Data Factory deployment utilities"""
import json
import zipfile
from pathlib import Path
from typing import Dict

try:
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.storage.blob import BlobServiceClient
    HAS_AZURE_SDK = True
except ImportError:
    HAS_AZURE_SDK = False


class AzureDataFactoryDeployer:
    """Deploy DAGs to Azure Data Factory with Airflow integration"""
    
    def __init__(self, config):
        if not HAS_AZURE_SDK:
            raise ImportError("Azure SDK not installed. Run: pip install azure-mgmt-datafactory azure-storage-blob azure-identity")
        
        self.config = config
        self.subscription_id = config.azure_subscription_id
        self.resource_group = config.azure_resource_group
        self.data_factory_name = config.azure_data_factory
        self.storage_account = config.azure_storage_account
        self.container_name = config.dag_bucket_or_container
        
        # Setup Azure credentials
        if config.azure_client_id and config.azure_client_secret and config.azure_tenant_id:
            self.credential = ClientSecretCredential(
                tenant_id=config.azure_tenant_id,
                client_id=config.azure_client_id,
                client_secret=config.azure_client_secret
            )
        else:
            self.credential = DefaultAzureCredential()
        
        # Initialize clients
        try:
            self.df_client = DataFactoryManagementClient(
                self.credential, 
                self.subscription_id
            )
            
            storage_account_url = f"https://{self.storage_account}.blob.core.windows.net"
            self.blob_client = BlobServiceClient(
                account_url=storage_account_url,
                credential=self.credential
            )
        except Exception as e:
            raise ValueError(f"Failed to initialize Azure clients: {e}")
    
    def upload_dags(self, local_dags_path: Path) -> bool:
        """Upload DAG files to Azure Blob Storage"""
        try:
            print(f"Uploading DAGs to Azure Blob Storage: {self.storage_account}/{self.container_name}/{self.config.dag_prefix}/")
            
            # Get container client
            container_client = self.blob_client.get_container_client(self.container_name)
            
            # Create container if it doesn't exist
            try:
                container_client.create_container()
            except Exception:
                pass  # Container likely already exists
            
            # Upload all Python files in dags/ directory
            for py_file in local_dags_path.rglob("*.py"):
                # Get relative path from dags directory
                relative_path = py_file.relative_to(local_dags_path.parent)
                blob_name = f"{self.config.dag_prefix}/{relative_path}"
                
                blob_client = container_client.get_blob_client(blob_name)
                with open(py_file, 'rb') as data:
                    blob_client.upload_blob(data, overwrite=True)
                
                print(f"  Uploaded: {relative_path}")
            
            print("✅ DAG upload completed")
            return True
            
        except Exception as e:
            print(f"❌ DAG upload failed: {e}")
            return False
    
    def create_airflow_pipeline(self, variables: Dict[str, str]) -> bool:
        """Create Azure Data Factory pipeline that triggers Airflow DAG"""
        try:
            print("Creating Azure Data Factory pipeline for Airflow integration...")
            
            # Create a pipeline that can trigger Airflow DAGs
            # This is a simplified example - in practice you'd need an Airflow REST API endpoint
            pipeline_definition = {
                "activities": [
                    {
                        "name": "TriggerSkyDAG",
                        "type": "WebActivity",
                        "typeProperties": {
                            "url": f"https://your-airflow-webserver/api/v1/dags/skydag_pipeline/dagRuns",
                            "method": "POST",
                            "headers": {
                                "Content-Type": "application/json",
                                "Authorization": "Bearer <AIRFLOW_API_TOKEN>"
                            },
                            "body": {
                                "conf": variables
                            }
                        }
                    }
                ]
            }
            
            # Create the pipeline
            pipeline = self.df_client.pipelines.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                pipeline_name="SkyDAG_Pipeline",
                pipeline=pipeline_definition
            )
            
            print("✅ Azure Data Factory pipeline created")
            print("⚠️  Note: You'll need to configure Airflow REST API endpoint and authentication")
            return True
            
        except Exception as e:
            print(f"❌ Pipeline creation failed: {e}")
            return False
    
    def set_airflow_variables(self, variables: Dict[str, str]) -> bool:
        """Set up variables for Airflow (creates setup script)"""
        try:
            print("Creating Airflow variables setup script...")
            
            # Create a setup DAG for variables
            variables_dag = self._create_variables_setup_dag(variables)
            
            # Upload to blob storage
            container_client = self.blob_client.get_container_client(self.container_name)
            blob_name = f"{self.config.dag_prefix}/setup_skydag_variables.py"
            
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(variables_dag, overwrite=True)
            
            print("✅ Variables setup DAG uploaded")
            print("⚠️  Please trigger 'setup_skydag_variables' DAG in Airflow UI to set variables")
            return True
            
        except Exception as e:
            print(f"❌ Variable setup failed: {e}")
            return False
    
    def _create_variables_setup_dag(self, variables: Dict[str, str]) -> str:
        """Create a temporary DAG to set Airflow Variables"""
        variables_json = json.dumps(variables, indent=4)
        
        dag_template = f'''"""
Temporary DAG to set up SkyDAG Variables
Delete this DAG after first successful run
"""
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

default_args = {{
    'owner': 'skydag-setup',
    'start_date': datetime(2024, 1, 1),
}}

dag = DAG(
    'setup_skydag_variables',
    default_args=default_args,
    description='One-time setup of SkyDAG Variables',
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'skydag'],
)

@task
def set_variables():
    """Set SkyDAG Variables"""
    variables = {variables_json}
    
    for key, value in variables.items():
        Variable.set(key, value)
        print(f"Set Variable: {{key}} = {{value[:50]}}{'...' if len(value) > 50 else ''}")
    
    print("✅ All SkyDAG Variables set successfully")
    print("⚠️  You can now delete this setup DAG and trigger skydag_pipeline")
    
    return "Variables set successfully"

with dag:
    set_variables()
'''
        return dag_template
    
    def trigger_dag(self, dag_id: str = "skydag_pipeline") -> bool:
        """Trigger DAG execution via Azure Data Factory"""
        try:
            print(f"Triggering pipeline in Azure Data Factory...")
            
            # Trigger the ADF pipeline that calls Airflow
            run_response = self.df_client.pipeline_runs.create_run(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                pipeline_name="SkyDAG_Pipeline"
            )
            
            run_id = run_response.run_id
            print(f"✅ Pipeline triggered with run ID: {run_id}")
            return True
            
        except Exception as e:
            print(f"⚠️  Direct triggering failed: {e}")
            print(f"   Please trigger DAG manually in Airflow UI: {dag_id}")
            return False
    
    def deploy_full(self, local_dags_path: Path) -> bool:
        """Complete deployment: upload DAGs and configure Azure integration"""
        print(f"🚀 Starting deployment to Azure Data Factory + Airflow")
        print(f"   Data Factory: {self.data_factory_name}")
        print(f"   Resource Group: {self.resource_group}")
        print(f"   Storage Account: {self.storage_account}")
        print(f"   Container: {self.container_name}")
        
        # Upload DAG files
        if not self.upload_dags(local_dags_path):
            return False
        
        # Set up variables
        variables = self.config.get_airflow_variables()
        if variables:
            if not self.set_airflow_variables(variables):
                return False
        
        # Create ADF pipeline for Airflow integration (optional)
        if variables:
            print("Creating Azure Data Factory integration pipeline...")
            self.create_airflow_pipeline(variables)
        
        print(f"✅ Deployment completed successfully!")
        print(f"📋 Next steps:")
        print(f"   1. Set up Airflow environment with access to uploaded DAGs")
        print(f"   2. Configure Airflow REST API if using ADF integration")
        print(f"   3. Trigger 'setup_skydag_variables' DAG in Airflow UI")
        print(f"   4. Trigger 'skydag_pipeline' DAG")
        
        return True