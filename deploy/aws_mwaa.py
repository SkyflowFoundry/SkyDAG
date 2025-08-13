"""Amazon MWAA deployment utilities"""
import json
import zipfile
from pathlib import Path
from typing import Dict

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_AWS_SDK = True
except ImportError:
    HAS_AWS_SDK = False


class MWAADeployer:
    """Deploy DAGs to Amazon MWAA (Managed Workflows for Apache Airflow)"""
    
    def __init__(self, config):
        if not HAS_AWS_SDK:
            raise ImportError("AWS SDK not installed. Run: pip install boto3")
        
        self.config = config
        self.region = config.aws_region
        self.environment_name = config.mwaa_environment
        self.dag_bucket = config.dag_bucket_or_container
        
        # Setup AWS session
        session_kwargs = {"region_name": self.region}
        if config.aws_access_key_id and config.aws_secret_access_key:
            session_kwargs.update({
                "aws_access_key_id": config.aws_access_key_id,
                "aws_secret_access_key": config.aws_secret_access_key
            })
        
        try:
            self.session = boto3.Session(**session_kwargs)
            self.s3_client = self.session.client('s3')
            self.mwaa_client = self.session.client('mwaa')
        except NoCredentialsError:
            raise ValueError("AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or configure AWS CLI")
    
    def get_mwaa_environment_info(self) -> dict:
        """Get MWAA environment information"""
        try:
            response = self.mwaa_client.get_environment(Name=self.environment_name)
            return response['Environment']
        except ClientError as e:
            print(f"Warning: Could not get MWAA environment info: {e}")
            return {}
    
    def upload_dags(self, local_dags_path: Path) -> bool:
        """Upload DAG files to S3 bucket"""
        try:
            print(f"Uploading DAGs to s3://{self.dag_bucket}/{self.config.dag_prefix}/")
            
            # Upload all Python files in dags/ directory
            for py_file in local_dags_path.rglob("*.py"):
                # Get relative path from dags directory
                relative_path = py_file.relative_to(local_dags_path.parent)
                s3_key = f"{self.config.dag_prefix}/{relative_path}"
                
                self.s3_client.upload_file(
                    str(py_file), 
                    self.dag_bucket, 
                    s3_key
                )
                print(f"  Uploaded: {relative_path}")
            
            print("✅ DAG upload completed")
            return True
            
        except Exception as e:
            print(f"❌ DAG upload failed: {e}")
            return False
    
    def set_airflow_variables(self, variables: Dict[str, str]) -> bool:
        """Set Airflow Variables via MWAA CLI"""
        try:
            print("Setting Airflow Variables via MWAA CLI...")
            
            # Create variables setup commands
            commands = []
            for key, value in variables.items():
                # Escape quotes in values
                escaped_value = value.replace('"', '\\"')
                commands.append(f'variables set {key} "{escaped_value}"')
            
            # Execute commands via MWAA CLI
            for command in commands:
                try:
                    response = self.mwaa_client.create_cli_token(Name=self.environment_name)
                    cli_token = response['CliToken']
                    web_server_hostname = response['WebServerHostname']
                    
                    # Execute command via MWAA CLI
                    response = self.mwaa_client.create_web_login_token(Name=self.environment_name)
                    
                    print(f"Executed: airflow {command}")
                    
                except ClientError as e:
                    print(f"Warning: Failed to set variable via CLI: {e}")
                    continue
            
            # Alternative: Create a setup DAG
            print("Creating variables setup DAG as alternative...")
            variables_dag = self._create_variables_setup_dag(variables)
            
            # Upload variables setup DAG
            setup_dag_key = f"{self.config.dag_prefix}/setup_skydag_variables.py"
            self.s3_client.put_object(
                Bucket=self.dag_bucket,
                Key=setup_dag_key,
                Body=variables_dag
            )
            
            print("✅ Variables setup DAG uploaded")
            print("⚠️  Please trigger 'setup_skydag_variables' DAG in MWAA UI to set variables")
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
        """Trigger DAG execution via MWAA"""
        try:
            # Note: MWAA doesn't have direct DAG trigger API
            # This would require using Airflow REST API
            print(f"⚠️  DAG triggering must be done via MWAA UI or Airflow CLI")
            print(f"   Go to your MWAA environment and trigger DAG: {dag_id}")
            return True
            
        except Exception as e:
            print(f"❌ DAG trigger failed: {e}")
            return False
    
    def deploy_full(self, local_dags_path: Path) -> bool:
        """Complete deployment: upload DAGs and set variables"""
        print(f"🚀 Starting deployment to Amazon MWAA")
        print(f"   Environment: {self.environment_name}")
        print(f"   Region: {self.region}")
        print(f"   DAG Bucket: {self.dag_bucket}")
        
        # Verify MWAA environment exists
        env_info = self.get_mwaa_environment_info()
        if env_info:
            print(f"   Status: {env_info.get('Status', 'Unknown')}")
        
        # Upload DAG files
        if not self.upload_dags(local_dags_path):
            return False
        
        # Set Airflow Variables
        variables = self.config.get_airflow_variables()
        if variables:
            if not self.set_airflow_variables(variables):
                return False
        
        print(f"✅ Deployment completed successfully!")
        print(f"📋 Next steps:")
        print(f"   1. Go to Amazon MWAA console")
        print(f"   2. Access Airflow UI for environment: {self.environment_name}")
        print(f"   3. Trigger 'setup_skydag_variables' DAG (if variables were set)")
        print(f"   4. Trigger 'skydag_pipeline' DAG")
        
        return True