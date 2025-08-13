"""AWS infrastructure setup and teardown"""
import time
import json
from typing import Dict, List, Optional

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_AWS_SDK = True
except ImportError:
    HAS_AWS_SDK = False

from .state import DeploymentState, ResourceState


class AWSInfrastructure:
    """Manages AWS infrastructure for SkyDAG"""
    
    def __init__(self, config, state: DeploymentState):
        if not HAS_AWS_SDK:
            raise ImportError("AWS SDK not installed. Run: pip install boto3")
        
        self.config = config
        self.state = state
        self.region = config.aws_region
        
        # Setup AWS session
        session_kwargs = {"region_name": self.region}
        if hasattr(config, 'aws_access_key_id') and config.aws_access_key_id:
            session_kwargs.update({
                "aws_access_key_id": config.aws_access_key_id,
                "aws_secret_access_key": config.aws_secret_access_key
            })
        
        try:
            self.session = boto3.Session(**session_kwargs)
            self.s3_client = self.session.client('s3')
            self.mwaa_client = self.session.client('mwaa')
            self.iam_client = self.session.client('iam')
            self.ec2_client = self.session.client('ec2')
        except NoCredentialsError:
            raise ValueError("AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or configure AWS CLI")
    
    def setup_infrastructure(self) -> bool:
        """Create all required AWS infrastructure"""
        try:
            print(f"🔧 Setting up AWS infrastructure in region: {self.region}")
            
            # 1. Create S3 buckets
            if not self._create_s3_buckets():
                return False
            
            # 2. Create IAM role for MWAA
            if not self._create_iam_role():
                return False
            
            # 3. Create VPC and networking (simplified)
            if not self._create_networking():
                return False
            
            # 4. Create MWAA environment
            if not self._create_mwaa_environment():
                return False
            
            print("✅ AWS infrastructure setup completed successfully")
            return True
            
        except Exception as e:
            print(f"❌ AWS infrastructure setup failed: {e}")
            raise
    
    def destroy_infrastructure(self) -> bool:
        """Destroy all AWS infrastructure"""
        try:
            print(f"💥 Destroying AWS infrastructure in region: {self.region}")
            
            # Destroy in reverse order of creation
            success = True
            
            # 1. Delete MWAA environment
            if not self._delete_mwaa_environment():
                success = False
            
            # 2. Delete networking resources
            if not self._delete_networking():
                success = False
            
            # 3. Delete IAM role
            if not self._delete_iam_role():
                success = False
            
            # 4. Delete S3 buckets
            if not self._delete_s3_buckets():
                success = False
            
            if success:
                print("✅ AWS infrastructure destroyed successfully")
            else:
                print("⚠️  Some AWS resources may not have been fully destroyed")
            
            return success
            
        except Exception as e:
            print(f"❌ AWS infrastructure destruction failed: {e}")
            raise
    
    def _create_s3_buckets(self) -> bool:
        """Create required S3 buckets"""
        buckets = [
            {
                "name": self.config.dag_bucket_or_container,
                "type": "dag-bucket",
                "description": "DAG files storage"
            }
        ]
        
        # Add source/dest buckets if they don't exist
        if hasattr(self.config, 'get_airflow_variables'):
            variables = self.config.get_airflow_variables()
            source = variables.get('SKYDAG_SOURCE', '').split('/')[0]
            dest = variables.get('SKYDAG_DEST', '').split('/')[0]
            
            if source and source != self.config.dag_bucket_or_container:
                buckets.append({
                    "name": source,
                    "type": "source-bucket", 
                    "description": "Source files storage"
                })
            
            if dest and dest != self.config.dag_bucket_or_container and dest != source:
                buckets.append({
                    "name": dest,
                    "type": "dest-bucket",
                    "description": "Processed files storage"
                })
        
        for bucket_info in buckets:
            bucket_name = bucket_info["name"]
            
            try:
                self.state.add_resource("s3-bucket", bucket_name, bucket_info)
                
                # Check if bucket exists
                try:
                    self.s3_client.head_bucket(Bucket=bucket_name)
                    print(f"📦 Bucket {bucket_name} already exists")
                    self.state.mark_resource_created("s3-bucket", bucket_name, {
                        "exists": True,
                        "region": self.region
                    })
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] != '404':
                        raise
                
                # Create bucket
                print(f"📦 Creating S3 bucket: {bucket_name}")
                
                create_args = {'Bucket': bucket_name}
                if self.region != 'us-east-1':
                    create_args['CreateBucketConfiguration'] = {
                        'LocationConstraint': self.region
                    }
                
                self.s3_client.create_bucket(**create_args)
                
                # Set bucket tags
                self.s3_client.put_bucket_tagging(
                    Bucket=bucket_name,
                    Tagging={
                        'TagSet': [
                            {'Key': 'SkyDAG-Deployment', 'Value': self.state.state["deployment_id"]},
                            {'Key': 'SkyDAG-Type', 'Value': bucket_info["type"]},
                            {'Key': 'Created-By', 'Value': 'skydag'}
                        ]
                    }
                )
                
                # Enable versioning
                self.s3_client.put_bucket_versioning(
                    Bucket=bucket_name,
                    VersioningConfiguration={'Status': 'Enabled'}
                )
                
                print(f"✅ Created S3 bucket: {bucket_name}")
                self.state.mark_resource_created("s3-bucket", bucket_name, {
                    "exists": False,
                    "region": self.region,
                    "url": f"s3://{bucket_name}"
                })
                
            except Exception as e:
                error_msg = f"Failed to create S3 bucket {bucket_name}: {e}"
                print(f"❌ {error_msg}")
                self.state.mark_resource_failed("s3-bucket", bucket_name, error_msg)
                return False
        
        return True
    
    def _create_iam_role(self) -> bool:
        """Create IAM role for MWAA"""
        role_name = f"skydag-mwaa-role-{self.state.state['deployment_id']}"
        
        try:
            self.state.add_resource("iam-role", role_name, {
                "name": role_name,
                "type": "mwaa-execution-role"
            })
            
            # Check if role exists
            try:
                self.iam_client.get_role(RoleName=role_name)
                print(f"🔐 IAM role {role_name} already exists")
                self.state.mark_resource_created("iam-role", role_name, {"exists": True})
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchEntity':
                    raise
            
            print(f"🔐 Creating IAM role: {role_name}")
            
            # Trust policy for MWAA
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            # Create role
            self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=f"SkyDAG MWAA execution role for deployment {self.state.state['deployment_id']}",
                Tags=[
                    {'Key': 'SkyDAG-Deployment', 'Value': self.state.state["deployment_id"]},
                    {'Key': 'Created-By', 'Value': 'skydag'}
                ]
            )
            
            # Attach AWS managed policies
            managed_policies = [
                "arn:aws:iam::aws:policy/service-role/AmazonMWAAServiceRolePolicy"
            ]
            
            for policy_arn in managed_policies:
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
            
            print(f"✅ Created IAM role: {role_name}")
            self.state.mark_resource_created("iam-role", role_name, {
                "exists": False,
                "arn": f"arn:aws:iam::{self._get_account_id()}:role/{role_name}"
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to create IAM role {role_name}: {e}"
            print(f"❌ {error_msg}")
            self.state.mark_resource_failed("iam-role", role_name, error_msg)
            return False
    
    def _create_networking(self) -> bool:
        """Create basic networking for MWAA (simplified)"""
        try:
            print("🌐 Creating networking resources...")
            
            # For simplicity, we'll use default VPC and subnets
            # In production, you'd create dedicated VPC/subnets
            self.state.add_resource("networking", "default", {
                "type": "default-vpc",
                "description": "Using default VPC for MWAA"
            })
            
            print("✅ Networking resources configured (using default VPC)")
            self.state.mark_resource_created("networking", "default", {
                "type": "default-vpc"
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to setup networking: {e}"
            print(f"❌ {error_msg}")
            self.state.mark_resource_failed("networking", "default", error_msg)
            return False
    
    def _create_mwaa_environment(self) -> bool:
        """Create MWAA environment"""
        env_name = self.config.mwaa_environment
        
        try:
            self.state.add_resource("mwaa-environment", env_name, {
                "name": env_name,
                "region": self.region
            })
            
            # Check if environment exists
            try:
                env_response = self.mwaa_client.get_environment(Name=env_name)
                print(f"✈️  MWAA environment {env_name} already exists")
                self.state.mark_resource_created("mwaa-environment", env_name, {
                    "exists": True,
                    "status": env_response['Environment']['Status'],
                    "webserver_url": env_response['Environment'].get('WebserverUrl', '')
                })
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    raise
            
            print(f"✈️  Creating MWAA environment: {env_name}")
            print("⏳ This may take 20-30 minutes...")
            
            # Get default VPC info
            vpcs = self.ec2_client.describe_vpcs(Filters=[{'Name': 'isDefault', 'Values': ['true']}])
            if not vpcs['Vpcs']:
                raise Exception("No default VPC found")
            
            vpc_id = vpcs['Vpcs'][0]['VpcId']
            
            # Get subnets
            subnets = self.ec2_client.describe_subnets(
                Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
            )
            subnet_ids = [subnet['SubnetId'] for subnet in subnets['Subnets'][:2]]  # Use first 2 subnets
            
            # Get IAM role ARN
            role_resources = [
                r for r in self.state.state["resources"].values()
                if r["type"] == "iam-role" and r["state"] == ResourceState.CREATED.value
            ]
            if not role_resources:
                raise Exception("IAM role not found")
            
            execution_role_arn = role_resources[0]["data"]["arn"]
            
            # Create environment
            create_response = self.mwaa_client.create_environment(
                Name=env_name,
                ExecutionRoleArn=execution_role_arn,
                SourceBucketArn=f"arn:aws:s3:::{self.config.dag_bucket_or_container}",
                DagS3Path=f"{self.config.dag_prefix}/",
                NetworkConfiguration={
                    'SubnetIds': subnet_ids
                },
                AirflowVersion='2.5.1',
                EnvironmentClass='mw1.small',
                MaxWorkers=2,
                LoggingConfiguration={
                    'DagProcessingLogs': {'Enabled': True, 'LogLevel': 'INFO'},
                    'SchedulerLogs': {'Enabled': True, 'LogLevel': 'INFO'},
                    'TaskLogs': {'Enabled': True, 'LogLevel': 'INFO'},
                    'WebserverLogs': {'Enabled': True, 'LogLevel': 'INFO'},
                    'WorkerLogs': {'Enabled': True, 'LogLevel': 'INFO'}
                },
                Tags={
                    'SkyDAG-Deployment': self.state.state["deployment_id"],
                    'Created-By': 'skydag'
                }
            )
            
            # Wait for environment to be available
            print("⏳ Waiting for MWAA environment to become available...")
            waiter = self.mwaa_client.get_waiter('environment_created')
            waiter.wait(
                Name=env_name,
                WaiterConfig={
                    'Delay': 60,  # Check every minute
                    'MaxAttempts': 30  # Wait up to 30 minutes
                }
            )
            
            # Get final environment details
            env_response = self.mwaa_client.get_environment(Name=env_name)
            
            print(f"✅ Created MWAA environment: {env_name}")
            self.state.mark_resource_created("mwaa-environment", env_name, {
                "exists": False,
                "status": env_response['Environment']['Status'],
                "webserver_url": env_response['Environment'].get('WebserverUrl', ''),
                "arn": env_response['Environment']['Arn']
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to create MWAA environment {env_name}: {e}"
            print(f"❌ {error_msg}")
            self.state.mark_resource_failed("mwaa-environment", env_name, error_msg)
            return False
    
    def _delete_s3_buckets(self) -> bool:
        """Delete S3 buckets"""
        success = True
        
        bucket_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "s3-bucket" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in bucket_resources:
            bucket_name = resource["name"]
            
            try:
                self.state.update_resource("s3-bucket", bucket_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"🗑️  Deleting S3 bucket: {bucket_name}")
                    
                    # Delete all objects and versions
                    self._empty_s3_bucket(bucket_name)
                    
                    # Delete bucket
                    self.s3_client.delete_bucket(Bucket=bucket_name)
                    print(f"✅ Deleted S3 bucket: {bucket_name}")
                else:
                    print(f"📦 Skipping pre-existing bucket: {bucket_name}")
                
                self.state.mark_resource_destroyed("s3-bucket", bucket_name)
                
            except Exception as e:
                print(f"❌ Failed to delete S3 bucket {bucket_name}: {e}")
                success = False
        
        return success
    
    def _delete_mwaa_environment(self) -> bool:
        """Delete MWAA environment"""
        env_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "mwaa-environment" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in env_resources:
            env_name = resource["name"]
            
            try:
                self.state.update_resource("mwaa-environment", env_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"✈️  Deleting MWAA environment: {env_name}")
                    print("⏳ This may take 10-15 minutes...")
                    
                    self.mwaa_client.delete_environment(Name=env_name)
                    
                    # Wait for deletion
                    print("⏳ Waiting for MWAA environment deletion...")
                    # Note: MWAA doesn't have a delete waiter, so we'll poll manually
                    for _ in range(20):  # Wait up to 20 minutes
                        try:
                            self.mwaa_client.get_environment(Name=env_name)
                            time.sleep(60)  # Wait 1 minute
                        except ClientError as e:
                            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                                break
                            raise
                    
                    print(f"✅ Deleted MWAA environment: {env_name}")
                else:
                    print(f"✈️  Skipping pre-existing MWAA environment: {env_name}")
                
                self.state.mark_resource_destroyed("mwaa-environment", env_name)
                return True
                
            except Exception as e:
                print(f"❌ Failed to delete MWAA environment {env_name}: {e}")
                return False
        
        return True
    
    def _delete_iam_role(self) -> bool:
        """Delete IAM role"""
        role_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "iam-role" and r["state"] == ResourceState.CREATED.value
        ]
        
        success = True
        
        for resource in role_resources:
            role_name = resource["name"]
            
            try:
                self.state.update_resource("iam-role", role_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"🔐 Deleting IAM role: {role_name}")
                    
                    # Detach policies
                    policies = self.iam_client.list_attached_role_policies(RoleName=role_name)
                    for policy in policies['AttachedPolicies']:
                        self.iam_client.detach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy['PolicyArn']
                        )
                    
                    # Delete role
                    self.iam_client.delete_role(RoleName=role_name)
                    print(f"✅ Deleted IAM role: {role_name}")
                else:
                    print(f"🔐 Skipping pre-existing IAM role: {role_name}")
                
                self.state.mark_resource_destroyed("iam-role", role_name)
                
            except Exception as e:
                print(f"❌ Failed to delete IAM role {role_name}: {e}")
                success = False
        
        return success
    
    def _delete_networking(self) -> bool:
        """Delete networking resources"""
        # For default VPC, we don't need to delete anything
        net_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "networking" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in net_resources:
            print("🌐 Networking cleanup (default VPC - no action needed)")
            self.state.mark_resource_destroyed("networking", "default")
        
        return True
    
    def _empty_s3_bucket(self, bucket_name: str):
        """Empty all objects from S3 bucket"""
        # Delete all object versions
        paginator = self.s3_client.get_paginator('list_object_versions')
        for page in paginator.paginate(Bucket=bucket_name):
            objects_to_delete = []
            
            # Collect versions
            if 'Versions' in page:
                for version in page['Versions']:
                    objects_to_delete.append({
                        'Key': version['Key'],
                        'VersionId': version['VersionId']
                    })
            
            # Collect delete markers
            if 'DeleteMarkers' in page:
                for marker in page['DeleteMarkers']:
                    objects_to_delete.append({
                        'Key': marker['Key'],
                        'VersionId': marker['VersionId']
                    })
            
            # Delete objects
            if objects_to_delete:
                self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
    
    def _get_account_id(self) -> str:
        """Get AWS account ID"""
        sts = self.session.client('sts')
        return sts.get_caller_identity()['Account']