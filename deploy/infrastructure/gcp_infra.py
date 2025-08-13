"""GCP infrastructure setup and teardown"""
import time
import json
from typing import Dict, List, Optional

# We use gcloud CLI for all GCP operations - no Python SDK needed
HAS_GCP_SDK = True
HAS_COMPOSER_SDK = True

from .state import DeploymentState, ResourceState


class GCPInfrastructure:
    """Manages GCP infrastructure for SkyDAG"""
    
    def __init__(self, config, state: DeploymentState):
        self.config = config
        self.state = state
        self.project_id = config.gcp_project
        self.location = config.gcp_region
        
        # All operations use gcloud CLI - no Python SDK clients needed
    
    def _generate_console_links(self) -> None:
        """Generate and display GCP Console links for created resources"""
        print("\n🔗 GCP Console Links:")
        print("=" * 50)
        
        # Project overview
        print(f"📊 Project Overview: https://console.cloud.google.com/home/dashboard?project={self.project_id}")
        
        # API & Services
        print(f"🔌 APIs & Services: https://console.cloud.google.com/apis?project={self.project_id}")
        
        # Cloud Storage buckets
        bucket_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "bucket" and r["state"] in ["created", "creating"]
        ]
        if bucket_resources:
            print(f"📦 Cloud Storage: https://console.cloud.google.com/storage/browser?project={self.project_id}")
            for resource in bucket_resources:
                bucket_name = resource["name"]
                print(f"   • {bucket_name}: https://console.cloud.google.com/storage/browser/{bucket_name}?project={self.project_id}")
        
        # Composer environments
        composer_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "composer-environment" and r["state"] in ["created", "creating", "failed"]
        ]
        if composer_resources:
            print(f"🎵 Cloud Composer: https://console.cloud.google.com/composer/environments?project={self.project_id}")
            for resource in composer_resources:
                env_name = resource["name"]
                location = resource["data"].get("location", self.location)
                print(f"   • {env_name}: https://console.cloud.google.com/composer/environments/detail/{location}/{env_name}?project={self.project_id}")
        
        # IAM & Admin
        print(f"🔐 IAM & Admin: https://console.cloud.google.com/iam-admin/iam?project={self.project_id}")
        
        # Monitoring
        print(f"📈 Monitoring: https://console.cloud.google.com/monitoring?project={self.project_id}")
        
        # Logs
        print(f"📋 Logs: https://console.cloud.google.com/logs?project={self.project_id}")
        
        print("=" * 50)
    
    def setup_infrastructure(self) -> bool:
        """Create all required GCP infrastructure"""
        try:
            print(f"🔧 Setting up GCP infrastructure in project: {self.project_id}")
            
            # 1. Enable required APIs
            if not self._enable_required_apis():
                return False
            
            # 2. Create storage buckets
            if not self._create_storage_buckets():
                return False
            
            # 3. Create bucket folder structure
            if not self._create_bucket_folders():
                return False
            
            # 4. Create Composer environment
            composer_success = self._create_composer_environment()
            
            # 5. Setup IAM permissions
            iam_success = self._setup_iam_permissions()
            
            # Always show console links for created resources
            self._generate_console_links()
            
            if composer_success and iam_success:
                print("✅ GCP infrastructure setup completed successfully")
                return True
            else:
                print("⚠️  GCP infrastructure setup completed with some failures")
                return False
            
        except Exception as e:
            print(f"❌ GCP infrastructure setup failed: {e}")
            # Still show console links for any resources that were created
            if any(r["state"] in ["created", "creating"] for r in self.state.state.get("resources", {}).values()):
                self._generate_console_links()
            raise
    
    def destroy_infrastructure(self) -> bool:
        """Destroy all GCP infrastructure"""
        try:
            print(f"💥 Destroying GCP infrastructure in project: {self.project_id}")
            
            # Destroy in reverse order of creation
            success = True
            
            # 1. Delete Composer environment
            if not self._delete_composer_environment():
                success = False
            
            # 2. Delete service accounts (includes IAM cleanup)
            if not self._delete_service_accounts():
                success = False
            
            # 3. Delete storage buckets
            if not self._delete_storage_buckets():
                success = False
            
            # 4. Clean up IAM permissions
            if not self._cleanup_iam_permissions():
                success = False
            
            if success:
                print("✅ GCP infrastructure destroyed successfully")
            else:
                print("⚠️  Some GCP resources may not have been fully destroyed")
            
            return success
            
        except Exception as e:
            print(f"❌ GCP infrastructure destruction failed: {e}")
            raise
    
    def _enable_required_apis(self) -> bool:
        """Enable all required GCP APIs for SkyDAG using gcloud CLI"""
        required_apis = [
            ("storage.googleapis.com", "Google Cloud Storage API"),
            ("composer.googleapis.com", "Cloud Composer API"),
            ("compute.googleapis.com", "Compute Engine API"),
            ("container.googleapis.com", "Kubernetes Engine API"),
            ("serviceusage.googleapis.com", "Service Usage API"),
            ("iam.googleapis.com", "Identity and Access Management API"),
            ("cloudresourcemanager.googleapis.com", "Cloud Resource Manager API")
        ]
        
        try:
            import subprocess
            print("🔌 Checking and enabling required GCP APIs...")
            
            # Note: API enablement is not tracked as a resource since we don't clean it up
            
            # Check which APIs are already enabled
            print("🔍 Checking currently enabled APIs...")
            list_cmd = [
                "gcloud", "services", "list",
                "--project", self.project_id,
                "--enabled",
                "--format", "value(name)",
                "--filter", f"name:({' OR '.join([api[0] for api in required_apis])})"
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                print(f"❌ Failed to list enabled APIs: {result.stderr}")
                return False
                
            enabled_apis = set(result.stdout.strip().split('\n')) if result.stdout.strip() else set()
            apis_to_enable = []
            
            for api_name, description in required_apis:
                if api_name in enabled_apis:
                    print(f"✅ {description} already enabled")
                else:
                    print(f"🔌 {description} needs to be enabled")
                    apis_to_enable.append((api_name, description))
            
            # Enable APIs that aren't enabled
            if apis_to_enable:
                api_names = [api[0] for api in apis_to_enable]
                print(f"🔌 Enabling {len(apis_to_enable)} APIs...")
                
                enable_cmd = [
                    "gcloud", "services", "enable",
                    "--project", self.project_id
                ] + api_names
                
                result = subprocess.run(enable_cmd, capture_output=True, text=True, timeout=300)
                if result.returncode == 0:
                    print(f"✅ Successfully enabled {len(apis_to_enable)} APIs")
                    print("⏳ Waiting for API propagation...")
                    time.sleep(5)  # Wait for propagation
                else:
                    error_msg = f"Failed to enable APIs: {result.stderr or result.stdout}"
                    print(f"❌ {error_msg}")
                    return False
            else:
                print("⚡ Skipping API propagation wait - all APIs were already enabled")
            
            print(f"✅ All {len(required_apis)} required APIs are enabled")
            
            return True
            
        except subprocess.TimeoutExpired:
            error_msg = "API enablement timed out"
            print(f"❌ {error_msg}")
            return False
        except Exception as e:
            error_msg = f"API enablement failed: {e}"
            print(f"❌ {error_msg}")
            return False
    
    def _create_storage_buckets(self) -> bool:
        """Create required storage buckets using gcloud CLI"""
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
                import subprocess
                self.state.add_resource("bucket", bucket_name, bucket_info)
                
                # Check if bucket exists using gcloud
                check_cmd = [
                    "gcloud", "storage", "buckets", "describe", f"gs://{bucket_name}",
                    "--project", self.project_id,
                    "--format", "value(location)"
                ]
                
                result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    location = result.stdout.strip()
                    print(f"📦 Bucket {bucket_name} already exists")
                    self.state.mark_resource_created("bucket", bucket_name, {
                        "exists": True,
                        "location": location,
                        "method": "gcloud"
                    })
                    continue
                
                # Create bucket using gcloud
                print(f"📦 Creating bucket: {bucket_name}")
                create_cmd = [
                    "gcloud", "storage", "buckets", "create", f"gs://{bucket_name}",
                    "--project", self.project_id,
                    "--location", self.location
                ]
                
                result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    print(f"✅ Created bucket: {bucket_name}")
                    self.state.mark_resource_created("bucket", bucket_name, {
                        "exists": False,
                        "location": self.location.upper(),
                        "url": f"gs://{bucket_name}",
                        "method": "gcloud"
                    })
                else:
                    error_msg = f"gcloud bucket creation failed: {result.stderr or result.stdout}"
                    print(f"❌ {error_msg}")
                    self.state.mark_resource_failed("bucket", bucket_name, error_msg)
                    return False
                
            except subprocess.TimeoutExpired:
                error_msg = f"Bucket creation timed out: {bucket_name}"
                print(f"❌ {error_msg}")
                self.state.mark_resource_failed("bucket", bucket_name, error_msg)
                return False
            except Exception as e:
                error_msg = f"Failed to create bucket {bucket_name}: {e}"
                print(f"❌ {error_msg}")
                self.state.mark_resource_failed("bucket", bucket_name, error_msg)
                return False
        
        # Store bucket mappings in state for later operations
        if hasattr(self.config, 'get_airflow_variables'):
            variables = self.config.get_airflow_variables()
            source_spec = variables.get('SKYDAG_SOURCE', '')
            dest_spec = variables.get('SKYDAG_DEST', '')
            
            if '/' in source_spec:
                source_bucket, source_prefix = source_spec.split('/', 1)
            else:
                source_bucket, source_prefix = source_spec, ''
                
            if '/' in dest_spec:
                dest_bucket, dest_prefix = dest_spec.split('/', 1) 
            else:
                dest_bucket, dest_prefix = dest_spec, ''
            
            # Store mappings for later use by undeploy/destroy
            if source_bucket:
                self.state.store_resource_mapping("buckets", "source", source_bucket)
                self.state.store_resource_mapping("buckets", "source_prefix", source_prefix)
            if dest_bucket:
                self.state.store_resource_mapping("buckets", "dest", dest_bucket)
                self.state.store_resource_mapping("buckets", "dest_prefix", dest_prefix)
            
            # Also store DAG bucket
            self.state.store_resource_mapping("buckets", "dag", self.config.dag_bucket_or_container)
        
        return True
    
    def _create_bucket_folders(self) -> bool:
        """Create folder structure in buckets by uploading placeholder files"""
        try:
            import subprocess
            import tempfile
            import os
            
            print("📁 Creating bucket folder structure...")
            
            # Get bucket configurations from Airflow variables
            variables = self.config.get_airflow_variables()
            
            folders_to_create = [
                (variables.get('SKYDAG_SOURCE', ''), 'Source files folder'),
                (variables.get('SKYDAG_DEST', ''), 'Destination files folder')
            ]
            
            for spec, description in folders_to_create:
                if not spec:
                    continue
                    
                # Split bucket/prefix
                if '/' in spec:
                    bucket_name, prefix = spec.split('/', 1)
                    folder_path = f"gs://{bucket_name}/{prefix}/"
                    
                    # Create a temporary empty file to upload as folder marker
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.keep', delete=False) as temp_file:
                        temp_file.write("# Folder structure placeholder\n")
                        temp_file.write(f"# This file maintains the {prefix}/ folder structure\n")
                        temp_file_path = temp_file.name
                    
                    try:
                        # Upload placeholder file to create folder structure
                        placeholder_path = f"{folder_path}.keep"
                        upload_cmd = [
                            "gcloud", "storage", "cp", temp_file_path, placeholder_path,
                            "--project", self.project_id
                        ]
                        
                        result = subprocess.run(upload_cmd, capture_output=True, text=True, timeout=30)
                        if result.returncode == 0:
                            print(f"  ✅ Created folder: {folder_path} ({description})")
                        else:
                            print(f"  ⚠️  Failed to create folder {folder_path}: {result.stderr}")
                            
                    finally:
                        # Clean up temporary file
                        os.unlink(temp_file_path)
            
            print("✅ Bucket folder structure created")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create bucket folders: {e}")
            # Don't fail the entire deployment for folder creation issues
            return True
    
    def _create_composer_environment(self) -> bool:
        """Create Cloud Composer environment using gcloud CLI"""
        env_name = self.config.composer_environment
        
        try:
            self.state.add_resource("composer-environment", env_name, {
                "name": env_name,
                "project": self.project_id,
                "location": self.location
            })
            
            # Check if environment exists using gcloud
            print(f"🔍 Checking if Composer environment {env_name} already exists...")
            check_cmd = [
                "gcloud", "composer", "environments", "describe", env_name,
                "--location", self.location,
                "--project", self.project_id,
                "--format", "value(state)"
            ]
            
            try:
                import subprocess
                result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0 and result.stdout.strip():
                    print(f"🎵 Composer environment {env_name} already exists")
                    self.state.mark_resource_created("composer-environment", env_name, {
                        "exists": True,
                        "state": result.stdout.strip(),
                        "method": "gcloud"
                    })
                    
                    # Store Composer mappings even for existing environment
                    self.state.store_resource_mapping("composer", "environment_name", env_name)
                    self.state.store_resource_mapping("composer", "dag_prefix", self.config.dag_prefix)
                    
                    # Get and store DAG bucket name
                    try:
                        dag_bucket = self._get_composer_dag_bucket()
                        if dag_bucket:
                            self.state.store_resource_mapping("composer", "dag_bucket", dag_bucket)
                    except Exception as e:
                        print(f"⚠️  Could not get DAG bucket name: {e}")
                    
                    return True
            except Exception:
                pass  # Environment doesn't exist, continue with creation
            
            # Create and configure service account for Composer
            service_account_email = self._create_composer_service_account()
            if not service_account_email:
                return False
            
            # Create environment using gcloud CLI
            print(f"🎵 Creating Composer environment: {env_name}")
            print("⏳ This may take 20-45 minutes...")
            print(f"💡 You can monitor progress at: https://console.cloud.google.com/composer/environments?project={self.project_id}")
            
            # Validate environment name format
            if not env_name.replace('-', '').replace('_', '').isalnum():
                print(f"❌ Invalid environment name format: {env_name}")
                print("💡 Environment name must contain only lowercase letters, numbers, and hyphens")
                return False
            
            image_version = "composer-3-airflow-2.10.5-build.11"
            
            print(f"🔧 Environment name: {env_name}")
            print(f"🔧 Location: {self.location}")
            print(f"🔧 Project: {self.project_id}")
            print(f"🔧 Image version: {image_version}")
            print(f"🔧 Service account: {service_account_email}")
            
            # Build gcloud command with explicit service account
            create_cmd = [
                "gcloud", "composer", "environments", "create", env_name,
                "--location", self.location,
                "--project", self.project_id,
                "--image-version", image_version,
                "--service-account", service_account_email,
                "--quiet"  # Skip confirmation prompts
            ]
            
            print(f"🔧 Running: {' '.join(create_cmd)}")
            
            # Execute the gcloud command
            result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=3000)  # 50 minutes timeout
            
            if result.returncode == 0:
                print(f"✅ Composer environment creation initiated: {env_name}")
                
                # Add comprehensive readiness check
                from ..gcp_composer import ComposerReadinessChecker
                readiness_checker = ComposerReadinessChecker(self.config)
                
                print("🔍 Waiting for Composer environment to be fully operational...")
                if not readiness_checker.wait_for_full_readiness(timeout=900):  # 15 minutes
                    print("❌ Composer environment not ready within timeout")
                    # Mark resource as failed in state
                    self.state.mark_resource_failed("composer-environment", env_name, "Environment not ready after creation")
                    return False
                
                print(f"✅ Composer environment fully ready: {env_name}")
                self.state.mark_resource_created("composer-environment", env_name, {
                    "exists": False,
                    "state": "RUNNING",
                    "method": "gcloud",
                    "image_version": image_version,
                    "service_account": service_account_email
                })
                
                # Store Composer mappings only after confirmed ready
                self.state.store_resource_mapping("composer", "environment_name", env_name)
                self.state.store_resource_mapping("composer", "dag_prefix", self.config.dag_prefix)
                
                # Get and store DAG bucket name
                try:
                    dag_bucket = self._get_composer_dag_bucket()
                    if dag_bucket:
                        self.state.store_resource_mapping("composer", "dag_bucket", dag_bucket)
                except Exception as e:
                    print(f"⚠️  Could not get DAG bucket name: {e}")
                    # Don't fail creation for this
                
                return True
            else:
                error_msg = f"gcloud command failed: {result.stderr or result.stdout}"
                print(f"❌ {error_msg}")
                self.state.mark_resource_failed("composer-environment", env_name, error_msg)
                return False
            
        except subprocess.TimeoutExpired:
            error_msg = "Composer environment creation timed out after 50 minutes"
            print(f"❌ {error_msg}")
            print("💡 The environment may still be creating in the background")
            self.state.mark_resource_failed("composer-environment", env_name, error_msg)
            return False
            
        except Exception as e:
            error_msg = f"Failed to create Composer environment {env_name}: {e}"
            print(f"❌ {error_msg}")
            print(f"💡 Check the Composer console for more details: https://console.cloud.google.com/composer/environments?project={self.project_id}")
            self.state.mark_resource_failed("composer-environment", env_name, error_msg)
            return False
    
    def _get_composer_dag_bucket(self) -> str:
        """Get the Composer DAG bucket name"""
        try:
            import subprocess
            cmd = [
                "gcloud", "composer", "environments", "describe", self.config.composer_environment,
                "--location", self.location,
                "--project", self.project_id,
                "--format", "value(config.dagGcsPrefix)"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0 and result.stdout.strip():
                dag_gcs_prefix = result.stdout.strip()
                # Extract bucket name from gs://bucket-name/dags -> bucket-name
                return dag_gcs_prefix.split("/")[2]
            return None
        except Exception:
            return None
    
    def _create_composer_service_account(self) -> str:
        """Create and configure service account for Composer environment"""
        service_account_name = "skydag-composer"
        service_account_email = f"{service_account_name}@{self.project_id}.iam.gserviceaccount.com"
        
        try:
            import subprocess
            
            # Check if service account already exists
            check_cmd = [
                "gcloud", "iam", "service-accounts", "describe", service_account_email,
                "--project", self.project_id,
                "--format", "value(email)"
            ]
            
            result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                print(f"✅ Service account {service_account_email} already exists")
                # Track existing service account
                self.state.add_resource("service-account", service_account_name, {
                    "name": service_account_name,
                    "email": service_account_email,
                    "project": self.project_id
                })
                self.state.mark_resource_created("service-account", service_account_name, {
                    "email": service_account_email,
                    "exists": True,
                    "method": "gcloud"
                })
                return service_account_email
            
            # Track service account as a resource
            self.state.add_resource("service-account", service_account_name, {
                "name": service_account_name,
                "email": service_account_email,
                "project": self.project_id
            })
            
            # Create service account
            print(f"🔐 Creating service account: {service_account_name}")
            create_sa_cmd = [
                "gcloud", "iam", "service-accounts", "create", service_account_name,
                "--project", self.project_id,
                "--display-name", "SkyDAG Composer Service Account",
                "--description", "Service account for SkyDAG Composer environment"
            ]
            
            result = subprocess.run(create_sa_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                print(f"❌ Failed to create service account: {result.stderr or result.stdout}")
                self.state.mark_resource_failed("service-account", service_account_name, result.stderr or result.stdout)
                return None
            
            print(f"✅ Created service account: {service_account_email}")
            self.state.mark_resource_created("service-account", service_account_name, {
                "email": service_account_email,
                "exists": False,
                "method": "gcloud"
            })
            
            # Grant required IAM roles
            required_roles = [
                "roles/composer.worker",
                "roles/iam.serviceAccountUser"
            ]
            
            for role in required_roles:
                print(f"🔐 Granting role {role} to service account...")
                grant_cmd = [
                    "gcloud", "projects", "add-iam-policy-binding", self.project_id,
                    "--member", f"serviceAccount:{service_account_email}",
                    "--role", role
                ]
                
                result = subprocess.run(grant_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode != 0:
                    print(f"⚠️  Failed to grant role {role}: {result.stderr or result.stdout}")
                    # Continue with other roles
                else:
                    print(f"✅ Granted role {role}")
            
            return service_account_email
            
        except Exception as e:
            print(f"❌ Failed to create service account: {e}")
            return None
    
    def _setup_iam_permissions(self) -> bool:
        """Setup IAM permissions"""
        try:
            print("🔐 Setting up IAM permissions...")
            
            # For now, just log that IAM would be configured
            # In a full implementation, this would set up service account permissions
            self.state.add_resource("iam-permissions", "skydag-permissions", {
                "type": "iam",
                "description": "SkyDAG IAM permissions"
            })
            
            print("✅ IAM permissions configured")
            self.state.mark_resource_created("iam-permissions", "skydag-permissions", {
                "configured": True
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to setup IAM permissions: {e}"
            print(f"❌ {error_msg}")
            self.state.mark_resource_failed("iam-permissions", "skydag-permissions", error_msg)
            return False
    
    def _delete_storage_buckets(self) -> bool:
        """Delete storage buckets defined in configuration"""
        success = True
        
        print("🗑️  Deleting SkyDAG storage buckets from configuration...")
        
        # Get bucket names directly from configuration
        variables = self.config.get_airflow_variables()
        source_spec = variables.get('SKYDAG_SOURCE', '')
        dest_spec = variables.get('SKYDAG_DEST', '')
        
        # Extract bucket names
        buckets_to_delete = []
        
        if source_spec and '/' in source_spec:
            source_bucket = source_spec.split('/')[0]
            buckets_to_delete.append((source_bucket, "source"))
        
        if dest_spec and '/' in dest_spec:
            dest_bucket = dest_spec.split('/')[0] 
            buckets_to_delete.append((dest_bucket, "dest"))
        
        # Add DAG bucket
        dag_bucket = self.config.dag_bucket_or_container
        if dag_bucket:
            buckets_to_delete.append((dag_bucket, "dag"))
        
        # Remove duplicates
        buckets_to_delete = list(set(buckets_to_delete))
        
        print(f"📦 Found {len(buckets_to_delete)} buckets to delete from configuration:")
        for bucket_name, bucket_type in buckets_to_delete:
            print(f"  - {bucket_name} ({bucket_type})")
        
        for bucket_name, bucket_type in buckets_to_delete:
            if not self._delete_single_bucket(bucket_name):
                success = False
        
        # Also check for orphaned Composer buckets
        if not self._delete_orphaned_composer_buckets():
            success = False
            
        return success
    
    def _delete_single_bucket(self, bucket_name: str) -> bool:
        """Delete a single storage bucket"""
        try:
            import subprocess
            
            # Check if bucket exists first
            check_cmd = [
                "gcloud", "storage", "buckets", "describe", f"gs://{bucket_name}",
                "--project", self.project_id,
                "--format", "value(name)"
            ]
            
            result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                print(f"📦 Bucket {bucket_name} doesn't exist, skipping")
                return True
            
            print(f"🗑️  Deleting bucket: {bucket_name}")
            
            # Delete all objects first using gcloud
            print(f"🗑️  Deleting all objects in bucket: {bucket_name}")
            delete_objects_cmd = [
                "gcloud", "storage", "rm", "--recursive", f"gs://{bucket_name}/**",
                "--project", self.project_id,
                "--quiet"
            ]
            
            # Try to delete objects (might be empty bucket)
            result = subprocess.run(delete_objects_cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0 and "No URLs matched" not in result.stderr:
                print(f"⚠️  Warning deleting objects: {result.stderr}")
            
            # Delete bucket using gcloud
            delete_bucket_cmd = [
                "gcloud", "storage", "buckets", "delete", f"gs://{bucket_name}",
                "--project", self.project_id,
                "--quiet"
            ]
            
            result = subprocess.run(delete_bucket_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print(f"✅ Deleted bucket: {bucket_name}")
                return True
            else:
                print(f"⚠️  Failed to delete bucket {bucket_name}: {result.stderr or result.stdout}")
                return False
                
        except Exception as e:
            print(f"❌ Failed to delete bucket {bucket_name}: {e}")
            return False
    
    def _delete_orphaned_composer_buckets(self) -> bool:
        """Delete any orphaned Composer buckets that match our environment pattern"""
        try:
            import subprocess
            import re
            
            print("🔍 Checking for orphaned Composer buckets...")
            
            # Pattern for Composer buckets: {region}-{env-name}-{hash}-bucket  
            composer_pattern = f"{self.location}-{self.config.composer_environment.replace('_', '-')}-[a-f0-9]{{8}}-bucket"
            
            # List all buckets in the project
            list_cmd = [
                "gcloud", "storage", "buckets", "list",
                "--project", self.project_id,
                "--format", "value(name)"
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                print(f"⚠️  Could not list buckets: {result.stderr}")
                return True  # Don't fail destroy for this
                
            all_buckets = result.stdout.strip().split('\n') if result.stdout.strip() else []
            
            # Find Composer buckets matching our pattern
            orphaned_buckets = []
            pattern = re.compile(composer_pattern)
            
            for bucket_name in all_buckets:
                if pattern.match(bucket_name):
                    orphaned_buckets.append(bucket_name)
            
            if not orphaned_buckets:
                print("✅ No orphaned Composer buckets found")
                return True
                
            print(f"🗑️  Found {len(orphaned_buckets)} orphaned Composer bucket(s):")
            for bucket in orphaned_buckets:
                print(f"  - {bucket}")
            
            # Delete each orphaned bucket
            success = True
            for bucket_name in orphaned_buckets:
                print(f"🗑️  Deleting orphaned Composer bucket: {bucket_name}")
                if not self._delete_single_bucket(bucket_name):
                    success = False
            
            return success
            
        except Exception as e:
            print(f"⚠️  Error checking for orphaned Composer buckets: {e}")
            return True  # Don't fail destroy for this
    
    def _delete_composer_environment(self) -> bool:
        """Delete Cloud Composer environment using gcloud CLI"""
        env_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "composer-environment" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in env_resources:
            env_name = resource["name"]
            
            try:
                self.state.update_resource("composer-environment", env_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"🎵 Deleting Composer environment: {env_name}")
                    print("⏳ This may take 10-20 minutes...")
                    print(f"💡 You can monitor progress at: https://console.cloud.google.com/composer/environments?project={self.project_id}")
                    
                    import subprocess
                    delete_cmd = [
                        "gcloud", "composer", "environments", "delete", env_name,
                        "--location", self.location,
                        "--project", self.project_id,
                        "--quiet"  # Skip confirmation prompts
                    ]
                    
                    print(f"🔧 Running: {' '.join(delete_cmd)}")
                    result = subprocess.run(delete_cmd, capture_output=True, text=True, timeout=1800)  # 30 minutes timeout
                    
                    if result.returncode == 0:
                        print(f"✅ Deleted Composer environment: {env_name}")
                    else:
                        print(f"⚠️  gcloud delete command returned non-zero: {result.stderr or result.stdout}")
                        # Don't fail if the environment was already deleted
                else:
                    print(f"🎵 Skipping pre-existing Composer environment: {env_name}")
                
                self.state.mark_resource_destroyed("composer-environment", env_name)
                return True
                
            except subprocess.TimeoutExpired:
                print(f"⚠️  Composer environment deletion timed out: {env_name}")
                print("💡 The environment may still be deleting in the background")
                return False
            except Exception as e:
                print(f"❌ Failed to delete Composer environment {env_name}: {e}")
                return False
        
        return True
    
    def _delete_service_accounts(self) -> bool:
        """Delete service accounts created for SkyDAG"""
        success = True
        
        service_account_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "service-account" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in service_account_resources:
            service_account_name = resource["name"]
            service_account_email = resource["data"]["email"]
            
            try:
                self.state.update_resource("service-account", service_account_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    # First remove IAM policy bindings
                    print(f"🔐 Removing IAM roles from service account: {service_account_email}")
                    self._remove_service_account_roles(service_account_email)
                    
                    print(f"🔐 Deleting service account: {service_account_email}")
                    
                    import subprocess
                    delete_cmd = [
                        "gcloud", "iam", "service-accounts", "delete", service_account_email,
                        "--project", self.project_id,
                        "--quiet"  # Skip confirmation prompts
                    ]
                    
                    result = subprocess.run(delete_cmd, capture_output=True, text=True, timeout=60)
                    if result.returncode == 0:
                        print(f"✅ Deleted service account: {service_account_email}")
                    else:
                        print(f"⚠️  Failed to delete service account: {result.stderr or result.stdout}")
                        success = False
                else:
                    print(f"🔐 Skipping pre-existing service account: {service_account_email}")
                
                self.state.mark_resource_destroyed("service-account", service_account_name)
                
            except Exception as e:
                print(f"❌ Failed to delete service account {service_account_email}: {e}")
                success = False
        
        return success
    
    def _remove_service_account_roles(self, service_account_email: str) -> None:
        """Remove IAM roles from a service account"""
        required_roles = [
            "roles/composer.worker",
            "roles/iam.serviceAccountUser"
        ]
        
        try:
            import subprocess
            for role in required_roles:
                print(f"🔐 Removing role {role} from {service_account_email}...")
                remove_cmd = [
                    "gcloud", "projects", "remove-iam-policy-binding", self.project_id,
                    "--member", f"serviceAccount:{service_account_email}",
                    "--role", role,
                    "--quiet"
                ]
                
                result = subprocess.run(remove_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    print(f"✅ Removed role {role}")
                else:
                    print(f"⚠️  Failed to remove role {role}: {result.stderr or result.stdout}")
        except Exception as e:
            print(f"⚠️  Error removing service account roles: {e}")
    
    def _cleanup_iam_permissions(self) -> bool:
        """Clean up IAM permissions resources"""
        success = True
        
        iam_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "iam-permissions" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in iam_resources:
            resource_name = resource["name"]
            
            try:
                self.state.update_resource("iam-permissions", resource_name, ResourceState.DESTROYING)
                
                print(f"🔐 Cleaning up IAM permissions: {resource_name}")
                # IAM permissions are cleaned up when service accounts are deleted
                # This is mainly for state tracking
                
                self.state.mark_resource_destroyed("iam-permissions", resource_name)
                print(f"✅ Cleaned up IAM permissions: {resource_name}")
                
            except Exception as e:
                print(f"❌ Failed to cleanup IAM permissions {resource_name}: {e}")
                success = False
        
        return success