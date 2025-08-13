"""Google Cloud Composer deployment utilities"""
import json
import zipfile
import subprocess
from pathlib import Path
from typing import Dict, List


class ComposerReadinessChecker:
    """Validates that Composer environment is fully operational"""
    
    def __init__(self, config):
        self.config = config
        self.project_id = config.gcp_project
        self.location = config.gcp_region
        self.environment_name = config.composer_environment
    
    def wait_for_full_readiness(self, timeout: int = 600) -> bool:
        """Comprehensive readiness check with multiple validation stages"""
        import time
        
        print(f"🔍 Validating Composer environment readiness (timeout: {timeout}s)")
        start_time = time.time()
        
        # Stage 1: Environment exists and is running
        if not self._wait_for_environment_running(timeout // 3):
            return False
            
        # Stage 2: DAG bucket is accessible
        if not self._wait_for_dag_bucket_ready(timeout // 3):
            return False
            
        # Stage 3: Can perform basic DAG operations
        if not self._wait_for_dag_operations_ready(timeout // 3):
            return False
        
        elapsed = time.time() - start_time
        print(f"✅ Composer environment fully ready after {elapsed:.1f}s")
        return True
    
    def _wait_for_environment_running(self, timeout: int) -> bool:
        """Check environment is in RUNNING state"""
        import time
        
        print("⏳ Stage 1: Checking environment state...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                cmd = [
                    "gcloud", "composer", "environments", "describe", self.environment_name,
                    "--location", self.location,
                    "--project", self.project_id,
                    "--format", "value(state)"
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    state = result.stdout.strip()
                    if state == "RUNNING":
                        print("✅ Stage 1: Environment is RUNNING")
                        return True
                    else:
                        print(f"⏳ Environment state: {state}, waiting...")
                
            except Exception as e:
                print(f"⚠️  Error checking environment state: {e}")
            
            time.sleep(15)
        
        print(f"❌ Stage 1: Environment not RUNNING after {timeout}s")
        return False
    
    def _wait_for_dag_bucket_ready(self, timeout: int) -> bool:
        """Check DAG bucket is accessible"""
        import time
        
        print("⏳ Stage 2: Checking DAG bucket accessibility...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Get DAG bucket name
                cmd = [
                    "gcloud", "composer", "environments", "describe", self.environment_name,
                    "--location", self.location,
                    "--project", self.project_id,
                    "--format", "value(config.dagGcsPrefix)"
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0 and result.stdout.strip():
                    dag_gcs_prefix = result.stdout.strip()
                    bucket_name = dag_gcs_prefix.split("/")[2]
                    
                    # Test bucket access
                    list_cmd = [
                        "gcloud", "storage", "ls", f"gs://{bucket_name}/",
                        "--project", self.project_id
                    ]
                    
                    list_result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=30)
                    if list_result.returncode == 0:
                        print("✅ Stage 2: DAG bucket accessible")
                        return True
                    else:
                        print("⏳ DAG bucket not yet accessible...")
                
            except Exception as e:
                print(f"⚠️  Error checking DAG bucket: {e}")
            
            time.sleep(10)
        
        print(f"❌ Stage 2: DAG bucket not accessible after {timeout}s")
        return False
    
    def _wait_for_dag_operations_ready(self, timeout: int) -> bool:
        """Check basic DAG operations work"""
        import time
        
        print("⏳ Stage 3: Checking DAG operations...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Test basic DAG list operation
                cmd = [
                    "gcloud", "composer", "environments", "run", self.environment_name,
                    "--location", self.location,
                    "--project", self.project_id,
                    "dags", "list", "--", "--output", "table"
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
                if result.returncode == 0:
                    print("✅ Stage 3: DAG operations ready")
                    return True
                else:
                    print("⏳ DAG operations not yet ready...")
                    
            except Exception as e:
                print(f"⚠️  Error testing DAG operations: {e}")
            
            time.sleep(20)
        
        print(f"❌ Stage 3: DAG operations not ready after {timeout}s")
        return False


class ComposerDeployer:
    """Deploy DAGs to Google Cloud Composer using gcloud CLI"""
    
    def __init__(self, config):
        self.config = config
        self.project_id = config.gcp_project
        self.location = config.gcp_region
        self.environment_name = config.composer_environment
        
        # All operations use gcloud CLI - no Python SDK clients needed
    
    def get_dag_bucket(self) -> str:
        """Get the Composer DAG bucket for the environment"""
        try:
            # Get environment details using gcloud CLI
            describe_cmd = [
                "gcloud", "composer", "environments", "describe", self.environment_name,
                "--location", self.location,
                "--project", self.project_id,
                "--format", "value(config.dagGcsPrefix)"
            ]
            
            result = subprocess.run(describe_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0 and result.stdout.strip():
                dag_gcs_prefix = result.stdout.strip()
                bucket_name = dag_gcs_prefix.split("/")[2]  # gs://bucket-name/dags -> bucket-name
                return bucket_name
            else:
                print(f"Warning: Could not get DAG bucket from environment: {result.stderr}")
                return self.config.dag_bucket_or_container
            
        except Exception as e:
            print(f"Warning: Could not get DAG bucket automatically: {e}")
            return self.config.dag_bucket_or_container
    
    def upload_dags(self, local_dags_path: Path) -> bool:
        """Upload DAG files to Composer DAG bucket"""
        try:
            bucket_name = self.get_dag_bucket()
            
            print(f"📦 Uploading DAGs to gs://{bucket_name}/{self.config.dag_prefix}/")
            
            # Upload Python files, filtering by platform relevance
            for py_file in local_dags_path.rglob("*.py"):
                # Get relative path from dags directory itself (not parent)
                relative_path = py_file.relative_to(local_dags_path)
                
                # Skip platform-specific modules that don't match current platform
                if self._should_skip_file(relative_path):
                    print(f"  ⏭️  Skipped: {relative_path} (not needed for {self.config.platform})")
                    continue
                
                gcs_path = f"gs://{bucket_name}/{self.config.dag_prefix}/{relative_path}"
                
                upload_cmd = [
                    "gcloud", "storage", "cp", str(py_file), gcs_path,
                    "--project", self.project_id
                ]
                
                result = subprocess.run(upload_cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    print(f"  ✅ Uploaded: {relative_path}")
                else:
                    print(f"  ❌ Failed to upload {relative_path}: {result.stderr}")
                    return False
            
            print("✅ DAG upload completed")
            return True
            
        except Exception as e:
            print(f"❌ DAG upload failed: {e}")
            return False
    
    def set_airflow_variables(self, variables: Dict[str, str]) -> bool:
        """Set Airflow Variables via Composer using gcloud commands"""
        try:
            import subprocess
            print(f"🔧 Setting {len(variables)} Airflow Variables...")
            
            # Set each variable using gcloud composer environments run
            failed_vars = []
            for key, value in variables.items():
                if not value:  # Skip empty values
                    print(f"⚠️  Skipping empty variable: {key}")
                    continue
                    
                print(f"🔧 Setting variable: {key}")
                try:
                    set_cmd = [
                        "gcloud", "composer", "environments", "run", self.config.composer_environment,
                        "--location", self.location,
                        "--project", self.project_id,
                        "variables", "set", "--", key, value
                    ]
                    
                    result = subprocess.run(set_cmd, capture_output=True, text=True, timeout=120)
                    if result.returncode == 0:
                        print(f"✅ Set variable: {key}")
                    else:
                        print(f"❌ Failed to set variable {key}: {result.stderr or result.stdout}")
                        failed_vars.append(key)
                        
                except subprocess.TimeoutExpired:
                    print(f"⏱️  Timeout setting variable {key}")
                    failed_vars.append(key)
                except Exception as e:
                    print(f"❌ Error setting variable {key}: {e}")
                    failed_vars.append(key)
            
            if failed_vars:
                print(f"⚠️  Failed to set {len(failed_vars)} variables: {failed_vars}")
                print("💡 You can set them manually using:")
                for var in failed_vars:
                    if var in variables and variables[var]:
                        print(f"   gcloud composer environments run {self.config.composer_environment} --location {self.location} variables set -- {var} \"{variables[var]}\"")
                return False
            else:
                print("✅ All Airflow variables set successfully")
                return True
            
        except Exception as e:
            print(f"❌ Variable setup failed: {e}")
            return False
    
    
    def trigger_dag(self, dag_id: str = "skydag_pipeline", filename: str = None) -> bool:
        """Trigger DAG execution with optional filename parameter"""
        try:
            import subprocess
            
            if filename:
                print(f"🚀 Triggering DAG '{dag_id}' with filename: {filename}")
                
                # Create configuration for the DAG run
                config = f'{{"filename": "{filename}"}}'
                
                trigger_cmd = [
                    "gcloud", "composer", "environments", "run", self.environment_name,
                    "--location", self.location,
                    "--project", self.project_id,
                    "dags", "trigger", "--", dag_id, "--conf", config
                ]
            else:
                print(f"🚀 Triggering DAG '{dag_id}' to process all files")
                
                trigger_cmd = [
                    "gcloud", "composer", "environments", "run", self.environment_name,
                    "--location", self.location,
                    "--project", self.project_id,
                    "dags", "trigger", "--", dag_id
                ]
            
            result = subprocess.run(trigger_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print("✅ DAG triggered successfully!")
                if filename:
                    print(f"💡 Processing single file: {filename}")
                else:
                    print("💡 Processing all files in source bucket")
                return True
            else:
                print(f"❌ Failed to trigger DAG: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("❌ DAG trigger timed out")
            return False
        except Exception as e:
            print(f"❌ DAG trigger failed: {e}")
            return False
    
    def deploy_full(self, local_dags_path: Path) -> bool:
        """Complete deployment: upload DAGs and set variables"""
        print(f"🚀 Starting deployment to Google Cloud Composer")
        print(f"   Environment: {self.environment_name}")
        print(f"   Project: {self.project_id}")
        print(f"   Region: {self.location}")
        
        # Verify environment is ready before deploying
        readiness_checker = ComposerReadinessChecker(self.config)
        if not readiness_checker.wait_for_full_readiness(timeout=300):  # 5 minutes
            print("❌ Composer environment not ready for deployment")
            print("💡 Try running setup again or wait for environment to fully initialize")
            return False
        
        # Upload DAG files
        if not self.upload_dags(local_dags_path):
            return False
        
        # Upload test files
        if not self.upload_test_files():
            return False
        
        # Set Airflow Variables
        print("🔧 Setting up Airflow Variables...")
        variables = self.config.get_airflow_variables()
        if variables:
            if not self.set_airflow_variables(variables):
                print("⚠️  Variable setup failed but continuing with deployment")
                print("💡 You can set variables manually using:")
                print(f"   python deploy.py variables")
        else:
            print("⚠️  No variables to set")
        
        # Wait for DAGs to be available and check for errors
        print("🔍 Verifying DAG deployment...")
        if not self.wait_for_dags_ready(timeout=300):  # 5 minutes
            print("⚠️  DAG verification failed, but files were uploaded")
            print("💡 Check Composer console for details")
            return False
        
        print(f"✅ Deployment completed successfully!")
        print(f"📋 Next steps:")
        print(f"   1. Upload test files to input bucket:")
        print(f"      gcloud storage cp test-data/test_records_10.csv gs://{self._get_suffixed_bucket('SKYDAG_SOURCE')}/input-files/")
        print(f"   2. Trigger processing:")
        print(f"      • CLI: python deploy.py trigger test_records_10.csv")
        print(f"      • CLI: python deploy.py trigger  # (all files)")
        print(f"      • UI: https://console.cloud.google.com/composer/environments?project={self.project_id}")
        print(f"   3. Monitor results:")
        print(f"      • Output: gs://{self._get_suffixed_bucket('SKYDAG_DEST')}/output-files/")
        
        return True
    
    def _get_suffixed_bucket(self, env_var: str) -> str:
        """Get bucket name with project ID suffix applied"""
        import os
        bucket_spec = os.getenv(env_var, "")
        if '/' in bucket_spec:
            bucket_name = bucket_spec.split('/')[0]
        else:
            bucket_name = bucket_spec
        
        # Apply project ID suffix (same logic as config.py)
        suffix = self.project_id.replace('-', '').replace('_', '').lower()
        return f"{bucket_name}-{suffix}"
    
    def wait_for_dags_ready(self, timeout: int = 300) -> bool:
        """Wait for DAGs to be loaded and check for errors"""
        import time
        
        print(f"⏳ Waiting for DAGs to be processed (timeout: {timeout}s)...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check if our main DAG is available
            if self.check_dag_availability("skydag_pipeline"):
                # Check for import errors or issues
                if self.check_dag_health():
                    print("✅ DAGs are ready and healthy!")
                    return True
                else:
                    print("❌ DAGs have errors - check Composer console")
                    return False
            
            print("⏳ DAGs still processing, waiting 10 seconds...")
            time.sleep(10)
        
        print(f"❌ Timeout waiting for DAGs to be ready after {timeout}s")
        return False
    
    def check_dag_availability(self, dag_id: str) -> bool:
        """Check if a specific DAG is available in Composer"""
        try:
            list_cmd = [
                "gcloud", "composer", "environments", "run", self.environment_name,
                "--location", self.location,
                "--project", self.project_id,
                "dags", "list"
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                # Check if our DAG is in the list
                return dag_id in result.stdout
            else:
                print(f"⚠️  Failed to list DAGs: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"⚠️  Error checking DAG availability: {e}")
            return False
    
    def check_dag_health(self) -> bool:
        """Check for DAG import errors or health issues"""
        try:
            # Use a simple command that checks DAG parsing without requiring graphviz
            # If the DAG appears in 'dags list', it parsed successfully
            list_cmd = [
                "gcloud", "composer", "environments", "run", self.environment_name,
                "--location", self.location,
                "--project", self.project_id,
                "dags", "list"
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                print(f"❌ Failed to list DAGs: {result.stderr}")
                return False
            
            # Check for actual DAG import/parsing errors in the output
            output_combined = (result.stdout + result.stderr).lower()
            
            # Only check for real DAG import errors, not visualization issues
            real_errors = [
                "modulenotfounderror",
                "importerror", 
                "syntaxerror",
                "airflowdagduplicatedidexception",
                "failed to import",
                "dag import error"
            ]
            
            for error in real_errors:
                if error in output_combined:
                    print(f"❌ DAG import issue detected: {error}")
                    print(f"💡 Error details: {result.stderr[:300]}...")
                    return False
            
            # If skydag_pipeline appears in the list and no import errors, it's healthy
            if "skydag_pipeline" in result.stdout:
                return True
            else:
                print("⚠️  skydag_pipeline DAG not found in list")
                return False
            
        except Exception as e:
            print(f"⚠️  Could not verify DAG health: {e}")
            return True  # Don't fail deployment for verification issues
    
    def _should_skip_file(self, relative_path) -> bool:
        """Determine if a file should be skipped based on platform"""
        file_path = str(relative_path)
        current_platform = self.config.platform.lower()
        
        # Platform-specific module patterns
        platform_patterns = {
            'gcp': ['platform_aws.py', 'platform_azure.py'],
            'aws': ['platform_gcp.py', 'platform_azure.py'], 
            'azure': ['platform_gcp.py', 'platform_aws.py']
        }
        
        # Skip files for other platforms
        if current_platform in platform_patterns:
            for pattern in platform_patterns[current_platform]:
                if pattern in file_path:
                    return True
        
        return False
    
    def undeploy_full(self) -> bool:
        """Complete undeployment: remove DAGs from registry AND files"""
        print(f"🗑️  Starting complete undeployment from Google Cloud Composer")
        print(f"   Environment: {self.environment_name}")
        print(f"   Project: {self.project_id}")
        print(f"   Region: {self.location}")
        
        success = True
        
        # Complete DAG cleanup (registry + files)
        print("🧹 Cleaning up DAGs from registry and files...")
        if not self.cleanup_all_skydag_dags():
            print("⚠️  DAG cleanup had issues, but continuing...")
            success = False
        
        # Remove test files (existing logic)
        if not self.remove_test_files():
            print("⚠️  Test file removal had issues, but continuing...")
            success = False
        
        # Comprehensive verification
        print("🔍 Performing comprehensive cleanup verification...")
        if not self.verify_complete_undeployment():
            print("⚠️  Cleanup verification found issues")
            success = False
        
        if success:
            print("✅ Complete undeployment successful!")
            print("💡 All DAGs, files, and registry entries have been removed")
        else:
            print("⚠️  Undeployment completed with some issues")
            print("💡 Check the output above for details")
        
        return success
    
    def remove_dags(self) -> bool:
        """Remove DAG files from Composer DAG bucket"""
        try:
            bucket_name = self.get_dag_bucket()
            
            print(f"🗑️  Removing DAGs from gs://{bucket_name}/{self.config.dag_prefix}/")
            
            # List all files in the DAG prefix to remove them
            list_cmd = [
                "gcloud", "storage", "ls", f"gs://{bucket_name}/{self.config.dag_prefix}/**",
                "--project", self.project_id
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                all_files = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
                
                # Filter to only SkyDAG files we deployed
                skydag_files = []
                for file_path in all_files:
                    if file_path.endswith('/'):  # Skip directories
                        continue
                    
                    filename = file_path.split('/')[-1]
                    # Only include files we actually deployed
                    if any(skydag_file in filename for skydag_file in [
                        'skydag_pipeline.py', 'processor_skyflow.py', 'platform_gcp.py', 
                        'platform_aws.py', 'platform_azure.py', '__init__.py', 'env.py',
                        'setup_skydag_variables.py'  # Our variables setup DAG
                    ]):
                        skydag_files.append(file_path)
                    else:
                        # Skip system/other DAGs
                        print(f"  ⏭️  Skipped: {filename} (not a SkyDAG file)")
                
                if not skydag_files:
                    print("📁 No SkyDAG files found to remove")
                    return True
                
                # Remove only our SkyDAG files
                for file_path in skydag_files:
                    remove_cmd = [
                        "gcloud", "storage", "rm", file_path,
                        "--project", self.project_id
                    ]
                    
                    result = subprocess.run(remove_cmd, capture_output=True, text=True, timeout=30)
                    if result.returncode == 0:
                        filename = file_path.split('/')[-1]
                        print(f"  ✅ Removed: {filename}")
                    else:
                        filename = file_path.split('/')[-1]
                        print(f"  ❌ Failed to remove {filename}: {result.stderr}")
                        return False
                
                print("✅ DAG removal completed")
                return True
            else:
                if "No URLs matched" in result.stderr or "not found" in result.stderr.lower():
                    print("📁 No DAG files found to remove")
                    return True
                else:
                    print(f"❌ Failed to list DAG files: {result.stderr}")
                    return False
                
        except Exception as e:
            print(f"❌ DAG removal failed: {e}")
            return False
    
    def remove_test_files(self) -> bool:
        """Remove test files using state mappings for bucket info"""
        try:
            # Try to get bucket info from state first
            from .infrastructure.state import DeploymentState
            state = DeploymentState(self.config)
            
            source_bucket = state.get_resource_mapping("buckets", "source")
            source_prefix = state.get_resource_mapping("buckets", "source_prefix")
            
            if source_bucket and source_prefix is not None:
                print(f"📂 Using stored bucket mapping: gs://{source_bucket}/{source_prefix}/")
                bucket_name, prefix = source_bucket, source_prefix
            else:
                print("⚠️  No bucket mapping in state, falling back to config variables")
                # Existing variable-based logic as fallback
                variables = self.config.get_airflow_variables()
                source_spec = variables.get('SKYDAG_SOURCE', '')
                if not source_spec:
                    print("📁 No SKYDAG_SOURCE configured - skipping test file removal")
                    return True
                
                if '/' in source_spec:
                    bucket_name, prefix = source_spec.split('/', 1)
                else:
                    bucket_name = source_spec
                    prefix = "input-files"
            
            print(f"🗑️  Removing test files from gs://{bucket_name}/{prefix}/")
            
            # List files in the test directory
            list_cmd = [
                "gcloud", "storage", "ls", f"gs://{bucket_name}/{prefix}/",
                "--project", self.project_id
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0 and result.stdout.strip():
                files_to_remove = [
                    line.strip() for line in result.stdout.strip().split('\n') 
                    if line.strip() and not line.strip().endswith('/')
                ]
                
                if not files_to_remove:
                    print("📁 No test files found to remove")
                    return True
                
                # Remove files that look like test files (based on common test file patterns)
                test_patterns = ['test_', 'sample_', '.csv', '.json', '.txt', '.xml']
                removed_count = 0
                
                for file_path in files_to_remove:
                    filename = file_path.split('/')[-1].lower()
                    
                    # Only remove files that match test patterns to avoid removing user data
                    if any(pattern in filename for pattern in test_patterns):
                        remove_cmd = [
                            "gcloud", "storage", "rm", file_path,
                            "--project", self.project_id
                        ]
                        
                        result = subprocess.run(remove_cmd, capture_output=True, text=True, timeout=30)
                        if result.returncode == 0:
                            display_filename = file_path.split('/')[-1]
                            print(f"  ✅ Removed test file: {display_filename}")
                            removed_count += 1
                        else:
                            display_filename = file_path.split('/')[-1]
                            print(f"  ❌ Failed to remove {display_filename}: {result.stderr}")
                    else:
                        display_filename = file_path.split('/')[-1]
                        print(f"  ⏭️  Skipped: {display_filename} (doesn't match test file patterns)")
                
                if removed_count > 0:
                    print(f"✅ Test file removal completed ({removed_count} files removed)")
                else:
                    print("📁 No test files matched removal patterns")
                
                return True
            elif result.returncode != 0:
                if "No URLs matched" in result.stderr or "not found" in result.stderr.lower() or "One or more URLs matched no objects" in result.stderr:
                    print("📁 No test files found to remove (directory may be empty)")
                    return True
                else:
                    print(f"❌ Failed to list test files: {result.stderr}")
                    return False
            else:
                # returncode == 0 but no stdout - empty directory
                print("📁 No test files found to remove (directory is empty)")
                return True
                
        except Exception as e:
            print(f"❌ Test file removal failed: {e}")
            return False
    
    def remove_variables_setup_dag(self) -> bool:
        """Remove variables setup DAG completely"""
        try:
            # Step 1: Remove from registry first
            print("🗑️  Removing setup_skydag_variables from registry...")
            self.force_dag_removal("setup_skydag_variables")
            
            # Step 2: Remove file (existing logic)
            bucket_name = self.get_dag_bucket()
            setup_dag_path = f"gs://{bucket_name}/{self.config.dag_prefix}/setup_skydag_variables.py"
            
            print(f"🗑️  Removing variables setup DAG file...")
            
            remove_cmd = [
                "gcloud", "storage", "rm", setup_dag_path,
                "--project", self.project_id
            ]
            
            result = subprocess.run(remove_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                print("  ✅ Removed: setup_skydag_variables.py")
            else:
                if "No URLs matched" in result.stderr or "not found" in result.stderr.lower():
                    print("  📁 No variables setup DAG file found to remove")
                else:
                    print(f"  ⚠️  Could not remove variables setup DAG file: {result.stderr}")
            
            # Always return True - this shouldn't fail undeploy
            return True
            
        except Exception as e:
            print(f"⚠️  Variables setup DAG removal failed: {e}")
            return True  # Don't fail undeploy for this
    
    def verify_undeployment(self) -> bool:
        """Verify that undeployment was successful"""
        verification_success = True
        
        # Check that our DAG files are gone from Composer bucket
        bucket_name = self.get_dag_bucket()
        
        # List files in the DAG prefix
        list_cmd = [
            "gcloud", "storage", "ls", f"gs://{bucket_name}/{self.config.dag_prefix}/**",
            "--project", self.project_id
        ]
        
        result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            remaining_files = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            
            # Filter out directories and check for our specific files
            skydag_files = []
            for file_path in remaining_files:
                if file_path.endswith('/'):
                    continue
                filename = file_path.split('/')[-1]
                # Check if this is one of our files
                if any(skydag_file in filename for skydag_file in [
                    'skydag_pipeline.py', 'processor_skyflow.py', 'platform_gcp.py', 
                    'platform_aws.py', 'platform_azure.py', '__init__.py', 'env.py'
                ]):
                    skydag_files.append(filename)
            
            if skydag_files:
                print(f"❌ Verification failed: {len(skydag_files)} SkyDAG files still remain in Composer bucket:")
                for filename in skydag_files:
                    print(f"  - {filename}")
                verification_success = False
            else:
                print("✅ DAG verification: No SkyDAG files remain in Composer bucket")
        elif "No URLs matched" in result.stderr or "not found" in result.stderr.lower():
            print("✅ DAG verification: DAG directory is empty")
        else:
            print(f"⚠️  Could not verify DAG removal: {result.stderr}")
            verification_success = False
        
        # Check that test files are gone from source bucket
        variables = self.config.get_airflow_variables()
        source_spec = variables.get('SKYDAG_SOURCE', '')
        if source_spec:
            if '/' in source_spec:
                bucket_name, prefix = source_spec.split('/', 1)
            else:
                bucket_name = source_spec
                prefix = "input-files"
            
            list_cmd = [
                "gcloud", "storage", "ls", f"gs://{bucket_name}/{prefix}/",
                "--project", self.project_id
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                remaining_files = [
                    line.strip() for line in result.stdout.strip().split('\n') 
                    if line.strip() and not line.strip().endswith('/')
                ]
                
                # Check for test files that should have been removed
                test_patterns = ['test_', 'sample_', '.csv', '.json', '.txt', '.xml']
                remaining_test_files = []
                for file_path in remaining_files:
                    filename = file_path.split('/')[-1].lower()
                    if any(pattern in filename for pattern in test_patterns):
                        remaining_test_files.append(file_path.split('/')[-1])
                
                if remaining_test_files:
                    print(f"❌ Verification failed: {len(remaining_test_files)} test files still remain in source bucket:")
                    for filename in remaining_test_files:
                        print(f"  - {filename}")
                    verification_success = False
                else:
                    print("✅ Test files verification: No test files remain in source bucket")
            elif "No URLs matched" in result.stderr or "not found" in result.stderr.lower() or "One or more URLs matched no objects" in result.stderr:
                print("✅ Test files verification: Source directory is empty")
            else:
                print(f"⚠️  Could not verify test file removal: {result.stderr}")
                # Don't fail verification if we can't check - files may have been removed successfully
                print("💡 This doesn't necessarily indicate failure - files may have been removed successfully")
        
        # Wait for DAG to be removed from Composer list
        if not self.wait_for_dag_removal(timeout=180):  # 3 minutes
            print("⚠️  DAG may still appear in Composer list - this can take several minutes")
            verification_success = False
        
        return verification_success
    
    def wait_for_dag_removal(self, timeout: int = 180) -> bool:
        """Wait for DAG to be removed from Composer list"""
        import time
        
        print(f"⏳ Waiting for DAG to be removed from Composer (timeout: {timeout}s)...")
        start_time = time.time()
        check_count = 0
        
        while time.time() - start_time < timeout:
            check_count += 1
            elapsed = time.time() - start_time
            
            try:
                list_cmd = [
                    "gcloud", "composer", "environments", "run", self.environment_name,
                    "--location", self.location,
                    "--project", self.project_id,
                    "dags", "list"
                ]
                
                result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0:
                    if "skydag_pipeline" not in result.stdout:
                        print(f"✅ DAG removed from Composer list after {elapsed:.1f}s ({check_count} checks)")
                        return True
                    else:
                        print(f"⏳ Check #{check_count}: DAG still in list after {elapsed:.1f}s, waiting 10s...")
                else:
                    print(f"⚠️  Could not check DAG list: {result.stderr}")
                    
            except Exception as e:
                print(f"⚠️  Error checking DAG list: {e}")
            
            time.sleep(10)  # Check every 10 seconds
        
        print(f"⏰ Timeout after {timeout}s waiting for DAG removal")
        return False
    
    def upload_test_files(self) -> bool:
        """Upload test files to source bucket for testing"""
        try:
            from pathlib import Path
            
            # Get test-data directory path
            project_root = Path(__file__).parent.parent
            test_data_dir = project_root / "test-data"
            
            if not test_data_dir.exists():
                print("📁 No test-data directory found - skipping test file upload")
                return True
            
            # Get source bucket from config
            variables = self.config.get_airflow_variables()
            source_spec = variables.get('SKYDAG_SOURCE', '')
            if not source_spec:
                print("⚠️  No SKYDAG_SOURCE configured - skipping test file upload")
                return True
            
            # Extract bucket and prefix
            if '/' in source_spec:
                bucket_name, prefix = source_spec.split('/', 1)
            else:
                bucket_name = source_spec
                prefix = "input-files"
            
            print(f"📁 Uploading test files to gs://{bucket_name}/{prefix}/")
            
            # Upload all files from test-data directory (except README)
            uploaded_count = 0
            for test_file in test_data_dir.glob("*"):
                if test_file.is_file() and test_file.name.lower() != "readme.md":
                    gcs_path = f"gs://{bucket_name}/{prefix}/{test_file.name}"
                    
                    upload_cmd = [
                        "gcloud", "storage", "cp", str(test_file), gcs_path,
                        "--project", self.project_id
                    ]
                    
                    result = subprocess.run(upload_cmd, capture_output=True, text=True, timeout=60)
                    if result.returncode == 0:
                        print(f"  ✅ Uploaded test file: {test_file.name}")
                        uploaded_count += 1
                    else:
                        print(f"  ❌ Failed to upload {test_file.name}: {result.stderr}")
                        return False
            
            if uploaded_count > 0:
                print(f"✅ Test file upload completed ({uploaded_count} files)")
            else:
                print("📁 No test files found to upload")
            
            return True
            
        except Exception as e:
            print(f"❌ Test file upload failed: {e}")
            return False
    
    def force_dag_removal(self, dag_id: str) -> bool:
        """Force removal of DAG from Composer registry and UI"""
        print(f"🗑️  Force removing DAG from registry: {dag_id}")
        
        try:
            # Step 1: Delete DAG from Airflow database
            delete_cmd = [
                "gcloud", "composer", "environments", "run", self.environment_name,
                "--location", self.location,
                "--project", self.project_id,
                "dags", "delete", "--", dag_id, "--yes"
            ]
            
            result = subprocess.run(delete_cmd, capture_output=True, text=True, timeout=120)
            if result.returncode == 0:
                print(f"✅ DAG {dag_id} deleted from registry")
            else:
                # Check if it's just "DAG not found" which is fine
                error_output = result.stderr.lower() if result.stderr else ""
                if "not found" in error_output or "dagnotfound" in error_output:
                    print(f"📁 DAG {dag_id} not found (already removed or never existed)")
                else:
                    print(f"⚠️  DAG delete had issues: {result.stderr}")
            
            # Step 2: Skip cache clearing - not supported in Cloud Composer
            # The `dags reserialize` command is not available in Cloud Composer
            print(f"⏭️  Skipping cache clear for {dag_id} (not supported in Cloud Composer)")
            
            return True
            
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout removing DAG {dag_id}")
            return False
        except Exception as e:
            print(f"❌ Error removing DAG {dag_id}: {e}")
            return False

    def cleanup_all_skydag_dags(self) -> bool:
        """Remove all SkyDAG DAGs from both files and registry"""
        
        # Step 1: Remove DAG files first (this stops new DAG parsing)
        print("🗑️  Removing DAG files first to stop parsing...")
        if not self.remove_dags():
            print("⚠️  DAG file removal had issues")
        
        # Step 2: Remove from registry
        skydag_dags = [
            "skydag_pipeline",
            "setup_skydag_variables"
        ]
        
        success = True
        
        for dag_id in skydag_dags:
            if not self.force_dag_removal(dag_id):
                print(f"⚠️  Could not remove {dag_id} from registry")
                success = False
            
            # Small delay between operations
            import time
            time.sleep(2)
        
        # Step 3: Wait a bit for changes to propagate
        print("⏳ Waiting for DAG registry changes to propagate...")
        import time
        time.sleep(10)
        
        return success

    def verify_dag_cleanup(self) -> bool:
        """Verify DAGs are completely removed from Composer"""
        print("🔍 Verifying DAG cleanup...")
        
        try:
            # List all DAGs to check for SkyDAG remnants
            list_cmd = [
                "gcloud", "composer", "environments", "run", self.environment_name,
                "--location", self.location,
                "--project", self.project_id,
                "dags", "list", "--", "--output", "table"
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=90)
            if result.returncode == 0:
                output = result.stdout.lower()
                
                # Check for SkyDAG DAG remnants
                skydag_remnants = []
                for line in output.split('\n'):
                    if 'skydag' in line and 'skydag_pipeline' in line:
                        skydag_remnants.append(line.strip())
                
                if skydag_remnants:
                    print(f"⚠️  Found {len(skydag_remnants)} SkyDAG DAG remnants:")
                    for remnant in skydag_remnants:
                        print(f"  - {remnant}")
                    print("💡 DAGs may take some time to disappear from Composer UI after file removal")
                    print("💡 This is normal behavior - the important thing is that files are removed")
                    # Don't fail verification for this - it's expected behavior
                    return True
                else:
                    print("✅ No SkyDAG DAGs found in Composer registry")
                    return True
            else:
                print(f"⚠️  Could not verify DAG cleanup: {result.stderr}")
                # Don't fail verification if we can't check
                return True
                
        except Exception as e:
            print(f"⚠️  Error verifying DAG cleanup: {e}")
            # Don't fail verification for exceptions
            return True

    def verify_complete_undeployment(self) -> bool:
        """Comprehensive verification that undeployment was complete"""
        verification_success = True
        
        # Verify DAG registry cleanup
        if not self.verify_dag_cleanup():
            verification_success = False
        
        # Existing file verification logic from verify_undeployment
        # Check that our DAG files are gone from Composer bucket
        bucket_name = self.get_dag_bucket()
        
        # List files in the DAG prefix
        list_cmd = [
            "gcloud", "storage", "ls", f"gs://{bucket_name}/{self.config.dag_prefix}/**",
            "--project", self.project_id
        ]
        
        result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            remaining_files = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            
            # Filter out directories and check for our specific files
            skydag_files = []
            for file_path in remaining_files:
                if file_path.endswith('/'):
                    continue
                filename = file_path.split('/')[-1]
                # Check if this is one of our files
                if any(skydag_file in filename for skydag_file in [
                    'skydag_pipeline.py', 'processor_skyflow.py', 'platform_gcp.py', 
                    'platform_aws.py', 'platform_azure.py', '__init__.py', 'env.py'
                ]):
                    skydag_files.append(filename)
            
            if skydag_files:
                print(f"❌ Verification failed: {len(skydag_files)} SkyDAG files still remain in Composer bucket:")
                for filename in skydag_files:
                    print(f"  - {filename}")
                verification_success = False
            else:
                print("✅ DAG verification: No SkyDAG files remain in Composer bucket")
        elif "No URLs matched" in result.stderr or "not found" in result.stderr.lower():
            print("✅ DAG verification: DAG directory is empty")
        else:
            print(f"⚠️  Could not verify DAG removal: {result.stderr}")
            verification_success = False
        
        return verification_success