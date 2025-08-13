"""SkyDAG Pipeline - Demo Airflow DAG for cloud file processing through Skyflow"""
import base64
import os
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# Load .env.local if available (demo convenience)
from skydag.env import load_env_if_available
from skydag.processor_skyflow import SkyflowProcessor

# Ensure .env.local is loaded before defining configuration
load_env_if_available()


# Configuration for Cloud Composer deployment (from .env.local values)
DEFAULT_CONFIG = {
    "SKYDAG_PLATFORM": os.getenv("SKYDAG_PLATFORM", "gcp"),
    "SKYDAG_SOURCE": os.getenv("SKYDAG_SOURCE", "skydag-test-source/input-files"),
    "SKYDAG_DEST": os.getenv("SKYDAG_DEST", "skydag-test-dest/output-files"),
    "SKYDAG_POLL_MAX_WAIT": os.getenv("SKYDAG_POLL_MAX_WAIT", "1800"),
    "SKYDAG_POLL_INITIAL": os.getenv("SKYDAG_POLL_INITIAL", "2.0"),
    "SKYDAG_POLL_BACKOFF": os.getenv("SKYDAG_POLL_BACKOFF", "1.7"),
    "SKYDAG_POLL_MAX_INTERVAL": os.getenv("SKYDAG_POLL_MAX_INTERVAL", "20.0"),
    "SKYFLOW_START_URL": os.getenv("SKYFLOW_START_URL", "https://ebfc9bee4242.vault.skyflowapis.com/v1/detect/deidentify/file"),
    "SKYFLOW_POLL_URL_TEMPLATE": os.getenv("SKYFLOW_POLL_URL_TEMPLATE", "https://ebfc9bee4242.vault.skyflowapis.com/v1/detect/runs/{run_id}?vault_id=a55459f3681948a1baf6756d28f62993"),  
    "SKYFLOW_AUTH_HEADER": os.getenv("SKYFLOW_AUTH_HEADER")
}


def get_config(key: str, default: str = None) -> str:
    """Get config from Airflow Variables with fallback to defaults
    
    Priority order:
    1. Airflow Variables (set during deploy)
    2. Fallback defaults (from DEFAULT_CONFIG)
    3. Provided default parameter
    """
    from airflow.models import Variable
    
    # Try Airflow Variable first
    try:
        airflow_value = Variable.get(key, default_var=None)
        if airflow_value is not None and str(airflow_value).strip():
            return str(airflow_value).strip()
    except Exception as e:
        # Variable.get can raise exceptions if Variable doesn't exist
        pass
    
    # Then try our fallback config
    if key in DEFAULT_CONFIG and DEFAULT_CONFIG[key]:
        return DEFAULT_CONFIG[key]
        
    # Use provided default
    if default is not None:
        return default
        
    raise ValueError(f"Configuration {key} is required and not found in Airflow Variables or defaults")


def get_platform_module():
    """Dynamically import platform module based on SKYDAG_PLATFORM"""
    platform = get_config("SKYDAG_PLATFORM").lower()
    
    if platform == "gcp":
        from skydag.platform_gcp import split_spec, list_keys, read_bytes, write_bytes
    elif platform == "aws":
        from skydag.platform_aws import split_spec, list_keys, read_bytes, write_bytes
    elif platform == "azure":
        from skydag.platform_azure import split_spec, list_keys, read_bytes, write_bytes
    else:
        raise ValueError(f"Unsupported platform: {platform}. Must be one of: gcp, aws, azure")
    
    return split_spec, list_keys, read_bytes, write_bytes


# DAG definition
default_args = {
    'owner': 'skydag',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries for demo clarity
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'skydag_pipeline',
    default_args=default_args,
    description='Demo DAG for cloud file processing through Skyflow',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['demo', 'skyflow', 'cloud'],
)


@task
def get_files_to_process(**context) -> List[str]:
    """Get files to process - either from DAG params or list all files"""
    # Check if filename was provided in DAG run config
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf and dag_run.conf.get('filename'):
        filename = dag_run.conf['filename']
        print(f"Processing specific file from trigger: {filename}")
        
        # Need to construct full path with prefix for consistency
        split_spec, _, _, _ = get_platform_module()
        source_spec = get_config("SKYDAG_SOURCE")
        bucket_or_container, prefix = split_spec(source_spec)
        
        # Construct full file path (prefix + filename)
        if prefix:
            full_file_path = f"{prefix}/{filename}" if not filename.startswith(prefix) else filename
        else:
            full_file_path = filename
            
        print(f"Constructed full file path: {full_file_path}")
        return [full_file_path]
    
    # No filename specified, list all files (original behavior)
    split_spec, list_keys, _, _ = get_platform_module()
    
    source_spec = get_config("SKYDAG_SOURCE")
    bucket_or_container, prefix = split_spec(source_spec)
    
    keys = list_keys(bucket_or_container, prefix)
    print(f"No filename specified, found {len(keys)} files in {source_spec}")
    
    # Filter out .keep files and other system files
    filtered_keys = [
        key for key in keys 
        if not key.endswith('.keep') and not key.endswith('/')
    ]
    
    print(f"After filtering system files: {len(filtered_keys)} processable files")
    
    if not filtered_keys:
        print("No processable files found (excluding .keep files)")
    
    return filtered_keys


@task
def process_with_skyflow(file_key: str) -> dict:
    """Process a single file with Skyflow: read -> skyflow API -> return processed data"""
    split_spec, _, read_bytes, _ = get_platform_module()
    
    # Get configuration
    source_spec = get_config("SKYDAG_SOURCE")
    
    max_wait = float(get_config("SKYDAG_POLL_MAX_WAIT", DEFAULT_CONFIG["SKYDAG_POLL_MAX_WAIT"]))
    initial = float(get_config("SKYDAG_POLL_INITIAL", DEFAULT_CONFIG["SKYDAG_POLL_INITIAL"]))
    backoff = float(get_config("SKYDAG_POLL_BACKOFF", DEFAULT_CONFIG["SKYDAG_POLL_BACKOFF"]))
    max_interval = float(get_config("SKYDAG_POLL_MAX_INTERVAL", DEFAULT_CONFIG["SKYDAG_POLL_MAX_INTERVAL"]))
    
    source_bucket, source_prefix = split_spec(source_spec)
    
    print(f"Processing file: {file_key}")
    
    # Step 1: Read file bytes
    try:
        file_data = read_bytes(source_bucket, file_key)
        if not file_data:
            print(f"Skipping empty file: {file_key}")
            return {"status": "skipped", "reason": "empty file"}
        
        print(f"Read {len(file_data)} bytes from {file_key}")
    except Exception as e:
        print(f"Failed to read {file_key}: {e}")
        raise
    
    # Step 2: Base64 encode
    file_b64 = base64.b64encode(file_data).decode('utf-8')
    filename = file_key.split("/")[-1]  # Extract basename
    
    # Step 3: Create Skyflow processor and kickoff
    try:
        # Get Skyflow configuration using our config system
        start_url = get_config("SKYFLOW_START_URL")
        poll_url_template = get_config("SKYFLOW_POLL_URL_TEMPLATE")
        auth_header = get_config("SKYFLOW_AUTH_HEADER")
        
        processor = SkyflowProcessor(
            start_url=start_url,
            poll_url_template=poll_url_template,
            auth_header=auth_header,
            max_wait=max_wait,
            initial_interval=initial,
            backoff_multiplier=backoff,
            max_interval=max_interval
        )
        
        # Get vault_id from poll URL (extract from template)
        poll_url_template = get_config("SKYFLOW_POLL_URL_TEMPLATE")
        # Extract vault_id from URL like: .../runs/{run_id}?vault_id=a55459f3681948a1baf6756d28f62993
        import re
        vault_id_match = re.search(r'vault_id=([^&]+)', poll_url_template)
        if not vault_id_match:
            raise ValueError(f"Could not extract vault_id from poll URL template: {poll_url_template}")
        vault_id = vault_id_match.group(1)
        
        run_id = processor.kickoff_b64(file_b64, filename, vault_id)
        print(f"Started Skyflow processing: run_id={run_id}")
        
    except Exception as e:
        print(f"Failed to start Skyflow processing for {file_key}: {e}")
        raise
    
    # Step 4: Poll until completion
    try:
        result = processor.poll_until_done(run_id)
        print(f"Skyflow processing completed: run_id={run_id}")
        
    except Exception as e:
        print(f"Skyflow processing failed for {file_key}, run_id={run_id}: {e}")
        raise
    
    # Step 5: Extract processed file and return data for next task
    processed_b64 = result.get("file_b64")
    if not processed_b64:
        raise ValueError(f"No file_b64 in successful result for run_id={run_id}: {result}")
    
    print(f"✅ Skyflow processing completed for {file_key}")
    
    return {
        "status": "success",
        "source_key": file_key,
        "filename": filename,
        "run_id": run_id,
        "original_size": len(file_data),
        "processed_b64": processed_b64,
        "processed_size": len(processed_b64)
    }


@task(trigger_rule="none_failed_min_one_success")
def write_output_file(processing_result: dict) -> dict:
    """Write processed file to destination bucket"""
    split_spec, _, _, write_bytes = get_platform_module()
    
    # Get configuration
    dest_spec = get_config("SKYDAG_DEST")
    dest_bucket, dest_prefix = split_spec(dest_spec)
    
    # Extract data from processing result
    source_key = processing_result["source_key"]
    filename = processing_result["filename"]
    processed_b64 = processing_result["processed_b64"]
    run_id = processing_result["run_id"]
    
    print(f"Writing processed file for: {source_key}")
    
    try:
        # Decode processed data
        processed_data = base64.b64decode(processed_b64)
        
        # Build destination key: dest_prefix + basename
        dest_key = f"{dest_prefix}/{filename}" if dest_prefix else filename
        
        # Write to destination bucket
        write_bytes(dest_bucket, dest_key, processed_data)
        print(f"✅ Wrote processed file to: {dest_key}")
        
        return {
            "status": "success",
            "source_key": source_key,
            "dest_key": dest_key,
            "run_id": run_id,
            "original_size": processing_result["original_size"],
            "processed_size": len(processed_data)
        }
        
    except Exception as e:
        print(f"❌ Failed to write processed file for {source_key}, run_id={run_id}: {e}")
        raise


# Define task dependencies
with dag:
    file_list = get_files_to_process()
    
    # Step 2: Process each file with Skyflow (parallel)
    processing_results = process_with_skyflow.expand(file_key=file_list)
    
    # Step 3: Write output files (parallel) - only for successful processing results  
    write_results = write_output_file.expand(processing_result=processing_results)