"""AWS platform module for S3 operations using Airflow Amazon provider"""
from typing import List, Optional, Tuple

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def split_spec(spec: str) -> Tuple[str, str]:
    """Split 'bucket/prefix' into (bucket, prefix)"""
    parts = spec.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def list_keys(bucket: str, prefix: str) -> List[str]:
    """List object keys in S3 bucket with given prefix"""
    hook = S3Hook(aws_conn_id="aws_default")
    
    # list_keys returns full key names
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    
    # Filter out directories (keys ending with /)
    return [key for key in (keys or []) if not key.endswith("/")]


def read_bytes(bucket: str, key: str) -> bytes:
    """Read object bytes from S3"""
    hook = S3Hook(aws_conn_id="aws_default")
    
    # Get object body as bytes
    s3_obj = hook.get_key(key=key, bucket_name=bucket)
    if not s3_obj:
        raise FileNotFoundError(f"Object s3://{bucket}/{key} not found")
    
    return s3_obj.get()["Body"].read()


def write_bytes(bucket: str, key: str, data: bytes, content_type: Optional[str] = None) -> None:
    """Write bytes to S3 object"""
    hook = S3Hook(aws_conn_id="aws_default")
    
    # Load bytes data to S3
    hook.load_bytes(
        bytes_data=data,
        key=key,
        bucket_name=bucket,
        replace=True,
        content_type=content_type
    )