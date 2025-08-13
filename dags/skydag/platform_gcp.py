"""GCP platform module for GCS operations using Airflow Google provider"""
from typing import List, Optional, Tuple

from airflow.providers.google.cloud.hooks.gcs import GCSHook


def split_spec(spec: str) -> Tuple[str, str]:
    """Split 'bucket/prefix' into (bucket, prefix)"""
    parts = spec.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def list_keys(bucket: str, prefix: str) -> List[str]:
    """List object keys in GCS bucket with given prefix"""
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    # GCSHook.list returns object names (keys)
    objects = hook.list(bucket_name=bucket, prefix=prefix)
    
    # Filter out directories (objects ending with /)
    return [obj for obj in objects if not obj.endswith("/")]


def read_bytes(bucket: str, key: str) -> bytes:
    """Read object bytes from GCS"""
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    # Download as bytes
    return hook.download(bucket_name=bucket, object_name=key)


def write_bytes(bucket: str, key: str, data: bytes, content_type: Optional[str] = None) -> None:
    """Write bytes to GCS object"""
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    # Upload bytes data
    hook.upload(
        bucket_name=bucket,
        object_name=key,
        data=data,
        mime_type=content_type
    )