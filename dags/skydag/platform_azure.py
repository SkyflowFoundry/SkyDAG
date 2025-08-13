"""Azure platform module for Blob Storage operations using Airflow Microsoft Azure provider"""
from typing import List, Optional, Tuple

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


def split_spec(spec: str) -> Tuple[str, str]:
    """Split 'container/prefix' into (container, prefix)"""
    parts = spec.split("/", 1)
    container = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return container, prefix


def list_keys(container: str, prefix: str) -> List[str]:
    """List blob keys in Azure container with given prefix"""
    hook = WasbHook(wasb_conn_id="wasb_default")
    
    # get_blobs_list returns blob names
    blobs = hook.get_blobs_list(container_name=container, prefix=prefix)
    
    # Filter out directories (blobs ending with /)
    return [blob for blob in blobs if not blob.endswith("/")]


def read_bytes(container: str, key: str) -> bytes:
    """Read blob bytes from Azure Blob Storage"""
    hook = WasbHook(wasb_conn_id="wasb_default")
    
    # Download blob as bytes
    blob_data = hook.read_file(container_name=container, blob_name=key)
    
    if isinstance(blob_data, str):
        return blob_data.encode('utf-8')
    return blob_data


def write_bytes(container: str, key: str, data: bytes, content_type: Optional[str] = None) -> None:
    """Write bytes to Azure blob"""
    hook = WasbHook(wasb_conn_id="wasb_default")
    
    # Upload bytes data
    hook.load_bytes(
        data=data,
        container_name=container,
        blob_name=key,
        content_type=content_type
    )