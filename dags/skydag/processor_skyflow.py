"""Skyflow HTTP client for kickoff and polling operations"""
import json
import os
import random
import time
from typing import Dict, Optional

import requests


class SkyflowProcessor:
    def __init__(
        self,
        start_url: str,
        poll_url_template: str,
        auth_header: Optional[str] = None,
        max_wait: float = 1800,
        initial_interval: float = 2.0,
        backoff_multiplier: float = 1.7,
        max_interval: float = 20.0
    ):
        self.start_url = start_url
        self.poll_url_template = poll_url_template
        self.auth_header = auth_header
        self.max_wait = max_wait
        self.initial_interval = initial_interval
        self.backoff_multiplier = backoff_multiplier
        self.max_interval = max_interval
    
    @classmethod
    def from_env(
        cls,
        max_wait: float = 1800,
        initial_interval: float = 2.0,
        backoff_multiplier: float = 1.7,
        max_interval: float = 20.0
    ) -> "SkyflowProcessor":
        """Create SkyflowProcessor from environment variables"""
        start_url = os.getenv("SKYFLOW_START_URL")
        poll_url_template = os.getenv("SKYFLOW_POLL_URL_TEMPLATE")
        auth_header = os.getenv("SKYFLOW_AUTH_HEADER")
        
        if not start_url:
            raise ValueError("SKYFLOW_START_URL environment variable is required")
        if not poll_url_template:
            raise ValueError("SKYFLOW_POLL_URL_TEMPLATE environment variable is required")
        
        return cls(
            start_url=start_url,
            poll_url_template=poll_url_template,
            auth_header=auth_header,
            max_wait=max_wait,
            initial_interval=initial_interval,
            backoff_multiplier=backoff_multiplier,
            max_interval=max_interval
        )
    
    def kickoff_b64(self, payload_b64: str, filename: str, vault_id: str) -> str:
        """Start processing with base64 payload, return run_id"""
        import logging
        logger = logging.getLogger(__name__)
        
        headers = {"Content-Type": "application/json"}
        if self.auth_header:
            headers["Authorization"] = self.auth_header
        
        # Determine data format from file extension
        import os
        file_ext = os.path.splitext(filename)[1].lower().lstrip('.')
        # Map common extensions to data_format, default to the extension itself
        format_mapping = {
            'txt': 'txt',
            'csv': 'csv', 
            'json': 'json',
            'xml': 'xml',
            'tsv': 'tsv'
        }
        data_format = format_mapping.get(file_ext, file_ext or 'txt')
        
        # Use actual Skyflow API format from .env.local.bak
        data = {
            "file": {
                "base64": payload_b64,
                "data_format": data_format
            },
            "vault_id": vault_id
        }
        
        logger.info(f"🚀 Starting Skyflow processing for file: {filename}")
        logger.info(f"🔗 API Endpoint: {self.start_url}")
        logger.info(f"📦 Payload size: {len(payload_b64)} characters")
        logger.info(f"📄 Data format: {data_format} (from extension: {file_ext})")
        logger.info(f"📋 Request headers: {headers}")
        logger.info(f"📋 Request data keys: {list(data.keys())}")
        
        try:
            response = requests.post(
                self.start_url,
                json=data,
                headers=headers,
                timeout=30
            )
            
            logger.info(f"📡 API Response Status: {response.status_code}")
            
            if not response.ok:
                logger.error(f"❌ API Error Response: {response.status_code}")
                logger.error(f"❌ Response Headers: {dict(response.headers)}")
                logger.error(f"❌ Response Body: {response.text[:1000]}...")
                
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"✅ API Response: {result}")
            
            run_id = result.get("run_id")
            
            if not run_id:
                logger.error(f"❌ No run_id in response: {result}")
                raise ValueError(f"No run_id returned from kickoff API: {result}")
            
            logger.info(f"🎯 Started processing with run_id: {run_id}")
            return run_id
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {e}")
            logger.error(f"❌ Request URL: {self.start_url}")
            logger.error(f"❌ Request headers: {headers}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error in kickoff: {e}")
            raise
    
    def poll_until_done(self, run_id: str) -> Dict:
        """Poll status until terminal state, return final result"""
        import logging
        logger = logging.getLogger(__name__)
        
        headers = {}
        if self.auth_header:
            headers["Authorization"] = self.auth_header
        
        poll_url = self.poll_url_template.format(run_id=run_id)
        start_time = time.time()
        interval = self.initial_interval
        
        logger.info(f"🔄 Starting to poll Skyflow processing status for run_id: {run_id}")
        logger.info(f"🔗 Poll URL: {poll_url}")
        logger.info(f"⏰ Max wait time: {self.max_wait}s")
        logger.info(f"📋 Poll headers: {headers}")
        
        # Success statuses (case insensitive)
        success_statuses = {"succeeded", "success", "completed", "complete", "done"}
        failure_statuses = {"failed", "error", "canceled", "cancelled"}
        
        poll_count = 0
        while time.time() - start_time < self.max_wait:
            poll_count += 1
            elapsed = time.time() - start_time
            
            try:
                logger.info(f"📡 Poll #{poll_count}: Checking status after {elapsed:.1f}s")
                
                response = requests.get(poll_url, headers=headers, timeout=30)
                
                logger.info(f"📡 Poll Response Status: {response.status_code}")
                
                if not response.ok:
                    logger.error(f"❌ Poll API Error Response: {response.status_code}")
                    logger.error(f"❌ Poll Response Headers: {dict(response.headers)}")
                    logger.error(f"❌ Poll Response Body: {response.text[:1000]}...")
                
                response.raise_for_status()
                
                result = response.json()
                status = result.get("status", "").lower()
                
                logger.info(f"📊 Current status: {status}")
                
                if status in success_statuses:
                    logger.info(f"🎉 Processing completed successfully! Status: {status}")
                    
                    # Parse actual Skyflow API response format
                    output = result.get("output", [])
                    if not output:
                        logger.error(f"❌ Success status but no output in response: {result}")
                        raise ValueError(f"Success status but no output in response for run_id {run_id}: {result}")
                    
                    # Find the processed file (redacted_file type)
                    processed_file = None
                    for item in output:
                        if item.get("processedFileType") == "redacted_file":
                            processed_file = item.get("processedFile")
                            break
                    
                    if not processed_file:
                        logger.error(f"❌ Success status but no redacted_file in output: {result}")
                        raise ValueError(f"Success status but no redacted_file in output for run_id {run_id}: {result}")
                    
                    logger.info(f"✅ Received processed file (size: {len(processed_file)} characters)")
                    
                    # Return result with file_b64 key for backward compatibility
                    result["file_b64"] = processed_file
                    return result
                
                if status in failure_statuses:
                    logger.error(f"❌ Processing failed with status: {status}")
                    logger.error(f"❌ Failure details: {result}")
                    raise RuntimeError(f"Processing failed for run_id {run_id}: status={status}, result={result}")
                
                # Log intermediate status for monitoring
                if status:
                    logger.info(f"⏳ Processing in progress, status: {status}")
                else:
                    logger.info(f"⏳ Processing in progress (no status field)")
                
                # Add jitter to avoid thundering herd
                jitter = random.uniform(0.8, 1.2)
                sleep_time = min(interval * jitter, self.max_interval)
                
                logger.info(f"😴 Waiting {sleep_time:.1f}s before next poll (interval: {interval:.1f}s)")
                time.sleep(sleep_time)
                
                interval = min(interval * self.backoff_multiplier, self.max_interval)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Poll request failed: {e}")
                logger.error(f"❌ Poll URL: {poll_url}")
                logger.error(f"❌ Poll headers: {headers}")
                raise
            except Exception as e:
                logger.error(f"❌ Unexpected error during polling: {e}")
                raise
        
        total_elapsed = time.time() - start_time
        logger.error(f"❌ Polling timeout after {total_elapsed:.1f}s (max: {self.max_wait}s)")
        logger.error(f"❌ Total polls attempted: {poll_count}")
        raise TimeoutError(f"Polling timeout after {self.max_wait}s for run_id {run_id}")