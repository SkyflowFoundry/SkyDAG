"""Deployment state management for atomic operations"""
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from enum import Enum


class ResourceState(Enum):
    """Resource deployment states"""
    CREATING = "creating"
    CREATED = "created"
    FAILED = "failed"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"


class DeploymentState:
    """Manages deployment state for atomic operations"""
    
    def __init__(self, config):
        self.config = config
        self.state_file = Path.cwd() / ".skydag_state.json"
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """Load state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                pass
        
        return {
            "deployment_id": None,
            "platform": None,
            "environment": None,
            "resources": {},
            "resource_mappings": {
                "buckets": {
                    "source": None,
                    "source_prefix": None,
                    "dest": None,
                    "dest_prefix": None,
                    "dag": None
                },
                "composer": {
                    "environment_name": None,
                    "dag_bucket": None,
                    "dag_prefix": None
                }
            },
            "status": "clean",
            "created_at": None,
            "last_updated": None
        }
    
    def _save_state(self):
        """Save state to file"""
        self.state["last_updated"] = time.time()
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def start_deployment(self, deployment_id: str = None) -> str:
        """Start a new deployment"""
        if deployment_id is None:
            deployment_id = f"skydag-{int(time.time())}"
        
        self.state = {
            "deployment_id": deployment_id,
            "platform": self.config.platform,
            "environment": self.config.environment,
            "resources": {},
            "resource_mappings": {
                "buckets": {
                    "source": None,
                    "source_prefix": None,
                    "dest": None,
                    "dest_prefix": None,
                    "dag": None
                },
                "composer": {
                    "environment_name": None,
                    "dag_bucket": None,
                    "dag_prefix": None
                }
            },
            "status": "deploying",
            "created_at": time.time(),
            "last_updated": time.time()
        }
        self._save_state()
        return deployment_id
    
    def start_destruction(self):
        """Start destruction process"""
        if self.state["status"] == "clean":
            raise ValueError("No deployment found to destroy")
        
        self.state["status"] = "destroying"
        self._save_state()
    
    def add_resource(self, resource_type: str, resource_name: str, resource_data: Dict[str, Any]):
        """Add a resource to tracking"""
        resource_id = f"{resource_type}:{resource_name}"
        self.state["resources"][resource_id] = {
            "type": resource_type,
            "name": resource_name,
            "state": ResourceState.CREATING.value,
            "data": resource_data,
            "created_at": time.time()
        }
        self._save_state()
    
    def update_resource(self, resource_type: str, resource_name: str, state: ResourceState, data: Dict[str, Any] = None):
        """Update resource state"""
        resource_id = f"{resource_type}:{resource_name}"
        if resource_id in self.state["resources"]:
            self.state["resources"][resource_id]["state"] = state.value
            self.state["resources"][resource_id]["updated_at"] = time.time()
            if data:
                self.state["resources"][resource_id]["data"].update(data)
            self._save_state()
    
    def mark_resource_created(self, resource_type: str, resource_name: str, data: Dict[str, Any] = None):
        """Mark resource as successfully created"""
        self.update_resource(resource_type, resource_name, ResourceState.CREATED, data)
    
    def mark_resource_failed(self, resource_type: str, resource_name: str, error: str):
        """Mark resource as failed"""
        self.update_resource(resource_type, resource_name, ResourceState.FAILED, {"error": error})
    
    def mark_resource_destroyed(self, resource_type: str, resource_name: str):
        """Mark resource as destroyed"""
        self.update_resource(resource_type, resource_name, ResourceState.DESTROYED)
    
    def get_resources_by_state(self, state: ResourceState) -> List[Dict]:
        """Get all resources in a specific state"""
        return [
            resource for resource in self.state["resources"].values()
            if resource["state"] == state.value
        ]
    
    def get_resources_to_destroy(self) -> List[Dict]:
        """Get all resources that need to be destroyed"""
        return [
            resource for resource in self.state["resources"].values()
            if resource["state"] in [ResourceState.CREATED.value, ResourceState.FAILED.value]
        ]
    
    def complete_deployment(self, success: bool = True):
        """Mark deployment as complete"""
        if success:
            failed_resources = self.get_resources_by_state(ResourceState.FAILED)
            if failed_resources:
                self.state["status"] = "partial"
            else:
                self.state["status"] = "deployed"
        else:
            self.state["status"] = "failed"
        self._save_state()
    
    def complete_destruction(self):
        """Mark destruction as complete and clean state"""
        self.state = {
            "deployment_id": None,
            "platform": None,
            "environment": None,
            "resources": {},
            "resource_mappings": {
                "buckets": {
                    "source": None,
                    "source_prefix": None,
                    "dest": None,
                    "dest_prefix": None,
                    "dag": None
                },
                "composer": {
                    "environment_name": None,
                    "dag_bucket": None,
                    "dag_prefix": None
                }
            },
            "status": "clean",
            "created_at": None,
            "last_updated": time.time()
        }
        self._save_state()
    
    def is_clean(self) -> bool:
        """Check if environment is clean (no resources deployed)"""
        return self.state["status"] == "clean"
    
    def is_deployed(self) -> bool:
        """Check if deployment is complete"""
        return self.state["status"] in ["deployed", "partial"]
    
    def is_deploying(self) -> bool:
        """Check if deployment is in progress"""
        return self.state["status"] == "deploying"
    
    def is_destroying(self) -> bool:
        """Check if destruction is in progress"""
        return self.state["status"] == "destroying"
    
    def has_failed_resources(self) -> bool:
        """Check if there are any failed resources"""
        return len(self.get_resources_by_state(ResourceState.FAILED)) > 0
    
    def get_deployment_summary(self) -> Dict:
        """Get deployment summary"""
        total_resources = len(self.state["resources"])
        created_resources = len(self.get_resources_by_state(ResourceState.CREATED))
        failed_resources = len(self.get_resources_by_state(ResourceState.FAILED))
        
        return {
            "deployment_id": self.state["deployment_id"],
            "platform": self.state["platform"],
            "environment": self.state["environment"],
            "status": self.state["status"],
            "total_resources": total_resources,
            "created_resources": created_resources,
            "failed_resources": failed_resources,
            "created_at": self.state["created_at"],
            "last_updated": self.state["last_updated"]
        }
    
    def store_resource_mapping(self, category: str, key: str, value: str):
        """Store resource name mapping for use across operations"""
        if "resource_mappings" not in self.state:
            self.state["resource_mappings"] = {}
        if category not in self.state["resource_mappings"]:
            self.state["resource_mappings"][category] = {}
        
        self.state["resource_mappings"][category][key] = value
        self._save_state()
        print(f"📝 Stored resource mapping: {category}.{key} = {value}")

    def get_resource_mapping(self, category: str, key: str) -> Optional[str]:
        """Get stored resource name mapping"""
        return self.state.get("resource_mappings", {}).get(category, {}).get(key)

    def cleanup_state_file(self):
        """Remove state file"""
        if self.state_file.exists():
            self.state_file.unlink()


class AtomicOperation:
    """Context manager for atomic operations"""
    
    def __init__(self, state: DeploymentState, operation: str):
        self.state = state
        self.operation = operation
        self.success = False
    
    def __enter__(self):
        if self.operation == "setup":
            deployment_id = self.state.start_deployment()
            print(f"🚀 Starting deployment: {deployment_id}")
        elif self.operation == "destroy":
            self.state.start_destruction()
            print(f"💥 Starting destruction of deployment: {self.state.state['deployment_id']}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.success = True
            if self.operation == "setup":
                self.state.complete_deployment(True)
                print("✅ Deployment completed successfully")
            elif self.operation == "destroy":
                self.state.complete_destruction()
                print("✅ Destruction completed successfully")
        else:
            if self.operation == "setup":
                self.state.complete_deployment(False)
                print(f"❌ Deployment failed: {exc_val}")
                print("🧹 Starting automatic cleanup...")
                self._cleanup_failed_deployment()
            elif self.operation == "destroy":
                print(f"❌ Destruction failed: {exc_val}")
                print("⚠️  Manual cleanup may be required")
        
        return False  # Don't suppress exceptions
    
    def _cleanup_failed_deployment(self):
        """Cleanup failed deployment"""
        try:
            # Import here to avoid circular imports
            from .gcp_infra import GCPInfrastructure
            from .aws_infra import AWSInfrastructure
            from .azure_infra import AzureInfrastructure
            
            platform = self.state.config.platform.lower()
            if platform == "gcp":
                infra = GCPInfrastructure(self.state.config, self.state)
            elif platform == "aws":
                infra = AWSInfrastructure(self.state.config, self.state)
            elif platform == "azure":
                infra = AzureInfrastructure(self.state.config, self.state)
            else:
                print(f"⚠️  Unknown platform for cleanup: {platform}")
                return
            
            # Try to destroy any created resources
            resources_to_cleanup = self.state.get_resources_by_state(ResourceState.CREATED)
            if resources_to_cleanup:
                print(f"🧹 Cleaning up {len(resources_to_cleanup)} created resources...")
                infra.destroy_infrastructure()
                print("✅ Cleanup completed")
            else:
                print("ℹ️  No resources to cleanup")
                
        except Exception as cleanup_error:
            print(f"❌ Cleanup failed: {cleanup_error}")
            print("⚠️  Manual resource cleanup may be required")