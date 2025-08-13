"""Azure infrastructure setup and teardown"""
import time
import json
from typing import Dict, List, Optional

try:
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.storage import StorageManagementClient
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.authorization import AuthorizationManagementClient
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
    HAS_AZURE_SDK = True
except ImportError:
    HAS_AZURE_SDK = False

from .state import DeploymentState, ResourceState


class AzureInfrastructure:
    """Manages Azure infrastructure for SkyDAG"""
    
    def __init__(self, config, state: DeploymentState):
        if not HAS_AZURE_SDK:
            raise ImportError("Azure SDK not installed. Run: pip install azure-mgmt-datafactory azure-mgmt-storage azure-mgmt-resource azure-storage-blob azure-identity")
        
        self.config = config
        self.state = state
        self.subscription_id = config.azure_subscription_id
        self.resource_group = config.azure_resource_group
        self.location = getattr(config, 'azure_location', 'East US')
        
        # Setup Azure credentials
        if hasattr(config, 'azure_client_id') and config.azure_client_id:
            self.credential = ClientSecretCredential(
                tenant_id=config.azure_tenant_id,
                client_id=config.azure_client_id,
                client_secret=config.azure_client_secret
            )
        else:
            self.credential = DefaultAzureCredential()
        
        # Initialize clients
        try:
            self.resource_client = ResourceManagementClient(
                self.credential, 
                self.subscription_id
            )
            self.storage_client = StorageManagementClient(
                self.credential,
                self.subscription_id
            )
            self.df_client = DataFactoryManagementClient(
                self.credential, 
                self.subscription_id
            )
            
            # For blob operations
            if hasattr(config, 'azure_storage_account'):
                storage_account_url = f"https://{config.azure_storage_account}.blob.core.windows.net"
                self.blob_client = BlobServiceClient(
                    account_url=storage_account_url,
                    credential=self.credential
                )
        except Exception as e:
            raise ValueError(f"Failed to initialize Azure clients: {e}")
    
    def setup_infrastructure(self) -> bool:
        """Create all required Azure infrastructure"""
        try:
            print(f"🔧 Setting up Azure infrastructure")
            print(f"   Subscription: {self.subscription_id}")
            print(f"   Resource Group: {self.resource_group}")
            
            # 1. Create resource group
            if not self._create_resource_group():
                return False
            
            # 2. Create storage account
            if not self._create_storage_account():
                return False
            
            # 3. Create blob containers
            if not self._create_blob_containers():
                return False
            
            # 4. Create Data Factory
            if not self._create_data_factory():
                return False
            
            print("✅ Azure infrastructure setup completed successfully")
            return True
            
        except Exception as e:
            print(f"❌ Azure infrastructure setup failed: {e}")
            raise
    
    def destroy_infrastructure(self) -> bool:
        """Destroy all Azure infrastructure"""
        try:
            print(f"💥 Destroying Azure infrastructure")
            print(f"   Subscription: {self.subscription_id}")
            print(f"   Resource Group: {self.resource_group}")
            
            # Destroy in reverse order of creation
            success = True
            
            # 1. Delete Data Factory
            if not self._delete_data_factory():
                success = False
            
            # 2. Delete blob containers
            if not self._delete_blob_containers():
                success = False
            
            # 3. Delete storage account
            if not self._delete_storage_account():
                success = False
            
            # 4. Delete resource group (if we created it)
            if not self._delete_resource_group():
                success = False
            
            if success:
                print("✅ Azure infrastructure destroyed successfully")
            else:
                print("⚠️  Some Azure resources may not have been fully destroyed")
            
            return success
            
        except Exception as e:
            print(f"❌ Azure infrastructure destruction failed: {e}")
            raise
    
    def _create_resource_group(self) -> bool:
        """Create resource group"""
        try:
            self.state.add_resource("resource-group", self.resource_group, {
                "name": self.resource_group,
                "location": self.location
            })
            
            # Check if resource group exists
            try:
                rg = self.resource_client.resource_groups.get(self.resource_group)
                print(f"📁 Resource group {self.resource_group} already exists")
                self.state.mark_resource_created("resource-group", self.resource_group, {
                    "exists": True,
                    "location": rg.location
                })
                return True
            except ResourceNotFoundError:
                pass
            
            print(f"📁 Creating resource group: {self.resource_group}")
            
            rg_params = {
                'location': self.location,
                'tags': {
                    'SkyDAG-Deployment': self.state.state["deployment_id"],
                    'Created-By': 'skydag'
                }
            }
            
            result = self.resource_client.resource_groups.create_or_update(
                self.resource_group,
                rg_params
            )
            
            print(f"✅ Created resource group: {self.resource_group}")
            self.state.mark_resource_created("resource-group", self.resource_group, {
                "exists": False,
                "location": result.location,
                "id": result.id
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to create resource group {self.resource_group}: {e}"
            print(f"❌ {error_msg}")
            self.state.mark_resource_failed("resource-group", self.resource_group, error_msg)
            return False
    
    def _create_storage_account(self) -> bool:
        """Create storage account"""
        storage_account_name = self.config.azure_storage_account
        
        try:
            self.state.add_resource("storage-account", storage_account_name, {
                "name": storage_account_name,
                "resource_group": self.resource_group,
                "location": self.location
            })
            
            # Check if storage account exists
            try:
                account = self.storage_client.storage_accounts.get_properties(
                    self.resource_group,
                    storage_account_name
                )
                print(f"💾 Storage account {storage_account_name} already exists")
                self.state.mark_resource_created("storage-account", storage_account_name, {
                    "exists": True,
                    "location": account.location,
                    "endpoint": account.primary_endpoints.blob
                })
                return True
            except ResourceNotFoundError:
                pass
            
            print(f"💾 Creating storage account: {storage_account_name}")
            
            storage_params = {
                'sku': {'name': 'Standard_LRS'},
                'kind': 'StorageV2',
                'location': self.location,
                'tags': {
                    'SkyDAG-Deployment': self.state.state["deployment_id"],
                    'Created-By': 'skydag'
                }
            }
            
            # Start creation
            async_operation = self.storage_client.storage_accounts.begin_create(
                self.resource_group,
                storage_account_name,
                storage_params
            )
            
            # Wait for completion
            print("⏳ Waiting for storage account creation...")
            result = async_operation.result()
            
            print(f"✅ Created storage account: {storage_account_name}")
            self.state.mark_resource_created("storage-account", storage_account_name, {
                "exists": False,
                "location": result.location,
                "endpoint": result.primary_endpoints.blob,
                "id": result.id
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to create storage account {storage_account_name}: {e}"
            print(f"❌ {error_msg}")
            self.state.mark_resource_failed("storage-account", storage_account_name, error_msg)
            return False
    
    def _create_blob_containers(self) -> bool:
        """Create blob containers"""
        containers = [
            {
                "name": self.config.dag_bucket_or_container,
                "type": "dag-container",
                "description": "DAG files container"
            }
        ]
        
        # Add source/dest containers if they don't exist
        if hasattr(self.config, 'get_airflow_variables'):
            variables = self.config.get_airflow_variables()
            source = variables.get('SKYDAG_SOURCE', '').split('/')[0]
            dest = variables.get('SKYDAG_DEST', '').split('/')[0]
            
            if source and source != self.config.dag_bucket_or_container:
                containers.append({
                    "name": source,
                    "type": "source-container", 
                    "description": "Source files container"
                })
            
            if dest and dest != self.config.dag_bucket_or_container and dest != source:
                containers.append({
                    "name": dest,
                    "type": "dest-container",
                    "description": "Processed files container"
                })
        
        for container_info in containers:
            container_name = container_info["name"]
            
            try:
                self.state.add_resource("blob-container", container_name, container_info)
                
                # Check if container exists
                try:
                    container_client = self.blob_client.get_container_client(container_name)
                    container_client.get_container_properties()
                    print(f"📦 Container {container_name} already exists")
                    self.state.mark_resource_created("blob-container", container_name, {
                        "exists": True,
                        "account": self.config.azure_storage_account
                    })
                    continue
                except ResourceNotFoundError:
                    pass
                
                print(f"📦 Creating blob container: {container_name}")
                
                container_client = self.blob_client.get_container_client(container_name)
                container_client.create_container(
                    metadata={
                        'skydag-deployment': self.state.state["deployment_id"],
                        'skydag-type': container_info["type"],
                        'created-by': 'skydag'
                    }
                )
                
                print(f"✅ Created blob container: {container_name}")
                self.state.mark_resource_created("blob-container", container_name, {
                    "exists": False,
                    "account": self.config.azure_storage_account,
                    "url": f"https://{self.config.azure_storage_account}.blob.core.windows.net/{container_name}"
                })
                
            except Exception as e:
                error_msg = f"Failed to create blob container {container_name}: {e}"
                print(f"❌ {error_msg}")
                self.state.mark_resource_failed("blob-container", container_name, error_msg)
                return False
        
        return True
    
    def _create_data_factory(self) -> bool:
        """Create Data Factory"""
        df_name = self.config.azure_data_factory
        
        try:
            self.state.add_resource("data-factory", df_name, {
                "name": df_name,
                "resource_group": self.resource_group,
                "location": self.location
            })
            
            # Check if data factory exists
            try:
                df = self.df_client.factories.get(self.resource_group, df_name)
                print(f"🏭 Data Factory {df_name} already exists")
                self.state.mark_resource_created("data-factory", df_name, {
                    "exists": True,
                    "location": df.location
                })
                return True
            except ResourceNotFoundError:
                pass
            
            print(f"🏭 Creating Data Factory: {df_name}")
            
            df_params = {
                'location': self.location,
                'tags': {
                    'SkyDAG-Deployment': self.state.state["deployment_id"],
                    'Created-By': 'skydag'
                }
            }
            
            result = self.df_client.factories.create_or_update(
                self.resource_group,
                df_name,
                df_params
            )
            
            print(f"✅ Created Data Factory: {df_name}")
            self.state.mark_resource_created("data-factory", df_name, {
                "exists": False,
                "location": result.location,
                "id": result.id
            })
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to create Data Factory {df_name}: {e}"
            print(f"❌ {error_msg}")
            self.state.mark_resource_failed("data-factory", df_name, error_msg)
            return False
    
    def _delete_blob_containers(self) -> bool:
        """Delete blob containers"""
        success = True
        
        container_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "blob-container" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in container_resources:
            container_name = resource["name"]
            
            try:
                self.state.update_resource("blob-container", container_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"🗑️  Deleting blob container: {container_name}")
                    
                    container_client = self.blob_client.get_container_client(container_name)
                    
                    # Delete all blobs first
                    blobs = container_client.list_blobs()
                    for blob in blobs:
                        container_client.delete_blob(blob.name)
                    
                    # Delete container
                    container_client.delete_container()
                    print(f"✅ Deleted blob container: {container_name}")
                else:
                    print(f"📦 Skipping pre-existing container: {container_name}")
                
                self.state.mark_resource_destroyed("blob-container", container_name)
                
            except Exception as e:
                print(f"❌ Failed to delete blob container {container_name}: {e}")
                success = False
        
        return success
    
    def _delete_storage_account(self) -> bool:
        """Delete storage account"""
        storage_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "storage-account" and r["state"] == ResourceState.CREATED.value
        ]
        
        success = True
        
        for resource in storage_resources:
            storage_name = resource["name"]
            
            try:
                self.state.update_resource("storage-account", storage_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"💾 Deleting storage account: {storage_name}")
                    
                    self.storage_client.storage_accounts.delete(
                        self.resource_group,
                        storage_name
                    )
                    
                    print(f"✅ Deleted storage account: {storage_name}")
                else:
                    print(f"💾 Skipping pre-existing storage account: {storage_name}")
                
                self.state.mark_resource_destroyed("storage-account", storage_name)
                
            except Exception as e:
                print(f"❌ Failed to delete storage account {storage_name}: {e}")
                success = False
        
        return success
    
    def _delete_data_factory(self) -> bool:
        """Delete Data Factory"""
        df_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "data-factory" and r["state"] == ResourceState.CREATED.value
        ]
        
        success = True
        
        for resource in df_resources:
            df_name = resource["name"]
            
            try:
                self.state.update_resource("data-factory", df_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"🏭 Deleting Data Factory: {df_name}")
                    
                    self.df_client.factories.delete(
                        self.resource_group,
                        df_name
                    )
                    
                    print(f"✅ Deleted Data Factory: {df_name}")
                else:
                    print(f"🏭 Skipping pre-existing Data Factory: {df_name}")
                
                self.state.mark_resource_destroyed("data-factory", df_name)
                
            except Exception as e:
                print(f"❌ Failed to delete Data Factory {df_name}: {e}")
                success = False
        
        return success
    
    def _delete_resource_group(self) -> bool:
        """Delete resource group"""
        rg_resources = [
            r for r in self.state.state["resources"].values()
            if r["type"] == "resource-group" and r["state"] == ResourceState.CREATED.value
        ]
        
        for resource in rg_resources:
            rg_name = resource["name"]
            
            try:
                self.state.update_resource("resource-group", rg_name, ResourceState.DESTROYING)
                
                # Only delete if we created it
                if not resource["data"].get("exists", False):
                    print(f"📁 Deleting resource group: {rg_name}")
                    print("⏳ This may take several minutes...")
                    
                    # Start deletion
                    async_operation = self.resource_client.resource_groups.begin_delete(rg_name)
                    
                    # Wait for completion
                    async_operation.result()
                    
                    print(f"✅ Deleted resource group: {rg_name}")
                else:
                    print(f"📁 Skipping pre-existing resource group: {rg_name}")
                
                self.state.mark_resource_destroyed("resource-group", rg_name)
                return True
                
            except Exception as e:
                print(f"❌ Failed to delete resource group {rg_name}: {e}")
                return False
        
        return True