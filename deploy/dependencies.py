"""Dependency management for SkyDAG deployment"""
import subprocess
import sys
from typing import Dict, List, Optional


def check_python_version() -> bool:
    """Check if Python version is compatible"""
    min_version = (3, 8)
    current_version = sys.version_info[:2]
    
    if current_version < min_version:
        print(f"❌ Python {min_version[0]}.{min_version[1]}+ required, found {current_version[0]}.{current_version[1]}")
        return False
    
    print(f"✅ Python {current_version[0]}.{current_version[1]} detected")
    return True


def is_package_installed(package_name: str) -> bool:
    """Check if a Python package is installed"""
    # Extract base package name (remove version specifiers and extras)
    base_package = package_name.split('>=')[0].split('==')[0].split('[')[0].strip()
    
    # Try modern importlib.metadata first (Python 3.8+)
    try:
        import importlib.metadata as metadata
        metadata.version(base_package)
        return True
    except ImportError:
        # Fallback to pkg_resources for older Python versions
        try:
            import pkg_resources
            pkg_resources.get_distribution(base_package)
            return True
        except (ImportError, pkg_resources.DistributionNotFound):
            pass
    except metadata.PackageNotFoundError:
        pass
    except Exception:
        pass
    
    # Final fallback to import check
    try:
        # Try common import name transformations
        import_names = [
            base_package.replace('-', '_'),
            base_package.replace('-', '.'),
            base_package
        ]
        
        for import_name in import_names:
            try:
                __import__(import_name)
                return True
            except ImportError:
                continue
        return False
    except Exception:
        return False


def install_package(package: str, description: str = None) -> bool:
    """Install a Python package using pip"""
    desc = description or package
    print(f"📦 Installing {desc}...")
    
    try:
        # Try pip first
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", package
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"✅ Successfully installed {desc}")
            return True
        else:
            # If pip fails, try with --user flag
            print(f"⚠️  Standard pip install failed, trying with --user flag...")
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", "--user", package
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                print(f"✅ Successfully installed {desc} (user install)")
                return True
            else:
                print(f"❌ Failed to install {desc}: {result.stderr}")
                return False
                
    except subprocess.TimeoutExpired:
        print(f"❌ Installation of {desc} timed out")
        return False
    except Exception as e:
        print(f"❌ Error installing {desc}: {e}")
        return False


def get_platform_dependencies(platform: str) -> Dict[str, List[str]]:
    """Get platform-specific dependencies"""
    base_deps = [
        ("python-dotenv", "Environment configuration loader"),
        ("requests", "HTTP client library")
    ]
    
    platform_deps = {
        "gcp": [
            ("google-cloud-storage", "Google Cloud Storage client"),
            ("google-cloud-resource-manager", "Google Cloud Resource Manager client"),
            ("google-cloud-orchestration-airflow", "Google Cloud Composer client"),
            ("google-cloud-service-usage", "Google Cloud Service Usage API client")
        ],
        "aws": [
            ("boto3", "AWS SDK for Python"),
            ("botocore", "AWS SDK core library")
        ],
        "azure": [
            ("azure-mgmt-datafactory", "Azure Data Factory management"),
            ("azure-mgmt-storage", "Azure Storage management"),
            ("azure-mgmt-resource", "Azure Resource management"),
            ("azure-storage-blob", "Azure Blob Storage client"),
            ("azure-identity", "Azure authentication library")
        ]
    }
    
    deps = base_deps.copy()
    if platform.lower() in platform_deps:
        deps.extend(platform_deps[platform.lower()])
    
    return deps


def check_and_install_dependencies(platform: str, force_install: bool = False) -> bool:
    """Check for and install required dependencies"""
    print(f"🔍 Checking dependencies for platform: {platform}")
    
    # Check Python version first
    if not check_python_version():
        return False
    
    # Get required dependencies
    dependencies = get_platform_dependencies(platform)
    missing_deps = []
    
    # Check which dependencies are missing
    for package, description in dependencies:
        if force_install or not is_package_installed(package):
            missing_deps.append((package, description))
        else:
            print(f"✅ {description} already installed")
    
    if not missing_deps and not force_install:
        print("✅ All dependencies are already installed")
        return True
    
    if missing_deps:
        print(f"\n📦 Installing {len(missing_deps)} missing dependencies...")
    
    # Install missing dependencies
    success = True
    for package, description in missing_deps:
        if not install_package(package, description):
            success = False
    
    if success:
        print("✅ All dependencies installed successfully")
    else:
        print("❌ Some dependencies failed to install")
        print("💡 You may need to install them manually or use a virtual environment")
    
    return success


def install_optional_dependencies() -> bool:
    """Install optional dependencies for enhanced functionality"""
    optional_deps = [
        ("pyjwt[crypto]", "JWT token generation for Skyflow authentication")
    ]
    
    print("📦 Installing optional dependencies...")
    success = True
    
    for package, description in optional_deps:
        if not is_package_installed(package):
            if not install_package(package, description):
                print(f"⚠️  Failed to install optional dependency: {description}")
                # Don't fail completely for optional deps
        else:
            print(f"✅ {description} already installed")
    
    return success


def verify_cloud_sdk_auth(platform: str) -> bool:
    """Verify cloud SDK authentication is configured"""
    print(f"🔐 Checking {platform.upper()} authentication...")
    
    try:
        if platform.lower() == "gcp":
            try:
                # Use gcloud CLI to check authentication
                import subprocess
                auth_cmd = ["gcloud", "auth", "list", "--format", "value(account)", "--filter", "status:ACTIVE"]
                result = subprocess.run(auth_cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0 and result.stdout.strip():
                    account = result.stdout.strip().split('\n')[0]
                    
                    # Get current project
                    project_cmd = ["gcloud", "config", "get-value", "project"]
                    project_result = subprocess.run(project_cmd, capture_output=True, text=True, timeout=30)
                    project = project_result.stdout.strip() if project_result.returncode == 0 else "unknown"
                    
                    print(f"✅ GCP credentials found for project: {project}")
                    return True
                else:
                    print("⚠️  No GCP credentials found")
                    print("💡 Run 'gcloud auth application-default login' or 'gcloud auth login'")
                    return False
            except Exception:
                print("⚠️  Could not verify GCP credentials")
                print("💡 Run 'gcloud auth application-default login' or 'gcloud auth login'")
                return False
                
        elif platform.lower() == "aws":
            import boto3
            from botocore.exceptions import NoCredentialsError
            
            try:
                session = boto3.Session()
                sts = session.client('sts')
                identity = sts.get_caller_identity()
                print(f"✅ AWS credentials found for account: {identity.get('Account')}")
                return True
            except NoCredentialsError:
                print("⚠️  No AWS credentials found")
                print("💡 Run 'aws configure' or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
                return False
                
        elif platform.lower() == "azure":
            from azure.identity import DefaultAzureCredential
            from azure.core.exceptions import ClientAuthenticationError
            
            try:
                credential = DefaultAzureCredential()
                # Try to get a token to verify credentials work
                token = credential.get_token("https://management.azure.com/.default")
                print("✅ Azure credentials found")
                return True
            except ClientAuthenticationError:
                print("⚠️  No Azure credentials found")
                print("💡 Run 'az login' or set Azure service principal environment variables")
                return False
                
    except ImportError as e:
        print(f"❌ Missing required library for {platform} authentication: {e}")
        return False
    except Exception as e:
        print(f"⚠️  Could not verify {platform} credentials: {e}")
        return False
    
    return True