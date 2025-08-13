#!/usr/bin/env python3
"""
SkyDAG Remote Deployment Script

A convenient wrapper script for deploying SkyDAG to cloud Airflow services.
This script automatically handles dependency installation and provides a simple 
interface to the deployment CLI.

Usage:
    python deploy.py [action] [options]
    
Examples:
    python deploy.py deps            # Check and install dependencies
    python deploy.py setup           # Create complete infrastructure
    python deploy.py deploy          # Deploy everything
    python deploy.py undeploy        # Remove deployed DAGs and test files
    python deploy.py trigger         # Trigger DAG (process all files)
    python deploy.py trigger filename.csv  # Trigger DAG for specific file
    python deploy.py destroy         # Destroy all infrastructure
    python deploy.py status          # Show deployment status
    python deploy.py --help          # Show detailed help
"""

import sys
from pathlib import Path

# Add the deploy module to Python path
deploy_dir = Path(__file__).parent / "deploy"
sys.path.insert(0, str(deploy_dir.parent))

try:
    from deploy.cli import main
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("💡 Make sure you have installed the required dependencies:")
    print("   pip install -r requirements.txt")
    sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        sys.exit(1)