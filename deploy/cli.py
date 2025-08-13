"""Command-line interface for SkyDAG deployment"""
import argparse
import sys
from pathlib import Path

from .config import DeploymentConfig
from .dependencies import check_and_install_dependencies, verify_cloud_sdk_auth, install_optional_dependencies
from .infrastructure.state import DeploymentState, AtomicOperation


def setup_infrastructure(config: DeploymentConfig, skip_deps: bool = False) -> bool:
    """Set up complete infrastructure atomically"""
    state = DeploymentState(config)
    
    if not state.is_clean():
        print(f"❌ Environment is not clean. Current status: {state.state['status']}")
        print("💡 Run 'destroy' first to clean up existing deployment")
        return False
    
    platform = config.platform.lower()
    
    # Check and install dependencies
    if not skip_deps:
        print("🔧 Checking and installing required dependencies...")
        if not check_and_install_dependencies(platform):
            print("❌ Dependency installation failed")
            return False
        
        # Verify cloud authentication
        if not verify_cloud_sdk_auth(platform):
            print("❌ Cloud authentication not configured")
            return False
    
    try:
        with AtomicOperation(state, "setup"):
            if platform == "gcp":
                from .infrastructure.gcp_infra import GCPInfrastructure
                infra = GCPInfrastructure(config, state)
            elif platform == "aws":
                from .infrastructure.aws_infra import AWSInfrastructure
                infra = AWSInfrastructure(config, state)
            elif platform == "azure":
                from .infrastructure.azure_infra import AzureInfrastructure
                infra = AzureInfrastructure(config, state)
            else:
                raise ValueError(f"Unsupported platform: {platform}")
            
            return infra.setup_infrastructure()
    
    except Exception as e:
        print(f"❌ Infrastructure setup failed: {e}")
        return False


def undeploy_and_destroy(config: DeploymentConfig, dags_path: Path) -> bool:
    """Complete teardown: undeploy DAGs then destroy infrastructure"""
    print("🗑️ Starting complete teardown: undeploy + destroy")
    print("=" * 50)
    
    # Step 1: Undeploy DAGs and clean up files
    print("📋 Step 1/2: Undeploying DAGs and cleaning up files...")
    try:
        undeploy_success = deploy_to_platform(config, dags_path, "undeploy")
        if not undeploy_success:
            print("⚠️  Undeploy failed, but continuing with infrastructure destruction...")
    except Exception as e:
        print(f"⚠️  Undeploy error: {e}, continuing with infrastructure destruction...")
    
    print("\n" + "=" * 50)
    
    # Step 2: Destroy infrastructure
    print("💥 Step 2/2: Destroying infrastructure...")
    return destroy_infrastructure(config)


def destroy_infrastructure(config: DeploymentConfig) -> bool:
    """Destroy complete infrastructure atomically"""
    state = DeploymentState(config)
    
    if state.is_clean():
        print("ℹ️  No infrastructure to destroy - environment is already clean")
        return True
    
    platform = config.platform.lower()
    
    try:
        with AtomicOperation(state, "destroy"):
            if platform == "gcp":
                from .infrastructure.gcp_infra import GCPInfrastructure
                infra = GCPInfrastructure(config, state)
            elif platform == "aws":
                from .infrastructure.aws_infra import AWSInfrastructure
                infra = AWSInfrastructure(config, state)
            elif platform == "azure":
                from .infrastructure.azure_infra import AzureInfrastructure
                infra = AzureInfrastructure(config, state)
            else:
                raise ValueError(f"Unsupported platform: {platform}")
            
            return infra.destroy_infrastructure()
    
    except Exception as e:
        print(f"❌ Infrastructure destruction failed: {e}")
        return False


def show_status(config: DeploymentConfig) -> bool:
    """Show deployment status"""
    state = DeploymentState(config)
    summary = state.get_deployment_summary()
    
    print("📊 SkyDAG Deployment Status")
    print("=" * 40)
    
    if state.is_clean():
        print("✅ Environment is clean - no resources deployed")
        return True
    
    print(f"🆔 Deployment ID: {summary['deployment_id']}")
    print(f"☁️  Platform: {summary['platform']}")
    print(f"🏷️  Environment: {summary['environment']}")
    print(f"📊 Status: {summary['status']}")
    print(f"📦 Total Resources: {summary['total_resources']}")
    print(f"✅ Created: {summary['created_resources']}")
    print(f"❌ Failed: {summary['failed_resources']}")
    
    if summary['created_at']:
        import datetime
        created_time = datetime.datetime.fromtimestamp(summary['created_at'])
        print(f"📅 Created: {created_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if summary['last_updated']:
        import datetime
        updated_time = datetime.datetime.fromtimestamp(summary['last_updated'])
        print(f"🔄 Last Updated: {updated_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Show resource details
    if summary['total_resources'] > 0:
        print("\n📋 Resource Details:")
        print("-" * 40)
        
        for resource_id, resource in state.state["resources"].items():
            status_emoji = {
                "created": "✅",
                "creating": "⏳",
                "failed": "❌",
                "destroying": "💥",
                "destroyed": "🗑️"
            }.get(resource["state"], "❓")
            
            print(f"{status_emoji} {resource['type']}: {resource['name']} ({resource['state']})")
            
            if resource["state"] == "failed" and "error" in resource["data"]:
                print(f"    Error: {resource['data']['error']}")
    
    return True


def deploy_to_platform(config: DeploymentConfig, dags_path: Path, action: str, filename: str = None) -> bool:
    """Deploy to the specified platform"""
    platform = config.platform.lower()
    
    if platform == "gcp":
        from .gcp_composer import ComposerDeployer
        deployer = ComposerDeployer(config)
    elif platform == "aws":
        from .aws_mwaa import MWAADeployer
        deployer = MWAADeployer(config)
    elif platform == "azure":
        from .azure_data_factory import AzureDataFactoryDeployer
        deployer = AzureDataFactoryDeployer(config)
    else:
        print(f"❌ Unsupported platform: {platform}")
        return False
    
    if action == "deploy":
        return deployer.deploy_full(dags_path)
    elif action == "upload":
        return deployer.upload_dags(dags_path)
    elif action == "variables":
        variables = config.get_airflow_variables()
        if variables:
            return deployer.set_airflow_variables(variables)
        else:
            print("⚠️  No variables to set")
            return True
    elif action == "trigger":
        return deployer.trigger_dag(filename=filename)
    elif action == "undeploy":
        return deployer.undeploy_full()
    else:
        print(f"❌ Unknown action: {action}")
        return False


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Deploy SkyDAG to cloud Airflow services",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dependency management
  python -m deploy.cli deps      # Check and install dependencies
  
  # Infrastructure management
  python -m deploy.cli setup                 # Create complete infrastructure
  python -m deploy.cli destroy               # Destroy all infrastructure
  python -m deploy.cli undeploy-and-destroy  # Undeploy DAGs then destroy infrastructure
  python -m deploy.cli status                # Show deployment status
  
  # DAG deployment (requires infrastructure)
  python -m deploy.cli deploy    # Deploy everything
  python -m deploy.cli undeploy  # Remove deployed DAGs and test files
  python -m deploy.cli upload    # Just upload DAG files
  python -m deploy.cli variables # Just set Airflow variables
  python -m deploy.cli trigger   # Trigger DAG execution
  
  # Advanced options
  python -m deploy.cli setup --skip-deps     # Skip dependency checks
  python -m deploy.cli deps --force-install  # Force reinstall dependencies
        """
    )
    
    parser.add_argument(
        "action",
        choices=["setup", "destroy", "undeploy-and-destroy", "status", "deploy", "undeploy", "upload", "variables", "trigger", "deps"],
        help="Action to perform"
    )
    
    parser.add_argument(
        "--dags-path",
        type=Path,
        default=Path(__file__).parent.parent / "dags",
        help="Path to DAGs directory (default: ./dags)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deployed without actually deploying"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    parser.add_argument(
        "--skip-deps",
        action="store_true",
        help="Skip dependency installation and verification"
    )
    
    parser.add_argument(
        "--force-install",
        action="store_true",
        help="Force reinstall all dependencies"
    )
    
    parser.add_argument(
        "filename",
        nargs="?",
        help="Filename to process (for trigger action only). If not specified, processes all files."
    )
    
    args = parser.parse_args()
    
    # Validate dags path for DAG operations only
    dag_operations = ["deploy", "undeploy", "undeploy-and-destroy", "upload", "variables", "trigger"]
    if args.action in dag_operations:
        if not args.dags_path.exists():
            print(f"❌ DAGs path does not exist: {args.dags_path}")
            sys.exit(1)
        
        if not (args.dags_path / "skydag_pipeline.py").exists():
            print(f"❌ skydag_pipeline.py not found in: {args.dags_path}")
            sys.exit(1)
    
    # Load configuration
    try:
        config = DeploymentConfig()
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure you have set up .env.local with deployment configuration")
        sys.exit(1)
    
    if args.verbose:
        print(f"Platform: {config.platform}")
        print(f"Environment: {config.environment}")
        print(f"DAGs path: {args.dags_path}")
        print(f"Action: {args.action}")
        print()
    
    if args.dry_run:
        print("🔍 DRY RUN - No actual deployment will happen")
        variables = config.get_airflow_variables()
        print(f"Would deploy to: {config.platform}")
        print(f"DAG files found: {len(list(args.dags_path.rglob('*.py')))}")
        print(f"Variables to set: {len(variables)}")
        if args.verbose and variables:
            for key in variables:
                print(f"  - {key}")
        return
    
    # Perform action
    try:
        if args.action == "deps":
            # Just check/install dependencies
            success = check_and_install_dependencies(config.platform, args.force_install)
            if success:
                install_optional_dependencies()
                verify_cloud_sdk_auth(config.platform)
        elif args.action == "setup":
            success = setup_infrastructure(config, args.skip_deps)
        elif args.action == "destroy":
            success = destroy_infrastructure(config)
        elif args.action == "undeploy-and-destroy":
            success = undeploy_and_destroy(config, args.dags_path)
        elif args.action == "status":
            success = show_status(config)
        else:
            # For DAG operations, check dependencies first unless skipped
            if not args.skip_deps:
                if not check_and_install_dependencies(config.platform):
                    print("❌ Dependency check failed")
                    sys.exit(1)
            success = deploy_to_platform(config, args.dags_path, args.action, args.filename)
        
        if success:
            print(f"✅ {args.action.capitalize()} completed successfully!")
        else:
            print(f"❌ {args.action.capitalize()} failed!")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Operation failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()