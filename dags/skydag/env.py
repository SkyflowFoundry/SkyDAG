"""Environment helper for loading .env.local (demo convenience)"""
import os
from pathlib import Path


def load_env_if_available():
    """Load .env.local from project root if python-dotenv is available"""
    try:
        from dotenv import load_dotenv
        
        # Look for .env.local in project root (parent of dags/)
        project_root = Path(__file__).parent.parent.parent
        env_file = project_root / ".env.local"
        
        if env_file.exists():
            load_dotenv(env_file)
            return True
    except ImportError:
        # python-dotenv not installed, skip silently
        pass
    
    return False


# Load .env.local automatically on import
load_env_if_available()