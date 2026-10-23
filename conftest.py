"""
Pytest configuration and isolated module loader.
Because each provider directory uses identical module names (config.py, pipeline.py),
importing multiple providers inside a single pytest process would collide in sys.modules.
This helper loads provider modules into isolated namespaces for contract verification.
"""
import importlib.util
from pathlib import Path
from types import ModuleType
import pytest


def load_isolated_provider_module(provider_dir: str, module_name: str) -> ModuleType:
    """
    Dynamically loads `<provider_dir>/<module_name>.py` under a unique qualified
    namespace without polluting global `sys.modules[module_name]`.
    """
    repo_root = Path(__file__).resolve().parent
    target_path = repo_root / provider_dir / f"{module_name}.py"

    if not target_path.exists():
        raise FileNotFoundError(f"Module not found at {target_path}")

    qualified_name = f"isolated_{provider_dir}_{module_name}"
    spec = importlib.util.spec_from_file_location(qualified_name, target_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {target_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def provider_loader():
    """Fixture providing isolated module loading to contract unit tests."""
    return load_isolated_provider_module