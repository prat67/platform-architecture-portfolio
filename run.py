"""
Top-level orchestrator for multi-provider ingestion pipelines.
Executes each provider in an isolated subprocess to prevent module namespace
collisions (e.g., per-provider config.py and pipeline.py) in sys.modules.
"""
import argparse
import subprocess
import sys
from pathlib import Path

PROVIDERS = [
    "provider_sync_rest",
    "provider_async_jobs",
    "provider_odata_hierarchy",
    "provider_industrial_streams",
    "provider_mailbox_fallback",
]


def run_provider(provider_name: str) -> int:
    repo_root = Path(__file__).resolve().parent
    pipeline_script = repo_root / provider_name / "pipeline.py"

    if not pipeline_script.exists():
        print(f"[ERROR] Pipeline entry point not found: {pipeline_script}", file=sys.stderr)
        return 1

    print(f"[ORCHESTRATOR] Starting isolated subprocess for: {provider_name}")
    result = subprocess.run(
        [sys.executable, str(pipeline_script)],
        cwd=str(repo_root),
        check=False,
    )
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multi-provider ingestion pipelines.")
    parser.add_argument(
        "targets",
        nargs="+",
        help="Provider name(s) to execute, or 'all' to run every registered provider.",
    )
    args = parser.parse_args()

    selected = PROVIDERS if "all" in args.targets else args.targets
    exit_code = 0

    for provider in selected:
        if provider not in PROVIDERS:
            print(f"[ERROR] Unknown provider '{provider}'. Valid options: {PROVIDERS}", file=sys.stderr)
            exit_code = 1
            continue
        rc = run_provider(provider)
        if rc != 0:
            print(f"[ERROR] Provider '{provider}' exited with code {rc}", file=sys.stderr)
            exit_code = rc

    sys.exit(exit_code)


if __name__ == "__main__":
    main()