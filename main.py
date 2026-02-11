import sys
import subprocess
from pathlib import Path
import fnmatch
import argparse
import os


MODELS_DIR = Path(__file__).parent / "models"


def get_all_models():
    """Get all model names"""
    return [f.stem for f in MODELS_DIR.glob("*.py") if f.stem != "__init__"]


def filter_models(pattern: str):
    """Filter models by pattern (supports wildcards)"""
    all_models = get_all_models()
    if "*" in pattern or "?" in pattern:
        return [m for m in all_models if fnmatch.fnmatch(m, pattern)]
    return [pattern] if pattern in all_models else []


def list_models(pattern: str = None):
    """List available models, optionally filtered by pattern"""
    if pattern:
        models = filter_models(pattern)
        print(f"Models matching '{pattern}':")
    else:
        models = get_all_models()
        print("Available models:")

    for model in sorted(models):
        print(f"  - {model}")

    return models


def run_model(model_name: str):
    """Run a single model by name"""
    model_path = MODELS_DIR / f"{model_name}.py"

    if not model_path.exists():
        print(f"Error: Model '{model_name}' not found")
        return False

    print(f"\n{'=' * 60}")
    print(f"Running: {model_name}")
    print("=" * 60)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parent.resolve())

    result = subprocess.run([sys.executable, str(model_path)], env=env)

    if result.returncode != 0:
        print(f"✗ {model_name} failed with exit code {result.returncode}")
        return False

    print(f"✓ {model_name} completed")
    return True


def run_pattern(pattern: str):
    """Run all models matching pattern"""
    models = filter_models(pattern)

    if not models:
        print(f"No models match pattern: {pattern}")
        sys.exit(1)

    print(f"Found {len(models)} model(s) matching '{pattern}':")
    for model in models:
        print(f"  - {model}")
    print()

    successes = 0
    failures = 0

    for model in models:
        if run_model(model):
            successes += 1
        else:
            failures += 1

    print(f"\n{'=' * 60}")
    print(f"Summary: {successes} succeeded, {failures} failed")
    print("=" * 60)

    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run ingest models by name or pattern",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --select-models "rk_*"
  python main.py --select-models rk_dim_levfakt_koppl
  python main.py --list
  python main.py --list "ek_*"
        """,
    )

    parser.add_argument(
        "--select-models",
        metavar="PATTERN",
        help="Run models matching pattern (supports wildcards)",
    )

    parser.add_argument(
        "--list",
        nargs="?",
        const="",
        metavar="PATTERN",
        help="List available models, optionally filtered by pattern",
    )

    args = parser.parse_args()

    if args.list is not None:
        pattern = args.list if args.list else None
        list_models(pattern)
    elif args.select_models:
        run_pattern(args.select_models)
    else:
        parser.print_help()
        sys.exit(1)
