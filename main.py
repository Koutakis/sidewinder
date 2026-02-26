import sys
import importlib
import os
from pathlib import Path
from core.logger import print_header, print_model_list, print_summary, print_failure, exit_with_error

MODELS_DIR = Path(__file__).parent / "models"


def discover_models() -> dict:
    models = {}
    for root, dirs, files in os.walk(MODELS_DIR):
        for name in files:
            if name.endswith(".py") and name != "__init__.py":
                path = Path(root) / name
                module_path = path.relative_to(MODELS_DIR).with_suffix("").as_posix().replace("/", ".")
                models[module_path] = f"models.{module_path}"
    return models


def main():
    available = discover_models()
    tag_filter = os.environ.get("TAGS")
    tags = [t.strip() for t in tag_filter.split(",")] if tag_filter else []

    if not available:
        exit_with_error("No models found")

    if tags:
        filtered = {}
        for name, import_path in available.items():
            module = importlib.import_module(import_path)
            if any(t in module.config.tags for t in tags):
                filtered[name] = import_path
        available = filtered

    print_model_list(available)

    successes = 0
    failures = 0

    for name, import_path in sorted(available.items()):
        try:
            module = importlib.import_module(import_path)
            print_header(module.config.name)
            module.execute()
            successes += 1
        except Exception as e:
            print_failure(name, e)
            failures += 1

    print_summary(successes, failures)
    sys.exit(0 if failures == 0 else 1)


main()
