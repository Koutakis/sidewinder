import sys
import importlib
import os
from pathlib import Path
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn
from core import run
from core.logger import print_header, print_model_list, print_summary, print_success, print_failure, exit_with_error

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


@with_env_config
def main(env: EnvConfig):
    available = discover_models()
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")

    if not available:
        exit_with_error("No models found")

    if not dest_dsn:
        exit_with_error("DEST_ENV not set")

    # Filter by tags
    if env.tags:
        filtered = {}
        for name, import_path in available.items():
            module = importlib.import_module(import_path)
            if any(t in module.config.tags for t in env.tags):
                filtered[name] = import_path
        available = filtered

    # Filter by model names
    if env.models:
        available = {k: v for k, v in available.items() if k in env.models}

    print_model_list(available)

    successes = 0
    failures = 0

    for name, import_path in sorted(available.items()):
        try:
            module = importlib.import_module(import_path)
            print_header(module.config.name)
            run(module.config, module.execute, env, dest_dsn)
            print_success(module.config.name)
            successes += 1
        except Exception as e:
            print_failure(name, e)
            failures += 1

    print_summary(successes, failures)
    sys.exit(0 if failures == 0 else 1)


main()
