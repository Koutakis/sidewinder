import sys


def print_header(name: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"Running: {name}")
    print("=" * 60)


def print_model_list(models: dict) -> None:
    print(f"Found {len(models)} model(s):")
    for name in sorted(models):
        print(f"  - {name}")
    print()


def print_summary(successes: int, failures: int) -> None:
    print(f"\n{'=' * 60}")
    print(f"Summary: {successes} succeeded, {failures} failed")
    print("=" * 60)


def print_success(name: str) -> None:
    print(f"✓ {name} completed")


def print_failure(name: str, error: Exception) -> None:
    print(f"✗ {name} failed: {error}")


def exit_with_error(message: str) -> None:
    print(f"Error: {message}")
    sys.exit(1)
