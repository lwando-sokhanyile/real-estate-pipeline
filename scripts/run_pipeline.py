import subprocess
import sys


def run_stage(name, command):
    print()
    print("=" * 60)
    print(f"STARTING: {name}")
    print("=" * 60)

    result = subprocess.run(command)

    if result.returncode != 0:
        raise RuntimeError(
            f"{name} failed with exit code {result.returncode}"
        )

    print()
    print(f"COMPLETED: {name}")


def main():
    run_stage(
        "Historical PostgreSQL Backfill",
        [sys.executable, "scripts/backfill_historical.py"],
    )


if __name__ == "__main__":
    main()
