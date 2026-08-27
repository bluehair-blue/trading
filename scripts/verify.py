"""Run the repository's complete, fail-fast verification sequence."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
COVERAGE_FLOOR = 70


def run(command: list[str], environment: dict[str, str]) -> None:
    """Run one verification command from the repository root."""
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, env=environment, check=True)


def main() -> None:
    """Run every required check with the current Python interpreter."""
    environment = os.environ.copy()
    # Make imports independent of the caller's shell and keep child processes
    # on this checkout's source tree.
    environment["PYTHONPATH"] = str(SRC)

    # Keep coverage data outside the checkout so verification never leaves a
    # cache or generated artifact behind.
    with tempfile.TemporaryDirectory(prefix="personal-trader-coverage-") as directory:
        coverage_file = str(Path(directory) / ".coverage")
        environment["PYTHONPYCACHEPREFIX"] = str(Path(directory) / "pycache")
        commands = (
            [sys.executable, "-m", "compileall", "-q", "src", "tests", "scripts"],
            [
                sys.executable,
                "-m",
                "coverage",
                "run",
                "--branch",
                "--source=src",
                f"--data-file={coverage_file}",
                "-m",
                "unittest",
                "discover",
                "-s",
                "tests",
                "-p",
                "test_*.py",
            ],
            [
                sys.executable,
                "-m",
                "coverage",
                "report",
                f"--data-file={coverage_file}",
                f"--fail-under={COVERAGE_FLOOR}",
            ],
            [sys.executable, "-m", "ruff", "check", "src", "tests", "scripts"],
            [sys.executable, "scripts/secret_scan.py", str(ROOT)],
            ["uv", "pip", "check", "--python", sys.executable],
        )
        for command in commands:
            run(command, environment)


if __name__ == "__main__":
    main()
