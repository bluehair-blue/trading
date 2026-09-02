"""Repository-local launcher for the offline Dry backtest entrypoint."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trader.entrypoints.backtest import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
