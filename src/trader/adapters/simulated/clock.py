from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite

from trader.domain.models import require_utc


@dataclass
class VirtualClock:
    """Deterministic UTC wall and monotonic clock advanced only by the Dry runner."""

    now: datetime
    monotonic_value: float = 0.0

    def __post_init__(self) -> None:
        require_utc(self.now, "now")
        if (
            type(self.monotonic_value) not in (int, float)
            or not isfinite(self.monotonic_value)
            or self.monotonic_value < 0
        ):
            raise ValueError("monotonic_value must be finite and non-negative")
        self.monotonic_value = float(self.monotonic_value)

    def wall(self) -> datetime:
        return self.now

    def monotonic(self) -> float:
        return self.monotonic_value

    def advance_to(self, target: datetime) -> None:
        require_utc(target, "target")
        if target < self.now:
            raise ValueError("virtual clock cannot move backwards")
        self.monotonic_value += (target - self.now).total_seconds()
        self.now = target

    def advance(self, elapsed: timedelta) -> None:
        if type(elapsed) is not timedelta or elapsed < timedelta(0):
            raise ValueError("elapsed must be a non-negative timedelta")
        self.advance_to(self.now + elapsed)
