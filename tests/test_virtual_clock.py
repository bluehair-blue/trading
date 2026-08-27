from datetime import datetime, timedelta, timezone
import unittest

from trader.adapters.simulated.clock import VirtualClock


class VirtualClockTests(unittest.TestCase):
    def test_wall_and_monotonic_time_advance_together(self) -> None:
        start = datetime(2026, 8, 27, tzinfo=timezone.utc)
        clock = VirtualClock(start, 10)
        clock.advance(timedelta(seconds=2.5))
        self.assertEqual(clock.wall(), start + timedelta(seconds=2.5))
        self.assertEqual(clock.monotonic(), 12.5)
        clock.advance_to(start + timedelta(seconds=5))
        self.assertEqual(clock.monotonic(), 15)

    def test_invalid_or_backward_time_is_rejected(self) -> None:
        start = datetime(2026, 8, 27, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            VirtualClock(start.replace(tzinfo=None))
        with self.assertRaises(ValueError):
            VirtualClock(start, float("nan"))
        clock = VirtualClock(start)
        with self.assertRaises(ValueError):
            clock.advance_to(start - timedelta(seconds=1))


if __name__ == "__main__":
    unittest.main()
