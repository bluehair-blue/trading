from collections.abc import Callable
from datetime import datetime
from time import monotonic
from uuid import uuid4


WallClock = Callable[[], datetime]
MonotonicClock = Callable[[], float]

# A monotonic reading is meaningful only inside this Python process.
PROCESS_CLOCK_SESSION_ID = str(uuid4())
system_monotonic: MonotonicClock = monotonic
