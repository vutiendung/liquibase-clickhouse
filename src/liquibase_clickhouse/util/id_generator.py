import time
import threading
from datetime import datetime, timezone

# Global variables to track the last generated timestamp and a counter.
# These must be protected by a lock to ensure global uniqueness and monotonicity
# across multiple threads.
_last_timestamp_ms: int = -1
_counter: int = 0
_lock = threading.Lock()

def generate_unique_id_int() -> int:
    """
    Generates a globally unique, monotonically increasing integer ID based on the
    current UTC time in milliseconds.

    If multiple IDs are requested within the same millisecond, a counter is used
    to ensure uniqueness.

    The generated ID is a Python `int`, which dynamically handles arbitrary precision
    and is suitable for storage as an `int64` in most database systems.

    Returns:
        int: A unique, monotonically increasing integer ID.
    """
    global _last_timestamp_ms, _counter # Declare intent to modify global variables

    # Acquire the lock to ensure thread-safe access to _last_timestamp_ms and _counter
    with _lock:
        current_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # If the current millisecond is the same as the last one, increment the counter.
        # Otherwise, reset the counter and update the last timestamp.
        if current_timestamp_ms == _last_timestamp_ms:
            _counter += 1
        else:
            _last_timestamp_ms = current_timestamp_ms
            _counter = 0

        # Combine the millisecond timestamp and the counter to form the unique ID.
        # Multiplying by 1000 provides space for up to 999 unique IDs within a single millisecond.
        # This structure ensures both monotonicity and uniqueness.
        unique_id = (current_timestamp_ms * 1000) + _counter
        return unique_id