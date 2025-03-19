import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator

import psutil  # type: ignore


# pylint: disable=invalid-name
@contextmanager
def TimeTracker(action_name: str) -> Generator[Any, Any, Any]:
    process = psutil.Process()
    start_time = time.time()
    start_memory = process.memory_info().rss / 1024 / 1024  # Convert to MB
    start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{start_timestamp}] Starting '{action_name}'...", end="\r")

    yield

    end_time = time.time()
    end_memory = process.memory_info().rss / 1024 / 1024
    elapsed_time = end_time - start_time
    max_memory_usage = max(start_memory, end_memory)
    end_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(
        f"[{end_timestamp}] Finished '{action_name}' in {elapsed_time:.4f} seconds "
        f"with max memory usage of {max_memory_usage:.2f} MB."
    )
