from __future__ import annotations

import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


@dataclass
class Settings:
    db_path: str = field(
        default_factory=lambda: os.environ.get("GIS_DB_PATH", "/data/gis.db")
    )
    busy_timeout_s: float = field(
        default_factory=lambda: _float("GIS_BUSY_TIMEOUT_S", 30.0)
    )
    max_commit_attempts: int = field(
        default_factory=lambda: _int("GIS_MAX_COMMIT_ATTEMPTS", 12)
    )
    # Bounded admission for POST /api/v1/batches.  At most this many batches
    # *execute* (each occupies one worker of the shared 40-thread anyio pool);
    # further accepted batches wait in-process without holding a thread or a
    # database connection.  Accepted in-flight requests per process =
    # executor slots + queue capacity = 8 + 32 = 40 by default.
    batch_executor_slots: int = field(
        default_factory=lambda: _int("GIS_BATCH_EXECUTOR_SLOTS", 8)
    )
    batch_queue_capacity: int = field(
        default_factory=lambda: _int("GIS_BATCH_QUEUE_CAPACITY", 32)
    )

settings = Settings()
