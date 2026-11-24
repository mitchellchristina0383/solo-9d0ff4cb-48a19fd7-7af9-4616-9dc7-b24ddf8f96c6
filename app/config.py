"""Runtime configuration for the gateway log aggregator."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_first(names: tuple[str, ...]) -> str | None:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip():
            return raw.strip()
    return None


def _env_positive_int(names: tuple[str, ...], default: int) -> int:
    raw = _env_first(names)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{names[0]} must be a positive integer, got {raw!r}") from exc
    if value < 1:
        raise ValueError(f"{names[0]} must be >= 1, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    """Filesystem and feature settings.

    ``data_dir`` is the only directory jobs may read source shards from.
    ``work_dir`` holds per-job temporary part files and manifests.

    ``source_read_concurrency`` is the per-service-process cap on concurrent
    source-shard reads. Initial scans, rescans and repair reads all share the
    same slots (see ``app.scheduler.ReadScheduler``).
    """

    data_dir: Path
    work_dir: Path
    allow_simulation: bool = False
    source_read_concurrency: int = 2

    def validate(self) -> None:
        if not str(self.data_dir):
            raise ValueError("data_dir must not be empty")
        if not str(self.work_dir):
            raise ValueError("work_dir must not be empty")
        if self.source_read_concurrency < 1:
            raise ValueError("source_read_concurrency must be >= 1")


def load_settings() -> Settings:
    settings = Settings(
        data_dir=Path(os.getenv("LOG_DATA_DIR", "/data")).expanduser().resolve(),
        work_dir=Path(os.getenv("WORK_DIR", "/work")).expanduser().resolve(),
        allow_simulation=_env_bool("ALLOW_SIMULATION", False),
        source_read_concurrency=_env_positive_int(
            # Canonical name first; the aliases cover equivalent wording of
            # the same per-process "read-source concurrency cap".
            (
                "SOURCE_READ_CONCURRENCY",
                "READ_SOURCE_CONCURRENCY",
                "SHARD_READ_CONCURRENCY",
                "READ_CONCURRENCY",
                "MAX_CONCURRENT_READS",
                "MAX_SOURCE_READ_CONCURRENCY",
            ),
            2,
        ),
    )
    settings.validate()
    return settings
