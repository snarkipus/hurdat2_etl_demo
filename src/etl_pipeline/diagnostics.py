"""Run-owned JSON diagnostics; stage and library callers keep stdlib logging."""

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog
from structlog.types import EventDict


def _event_context(logger: Any, method: str, event: EventDict) -> EventDict:
    event["severity"] = event.pop("level")
    exc_info = event.get("exc_info")
    if isinstance(exc_info, tuple) and exc_info[1] is not None:
        event["error_type"] = type(exc_info[1]).__name__
        event["error"] = str(exc_info[1])
    return event


@contextmanager
def run_diagnostics(level: int) -> Iterator[None]:
    """Attach one filtered bridge and release it, restoring the caller's context.

    Existing host handlers and library-specific thresholds are left intact.
    No global structlog configuration is needed for the stdlib formatter bridge.
    """
    path = Path("logs/pipeline.log")
    path.parent.mkdir(exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ExtraAdder(),
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                _event_context,
                structlog.processors.format_exc_info,
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
    )
    root = logging.getLogger()
    previous_level = root.level
    context = structlog.contextvars.get_contextvars()
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(run_id=uuid4().hex, operation="preflight")
    root.setLevel(level)
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)
        root.setLevel(previous_level)
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(**context)
        try:
            handler.close()
        except Exception as error:
            # The persistence outcome is already determined. Do not send this
            # warning back through the failed handler or the caller's logging.
            try:
                print(
                    f"Warning: Could not close pipeline log: {error}", file=sys.stderr
                )
            except Exception:  # noqa: S110 - warning sink must not mask the outcome
                pass
