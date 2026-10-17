"""Structured key=value logging that log aggregators (Splunk, Log Analytics, Datadog) can parse."""
from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s level=%(levelname)s logger=%(name)s %(message)s"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def kv(**fields: object) -> str:
    """Render fields as key=value pairs: kv(table="orders", rows=10) -> 'table=orders rows=10'."""
    return " ".join(f"{key}={value}" for key, value in fields.items())
