"""Typed, validated pipeline configuration loaded from YAML."""
from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_FORMATS = ("delta", "parquet")
REQUIRED_LAYERS = ("landing", "bronze", "silver", "gold", "quarantine", "audit")


class ConfigError(ValueError):
    """Raised when the pipeline configuration is invalid."""


@dataclass(frozen=True)
class PipelineConfig:
    app_name: str
    env: str
    base_path: str
    storage_format: str
    spark_master: str | None
    layers: dict[str, str]
    spark_conf: dict[str, str]
    max_reject_ratio: float
    valid_order_statuses: tuple[str, ...]
    cancelled_statuses: tuple[str, ...]
    valid_customer_tiers: tuple[str, ...]
    fx_rates_to_inr: dict[str, float]
    segment_thresholds_inr: dict[str, float]

    def path(self, layer: str, *parts: str) -> str:
        """Build a storage path, e.g. cfg.path("silver", "orders")."""
        if layer not in self.layers:
            raise ConfigError(f"Unknown layer '{layer}'")
        root = f"{self.base_path.rstrip('/')}/{self.layers[layer].strip('/')}"
        return "/".join([root, *parts]) if parts else root

    @property
    def is_delta(self) -> bool:
        return self.storage_format == "delta"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(config_path: str | Path, env: str) -> PipelineConfig:
    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
    environments = raw.pop("environments", {}) or {}
    if env not in environments:
        raise ConfigError(f"Environment '{env}' not defined. Known: {sorted(environments)}")
    merged = _deep_merge(raw, environments[env] or {})

    storage_format = os.getenv("SHOPSTREAM_STORAGE_FORMAT", merged.get("storage_format", "delta")).lower()
    if storage_format not in SUPPORTED_FORMATS:
        raise ConfigError(f"storage_format must be one of {SUPPORTED_FORMATS}, got '{storage_format}'")

    layers = merged.get("layers", {})
    missing = [layer for layer in REQUIRED_LAYERS if layer not in layers]
    if missing:
        raise ConfigError(f"Missing layer paths in config: {missing}")

    quality = merged.get("quality", {})
    max_reject_ratio = float(quality.get("max_reject_ratio", 0.05))
    if not 0.0 <= max_reject_ratio <= 1.0:
        raise ConfigError("quality.max_reject_ratio must be between 0 and 1")

    business = merged.get("business", {})
    fx = {k.upper(): float(v) for k, v in business.get("fx_rates_to_inr", {"INR": 1.0}).items()}
    if any(rate <= 0 for rate in fx.values()):
        raise ConfigError("All fx_rates_to_inr must be positive")

    return PipelineConfig(
        app_name=merged.get("app_name", "shopstream-lakehouse"),
        env=env,
        base_path=os.getenv("SHOPSTREAM_BASE_PATH", merged.get("base_path", "./data")),
        storage_format=storage_format,
        spark_master=merged.get("spark_master"),
        layers=layers,
        spark_conf={k: str(v) for k, v in (merged.get("spark") or {}).items()},
        max_reject_ratio=max_reject_ratio,
        valid_order_statuses=tuple(s.upper() for s in business.get("valid_order_statuses", [])),
        cancelled_statuses=tuple(s.upper() for s in business.get("cancelled_statuses", [])),
        valid_customer_tiers=tuple(s.upper() for s in business.get("valid_customer_tiers", [])),
        fx_rates_to_inr=fx,
        segment_thresholds_inr={k.upper(): float(v) for k, v in business.get("segment_thresholds_inr", {}).items()},
    )
