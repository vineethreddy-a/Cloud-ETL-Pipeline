"""
Configuration loader — supports dev, staging, prod environments.
Merges YAML config with environment variable overrides.
"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict


def load_config(env: str = "dev") -> Dict[str, Any]:
    """
    Load configuration for the given environment.

    Priority: environment variables > config/{env}.yaml > config/base.yaml
    """
    base_path = Path(__file__).parent.parent.parent / "config"

    base_config = _load_yaml(base_path / "base.yaml")
    env_config = _load_yaml(base_path / f"{env}.yaml")

    # Deep merge: env overrides base
    config = _deep_merge(base_config, env_config)
    config["environment"] = env

    # Allow env var overrides for secrets
    _apply_env_overrides(config)

    return config


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _apply_env_overrides(config: dict) -> None:
    """Map well-known environment variables into the config."""
    env_map = {
        "AWS_ACCESS_KEY_ID": ("aws", "access_key_id"),
        "AWS_SECRET_ACCESS_KEY": ("aws", "secret_access_key"),
        "AWS_DEFAULT_REGION": ("aws", "region"),
        "SNOWFLAKE_PASSWORD": ("snowflake", "password"),
        "SNOWFLAKE_USER": ("snowflake", "user"),
        "S3_BUCKET": ("s3", "bucket"),
    }

    for env_var, (section, key) in env_map.items():
        value = os.getenv(env_var)
        if value:
            if section not in config:
                config[section] = {}
            config[section][key] = value
