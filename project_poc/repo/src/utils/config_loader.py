"""
Utilities for locating and loading the project settings.yaml file without hardcoded paths.

Resolution order:
1) Environment variable SETTINGS_PATH (if it exists and the file is present)
2) settings_path value inside the repo's config/settings.yaml (if the file exists and the referenced path exists)
3) The repo-local config/settings.yaml path
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml

# Repository-local settings file (default fallback)
DEFAULT_SETTINGS_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"


def resolve_settings_path() -> Path:
    """Resolve the path to settings.yaml using env/config hints, avoiding hardcoded docker paths."""
    env_path = os.getenv("SETTINGS_PATH")
    if env_path:
        env_candidate = Path(env_path)
        if env_candidate.exists():
            return env_candidate

    default_candidate = DEFAULT_SETTINGS_PATH
    if default_candidate.exists():
        try:
            with open(default_candidate, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            configured_path = cfg.get("paths", {}).get("settings_path")
            if configured_path:
                configured_candidate = Path(configured_path)
                if configured_candidate.exists():
                    return configured_candidate
        except Exception:
            # Ignore config parsing issues and fall back to the repo-local file
            pass
        return default_candidate

    # Final fallback: return the repo-local path even if it may not exist; callers will handle errors
    return default_candidate


def load_settings(config_path: Path | None = None) -> Dict[str, Any]:
    """Load settings.yaml content as a dictionary."""
    target_path = config_path or resolve_settings_path()
    with open(target_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
