import logging
import logging.config
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

config_path = REPO_ROOT / "config" / "logging.conf"

logging.config.fileConfig(
    config_path, 
    defaults={"logdir": LOG_DIR.as_posix()}
)

logger = logging.getLogger("project")