"""
存储工厂

根据配置或环境变量选择存储后端：
- file：本地 JSON 文件（默认）
- mysql：结构化数据写入 MySQL，图片与原文保留在磁盘

环境变量优先级高于配置文件，便于 Docker 部署：
STORAGE_BACKEND / PROJECTS_DIR / MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import quote_plus

import yaml

from src.storage.project_store import ProjectStore


CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

_store: Optional[ProjectStore] = None
_store_key: Optional[tuple] = None


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config() -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    for name in ("default.yaml", "user.yaml"):
        path = CONFIG_DIR / name
        if not path.exists():
            continue
        with open(path, "r", encoding="utf-8") as f:
            config = _deep_merge(config, yaml.safe_load(f) or {})
    return config


def load_storage_config() -> tuple[str, str, Dict[str, Any]]:
    storage = load_config().get("storage") or {}

    backend = (os.getenv("STORAGE_BACKEND") or storage.get("backend") or "file").strip().lower()
    base_dir = os.getenv("PROJECTS_DIR") or storage.get("base_dir") or "./projects"

    mysql = dict(storage.get("mysql") or {})
    for key, env_name in (
        ("host", "MYSQL_HOST"),
        ("port", "MYSQL_PORT"),
        ("user", "MYSQL_USER"),
        ("password", "MYSQL_PASSWORD"),
        ("database", "MYSQL_DATABASE"),
    ):
        value = os.getenv(env_name)
        if value:
            mysql[key] = value

    return backend, str(base_dir), mysql


def _mysql_url(mysql: Dict[str, Any]) -> str:
    user = quote_plus(str(mysql.get("user", "root")))
    password = quote_plus(str(mysql.get("password", "")))
    host = mysql.get("host", "127.0.0.1")
    port = int(mysql.get("port", 3306))
    database = mysql.get("database", "ai_novel")
    return f"{host}:{port}/{database}"


def get_store(base_dir: Optional[Union[str, Path]] = None) -> ProjectStore:
    """获取当前配置下的存储实例（进程内复用）"""
    global _store, _store_key

    backend, configured_base_dir, mysql = load_storage_config()
    target_base_dir = str(base_dir) if base_dir else configured_base_dir

    key = (
        backend,
        target_base_dir,
        _mysql_url(mysql),
        str(mysql.get("pool_size", "")),
        str(mysql.get("max_overflow", "")),
    )

    if _store is None or _store_key != key:
        if backend == "mysql":
            from src.storage.mysql_store import MySQLProjectStore

            _store = MySQLProjectStore(base_dir=target_base_dir, mysql_config=mysql)
        else:
            _store = ProjectStore(base_dir=target_base_dir)
        _store_key = key

    return _store
