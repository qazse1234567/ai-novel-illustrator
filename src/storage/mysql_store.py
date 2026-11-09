"""
MySQL 存储实现

结构化数据（项目、章节、实体、提示词、世界观、AI 记录、文档型状态）写入 MySQL；
小说原文、生成图片、Markdown 导出仍保留在磁盘。
"""

import json
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

from src.storage.project_store import ProjectStore


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS projects (
        id VARCHAR(64) NOT NULL PRIMARY KEY,
        name VARCHAR(255) NOT NULL DEFAULT '',
        created_at VARCHAR(64) NOT NULL DEFAULT '',
        input_file VARCHAR(1024) NULL,
        novel_name VARCHAR(255) NULL,
        payload JSON NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS chapters (
        project_id VARCHAR(64) NOT NULL,
        seq INT NOT NULL,
        chapter_id VARCHAR(128) NULL,
        chapter_number INT NULL,
        payload JSON NOT NULL,
        PRIMARY KEY (project_id, seq),
        KEY idx_chapters_number (project_id, chapter_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS entities (
        project_id VARCHAR(64) NOT NULL,
        seq INT NOT NULL,
        entity_id VARCHAR(128) NULL,
        entity_type VARCHAR(32) NULL,
        name VARCHAR(255) NULL,
        payload JSON NOT NULL,
        PRIMARY KEY (project_id, seq),
        KEY idx_entities_type (project_id, entity_type),
        KEY idx_entities_name (project_id, name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS prompts (
        project_id VARCHAR(64) NOT NULL,
        seq INT NOT NULL,
        prompt_id VARCHAR(128) NULL,
        prompt_type VARCHAR(32) NULL,
        entity_id VARCHAR(128) NULL,
        payload JSON NOT NULL,
        PRIMARY KEY (project_id, seq),
        KEY idx_prompts_type (project_id, prompt_type),
        KEY idx_prompts_entity (project_id, entity_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS world_bibles (
        project_id VARCHAR(64) NOT NULL PRIMARY KEY,
        payload JSON NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_runs (
        project_id VARCHAR(64) NOT NULL,
        seq INT NOT NULL,
        run_id VARCHAR(128) NULL,
        payload JSON NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (project_id, seq)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS project_documents (
        project_id VARCHAR(64) NOT NULL,
        doc_key VARCHAR(128) NOT NULL,
        payload JSON NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (project_id, doc_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return {}


def _as_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return value
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return []


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class MySQLProjectStore(ProjectStore):
    """以 MySQL 作为结构化数据后端的存储实现。"""

    def __init__(self, base_dir: str = "./projects", mysql_config: Optional[Dict[str, Any]] = None):
        super().__init__(base_dir)
        config = mysql_config or {}

        host = config.get("host", "127.0.0.1")
        port = int(config.get("port", 3306))
        user = quote_plus(str(config.get("user", "root")))
        password = quote_plus(str(config.get("password", "")))
        database = config.get("database", "ai_novel")
        charset = config.get("charset", "utf8mb4")

        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={charset}"
        self.engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_size=int(config.get("pool_size", 5)),
            max_overflow=int(config.get("max_overflow", 5)),
            future=True,
        )
        self._ensure_schema()

    # ------------------------------------------------------------------ 基础
    def _ensure_schema(self) -> None:
        with self.engine.begin() as conn:
            for statement in SCHEMA_STATEMENTS:
                conn.execute(text(statement))

    def create_project(
        self,
        project_id: str,
        project_name: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        project_dir = self.base_dir / project_id
        for sub in ("data", "output", "prompts", "images"):
            (project_dir / sub).mkdir(parents=True, exist_ok=True)

        project_info = {
            "id": project_id,
            "name": project_name,
            "created_at": datetime.now().isoformat(),
            "config": config or {},
        }
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO projects (id, name, created_at, payload)
                    VALUES (:id, :name, :created_at, :payload)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        created_at = VALUES(created_at),
                        payload = VALUES(payload)
                    """
                ),
                {
                    "id": project_id,
                    "name": project_name,
                    "created_at": project_info["created_at"],
                    "payload": _dump(project_info),
                },
            )
        return project_dir

    def project_exists(self, project_id: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM projects WHERE id = :id"),
                {"id": project_id},
            ).first()
        return row is not None

    def load_project_info(self, project_id: str) -> Dict[str, Any]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT payload FROM projects WHERE id = :id"),
                {"id": project_id},
            ).first()
        if row is None:
            raise FileNotFoundError(f"项目不存在: {project_id}")
        return _as_dict(row[0])

    def save_project_info(self, project_id: str, info: Dict[str, Any]) -> None:
        info = dict(info)
        info["id"] = project_id
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO projects (id, name, created_at, input_file, novel_name, payload)
                    VALUES (:id, :name, :created_at, :input_file, :novel_name, :payload)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        created_at = VALUES(created_at),
                        input_file = VALUES(input_file),
                        novel_name = VALUES(novel_name),
                        payload = VALUES(payload)
                    """
                ),
                {
                    "id": project_id,
                    "name": info.get("name", ""),
                    "created_at": info.get("created_at", ""),
                    "input_file": info.get("input_file"),
                    "novel_name": info.get("novel_name"),
                    "payload": _dump(info),
                },
            )

    def list_projects(self) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT payload FROM projects")).all()
        projects = [_as_dict(row[0]) for row in rows]
        return sorted(projects, key=lambda item: item.get("created_at", ""), reverse=True)

    def delete_project(self, project_id: str) -> None:
        with self.engine.begin() as conn:
            for table in ("chapters", "entities", "prompts", "world_bibles", "ai_runs", "project_documents"):
                conn.execute(
                    text(f"DELETE FROM {table} WHERE project_id = :id"),
                    {"id": project_id},
                )
            conn.execute(text("DELETE FROM projects WHERE id = :id"), {"id": project_id})
        super().delete_project(project_id)

    # ------------------------------------------------------------------ 世界观
    def save_world_bible(self, project_id: str, world_bible: Dict[str, Any]) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO world_bibles (project_id, payload)
                    VALUES (:project_id, :payload)
                    ON DUPLICATE KEY UPDATE payload = VALUES(payload)
                    """
                ),
                {"project_id": project_id, "payload": _dump(world_bible)},
            )

    def load_world_bible(self, project_id: str) -> Dict[str, Any]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT payload FROM world_bibles WHERE project_id = :id"),
                {"id": project_id},
            ).first()
        if row is None:
            raise FileNotFoundError(f"世界观圣经不存在: {project_id}")
        return _as_dict(row[0])

    # ------------------------------------------------------------------ 章节
    def save_chapters(self, project_id: str, chapters: List[Dict[str, Any]]) -> None:
        rows = []
        for seq, chapter in enumerate(chapters):
            number = chapter.get("index", chapter.get("chapter_number", chapter.get("number", 0)))
            rows.append(
                {
                    "project_id": project_id,
                    "seq": seq,
                    "chapter_id": str(chapter.get("id", "") or ""),
                    "chapter_number": int(number) if isinstance(number, (int, str)) and str(number).isdigit() else None,
                    "payload": _dump(chapter),
                }
            )
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM chapters WHERE project_id = :id"), {"id": project_id})
            if rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO chapters (project_id, seq, chapter_id, chapter_number, payload)
                        VALUES (:project_id, :seq, :chapter_id, :chapter_number, :payload)
                        """
                    ),
                    rows,
                )

    def load_chapters(self, project_id: str) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT payload FROM chapters WHERE project_id = :id ORDER BY seq"),
                {"id": project_id},
            ).all()
        return [_as_dict(row[0]) for row in rows]

    # ------------------------------------------------------------------ 实体
    def save_entities(self, project_id: str, entities: List[Dict[str, Any]]) -> None:
        rows = []
        for seq, entity in enumerate(entities):
            entity_type = entity.get("type", "")
            if hasattr(entity_type, "value"):
                entity_type = entity_type.value
            rows.append(
                {
                    "project_id": project_id,
                    "seq": seq,
                    "entity_id": str(entity.get("id", "") or ""),
                    "entity_type": str(entity_type or ""),
                    "name": str(entity.get("name", "") or ""),
                    "payload": _dump(entity),
                }
            )
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM entities WHERE project_id = :id"), {"id": project_id})
            if rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO entities (project_id, seq, entity_id, entity_type, name, payload)
                        VALUES (:project_id, :seq, :entity_id, :entity_type, :name, :payload)
                        """
                    ),
                    rows,
                )

    def load_entities(self, project_id: str) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT payload FROM entities WHERE project_id = :id ORDER BY seq"),
                {"id": project_id},
            ).all()
        return [_as_dict(row[0]) for row in rows]

    # ------------------------------------------------------------------ 提示词
    def save_prompts(self, project_id: str, prompts: List[Dict[str, Any]]) -> None:
        rows = []
        for seq, prompt in enumerate(prompts):
            rows.append(
                {
                    "project_id": project_id,
                    "seq": seq,
                    "prompt_id": str(prompt.get("id", "") or ""),
                    "prompt_type": str(prompt.get("type", "") or ""),
                    "entity_id": str(prompt.get("entity_id", "") or ""),
                    "payload": _dump(prompt),
                }
            )
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM prompts WHERE project_id = :id"), {"id": project_id})
            if rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO prompts (project_id, seq, prompt_id, prompt_type, entity_id, payload)
                        VALUES (:project_id, :seq, :prompt_id, :prompt_type, :entity_id, :payload)
                        """
                    ),
                    rows,
                )

    def load_prompts(self, project_id: str) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT payload FROM prompts WHERE project_id = :id ORDER BY seq"),
                {"id": project_id},
            ).all()
        return [_as_dict(row[0]) for row in rows]

    # ------------------------------------------------------------------ AI 记录
    def save_ai_runs(self, project_id: str, runs: List[Dict[str, Any]]) -> None:
        rows = [
            {
                "project_id": project_id,
                "seq": seq,
                "run_id": str(run.get("id", "") or ""),
                "payload": _dump(run),
            }
            for seq, run in enumerate(runs)
        ]
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM ai_runs WHERE project_id = :id"), {"id": project_id})
            if rows:
                conn.execute(
                    text(
                        """
                        INSERT INTO ai_runs (project_id, seq, run_id, payload)
                        VALUES (:project_id, :seq, :run_id, :payload)
                        """
                    ),
                    rows,
                )

    def load_ai_runs(self, project_id: str) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT payload FROM ai_runs WHERE project_id = :id ORDER BY seq"),
                {"id": project_id},
            ).all()
        return [_as_dict(row[0]) for row in rows]

    def append_ai_run(self, project_id: str, run: Dict[str, Any]) -> Dict[str, Any]:
        runs = self.load_ai_runs(project_id)
        runs.insert(0, run)
        self.save_ai_runs(project_id, runs[:200])
        return run

    # ------------------------------------------------------------------ 文档型状态
    def save_document(self, project_id: str, doc_key: str, payload: Any) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO project_documents (project_id, doc_key, payload)
                    VALUES (:project_id, :doc_key, :payload)
                    ON DUPLICATE KEY UPDATE payload = VALUES(payload)
                    """
                ),
                {"project_id": project_id, "doc_key": doc_key, "payload": _dump(payload)},
            )

    def load_document(self, project_id: str, doc_key: str, default: Any = None) -> Any:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT payload FROM project_documents WHERE project_id = :id AND doc_key = :key"
                ),
                {"id": project_id, "key": doc_key},
            ).first()
        if row is None:
            return default
        value = row[0]
        if isinstance(value, (str, bytes, bytearray)):
            return json.loads(value)
        return value

    # ------------------------------------------------------------------ 导入导出
    def export_project(self, project_id: str, export_path: str) -> Path:
        zip_path = Path(export_path)
        info = self.load_project_info(project_id)
        chapters = self.load_chapters(project_id)
        entities = self.load_entities(project_id)
        prompts = self.load_prompts(project_id)
        ai_runs = self.load_ai_runs(project_id)
        try:
            world_bible = self.load_world_bible(project_id)
        except FileNotFoundError:
            world_bible = None

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr("project.json", _dump(info))
            zipf.writestr("data/chapters.json", _dump(chapters))
            zipf.writestr("data/entities.json", _dump(entities))
            zipf.writestr("data/ai_runs.json", _dump(ai_runs))
            if world_bible is not None:
                zipf.writestr("data/world_bible.json", _dump(world_bible))
            zipf.writestr("prompts/prompts.json", _dump(prompts))

            for doc_key, payload in self._load_all_documents(project_id).items():
                zipf.writestr(f"data/documents/{doc_key}.json", _dump(payload))

            project_dir = self.get_project_dir(project_id)
            if project_dir.exists():
                for file_path in project_dir.rglob("*"):
                    if file_path.is_file():
                        zipf.write(file_path, file_path.relative_to(project_dir))

        return zip_path

    def _load_all_documents(self, project_id: str) -> Dict[str, Any]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT doc_key, payload FROM project_documents WHERE project_id = :id"),
                {"id": project_id},
            ).all()
        return {row[0]: json.loads(row[1]) if isinstance(row[1], (str, bytes, bytearray)) else row[1] for row in rows}

    def import_project(self, zip_path: str) -> str:
        zip_path = Path(zip_path)
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, "r") as zipf:
                zipf.extractall(temp_dir)

            root = Path(temp_dir)
            info_file = root / "project.json"
            if not info_file.exists():
                candidates = list(root.glob("*/project.json"))
                if not candidates:
                    return zip_path.stem
                root = candidates[0].parent
                info_file = candidates[0]

            with open(info_file, "r", encoding="utf-8") as f:
                info = json.load(f)

            project_id = info.get("id") or zip_path.stem
            project_dir = self.get_project_dir(project_id)
            for sub in ("data", "output", "prompts", "images"):
                (project_dir / sub).mkdir(parents=True, exist_ok=True)

            for file_path in root.rglob("*"):
                if not file_path.is_file():
                    continue
                relative = file_path.relative_to(root)
                if relative.name == "project.json" and relative.parent == root:
                    continue
                target = project_dir / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file_path, target)

            self.save_project_info(project_id, info)
            self._import_json(project_id, root / "data" / "chapters.json", "chapters")
            self._import_json(project_id, root / "data" / "entities.json", "entities")
            self._import_json(project_id, root / "prompts" / "prompts.json", "prompts")
            self._import_json(project_id, root / "data" / "ai_runs.json", "ai_runs")
            self._import_json(project_id, root / "data" / "world_bible.json", "world_bible")

            documents_dir = root / "data" / "documents"
            if documents_dir.exists():
                for doc_file in documents_dir.glob("*.json"):
                    with open(doc_file, "r", encoding="utf-8") as f:
                        self.save_document(project_id, doc_file.stem, json.load(f))

            return project_id

    def _import_json(self, project_id: str, path: Path, kind: str) -> None:
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if kind == "chapters":
            self.save_chapters(project_id, payload)
        elif kind == "entities":
            self.save_entities(project_id, payload)
        elif kind == "prompts":
            self.save_prompts(project_id, payload)
        elif kind == "ai_runs":
            self.save_ai_runs(project_id, payload)
        elif kind == "world_bible":
            self.save_world_bible(project_id, payload)
