#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import os
import json
from contextlib import contextmanager
from typing import Dict, List
from ..utils import get_logger

logger = get_logger(__name__)


class TensorDatabaseManager:
    def __init__(self, db_path: str = "data/tensor.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_database()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_database(self):
        with self.get_connection() as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
            conn.execute('PRAGMA synchronous=NORMAL;')
            cursor = conn.cursor()

            # --- Tensor Items 表 ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tensor_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT NOT NULL,       -- JSON中的 "id"
                    name TEXT,                   -- "name"
                    type TEXT,                   -- "type" (LORA, CHECKPOINT...)

                    -- 模型信息 (Flattened from model)
                    base_model TEXT,             -- model.baseModel
                    model_version TEXT,          -- model.name

                    -- 作者信息 (Flattened from owner)
                    owner_id TEXT,
                    owner_name TEXT,

                    -- 统计信息 (Flattened from statisticInfo)
                    stat_run INTEGER DEFAULT 0,
                    stat_download INTEGER DEFAULT 0,
                    stat_like INTEGER DEFAULT 0,
                    stat_comment INTEGER DEFAULT 0,

                    -- 封面 (仅存URL)
                    cover_url TEXT,

                    -- 其他
                    project_tags TEXT,           -- tags summary

                    -- 【核心】全量原始数据
                    raw_data TEXT,

                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 【关键】唯一索引，防止重复
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tensor_item_id ON tensor_items(item_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tensor_type ON tensor_items(type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tensor_owner ON tensor_items(owner_id)")

            conn.commit()

    def save_item(self, item: Dict) -> bool:
        """保存单个 Item"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 提取 ID
                item_id = item.get('id')
                if not item_id: return False

                # 提取嵌套数据
                owner = item.get('owner') or {}
                stats = item.get('statisticInfo') or {}
                model = item.get('model') or {}
                cover = model.get('cover') or {}

                # 处理 Tags
                tags = item.get('projectTags') or []
                tags_str = ",".join([t.get('name', '') for t in tags])

                cursor.execute("""
                    INSERT INTO tensor_items (
                        item_id, name, type,
                        base_model, model_version,
                        owner_id, owner_name,
                        stat_run, stat_download, stat_like, stat_comment,
                        cover_url, project_tags, raw_data, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(item_id) DO UPDATE SET
                        name=excluded.name,
                        stat_run=excluded.stat_run,
                        stat_download=excluded.stat_download,
                        stat_like=excluded.stat_like,
                        raw_data=excluded.raw_data, -- 始终更新 raw_data
                        updated_at=CURRENT_TIMESTAMP
                """, (
                    item_id,
                    item.get('name'),
                    item.get('type'),
                    model.get('baseModel'),
                    model.get('name'),  # model version name
                    owner.get('id'),
                    owner.get('nickname'),
                    int(stats.get('runCount', 0)),
                    int(stats.get('downloadCount', 0)),
                    int(stats.get('likeCount', 0)),
                    int(stats.get('commentCount', 0)),
                    cover.get('url'),
                    tags_str,
                    json.dumps(item, ensure_ascii=False)  # 存入所有数据
                ))

                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Save Tensor Item Error: {e}")
            return False

    def batch_save(self, items: List[Dict]) -> int:
        count = 0
        for item in items:
            if self.save_item(item): count += 1
        return count