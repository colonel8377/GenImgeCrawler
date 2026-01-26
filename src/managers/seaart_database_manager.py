#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SeaArt 数据库管理器
职责：仅负责数据全量存储与核心字段提取
特点：
1. 包含 content_type, author, stat 等丰富字段
2. raw_data 全量 JSON 备份
3. 唯一索引防重
"""

import sqlite3
import os
import json
from contextlib import contextmanager
from typing import Dict, List
from ..utils import get_logger

logger = get_logger(__name__)


class SeaArtDatabaseManager:
    def __init__(self, db_path: str = "data/seaart.db"):
        self.db_path = db_path
        self._ensure_data_dir()
        self._init_database()

    def _ensure_data_dir(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

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

            # --- SeaArt Items 表 ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seaart_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT NOT NULL,      -- "id"
                    title TEXT,                 -- "title"
                    description TEXT,           -- "description"

                    -- 类型
                    obj_type INTEGER,
                    content_type TEXT,          -- "content_type" (e.g. Checkpoint)
                    content_sub_type TEXT,      -- "content_sub_type"

                    -- 作者信息 (Flattened)
                    author_id TEXT,
                    author_name TEXT,
                    author_head TEXT,

                    -- 封面信息 (Flattened)
                    cover_url TEXT,
                    cover_width INTEGER,
                    cover_height INTEGER,
                    cover_nsfw INTEGER,

                    -- 统计信息 (Flattened)
                    stat_like INTEGER DEFAULT 0,
                    stat_collection INTEGER DEFAULT 0,
                    stat_task INTEGER DEFAULT 0,
                    stat_view INTEGER DEFAULT 0,
                    stat_download INTEGER DEFAULT 0,
                    stat_comment INTEGER DEFAULT 0,
                    rating REAL DEFAULT 0.0,

                    -- 属性
                    nsfw INTEGER,
                    is_preset INTEGER,
                    tags_json TEXT,             -- sys_tag JSON数组

                    -- 【核心】原始数据全量备份
                    raw_data TEXT,

                    created_at_origin INTEGER,  -- "create_at"
                    created_at_db TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at_db TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 唯一索引保证不重复
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_seaart_item_id ON seaart_items(item_id)")

            # 常用查询索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seaart_ctype ON seaart_items(content_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seaart_author ON seaart_items(author_id)")

            conn.commit()

    def save_item(self, item: Dict) -> bool:
        """保存单个 Item，提取核心字段 + 存入 raw_data"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                mid = item.get('id')
                if not mid: return False

                # 提取嵌套字段，使用 .get() 防空
                author = item.get('author') or {}
                cover = item.get('cover') or {}
                stat = item.get('stat') or {}

                # 插入或更新
                cursor.execute("""
                    INSERT INTO seaart_items (
                        item_id, title, description,
                        obj_type, content_type, content_sub_type,
                        author_id, author_name, author_head,
                        cover_url, cover_width, cover_height, cover_nsfw,
                        stat_like, stat_collection, stat_task, stat_view, stat_download, stat_comment, rating,
                        nsfw, is_preset, tags_json,
                        created_at_origin, raw_data, updated_at_db
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(item_id) DO UPDATE SET
                        title=excluded.title,
                        stat_like=excluded.stat_like,
                        stat_collection=excluded.stat_collection,
                        stat_task=excluded.stat_task,
                        stat_view=excluded.stat_view,
                        stat_download=excluded.stat_download,
                        rating=excluded.rating,
                        raw_data=excluded.raw_data, -- 始终更新 raw_data 以保持数据最新
                        updated_at_db=CURRENT_TIMESTAMP
                """, (
                    mid,
                    item.get('title'),
                    item.get('description'),
                    item.get('obj_type'),
                    item.get('content_type'),
                    item.get('content_sub_type'),
                    author.get('id'),
                    author.get('name'),
                    author.get('head'),
                    cover.get('url'),
                    cover.get('width'),
                    cover.get('height'),
                    cover.get('nsfw'),
                    stat.get('num_of_like', 0),
                    stat.get('num_of_collection', 0),
                    stat.get('num_of_task', 0),
                    stat.get('num_of_view', 0),
                    stat.get('num_of_download', 0),
                    stat.get('num_of_comment', 0),
                    stat.get('rating', 0.0),
                    item.get('nsfw'),
                    item.get('is_preset'),
                    json.dumps(item.get('sys_tag', []), ensure_ascii=False),
                    item.get('create_at'),
                    json.dumps(item, ensure_ascii=False)  # 存入所有数据
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Save SeaArt Item {item.get('id')} Failed: {e}")
            return False

    def batch_save(self, items: List[Dict]) -> int:
        count = 0
        for item in items:
            if self.save_item(item): count += 1
        return count