#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import os
import json
from contextlib import contextmanager
from typing import Dict, List, Optional
from ..utils import get_logger

logger = get_logger(__name__)


class CivitaiDatabaseManager:
    def __init__(self, db_path: str = "../../data/civitai.db"):
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

            # 1. Models 表 (增加 raw_json 存储所有未提取字段)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_models (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_id INTEGER NOT NULL UNIQUE,
                    name TEXT,
                    type TEXT,
                    nsfw BOOLEAN,
                    nsfw_level INTEGER,
                    download_count INTEGER,
                    thumbs_up_count INTEGER,
                    user_id INTEGER,
                    username TEXT,
                    status TEXT,
                    created_at_api TEXT,
                    tags_summary TEXT,
                    raw_json TEXT, -- 核心：存储完整原始数据
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 2. Versions 表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL UNIQUE,
                    model_id INTEGER,
                    name TEXT,
                    base_model TEXT,
                    download_url TEXT,
                    trained_words TEXT,
                    raw_json TEXT, -- 存储完整原始数据
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_v_mid ON civitai_versions(model_id)")

            # 3. Images 表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL UNIQUE,
                    model_id INTEGER,
                    version_id INTEGER,
                    url TEXT,
                    hash TEXT,
                    nsfw_level INTEGER,
                    width INTEGER,
                    height INTEGER,
                    meta_json TEXT, -- 存储 prompt 等元数据
                    raw_json TEXT,  -- 存储完整原始数据
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_i_mid ON civitai_images(model_id)")

            # 4. Cursors 表 (断点续爬)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawler_cursors (
                    cursor TEXT PRIMARY KEY,
                    next_cursor TEXT,
                    item_count INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def save_item(self, item: Dict) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                mid = item.get('id')
                if not mid: return False

                # --- 保存 Model ---
                rank = item.get('rank', {}) or {}
                user = item.get('creator') or item.get('user') or {}

                cursor.execute("""
                    INSERT INTO civitai_models (
                        model_id, name, type, nsfw, nsfw_level,
                        download_count, thumbs_up_count, user_id, username,
                        status, created_at_api, tags_summary, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(model_id) DO UPDATE SET
                        download_count=excluded.download_count,
                        thumbs_up_count=excluded.thumbs_up_count,
                        raw_json=excluded.raw_json,
                        updated_at=CURRENT_TIMESTAMP
                """, (
                    mid, item.get('name'), item.get('type'),
                    item.get('nsfw'), item.get('nsfwLevel'),
                    rank.get('downloadCount', 0), rank.get('thumbsUpCount', 0),
                    user.get('id') or item.get('userId'), user.get('username'),
                    item.get('status'), item.get('createdAt'),
                    json.dumps(item.get('tags', []), ensure_ascii=False),
                    json.dumps(item, ensure_ascii=False)  # 保存所有数据！
                ))

                # --- 保存 Versions ---
                versions = item.get('modelVersions', [])
                if not versions and 'version' in item and item['version']:
                    versions = [item['version']]

                processed_img_ids = set()

                for v in versions:
                    vid = v.get('id')
                    if not vid: continue

                    cursor.execute("""
                        INSERT INTO civitai_versions (
                            version_id, model_id, name, base_model, download_url,
                            trained_words, raw_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(version_id) DO UPDATE SET raw_json=excluded.raw_json
                    """, (
                        vid, mid, v.get('name'), v.get('baseModel'), v.get('downloadUrl'),
                        json.dumps(v.get('trainedWords', []), ensure_ascii=False),
                        json.dumps(v, ensure_ascii=False)
                    ))

                    # Version Images
                    for img in v.get('images', []):
                        self._save_image(cursor, img, mid, vid)
                        processed_img_ids.add(img.get('id'))

                # --- 保存 Root Images ---
                for img in item.get('images', []):
                    if img.get('id') and img.get('id') not in processed_img_ids:
                        # 尝试推断 version_id
                        inferred_vid = versions[0]['id'] if len(versions) == 1 else None
                        self._save_image(cursor, img, mid, inferred_vid)

                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Save Item {item.get('id')} Error: {e}")
            return False

    def _save_image(self, cursor, img: Dict, mid: int, vid: Optional[int]):
        img_id = img.get('id')
        if not img_id: return

        real_vid = img.get('modelVersionId') or vid
        meta = img.get('meta') or img.get('metadata') or {}

        cursor.execute("""
            INSERT INTO civitai_images (
                image_id, model_id, version_id, url, hash, nsfw_level,
                width, height, meta_json, raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(image_id) DO UPDATE SET raw_json=excluded.raw_json
        """, (
            img_id, mid, real_vid, img.get('url'), img.get('hash'), img.get('nsfwLevel'),
            img.get('width'), img.get('height'),
            json.dumps(meta, ensure_ascii=False),
            json.dumps(img, ensure_ascii=False)
        ))

    def batch_save(self, items: List[Dict]) -> int:
        count = 0
        for item in items:
            if self.save_item(item): count += 1
        return count

    def record_cursor(self, cursor: Optional[str], next_cursor: Optional[str], count: int):
        with self.get_connection() as conn:
            c, n = str(cursor) if cursor else 'null', str(next_cursor) if next_cursor else 'null'
            conn.execute("INSERT OR REPLACE INTO crawler_cursors (cursor, next_cursor, item_count) VALUES (?, ?, ?)",
                         (c, n, count))
            conn.commit()

    def get_next_cursor(self, cursor: Optional[str]) -> Optional[str]:
        with self.get_connection() as conn:
            c = str(cursor) if cursor else 'null'
            row = conn.execute("SELECT next_cursor FROM crawler_cursors WHERE cursor = ?", (c,)).fetchone()
            if row: return None if row['next_cursor'] == 'null' else row['next_cursor']
        return None

    def get_last_cursor(self) -> Optional[str]:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT next_cursor FROM crawler_cursors WHERE next_cursor != 'null' ORDER BY processed_at DESC LIMIT 1").fetchone()
            if row: return row['next_cursor']
        return None