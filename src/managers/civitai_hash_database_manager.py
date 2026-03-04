#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import os
import json
from contextlib import contextmanager
from typing import Dict, List
from ..utils import get_logger

logger = get_logger(__name__)


class CivitaiHashDBManager:
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

            # 1. 任务队列表 (存储待爬取的 Hash)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_hash_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hash_string TEXT NOT NULL UNIQUE,
                    status INTEGER DEFAULT 0, -- 0: Pending, 1: Success, -1: Failed/NotFound
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_hq_status ON civitai_hash_queue(status)")

            # 2. Versions 表 (Hash 接口返回的根对象是 Version)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_by_hash_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL,
                    model_id INTEGER NOT NULL,
                    name TEXT,
                    model_name TEXT,
                    base_model TEXT,
                    base_model_type TEXT,
                    nsfw_level INTEGER,
                    status TEXT,
                    published_at TEXT,
                    download_url TEXT,
                    stats_json TEXT,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bh_v_mvid ON civitai_by_hash_versions(model_id, version_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bh_v_mid ON civitai_by_hash_versions(model_id)")

            # 3. Files 表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_by_hash_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id INTEGER NOT NULL,
                    version_id INTEGER NOT NULL,
                    model_id INTEGER NOT NULL,
                    name TEXT,
                    size_kb REAL,
                    type TEXT,
                    format TEXT,
                    pickle_scan_result TEXT,
                    virus_scan_result TEXT,
                    download_url TEXT,
                    primary_sha256 TEXT, -- 方便查询
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bh_f_mfid ON civitai_by_hash_files(model_id, file_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bh_f_vm ON civitai_by_hash_files(version_id, model_id)")

            # 5. Images 表 (注意：V1 API 的 images 没有 id 字段，使用 URL 作为唯一键)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_by_hash_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL, -- V1 API Image 唯一标识
                    version_id INTEGER NOT NULL,
                    model_id INTEGER NOT NULL,
                    hash TEXT, -- Image Hash (Blurhash/Fingerprint)
                    nsfw_level INTEGER,
                    width INTEGER,
                    height INTEGER,
                    meta_json TEXT,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bh_i_url ON civitai_by_hash_images(url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bh_i_vm ON civitai_by_hash_images(version_id, model_id)")

            conn.commit()

    # ==========================
    # 任务管理
    # ==========================
    def add_hashes_to_queue(self, hash_list: List[str]):
        """批量导入待爬取 Hash (自动忽略已存在的)"""
        if not hash_list: return
        with self.get_connection() as conn:
            # 构造数据元组
            data = [(h, 0) for h in hash_list]
            conn.executemany("""
                INSERT OR IGNORE INTO civitai_hash_queue (hash_string, status) VALUES (?, ?)
            """, data)
            conn.commit()
            logger.info(f"Queue updated. Added up to {len(hash_list)} new hashes.")

    def get_pending_hashes(self, limit: int = 10) -> List[str]:
        """获取待处理任务"""
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT hash_string FROM civitai_hash_queue 
                WHERE status = 0 
                AND hash_string is not NULL
                AND hash_string IS NOT ''
                LIMIT ?
            """, (limit,)).fetchall()
            return [r['hash_string'] for r in rows]

    def mark_hash_status(self, hash_string: str, status: int):
        """更新任务状态 (1: Success, -1: Failed)"""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE civitai_hash_queue 
                SET status = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE hash_string = ?
            """, (status, hash_string))
            conn.commit()

    # ==========================
    # 数据保存逻辑
    # ==========================
    def save_version_data(self, data: Dict, source_hash: str) -> bool:
        """
        保存 /api/v1/model-versions/by-hash/{hash} 返回的 JSON
        data 是一个 Version 对象
        """
        try:
            vid = data.get('id')
            mid = data.get('modelId')

            if not vid or not mid:
                logger.error(f"Invalid Data for hash {source_hash}: Missing ID/ModelID")
                return False

            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 1. 保存 Version
                model_meta = data.get('model', {})  # 内嵌的简略 model 信息
                stats = data.get('stats', {})

                cursor.execute("""
                    INSERT or IGNORE INTO civitai_by_hash_versions (
                        version_id, model_id, name, model_name, base_model, base_model_type,
                        nsfw_level, status, published_at, download_url, stats_json, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    
                """, (
                    vid, mid, data.get('name'), model_meta.get('name'),
                    data.get('baseModel'), data.get('baseModelType'),
                    data.get('nsfwLevel'), data.get('status'), data.get('publishedAt'),
                    data.get('downloadUrl'),
                    json.dumps(stats, ensure_ascii=False),
                    json.dumps(data, ensure_ascii=False)
                ))

                # 2. 保存 Files
                files = data.get('files', [])
                for f in files:
                    fid = f.get('id')
                    if not fid: continue

                    # V1 API Hashes 是字典: {"AutoV1": "...", "SHA256": "..."}
                    hashes_dict = f.get('hashes', {}) or {}
                    primary_sha256 = hashes_dict.get('SHA256')

                    # 插入 File
                    cursor.execute("""
                        INSERT or IGNORE INTO civitai_by_hash_files (
                            file_id, version_id, model_id, name, size_kb,
                            type, format, pickle_scan_result, virus_scan_result,
                            download_url, primary_sha256, raw_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        fid, vid, mid, f.get('name'), f.get('sizeKB'),
                        f.get('type'), f.get('metadata', {}).get('format'),
                        f.get('pickleScanResult'), f.get('virusScanResult'),
                        f.get('downloadUrl'), primary_sha256,
                        json.dumps(f, ensure_ascii=False)
                    ))

                # 3. 保存 Images
                images = data.get('images', [])
                for img in images:
                    url = img.get('url')
                    if not url: continue  # 没有 ID，必须有 URL

                    cursor.execute("""
                        INSERT or IGNORE INTO civitai_by_hash_images (
                            url, version_id, model_id, hash,
                            nsfw_level, width, height, meta_json, raw_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        url, vid, mid, img.get('hash'),
                        img.get('nsfwLevel'), img.get('width'), img.get('height'),
                        json.dumps(img.get('meta'), ensure_ascii=False),
                        json.dumps(img, ensure_ascii=False)
                    ))

                # 4. 更新队列状态 -> 成功
                cursor.execute(
                    "UPDATE civitai_hash_queue SET status = 1, updated_at = CURRENT_TIMESTAMP WHERE hash_string = ?",
                    (source_hash,))
                conn.commit()
                return True

        except Exception as e:
            logger.error(f"DB Save Error (Hash {source_hash}): {e}")
            return False