#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import os
import json
from contextlib import contextmanager
from typing import Dict, List, Optional
from ..utils import get_logger

logger = get_logger(__name__)


class CivitaiDetailDatabaseManager:
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

            # --- 1. Detail Models 表 ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_detail_models (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_id INTEGER NOT NULL,
                    name TEXT,
                    type TEXT,
                    nsfw BOOLEAN,
                    nsfw_level INTEGER,
                    description TEXT,
                    poi BOOLEAN,
                    minor BOOLEAN,
                    commercial_use TEXT,
                    download_count INTEGER,
                    thumbs_up_count INTEGER,
                    user_id INTEGER,
                    username TEXT,
                    tags_json TEXT,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_mid ON civitai_detail_models(model_id)")

            # --- 2. Detail Versions 表 ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_detail_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL,
                    model_id INTEGER,
                    name TEXT,
                    base_model TEXT,
                    description TEXT,
                    download_url TEXT,
                    trained_words TEXT,
                    epochs INTEGER,
                    steps INTEGER,
                    status TEXT,
                    published_at TEXT,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dv_mvid ON civitai_detail_versions(model_id, version_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dv_mid ON civitai_detail_versions(model_id)")

            # --- 3. Detail Files 表 (文件基础信息) ---
            # 注意：这里只存主要 SHA256 用于显示，全量 Hash 存入 Hash 表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_detail_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id INTEGER NOT NULL,
                    version_id INTEGER,
                    model_id INTEGER,
                    name TEXT,
                    size_kb REAL,
                    type TEXT,
                    format TEXT,
                    pickle_scan_result TEXT,
                    virus_scan_result TEXT,
                    download_url TEXT,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_df_mfid ON civitai_detail_files(model_id, file_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_df_vid ON civitai_detail_files(version_id)")


            # --- 5. Detail Images 表 ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS civitai_detail_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    version_id INTEGER,
                    model_id INTEGER,
                    url TEXT,
                    hash TEXT, -- 图片的 Hash (BlurHash 或 唯一指纹)
                    nsfw_level INTEGER,
                    width INTEGER,
                    height INTEGER,
                    meta_json TEXT,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_di_miid ON civitai_detail_images(model_id, image_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_di_vid ON civitai_detail_images(version_id)")

            conn.commit()

    # --- 任务调度 ---
    def get_pending_model_ids(self, limit: int = 10) -> List[int]:
        with self.get_connection() as conn:
            rows = conn.execute("""
                SELECT model_id FROM civitai_detail_versions
                WHERE model_id not in (select model_id from civitai_detail_models)
                GROUP BY model_id
                LIMIT ?
            """, (limit,)).fetchall()
            # rows = conn.execute("""
            #     SELECT model_id FROM civitai_by_hash_versions
            #     WHERE model_id not in (select model_id from civitai_detail_models)
            #     GROUP BY model_id
            #     LIMIT ?
            # """, (limit,)).fetchall()
            logger.info(f"found {len(rows)} model ids to crawl")
            return [r['model_id'] for r in rows]

    # --- 核心存储逻辑 ---
    def save_model_detail(self, data: Dict) -> bool:
        try:
            item = data.get('result', {}).get('data', {}).get('json', {})
            mid = item.get('id')
            if not mid: return False

            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 1. Model
                user = item.get('user', {})
                rank = item.get('rank', {})
                allow_comm = item.get('allowCommercialUse', [])
                comm_str = ",".join(allow_comm) if isinstance(allow_comm, list) else str(allow_comm)

                cursor.execute("""
                    INSERT or IGNORE INTO civitai_detail_models (
                        model_id, name, type, nsfw, nsfw_level, description,
                        poi, minor, commercial_use, download_count, thumbs_up_count,
                        user_id, username, tags_json, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    mid, item.get('name'), item.get('type'), item.get('nsfw'), item.get('nsfwLevel'),
                    item.get('description'), item.get('poi'), item.get('minor'), comm_str,
                    rank.get('downloadCountAllTime', 0), rank.get('thumbsUpCountAllTime', 0),
                    user.get('id'), user.get('username'),
                    json.dumps(item.get('tagsOnModels', []), ensure_ascii=False),
                    json.dumps(item, ensure_ascii=False)
                ))

                # 2. Versions & Files & Hashes
                for v in item.get('modelVersions', []):
                    vid = v.get('id')
                    if not vid: continue

                    # Version
                    trained_words = v.get('trainedWords', [])
                    tw_str = ",".join(trained_words) if isinstance(trained_words, list) else str(trained_words)

                    cursor.execute("""
                        INSERT or IGNORE INTO civitai_detail_versions (
                            version_id, model_id, name, base_model, description,
                            download_url, trained_words, epochs, steps, status,
                            published_at, raw_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        vid, mid, v.get('name'), v.get('baseModel'), v.get('description'),
                        v.get('downloadUrl'), tw_str, v.get('epochs'), v.get('steps'),
                        v.get('status'), v.get('publishedAt'),
                        json.dumps(v, ensure_ascii=False)
                    ))

                    # Files (Inner Loop)
                    for f in v.get('files', []):
                        fid = f.get('id')
                        if not fid: continue

                        # 先插入 Files 表
                        cursor.execute("""
                            INSERT or IGNORE INTO civitai_detail_files (
                                file_id, version_id, model_id, name, size_kb,
                                type, format, pickle_scan_result, virus_scan_result,
                                download_url, raw_json, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, (
                            fid, vid, mid, f.get('name'), f.get('sizeKB'),
                            f.get('type'), f.get('metadata', {}).get('format'),
                            f.get('pickleScanResult'), f.get('virusScanResult'),
                            f.get('url'),
                            json.dumps(f, ensure_ascii=False)
                        ))


                    # Images (Inner Loop)
                    for img in v.get('images', []):
                        iid = img.get('id')
                        if not iid: continue
                        cursor.execute("""
                            INSERT or IGNORE INTO civitai_detail_images (
                                image_id, version_id, model_id, url, hash,
                                nsfw_level, width, height, meta_json, raw_json, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, (
                            iid, vid, mid, img.get('url'), img.get('hash'),
                            img.get('nsfwLevel'), img.get('width'), img.get('height'),
                            json.dumps(img.get('meta'), ensure_ascii=False),
                            json.dumps(img, ensure_ascii=False)
                        ))

                conn.commit()
                return True

        except Exception as e:
            logger.error(f"DB Save Detail Error (Model {mid}): {e}")
            return False