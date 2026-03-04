import sqlite3
import json
import time

# --- 配置 ---
db_path = '/Users/lionelyip/PycharmProjects/GenImgeCrawler/data/civitai.db'
SOURCE_TABLE = 'civitai_detail_models'
TARGET_TABLE = 'civitai_file_hashes'


def migrate_json_to_hashes():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. 创建目标新表
    print("正在创建新表结构...")
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        model_id INTEGER,
        model_name TEXT,
        model_description TEXT,
        version_id INTEGER,
        version_name TEXT,
        version_description TEXT,
        file_id INTEGER,
        file_name TEXT,
        hash_type TEXT,
        hash_value TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_hash_val ON {TARGET_TABLE} (hash_value)")
    conn.commit()

    # 2. 从源表读取 raw_json
    # 使用流式游标，防止一次性加载导致内存溢出
    print(f"开始从 {SOURCE_TABLE} 读取数据...")
    cursor.execute(f"SELECT raw_json FROM {SOURCE_TABLE} WHERE raw_json IS NOT NULL")

    batch_data = []
    BATCH_SIZE = 10000  # 每积攒 10000 条 Hash 记录写入一次
    total_hashes = 0
    row_count = 0

    while True:
        # 每次从数据库 fetch 一行
        row = cursor.fetchone()
        if row is None:
            break

        row_count += 1
        json_str = row[0]

        try:
            # --- 解析逻辑开始 ---
            data = json.loads(json_str)

            # Level 1: Model (顶级)
            m_id = data.get('id')
            m_name = data.get('name')
            m_desc = data.get('description')

            # Level 2: Versions
            versions = data.get('modelVersions', [])
            if not versions: continue

            for ver in versions:
                v_id = ver.get('id')
                v_name = ver.get('name')
                v_desc = ver.get('description')

                # Level 3: Files
                files = ver.get('files', [])
                if not files: continue

                for f in files:
                    f_id = f.get('id')
                    f_name = f.get('name')

                    # Level 4: Hashes (核心目标)
                    # 你的例子中 hashes 是 [{"type": "AutoV1", "hash": "..."}, ...]
                    hashes = f.get('hashes', [])

                    if isinstance(hashes, list):
                        for h in hashes:
                            if isinstance(h, dict):
                                h_type = h.get('type')
                                h_val = h.get('hash')

                                # 将提取的字段加入待写入列表
                                batch_data.append((
                                    m_id, m_name, m_desc,
                                    v_id, v_name, v_desc,
                                    f_id, f_name, h_type, h_val
                                ))
            # --- 解析逻辑结束 ---

        except (json.JSONDecodeError, AttributeError) as e:
            print(e)
            continue

        # 3. 批量写入 (Batch Insert)
        if len(batch_data) >= BATCH_SIZE:
            _insert_batch(conn, batch_data)
            total_hashes += len(batch_data)
            print(f"已处理源数据 {row_count} 行，累计写入 Hash 记录: {total_hashes}")
            batch_data = []  # 清空缓存

    # 4. 写入剩余的数据
    if batch_data:
        _insert_batch(conn, batch_data)
        total_hashes += len(batch_data)

    conn.close()
    print(f"任务完成！总共提取并存储了 {total_hashes} 条 Hash 记录到表 {TARGET_TABLE}。")


def _insert_batch(conn, data):
    """辅助函数：执行批量插入"""
    cursor = conn.cursor()
    cursor.executemany(f"""
        INSERT or IGNORE INTO {TARGET_TABLE} 
        (model_id, model_name, model_description, version_id, version_name, version_description, file_id, file_name, hash_type, hash_value)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()


if __name__ == "__main__":
    start_time = time.time()
    migrate_json_to_hashes()
    print(f"耗时: {time.time() - start_time:.2f} 秒")