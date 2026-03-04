import sqlite3
import pyarrow.dataset as ds
import pandas as pd
import datetime

# 1. 定义路径
feather_path = '/Users/lionelyip/PycharmProjects/GenImgeCrawler/data/all_comfyui.feather'
db_path = '/Users/lionelyip/PycharmProjects/GenImgeCrawler/data/comfyui_data.db'

# 2. 连接 SQLite 数据库
conn = sqlite3.connect(db_path)

# 3. 创建 PyArrow Dataset 对象 (瞬间完成，不占内存)
dataset = ds.dataset(feather_path, format="ipc")

# 获取总行数用于估算进度（可选）
total_rows = dataset.count_rows()
print(f"开始处理，总行数: {total_rows}")

# 4. 设置分批大小（建议 5万-10万，根据内存情况调整）
batch_size = 1000
batch_count = 0

# 5. 开始迭代读取并写入
# to_batches 会自动处理解压，每次只吐出一块数据
for batch in dataset.to_batches(batch_size=batch_size):
    # 将这一块转为 Pandas DataFrame
    df_chunk = batch.to_pandas()

    # --- 数据清洗与格式转换 ---

    # A. 删除无用的索引列 (如果存在)
    if '__index_level_0__' in df_chunk.columns:
        df_chunk = df_chunk.drop(columns=['__index_level_0__'])

    # B. [关键步骤] 复杂类型转字符串
    # SQLite 只能存 Text/Int/Float，不能存 List/Dict
    # 根据你的数据预览，models 是 list，loras 是 dict
    complex_cols = ['models', 'loras', 'ai_json']
    for col in complex_cols:
        if col in df_chunk.columns:
            # 强制转换为字符串格式，确保能存入 SQLite
            df_chunk[col] = df_chunk[col].astype(str)

    # --- 写入数据库 ---

    # if_exists='append': 如果表存在就追加，不存在就创建
    # index=False: 不要把 Pandas 的索引存进去
    df_chunk.to_sql('comfyui_table', conn, if_exists='append', index=False)

    batch_count += 1
    rows_processed = batch_count * batch_size
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 已处理批次: {batch_count} (约 {rows_processed} 行)")

    # 显式删除变量，帮助垃圾回收
    del df_chunk

# 6. 关闭连接及创建索引（可选）
print("数据插入完成，正在创建索引以加速查询...")
cursor = conn.cursor()
# 建议给常用的查询字段（如 work_id）加索引
cursor.execute("CREATE INDEX IF NOT EXISTS idx_work_id ON comfyui_table (work_id)")
conn.commit()
conn.close()

print(f"全部完成！数据库保存在: {db_path}")