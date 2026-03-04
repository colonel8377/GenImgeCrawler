#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import random
from typing import List

import aiohttp

from src.managers.civitai_hash_database_manager import CivitaiHashDBManager
# 引用上面的 Manager

from src.managers.proxy_manager import ProxyNodeManager
from src.utils import get_logger

logger = get_logger(__name__)


class CivitaiHashCrawler:
    def __init__(self, max_concurrent=5):
        self.max_concurrent = max_concurrent
        self.api_base = "https://civitai.com/api/v1/model-versions/by-hash"
        self.db = CivitaiHashDBManager()
        self.proxy_manager = ProxyNodeManager()  # 假设你现有的代理配置

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/json"
        }

    async def _fetch_one(self, session: aiohttp.ClientSession, target_hash: str):
        """爬取单个 Hash"""
        url = f"{self.api_base}/{target_hash}"

        for retry in range(5):
            try:
                proxy = self.proxy_manager.get_proxy()
                async with session.get(url, headers=self.headers, proxy=proxy, timeout=30) as resp:

                    if resp.status == 200:
                        data = await resp.json()
                        # 校验返回是否有效（包含 id 和 modelId）
                        if 'id' in data and 'modelId' in data:
                            if self.db.save_version_data(data, target_hash):
                                logger.info(f"[OK] Hash {target_hash} -> Version {data['id']}")
                                return
                            else:
                                logger.error(f"[DB ERR] Hash {target_hash} save failed")
                                return
                        else:
                            # 有时候返回空对象或错误信息
                            logger.warning(f"[Invalid] Hash {target_hash} response: {data}")
                            self.db.mark_hash_status(target_hash, -1)  # 标记失败
                            return

                    if resp.status == 404:
                        logger.warning(f"[404] Hash {target_hash} not found")
                        self.db.mark_hash_status(target_hash, -1)  # 永久失败/不存在
                        return

                    if resp.status in [403, 429]:
                        logger.warning(f"[Block] {resp.status} for {target_hash}, switch proxy...")
                        self.proxy_manager.switch_node()
                        await asyncio.sleep(random.uniform(0.1, 1.0))
                        continue

                    # 其他错误
                    logger.warning(f"[Err] {resp.status} for {url} \n - {resp.text}")

            except Exception as e:
                logger.error(f"[Ex] {target_hash}: {e}")
                self.proxy_manager.switch_node()
                await asyncio.sleep(1)

        # 重试耗尽
        logger.error(f"[Fail] Gave up on {target_hash}")
        # 这里可以选择标记为 -1，或者保持 0 下次再试
        # self.db.mark_hash_status(target_hash, -1)

    async def start(self):
        logger.info("Starting Hash Crawler...")

        # 0. 先导入一批 Hash (模拟用户输入)
        # 实际使用时，你可以写个脚本读取文件调用 add_hashes_to_queue
        # initial_hashes = ["BEFC694A29", "093AEA13", "TESTHASH"]
        # self.db.add_hashes_to_queue(initial_hashes)

        conn = aiohttp.TCPConnector(limit=self.max_concurrent)
        async with aiohttp.ClientSession(connector=conn) as session:
            while True:
                # 1. 获取任务
                pending = self.db.get_pending_hashes(limit=self.max_concurrent)
                if not pending:
                    logger.info("No pending hashes. Sleeping 10s...")
                    await asyncio.sleep(10)
                    continue

                logger.info(f"Processing batch: {len(pending)} hashes")

                # 2. 并发执行
                tasks = [self._fetch_one(session, h) for h in pending]
                await asyncio.gather(*tasks)


if __name__ == "__main__":
    crawler = CivitaiHashCrawler(max_concurrent=50)
    with open("../../data/hashjson/base_model_hash_name.json", 'r') as f:
        base_model_hash_name = json.load(f)

    with open("../../data/hashjson/lora_model_hash_name.json", 'r') as f:
        lora_model_hash_name = json.load(f)

    test_hashes = set()
    for hash in base_model_hash_name.keys():
        test_hashes.add(hash)
    for hash in lora_model_hash_name.keys():
        test_hashes.add(hash)

    crawler.db.add_hashes_to_queue(list(test_hashes))

    asyncio.run(crawler.start())