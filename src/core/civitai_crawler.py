#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import random
import urllib.parse
from typing import Optional

import aiohttp

from src.config import get_settings, settings
from src.core.base_api_crawler import BaseApiCrawler
from src.managers.civitai_database_manager import CivitaiDatabaseManager
from src.managers.state_manager import CrawlStateManager

from src.utils import get_logger

logger = get_logger(__name__)


class CivitaiCrawler(BaseApiCrawler):
    def __init__(self, max_concurrent=1, db_path=settings.CivitaiDatabaseConfig.db_path, **kwargs):
        super().__init__(max_concurrent=max_concurrent, **kwargs)
        self.api_url = "https://civitai.com/api/trpc/model.getAll"
        self.db = CivitaiDatabaseManager(db_path=db_path)
        self.state = CrawlStateManager(state_dir=get_settings().state.state_dir, crawler_id="civitai_v3")

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Content-Type": "application/json",
            "x-trpc-source": "nextjs-data"
        }
        self.default_query = {
            "json": {"period": "AllTime", "sort": "Highest Rated", "browsingLevel": 1, "disablePoi": False,"disableMinor": False, "cursor": None},
            "meta": {"values": {"cursor": ["undefined"]}}
        }

    def _build_url(self, cursor: Optional[str]) -> str:
        q = json.loads(json.dumps(self.default_query))
        q["json"]["cursor"] = cursor
        if cursor: del q["meta"]["values"]["cursor"]
        return f"{self.api_url}?input={urllib.parse.quote(json.dumps(q, separators=(',', ':')))}"

    async def _fetch(self, session: aiohttp.ClientSession, cursor: Optional[str]):
        url = self._build_url(cursor)
        for i in range(5):
            try:
                proxy = self.proxy_manager.get_proxy()
                async with session.get(url, headers=self.headers, proxy=proxy, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # 解析 result.data.json
                        return data.get('result', {}).get('data', {}).get('json', {})

                    if resp.status in [403, 429]:
                        logger.warning(f"反爬 {resp.status} - 切换节点...")
                        self.proxy_manager.switch_node()  # 已修复 await
                        await asyncio.sleep(random.uniform(2, 5) * (i + 1))


            except Exception as e:
                logger.warning(f"请求异常: {e}")
                self.proxy_manager.switch_node()
                await asyncio.sleep(1)
        return None


    async def crawl(self, start_cursor=None, resume=True):
        logger.info("启动爬虫...")
        self.proxy_manager.switch_node()
        # 确定起始点
        curr = start_cursor
        if resume and not curr:
            curr = self.db.get_last_cursor() or self.state.get_next_cursor()
            if curr: logger.info(f"断点续爬: {curr}")

        conn = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(connector=conn) as session:
            while True:
                # 1. 查库防重
                db_next = self.db.get_next_cursor(curr)
                if db_next:
                    logger.info(f"Cursor {curr} 已存在，跳至 {db_next}")
                    self.state.set_next_cursor(db_next)
                    curr = db_next
                    continue

                # 2. 请求
                data = await self._fetch(session, curr)
                if not data:
                    logger.error(f"Cursor {curr} 失败，停止。")
                    break

                items = data.get('items', [])
                next_cursor = data.get('nextCursor')

                # 3. 入库
                saved = self.db.batch_save(items)
                self.db.record_cursor(curr, next_cursor, len(items))

                # 4. 更新状态
                self.state.add_crawled_cursor(curr)
                if next_cursor: self.state.set_next_cursor(next_cursor)

                logger.info(f"Cursor {curr} -> Saved {saved}/{len(items)} -> Next {next_cursor}")

                if not next_cursor or next_cursor == curr:
                    logger.info("爬取结束")
                    break

                curr = next_cursor
                await asyncio.sleep(random.uniform(0.5, 1.2))


if __name__ == "__main__":
    crawler = CivitaiCrawler()
    asyncio.run(crawler.crawl())