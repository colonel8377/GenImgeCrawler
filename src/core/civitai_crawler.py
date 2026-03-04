#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import random
import urllib.parse
from typing import Optional

import aiohttp

from src.config.settings import CivitaiDatabaseConfig, StateConfig
from src.core.base_api_crawler import BaseApiCrawler
from src.managers.civitai_database_manager import CivitaiDatabaseManager
from src.managers.state_manager import CrawlStateManager
from src.utils import get_logger

logger = get_logger(__name__)


class CivitaiCrawler(BaseApiCrawler):
    def __init__(self, max_concurrent=1, db_path=CivitaiDatabaseConfig.db_path, **kwargs):
        super().__init__(max_concurrent=max_concurrent, **kwargs)
        self.api_url = "https://civitai.com/api/trpc/model.getAll"
        self.db = CivitaiDatabaseManager(db_path=db_path)
        self.state = CrawlStateManager(state_dir=StateConfig.state_dir, crawler_id="civitai_v3")

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Cookie": "__Secure-civitai-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..vcfHe81VJXEpTalD.PLGDK0QRpMbsJ4iDrBc48uT-4ATPuQdkqhAXtYN1D35amDlzxWz0oURQFrArR-wIaiPUM4fExXw5ruNyejrhTSAo6CzEaBMdMxm6PrT9ZpjdQ0zWheL-5fItySVB1TCrSmWuBZyLCGknsfAf_c0KQvvoTWg3K9Lpa13mt-hio46fIcSsty2gsqNT0zllOGQfldtOM7mBt8SmQ_Oeov9VlS4XQ5WHXxF8P2Esg3Uwa03tCiOI8pPuGnSuTeEBaqNG2Kcn24TsjkAtfyS3tlnoQBJZDhpnXg6VAwt_vI422N1PhplPvosfmeGGi1uHygoEWG41XLPJXfdJTOeQHJhNXRVhErW8lRPdmRiYyYyMQ0c6BISy2hR5iFBrLSmhq3z-tib_JV5wI1fov5ssMVNZMbAQnYXSxqXnJb6hc3vSP3TcDHxl2Y8Mczibxbpo1ks8czWsXbfT7pDrALtxBDJvzUD4oeypycUvgtVURKtidMHio8JXaYdzK5F7iXN3bZ2jlar84gHjcCh-5-yjZUpm809lCsXnDXr-QbIU4c-Q2pTu-DFmuayC9JUDfJSqBfwy0XGG1hUVNCUuSYAOT4dgfDtISldiB5u_7pJVQuMlfO92TJOD4jjBQFTIbDKrLP3LaTuhl3YK9WJSOZogpUNMzYujAWDJ-SmeQxFPjNbXeFfXwoLBzbUoafppG4HMt2rVUjbuNeDzvikaV4HKBSTm8uLNyvHKCFR326cKwm_kCQ17zNAo30v75ugr0I37nZMaQu6d_H2D0NcBTwvuLULS5Q38QNU77KDG_cX1ZkWPWOAPaz_wR3qQIBXEWBUo7Q8hbapWt5dDEsmay5vL2KG8Oafwi8mIOFhJsN4zCH3sfC8eSLUo8CrgDomDwh6MhuKCiiIK4rAoIumlE0jCYOyEt_cVfVuSr0H16zyM4xZZawY1y7uzPnlpKNcjOSrY7HZ0S6AsS-gtQNq8kUq2Dj7HW1CYBxgQIA6lEQ0P-0J3oJAxqPCi.gW16Z5KAJZqKjs-k83NSFw; _ga_N6W8XF7DXE=GS2.1.s1769584262$o13$g1$t1769584999$j1$l0$h0",
            "Referer": "https://civitai.com/models",

            # "X-Client": "web",
            # "X-Client-Date": "1769585178764",
            # "X-Fingerprint": "3cdc6dc4d90c2aca6235bdf66dab4f31ccb3bbcb8e956672facdbfe482828a5d637d3afabee3c323df989be4595ee8cc"

        }
        self.default_query = {
            "json": {"period": "AllTime", "periodMode":"stats", "sort": "Newest", "browsingLevel": 1, "disablePoi": True,"disableMinor": False, "nsfw":True, "cursor": None},
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
                        await asyncio.sleep(random.uniform(1, 3))


            except Exception as e:
                logger.warning(f"请求异常: {e}")
                self.proxy_manager.switch_node()
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
                    logger.error(f"Cursor {curr} 失败。")
                    continue

                items = data.get('items', [])
                next_cursor = data.get('nextCursor')

                # 3. 入库
                saved = self.db.batch_save(items)
                self.db.record_cursor(curr, next_cursor, len(items))

                # 4. 更新状态
                self.state.add_crawled_cursor(curr)
                if next_cursor: self.state.set_next_cursor(next_cursor)

                logger.info(f"Cursor {curr} -> Saved {saved}/{len(items)} -> Next {next_cursor}")
                await asyncio.sleep(random.uniform(0.1, 1.0))
                if not next_cursor or next_cursor == curr:
                    logger.info("爬取结束")
                    break

                curr = next_cursor


if __name__ == "__main__":
    crawler = CivitaiCrawler()
    asyncio.run(crawler.crawl())