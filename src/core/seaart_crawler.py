#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import random
from typing import Optional, Dict

from src.config.settings import SeaartDatabaseConfig, StateConfig
# 引入你的 StateManager 和新写的 DB Manager

from src.core.base_api_crawler import BaseApiCrawler
from src.managers.seaart_database_manager import SeaArtDatabaseManager
from src.managers.state_manager import CrawlStateManager
from src.utils import get_logger

logger = get_logger(__name__)

class SeaArtCrawler(BaseApiCrawler):
    def __init__(self, max_concurrent=1, db_path=SeaartDatabaseConfig.db_path, **kwargs):
        super().__init__(max_concurrent=max_concurrent, **kwargs)

        self.api_url = "https://www.seaart.ai/api/v1/square/v3/model/recommend"
        self.db = SeaArtDatabaseManager(db_path=db_path)

        # 状态管理：复用 CrawlStateManager

        # crawler_id 设为 seaart_v1，这样会生成 seaart_v1.json 状态文件
        self.state_manager = CrawlStateManager(
            state_dir=StateConfig.state_dir,
            crawler_id="seaart_v1"
        )

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Origin": "https://www.seaart.ai",
            "Referer": "https://www.seaart.ai/models"
        }

        self.page_size = 24

    async def _fetch_page(self, session: aiohttp.ClientSession, page: int) -> Optional[Dict]:
        """POST 请求获取数据"""
        payload = {
            "offset": "",
            "page": page,
            "page_size": self.page_size,
            "canary_for_other": "sku"
        }

        for i in range(5):
            try:
                proxy = self.proxy_manager.get_proxy()
                async with session.post(
                        self.api_url,
                        json=payload,
                        headers=self.headers,
                        proxy=proxy,
                        timeout=30
                ) as resp:

                    if resp.status == 200:
                        data = await resp.json()
                        # 检查业务状态码
                        if data.get('status', {}).get('code') == 10000:
                            return data.get('data', {})
                        else:
                            logger.warning(f"SeaArt API 业务错误: {data.get('status')}")
                            # 业务错误通常不需要重试，除非是限流
                            return None

                    if resp.status in [403, 429]:
                        logger.warning(f"反爬 {resp.status} (Page {page}) - 切换节点...")
                        self.proxy_manager.switch_node()  # 已修复 await
                        # await asyncio.sleep(1)

            except Exception as e:
                logger.warning(f"请求异常 (Page {page}): {e}")
                self.proxy_manager.switch_node()  # 已修复 await
                # await asyncio.sleep(1)

        return None

    async def crawl(self, start_page: int = None, resume: bool = True):
        logger.info("启动 SeaArt 爬虫...")
        self.proxy_manager.switch_node()

        # 1. 确定起始页 (断点续爬逻辑)
        current_page = 1
        if resume:
            # 从 StateManager 读取 last_page
            # _load_state 逻辑中: 'last_page': 0
            last_page = self.state_manager.state.get('last_page', 0)

            if start_page:
                current_page = start_page
            elif last_page > 0:
                current_page = last_page + 1
                logger.info(f"检测到断点 (Page {last_page})，从第 {current_page} 页继续...")
            else:
                logger.info("无断点记录，从第 1 页开始")

        conn = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(connector=conn) as session:

            while True:
                # 检查是否已爬取 (双重保险)
                if resume and self.state_manager.is_page_crawled(current_page):
                    logger.info(f"Page {current_page} 已在状态文件中标记为完成，跳过。")
                    current_page += 1
                    continue

                logger.info(f"正在请求第 {current_page} 页...")

                # 2. 获取数据
                data_block = await self._fetch_page(session, current_page)

                if not data_block:
                    logger.error(f"第 {current_page} 页获取失败，程序停止。")
                    self.state_manager.add_failed_page(current_page)
                    break

                items = data_block.get('items', [])
                has_more = data_block.get('has_more', False)

                # 3. 入库
                if items:
                    saved = self.db.batch_save(items)
                    logger.info(f"Page {current_page} -> 获取 {len(items)} 条 -> 入库 {saved} 条")
                else:
                    logger.warning(f"Page {current_page} 返回 0 条数据")

                # 4. 更新断点状态 (CrawlStateManager)
                self.state_manager.add_crawled_page(current_page)
                self.state_manager.remove_failed_page(current_page)

                # 5. 终止条件
                if not has_more:
                    logger.info("SeaArt API 返回 has_more=False，爬取完成。")
                    break

                current_page += 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=None)
    parser.add_argument('--no-resume', action='store_true')
    args = parser.parse_args()

    crawler = SeaArtCrawler()
    try:
        asyncio.run(crawler.crawl(start_page=args.start, resume=not args.no_resume))
    except KeyboardInterrupt:
        logger.info("用户终止")