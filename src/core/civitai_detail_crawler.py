#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import random
import urllib.parse

import aiohttp

from src.config.settings import CivitaiDatabaseConfig
from src.core.base_api_crawler import BaseApiCrawler
from src.managers.civitai_detail_database_manager import CivitaiDetailDatabaseManager
from src.managers.proxy_manager import ProxyNodeManager
from src.utils import get_logger

logger = get_logger(__name__)


class CivitaiDetailCrawler(BaseApiCrawler):
    def __init__(self, max_concurrent=5, db_path=CivitaiDatabaseConfig.db_path, **kwargs):
        # max_concurrent: 同时发起的请求数量
        super().__init__(max_concurrent=max_concurrent, **kwargs)
        self.api_url = "https://civitai.com/api/trpc/model.getById"
        self.db = CivitaiDetailDatabaseManager(db_path=db_path)

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Sec-Ch-Ua-Platform": "\"macos\"",
            "Referer": "https://civitai.com/",
            "Content-Type": "application/json",
            "Cookie": "civitai-route=b969f7e5f68b092cee6a003a6b103359|86d931b62a0bfdebdb632d2af59dceef; __Host-next-auth.csrf-token=190f26b1f3a806046b651d6769250f24d70c01375320114055eb81fc2c0c5c14%7C834fc14dd47b56a7c68a699d57688b3658d56f62bdd8f45c91f992df77c281ad; _ga=GA1.1.1918540541.1769073295; _sharedID=3a0bcd54-8018-4e5a-aea7-98e896e9873a; _lr_env_src_ats=false; civitai-route=955e94f1c8280048aa80e71c67ecc31a|bf4092ed2cc1ac81a1918599cbb73e8c; ref_landing_page=%2F; _lr_retry_request=true; _sharedID_cst=TyylLI8srA%3D%3D; _sharedID_last=Wed%2C%2028%20Jan%202026%2009%3A52%3A55%20GMT; __Secure-next-auth.callback-url=https%3A%2F%2Fcivitai.com%2F; _ga_N6W8XF7DXE=GS2.1.s1769593929$o7$g1$t1769594023$j27$l0$h0; __Secure-civitai-token=eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..NAdCieKKJlDYv9Hj.KytahK6KK3dK4x69u3qjB2f3OP8L6MrMyWAGt6XbznXUDgjDX_t7tysz4tThl7otwvpL29ZM5pRFj2oc_NC1VBupScdiLTxcLuTBxzQnbhv_scsSipAhbRSChSFHa2-vWKdCefC7x4oK2RR5mx8uUblFSkqurXLyzA4B_LlEzjauHj8c9DgAob_yRvNzyOsw0m4FDz5eFhLyXlM4P0T5KTsYSfnp-3MQPSPp0VvUT7hYj89MpabbhI6uqzs5zwWoudnBxC7QXZQ9NeyZspMoVYdsmFn6fDBqI_zR-gGEykU7x9z9rV3v05RYHjkn0kcm0UhVeU1h5uYiitAxoyKoTWCtsn5_YKyMSC6cbIgEfwpx155tAVy3AZHI7zXq5f6EjGz3tnL2FeUuSFoHNgDVXTwtrb3WgFsBL-Lenz22OZORaO9dCoXKmF8cUVHrqM2mwWdvD-0tmAdZmXWhRTP4BCmVM2qtA0oCIvjDygbgNWjRR0nK42tqHG1mkkuEjCXdVCqkfxzML8ktxUZMCOuLLcbUAbAYuCae4JxfFppEbGiJ-RPil_wM3bQExwZYq28e73_huCbgQdHOWg57_r2arl4YhtVQXX5u7sGrrof422K8PiDXg67MKw7PRueWOb6q_eWhb3ajDeNguRzRdwRvBCF-IvDCXlXvVnTzCBlyrTVXemYDISLJWrPQJ7FVvW2qV49a2YKtBXFE-PqPpRoo2rV9mjgu5BB-WesKU6GXOPkAUed1Pn2f6jzwjIi17Ts9M_WihUtQ6-kWojEKPNTBXnigiWuzJY1H_xRYjWRjr3caFWbts1o7AI6iXXpLAzDADYeOGqcHEBFyfjQtbhxeGJ1sW_PYazjgIIdmVHxRBOB-S38ljoYTDAN9tCFc-otLLm3-1iReWL2Wwdz53aBdT0F-Pn4MrMLTk3Y17pWZ6oSAcSOekznzhQaRRRbZ6_SXufb2oiYOn7srJ4xP35WnOu4E3GgHICt_9QgLstdKy4w6MDc6.qdKASywtwuWgdQaNPAXLWw"
        }

    def _build_url(self, model_id: int) -> str:
        # 构建 trpc query input
        query_input = {"json": {"id": int(model_id), "excludeTrainingData": True, "authed":True}}
        encoded = urllib.parse.quote(json.dumps(query_input, separators=(',', ':')))
        return f"{self.api_url}?input={encoded}"

    async def _fetch_and_save(self, session: aiohttp.ClientSession, model_id: int):
        """处理单个 ID 的全流程：请求 -> 校验 -> 入库"""
        url = self._build_url(model_id)

        # 重试循环
        for retry in range(5):
            try:
                proxy = self.proxy_manager.get_proxy()
                async with session.get(url, headers=self.headers, proxy=proxy, timeout=30) as resp:

                    # 1. 成功响应
                    if resp.status == 200:
                        data = await resp.json()

                        # 检查是否有有效数据
                        if 'result' in data and 'data' in data['result']:
                            save_ok = self.db.save_model_detail(data)
                            if save_ok:
                                logger.info(f"[SUCCESS] Model {model_id} Saved.")
                                return True
                            else:
                                logger.error(f"[DB ERR] Model {model_id} parsing failed.")
                                return False

                        # API 返回错误（如模型不存在/被封禁）
                        if 'error' in data:
                            logger.warning(f"[API ERR] Model {model_id}: {data['error'].get('message')}")
                            return True

                    # 2. 反爬处理
                    if resp.status in [403, 429]:
                        logger.warning(f"[BLOCK] Model {model_id} Status {resp.status} - Switch Proxy")
                        await asyncio.sleep(random.uniform(0.5, 2.0))
                        self.proxy_manager.switch_node()
                        continue

                    # 3. 资源不存在
                    if resp.status == 404:
                        logger.warning(f"[404] Model {model_id} Not Found")
                        return True

            except Exception as e:
                logger.error(f"[NET ERR] Model {model_id} retry {retry}: {e}")
                self.proxy_manager.switch_node()

        logger.error(f"[FAIL] Model {model_id} gave up after retries.")
        return False

    async def crawl(self):
        logger.info(f"启动详情爬虫 (并发: {self.max_concurrent})...")
        self.proxy_manager.switch_node()

        conn = aiohttp.TCPConnector(limit=self.max_concurrent)
        async with aiohttp.ClientSession(connector=conn) as session:

            while True:
                # 1. 生产者：从 DB 领任务
                tasks_ids = self.db.get_pending_model_ids(limit=self.max_concurrent)

                if not tasks_ids:
                    logger.info("当前无待处理任务，休眠 30 秒...")
                    await asyncio.sleep(30)
                    continue

                logger.info(f"批次开始: IDs {tasks_ids}")

                # 2. 消费者：并发执行
                # 创建协程列表
                coroutines = [self._fetch_and_save(session, mid) for mid in tasks_ids]

                # 并发等待结果
                results = await asyncio.gather(*coroutines)

                # 3. 简单的流控
                success_cnt = sum(1 for r in results if r)
                logger.info(f"批次结束: 成功 {success_cnt}/{len(tasks_ids)}")

                # 随机间隔，模拟人类行为
                await asyncio.sleep(random.uniform(0.5, 1.0))


if __name__ == "__main__":
    crawler = CivitaiDetailCrawler(max_concurrent=24)  # 建议并发 5-10
    asyncio.run(crawler.crawl())