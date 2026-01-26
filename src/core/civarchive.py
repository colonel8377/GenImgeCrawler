#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CivArchive爬虫 - API全参数遍历版 (集成DB存储与断点续爬)
"""

import asyncio
import aiohttp
import random
import json
import hashlib
import os
import itertools
from typing import Optional, Dict, List, Any

# 复用你项目中的管理器
from src.managers import ProxyNodeManager, DatabaseManager, CrawlStateManager
from src.utils import get_logger
from src.config import get_settings

logger = get_logger(__name__)

# ==========================================
# 1. 定义全量筛选参数库
# ==========================================
FILTER_DEFINITIONS = {
    "types": [
        "AestheticGradient", "Checkpoint", "Controlnet", "Detection", "DoRA",
        "Hypernetwork", "LORA", "LoCon", "MotionModule", "Other", "Poses",
        "TextualInversion", "Upscaler", "VAE", "Wildcards", "Workflows"
    ],

    "baseModels": [
        "AuraFlow", "Chroma", "CogVideoX", "Flux.1+D", "Flux.1+Kontext",
        "Flux.1+Krea", "Flux.1+S", "Flux.2+D", "Flux.2+Klein+4B",
        "Flux.2+Klein+4B-base", "Flux.2+Klein+9B", "Flux.2+Klein+9B-base",
        "HiDream", "Hunyuan+1", "Hunyuan+Video", "Illustrious", "Imagen4",
        "Kolors", "LTXV", "LTXV2", "Lumina", "Mochi", "Nano+Banana",
        "NoobAI", "ODOR", "OpenAI", "Other", "PixArt+E", "PixArt+a",
        "Playground+v2", "Pony", "Pony+V7", "Qwen", "SD+1.4",
        "SD+1.5", "SD+1.5+Hyper", "SD+1.5+LCM", "SD+2.0",
        "SD+2.0+768", "SD+2.1", "SD+2.1+768", "SD+2.1+Unclip",
        "SD+3", "SD+3.5", "SD+3.5+Large", "SD+3.5+Large+Turbo",
        "SD+3.5+Medium", "SDXL+0.9", "SDXL+1.0", "SDXL+1.0+LCM",
        "SDXL+Distilled", "SDXL+Hyper", "SDXL+Lightning", "SDXL+Turbo",
        "SVD", "SVD+XT", "Seedream", "Sora+2", "Stable+Cascade",
        "Veo+3", "Wan+Video", "Wan+Video+1.3B+t2v",
        "Wan+Video+14B+i2v+480p", "Wan+Video+14B+i2v+720p",
        "Wan+Video+14B+t2v", "Wan+Video+2.2+I2V-A14B",
        "Wan+Video+2.2+T2V-A14B", "Wan+Video+2.2+TI2V-5B",
        "Wan+Video+2.5+I2V", "Wan+Video+2.5+T2V", "ZImageTurbo"
    ],

    "sorts": [
        "relevance", "top", "newest", "oldest", "deleted_newest", "deleted_oldest"
    ],

    "kinds": [
        "version", "user", "file"
    ],

    "periods": [
        "year", "month", "week", "Day"
    ],

    "platforms": [
        "civitai",
        "tensorart",
        "seaart"
    ],

    "platform_status": [
        "available",
        "deleted"
    ],

    "ratings": [
        "explicit",
        "safe"
    ]
}


class CivArchiveApiCrawler:
    def __init__(
            self,
            build_id: str,
            proxy_host: Optional[str] = None,
            proxy_port: Optional[int] = None,
            switch_api_url: Optional[str] = None,
            max_concurrent: Optional[int] = None,
            db_path: Optional[str] = None,
            state_dir: Optional[str] = None,
            output_dir: str = "./data_output"
    ):
        """初始化 API 爬虫"""
        self.build_id = build_id
        settings = get_settings()

        # 代理设置
        self.proxy_host = proxy_host or settings.proxy.host
        self.proxy_port = proxy_port or settings.proxy.port

        # API 基础 URL
        self.api_base_url = f"https://civarchive.com/_next/data/{self.build_id}/top-models.json"

        # 初始化管理器
        switch_url = switch_api_url or settings.proxy.switch_api_url
        proxy_group = settings.proxy.proxy_group
        self.proxy_manager = ProxyNodeManager(switch_api_url=switch_url, proxy_group=proxy_group)
        self.db_manager = DatabaseManager(db_path or settings.database.db_path)

        # 状态管理根目录 (每个 Job 会有自己的子状态)
        self.base_state_dir = state_dir or settings.state.state_dir
        self.output_dir = output_dir

        # 并发控制
        self.max_concurrent = max_concurrent or settings.crawler.max_workers
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # 强制限制：每个组合只爬前20页
        self.MAX_PAGES_PER_FILTER = 20

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 核心 Headers (包含 Cookie 防盾)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'x-nextjs-data': '1',
            'Referer': 'https://civarchive.com/top-models',
            # 你提供的 Cookie，用于通过权限验证和防爬验证
            'Cookie': '_ga=GA1.1.1760070262.1769073267; searchView=table; rating=explicit; _ga_ZHSD82YWZ6=GS2.1.s1769097449$o4$g1$t1769102405$j60$l0$h0'
        }

    def _get_proxy_url(self) -> Optional[str]:
        if self.proxy_host and self.proxy_port:
            return f"http://{self.proxy_host}:{self.proxy_port}"
        return None

    def _generate_param_id(self, params: Dict) -> str:
        """生成唯一标识符 (用于文件名和状态记录)"""
        sorted_items = sorted([f"{k}={v}" for k, v in params.items()])
        raw_str = "&".join(sorted_items)
        md5_hash = hashlib.md5(raw_str.encode()).hexdigest()

        # 文件名保留关键信息
        t = str(params.get('types', 'All'))[:3]
        b = str(params.get('baseModels', 'All')).replace(' ', '').replace('+', '')[:4]

        return f"{t}_{b}_{md5_hash[:8]}"

    async def _fetch_page_data(self, session: aiohttp.ClientSession, page_num: int, extra_params: Dict) -> Optional[
        Dict]:
        """
        请求 API 数据
        包含：重试逻辑、代理切换、429 处理、404 处理
        """
        query_params = {
            'page': str(page_num),
            'slug': 'top-models'
        }
        query_params.update(extra_params)

        max_retries = 3

        for attempt in range(max_retries):
            try:
                proxy_url = self._get_proxy_url()

                # 重试机制：等待并尝试切换代理
                if attempt > 0:
                    wait_time = random.uniform(2, 5) * attempt
                    await asyncio.sleep(wait_time)
                    # 每次重试都尝试切一下代理
                    self.proxy_manager.switch_node()
                    proxy_url = self._get_proxy_url()

                async with session.get(
                        self.api_base_url,
                        params=query_params,
                        proxy=proxy_url,
                        timeout=20,
                        headers=self.headers
                ) as response:

                    if response.status == 200:
                        try:
                            return await response.json()
                        except Exception:
                            text = await response.text()
                            logger.error(f"Response Not JSON | URL: {response.url} | Body: {text[:200]}")
                            raise Exception("Response is not JSON")

                    elif response.status == 404:
                        # 404 表示页码超限，返回 None 告知上层停止
                        return None

                    elif response.status == 429:
                        # 【重要】429 处理
                        logger.warning(f"⚠️ 429 Too Many Requests | P{page_num} | Switching Proxy & Sleep 10s...")
                        await asyncio.sleep(10)
                        self.proxy_manager.switch_node()
                        continue  # 继续下一次循环重试

                    else:
                        # 其他 HTTP 错误
                        try:
                            err_txt = await response.text()
                        except:
                            err_txt = "N/A"
                        logger.error(
                            f"HTTP {response.status} | P{page_num} | URL: {response.url} | Body: {err_txt[:200]}")
                        raise Exception(f"HTTP {response.status}")

            except Exception as e:
                # 仅在最后一次尝试失败时记录 Error，避免刷屏
                if attempt == max_retries - 1:
                    logger.warning(f"Fetch failed P{page_num} after {max_retries} attempts: {e}")

        return None

    def _save_to_json_file(self, items: List[Dict], filename: str):
        """将数据追加写入 JSONL 文件"""
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'a', encoding='utf-8') as f:
                for item in items:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error(f"Write file error: {e}")

    async def _process_single_page(self, session, page_num, params, state_mgr, filename) -> (str, int):
        """
        处理单页核心逻辑
        Returns: (status, count)
           status: 'ok', 'empty', 'error'
        """
        try:
            data = await self._fetch_page_data(session, page_num, params)

            # 情况1: API 返回 404，或者 null
            if data is None:
                return 'empty', 0

            # 情况2: 解析数据
            results = data.get('pageProps', {}).get('data', {}).get('results', [])
            count = len(results)

            # 情况3: 返回 200 但列表为空 (说明翻页到底了)
            if count == 0:
                state_mgr.add_crawled_page(page_num)
                return 'empty', 0

            # 情况4: 有数据 -> 存DB，存文件，更新状态
            self.db_manager.batch_insert_or_update_items(results)
            self._save_to_json_file(results, filename)
            state_mgr.add_crawled_page(page_num)

            return 'ok', count

        except Exception as e:
            # 异常情况 (网络一直不通等)
            return 'error', 0

    async def crawl_filter_job(self, filter_params: Dict):
        """
        单个筛选组合的爬取任务
        逻辑：顺序爬取 P1 -> P2 ... -> P20
        """
        param_id = self._generate_param_id(filter_params)
        json_filename = f"data_{param_id}.jsonl"

        # 为每个组合创建独立的状态记录，实现细粒度断点续爬
        # ID 格式: civ_BUILDID_Type_Base_Hash
        crawler_id = f"civ_{self.build_id}_{param_id}"
        state_manager = CrawlStateManager(state_dir=self.base_state_dir, crawler_id=crawler_id)

        # 检查是否该任务已完全完成 (可选优化：如果 Page 1 标记为 empty 且 crawled，直接跳过)
        # 这里直接进入循环，依靠 page 检查

        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:

            # 严格顺序爬取：从第1页开始，直到第20页
            for page_num in range(1, self.MAX_PAGES_PER_FILTER + 1):

                # 1. 检查断点：该页是否已爬过
                if state_manager.is_page_crawled(page_num):
                    continue

                # 2. 信号量控制：获取全局并发锁
                async with self.semaphore:
                    status, count = await self._process_single_page(
                        session, page_num, filter_params, state_manager, json_filename
                    )

                # 3. 逻辑判断
                if status == 'empty':
                    # 关键优化：如果当前页没数据，立即停止该 Filter 的所有后续请求
                    # logger.info(f"[{param_id}] Finished at P{page_num} (Empty).")
                    break

                if status == 'error':
                    # 发生严重错误且重试无效，暂停一下，尝试下一页 (或者也可以 break)
                    # 考虑到 429 已在 fetch 内部处理，这里多半是网络完全不可达
                    logger.warning(f"[{param_id}] Error at P{page_num}, checking next...")
                    await asyncio.sleep(2)

                if status == 'ok':
                    # 成功获取数据
                    logger.info(f"[{param_id}] P{page_num} OK: {count} items.")

                    # 智能判断：如果一页少于 18 条 (默认一页通常20条)，说明是最后一页了，不必爬下一页
                    if count < 18:
                        break

                    # 随机延迟，防止对同一 Filter 请求过快
                    await asyncio.sleep(random.uniform(0.5, 1.5))


def generate_full_combinations() -> List[Dict]:
    """
    生成所有筛选条件的笛卡尔积，并映射为 API 参数名
    """
    configs = []

    # 提取所有定义列表
    types = FILTER_DEFINITIONS['types']
    bases = FILTER_DEFINITIONS['baseModels']
    kinds = FILTER_DEFINITIONS['kinds']
    periods = FILTER_DEFINITIONS['periods']
    sorts = FILTER_DEFINITIONS['sorts']
    platforms = FILTER_DEFINITIONS['platforms']
    platform_status = FILTER_DEFINITIONS['platform_status']
    ratings = FILTER_DEFINITIONS['ratings']

    # 生成组合 (这会生成大量组合，请确保 workers 数量合适)
    combinations = itertools.product(
        types, bases, kinds, sorts, periods, platforms, platform_status, ratings
    )

    for t, b, k, s, pe, pl, ps, ra in combinations:
        # 【重要】参数名映射：这里将你的定义 Key 映射为 API 实际 Query Key
        params = {
            'types': t,  # API: types
            'baseModels': b,  # API: baseModels
            'resultType': k,  # API: resultType (对应 kinds)
            'sort': s,  # API: sort
            'period': pe,  # API: period
            'platform': pl,  # API: platform
            'status': ps,  # API: status (对应 platform_status)
            'rating': ra  # API: rating (对应 ratings)
        }
        configs.append(params)

    logger.info(f"Generated {len(configs)} total filter combinations.")
    return configs


async def main_async():
    import argparse
    parser = argparse.ArgumentParser()
    # 强制默认 Build ID
    parser.add_argument('--build-id', type=str, default='FPE72MYMh6So11rXzfw0a', help="Next.js Build ID")
    parser.add_argument('--workers', type=int, default=10, help="Max global concurrent requests")
    parser.add_argument('--shuffle', action='store_true', default=True, help="Randomize job order")
    args = parser.parse_args()

    # 初始化爬虫
    crawler = CivArchiveApiCrawler(
        build_id=args.build_id,
        max_concurrent=args.workers
    )

    # 生成任务列表
    logger.info("Generating combinations...")
    all_configs = generate_full_combinations()

    # 打乱顺序，避免集中请求某一类数据
    if args.shuffle:
        random.shuffle(all_configs)

    total = len(all_configs)
    logger.info(f"Start crawling {total} jobs (BuildID: {args.build_id})...")

    # 并发执行 Jobs
    # 注意：Semaphore 是在 crawl_filter_job 内部请求时生效的
    # 因此我们可以一次性 gather 所有 job，它们会自己在 semaphore 处排队

    async def run_safe(idx, config):
        # 简单的进度打印
        if idx % 100 == 0:
            logger.info(f"Job Progress: {idx}/{total} ({(idx / total) * 100:.2f}%)")
        try:
            await crawler.crawl_filter_job(config)
        except Exception as e:
            logger.error(f"Job failed: {e}")

    tasks = [run_safe(i, cfg) for i, cfg in enumerate(all_configs)]

    # 启动所有任务
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main_async())