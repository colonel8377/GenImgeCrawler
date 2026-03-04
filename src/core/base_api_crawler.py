#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API 爬虫基类
提取公共逻辑，减少代码冗余
"""

import asyncio
import aiohttp
import random
from typing import Optional, Dict
from abc import ABC, abstractmethod

from src.config.settings import ProxyConfig
from src.managers.civitai_database_manager import CivitaiDatabaseManager
from src.managers.proxy_manager import ProxyNodeManager
from src.utils import get_logger, AntiCrawlManager

logger = get_logger(__name__)


class BaseApiCrawler(ABC):
    """API 爬虫基类"""
    
    def __init__(
        self,
        proxy_host: Optional[str] = None,
        proxy_port: Optional[int] = None,
        switch_api_url: Optional[str] = None,
        max_concurrent: Optional[int] = None,
        db_path: Optional[str] = None
    ):

        
        # 代理设置
        self.proxy_host = proxy_host or ProxyConfig.host
        self.proxy_port = proxy_port or ProxyConfig.port
        

        self.proxy_manager = ProxyNodeManager(base_api_url="http://UFLJGXH3:5967CBD8CCE2@overseas.tunnel.qg.net", proxy_port=10911)
        # self.proxy_manager = ProxyNodeManager()

        self.db_manager = None
        
        # 反爬虫管理器
        self.anti_crawl = AntiCrawlManager()
        
        # 并发控制
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
    
    def _get_proxy_url(self) -> Optional[str]:
        """获取代理 URL"""
        if not self.proxy_host or not self.proxy_port:
            return None
        
        # 处理 host 可能已经包含协议的情况
        host = self.proxy_host.strip()
        if host.startswith(('http://', 'https://')):
            # 如果已经包含协议，直接使用
            return f"{host}:{self.proxy_port}"
        else:
            # 否则添加 http:// 协议
            return f"http://{host}:{self.proxy_port}"
    
    async def _fetch_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        timeout: int = 30,
        max_retries: int = 3,
        identifier: Optional[str] = None
    ) -> Optional[Dict]:
        """
        带重试的请求方法（通用）
        
        Args:
            session: aiohttp session
            url: 请求 URL
            params: 查询参数
            headers: 请求头
            timeout: 超时时间
            max_retries: 最大重试次数
            identifier: 标识符（用于日志，如 page_num 或 cursor）
            
        Returns:
            响应 JSON 数据，失败返回 None
        """
        for attempt in range(max_retries):
            try:
                proxy_url = self._get_proxy_url()
                
                # 重试机制：等待并尝试切换代理
                if attempt > 0:
                    wait_time = random.uniform(2, 5) * attempt
                    await asyncio.sleep(wait_time)
                    self.proxy_manager.switch_node()
                    proxy_url = self._get_proxy_url()
                
                async with session.get(
                    url,
                    params=params,
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    headers=headers
                ) as response:
                    
                    if response.status == 200:
                        try:
                            return await response.json()
                        except Exception as e:
                            text = await response.text()
                            logger.error(f"Response Not JSON | URL: {url[:100]} | Error: {e} | Body: {text[:200]}")
                            raise Exception("Response is not JSON")
                    
                    elif response.status == 404:
                        # 404 表示页码超限或数据不存在，返回 None
                        return None
                    
                    elif response.status == 429:
                        # 429 处理
                        ident_str = f" | {identifier}" if identifier else ""
                        logger.warning(f"⚠️ 429 Too Many Requests{ident_str} | Switching Proxy & Sleep 10s...")
                        await asyncio.sleep(10)
                        self.proxy_manager.switch_node()
                        continue
                    
                    else:
                        try:
                            err_txt = await response.text()
                        except:
                            err_txt = "N/A"
                        ident_str = f" | {identifier}" if identifier else ""
                        logger.error(f"HTTP {response.status}{ident_str} | URL: {url[:100]} | Body: {err_txt[:200]}")
                        raise Exception(f"HTTP {response.status}")
            
            except Exception as e:
                if attempt == max_retries - 1:
                    ident_str = f" ({identifier})" if identifier else ""
                    logger.warning(f"Fetch failed{ident_str} after {max_retries} attempts: {e}")
                else:
                    logger.debug(f"Fetch attempt {attempt + 1} failed: {e}")
        
        return None
